//! Automatic retry/backoff middleware for the [trillium](https://trillium.rs) HTTP client.
//!
//! [`RetryHandler`] is a [`ClientHandler`] that re-issues a request when it fails in a way that
//! is worth retrying — a transport-level error (connection refused, reset, timeout) or a
//! retryable response status (`429`, `503` by default) — spacing attempts out with a configurable
//! [`Backoff`] and honoring a server-advertised `Retry-After`.
//!
//! ```no_run
//! use std::time::Duration;
//! use trillium_client::Client;
//! use trillium_client_retry::{Backoff, RetryHandler};
//! use trillium_testing::client_config;
//!
//! let client = Client::new(client_config()).with_handler(
//!     RetryHandler::default()
//!         .with_backoff(Backoff::exponential(Duration::from_millis(100)))
//!         .with_max_attempts(5),
//! );
//! ```
//!
//! # Behavior
//!
//! Each attempt runs as a full client-handler cycle (queued via `set_followup`), so other
//! handlers — loggers, conn-id, metrics — observe every attempt. Place `RetryHandler` as the
//! outermost handler so those observers see each attempt before the backoff sleep.
//!
//! ## What is retried
//!
//! By default, retries are limited to idempotent methods (GET, HEAD, PUT, DELETE, OPTIONS,
//! TRACE — [`Methods::Idempotent`]). Within that gate, a request is retried when it fails with a
//! transport error or returns a status in the configured set. Adjust with [`with_methods`],
//! [`with_statuses`], and [`with_transport_errors`], or replace the whole decision with
//! [`retry_when`] / [`with_decision`].
//!
//! ## Request bodies
//!
//! A request body is replayed only if it can be cloned (static bodies — `Vec<u8>`, `String`,
//! `&'static str`, etc.). A streaming (one-shot) body cannot be replayed, so a request carrying
//! one is **not** retried; its result is surfaced as-is.
//!
//! ## Limits
//!
//! Retrying stops at whichever comes first: [`with_max_attempts`] total attempts, or the
//! [`with_max_elapsed`] wall-clock budget. The budget is a hard ceiling — each attempt's timeout
//! is clamped to the time remaining, so a single slow attempt can't overrun it. (The very first
//! attempt uses the client's own timeout, since the budget is established once the request is in
//! flight; keep `max_elapsed` at least as large as the client timeout.)
//!
//! ## `Retry-After`
//!
//! When [`honor_retry_after`](RetryHandler::with_honor_retry_after) is set (the default) and the
//! response carries a `Retry-After` header in delta-seconds form, that delay takes precedence
//! over the computed backoff (clamped by [`with_max_retry_after`] if set, and always by the
//! elapsed budget). `Retry-After` HTTP-date values are not yet parsed and fall back to the
//! computed backoff.
//!
//! [`with_methods`]: RetryHandler::with_methods
//! [`with_statuses`]: RetryHandler::with_statuses
//! [`with_transport_errors`]: RetryHandler::with_transport_errors
//! [`retry_when`]: RetryHandler::retry_when
//! [`with_decision`]: RetryHandler::with_decision
//! [`with_max_attempts`]: RetryHandler::with_max_attempts
//! [`with_max_elapsed`]: RetryHandler::with_max_elapsed
//! [`with_max_retry_after`]: RetryHandler::with_max_retry_after

#![forbid(unsafe_code)]
#![deny(
    clippy::dbg_macro,
    missing_copy_implementations,
    rustdoc::missing_crate_level_docs,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    unused_qualifications
)]

// Compile the README as a doctest so its examples stay in sync with the crate.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod readme {}

mod backoff;
pub use backoff::{Backoff, Jitter};
use std::{
    borrow::Cow,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};
use trillium_client::{
    Body, ClientHandler, Conn, ConnExt,
    KnownHeaderName::{Connection, ContentLength, Expect, Host, RetryAfter, TransferEncoding},
    Method, Result, Status,
};

/// Which request methods are eligible for retry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Methods {
    /// Only retry idempotent methods (GET, HEAD, PUT, DELETE, OPTIONS, TRACE), per
    /// [RFC 9110 §9.2.2](https://www.rfc-editor.org/rfc/rfc9110#section-9.2.2). This is the
    /// default: replaying a non-idempotent request (e.g. POST) risks a duplicate side effect.
    #[default]
    Idempotent,
    /// Retry regardless of method, including POST and other non-idempotent requests. Use only
    /// when the endpoint is known to be safe to replay (e.g. it is idempotent in practice or
    /// guarded by an idempotency key).
    All,
}

impl Methods {
    fn allows(self, method: Method) -> bool {
        match self {
            Self::All => true,
            Self::Idempotent => matches!(
                method,
                Method::Get
                    | Method::Head
                    | Method::Put
                    | Method::Delete
                    | Method::Options
                    | Method::Trace
            ),
        }
    }
}

type Predicate = Arc<dyn Fn(&Conn) -> bool + Send + Sync>;
type Decision = Arc<dyn Fn(&Conn, u32) -> Option<Duration> + Send + Sync>;

/// A [`ClientHandler`] that automatically retries failed requests with backoff.
///
/// See the [crate-level documentation][crate] for behavior and configuration.
#[derive(Clone)]
pub struct RetryHandler {
    backoff: Backoff,
    max_attempts: u32,
    max_elapsed: Duration,
    statuses: Arc<[Status]>,
    methods: Methods,
    transport_errors: bool,
    honor_retry_after: bool,
    max_retry_after: Option<Duration>,
    predicate: Option<Predicate>,
    decision: Option<Decision>,
}

impl Default for RetryHandler {
    fn default() -> Self {
        Self {
            backoff: Backoff::exponential(Duration::from_millis(100)),
            max_attempts: 4,
            max_elapsed: Duration::from_secs(30),
            statuses: Arc::from([Status::TooManyRequests, Status::ServiceUnavailable].as_slice()),
            methods: Methods::Idempotent,
            transport_errors: true,
            honor_retry_after: true,
            max_retry_after: None,
            predicate: None,
            decision: None,
        }
    }
}

impl RetryHandler {
    /// Construct a `RetryHandler` with default settings (see the [crate docs][crate]).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the [`Backoff`] schedule. Defaults to exponential from 100ms with full jitter.
    #[must_use]
    pub fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Set the maximum number of attempts, *including* the original request. Defaults to 4
    /// (the original plus up to 3 retries).
    #[must_use]
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set the total wall-clock budget across all attempts. Defaults to 30 seconds. This is a
    /// hard ceiling: each retry's timeout is clamped to the time remaining.
    #[must_use]
    pub fn with_max_elapsed(mut self, max_elapsed: Duration) -> Self {
        self.max_elapsed = max_elapsed;
        self
    }

    /// Replace the set of response statuses that trigger a retry. Defaults to `429` and `503`.
    #[must_use]
    pub fn with_statuses(mut self, statuses: impl IntoIterator<Item = Status>) -> Self {
        self.statuses = statuses.into_iter().collect();
        self
    }

    /// Set which methods are eligible for retry. Defaults to [`Methods::Idempotent`].
    #[must_use]
    pub fn with_methods(mut self, methods: Methods) -> Self {
        self.methods = methods;
        self
    }

    /// Set whether transport-level errors (connection refused, reset, timeout) are retried.
    /// Defaults to `true`.
    #[must_use]
    pub fn with_transport_errors(mut self, retry: bool) -> Self {
        self.transport_errors = retry;
        self
    }

    /// Set whether a server-advertised `Retry-After` overrides the computed backoff. Defaults to
    /// `true`.
    #[must_use]
    pub fn with_honor_retry_after(mut self, honor: bool) -> Self {
        self.honor_retry_after = honor;
        self
    }

    /// Cap how long a `Retry-After` will be honored for. Defaults to uncapped (bounded only by
    /// the elapsed budget).
    #[must_use]
    pub fn with_max_retry_after(mut self, max: Duration) -> Self {
        self.max_retry_after = Some(max);
        self
    }

    /// Replace the built-in retry predicate. The closure decides, from the conn carrying the
    /// response or transport error, whether to retry — fully replacing the method gate, status
    /// set, and transport-error toggle. Timing still comes from the configured [`Backoff`].
    #[must_use]
    pub fn retry_when(mut self, predicate: impl Fn(&Conn) -> bool + Send + Sync + 'static) -> Self {
        self.predicate = Some(Arc::new(predicate));
        self
    }

    /// Replace the entire retry decision — predicate *and* backoff. The closure receives the conn
    /// and the 1-based retry number and returns `Some(delay)` to retry after that delay, or
    /// `None` to give up. The attempt and elapsed-budget limits still apply.
    #[must_use]
    pub fn with_decision(
        mut self,
        decision: impl Fn(&Conn, u32) -> Option<Duration> + Send + Sync + 'static,
    ) -> Self {
        self.decision = Some(Arc::new(decision));
        self
    }

    fn decide(&self, conn: &Conn, retry_number: u32) -> Option<Duration> {
        if let Some(decision) = &self.decision {
            return decision(conn, retry_number);
        }
        self.should_retry(conn)
            .then(|| self.backoff.delay(retry_number, conn))
    }

    fn should_retry(&self, conn: &Conn) -> bool {
        if let Some(predicate) = &self.predicate {
            return predicate(conn);
        }
        if !self.methods.allows(conn.method()) {
            return false;
        }
        if conn.error().is_some() {
            return self.transport_errors;
        }
        conn.status()
            .is_some_and(|status| self.statuses.contains(&status))
    }

    fn effective_delay(&self, conn: &Conn, base_delay: Duration) -> Duration {
        if !self.honor_retry_after {
            return base_delay;
        }
        match retry_after(conn) {
            Some(advised) => self.max_retry_after.map_or(advised, |cap| advised.min(cap)),
            None => base_delay,
        }
    }

    fn build_followup(&self, conn: &Conn, state: RetryState, remaining: Duration) -> Conn {
        let mut followup = conn.client().build_conn(conn.method(), conn.url().clone());

        // Strip transport/body-description headers; `finalize_headers` re-derives them for the
        // replayed request. Same-origin retry, so credential headers are kept.
        let mut headers = conn.request_headers().clone();
        headers.remove_all([Host, ContentLength, TransferEncoding, Expect, Connection]);
        *followup.request_headers_mut() = headers;

        if let Some(BodyReplay::Replayable(body)) = conn.state::<BodyReplay>()
            && let Some(replayed) = body.try_clone()
        {
            followup.set_request_body(replayed);
        }

        let timeout = conn.timeout().map_or(remaining, |t| t.min(remaining));
        followup.set_timeout(timeout);

        followup.insert_state(RetryState {
            attempts: state.attempts + 1,
            deadline: state.deadline,
        });
        followup
    }
}

impl ClientHandler for RetryHandler {
    async fn run(&self, conn: &mut Conn) -> Result<()> {
        // Anchor the elapsed budget on the first attempt; follow-ups carry it forward.
        if conn.state::<RetryState>().is_none() {
            conn.insert_state(RetryState {
                attempts: 1,
                deadline: Instant::now() + self.max_elapsed,
            });
        }

        // Snapshot the body before the network consumes it, so it can be replayed.
        let replay = match conn.request_body() {
            None => BodyReplay::None,
            Some(body) => match body.try_clone() {
                Some(clone) => BodyReplay::Replayable(clone),
                None => BodyReplay::OneShot,
            },
        };
        conn.insert_state(replay);
        Ok(())
    }

    async fn after_response(&self, conn: &mut Conn) -> Result<()> {
        let Some(state) = conn.state::<RetryState>().copied() else {
            return Ok(());
        };
        if state.attempts >= self.max_attempts {
            return Ok(());
        }
        // A one-shot body can't be replayed; surface whatever happened.
        if matches!(conn.state::<BodyReplay>(), Some(BodyReplay::OneShot)) {
            return Ok(());
        }

        let retry_number = state.attempts;
        let Some(base_delay) = self.decide(conn, retry_number) else {
            return Ok(());
        };
        let delay = self.effective_delay(conn, base_delay);

        // Not enough budget left to both wait and attempt — give up now.
        if Instant::now() + delay >= state.deadline {
            return Ok(());
        }

        conn.client().connector().runtime().delay(delay).await;

        let remaining = state.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(());
        }

        let followup = self.build_followup(conn, state, remaining);
        // Clear any transport error so the loop runs the follow-up instead of propagating it.
        conn.take_error();
        conn.set_followup(followup);
        Ok(())
    }

    fn name(&self) -> Cow<'static, str> {
        "RetryHandler".into()
    }
}

fn retry_after(conn: &Conn) -> Option<Duration> {
    conn.response_headers()
        .get_str(RetryAfter)?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

/// Per-conn retry bookkeeping, stashed in conn state and carried across follow-ups.
#[derive(Clone, Copy)]
struct RetryState {
    attempts: u32,
    deadline: Instant,
}

/// Snapshot of the request body's replayability, taken in `run` before the network consumes it.
enum BodyReplay {
    None,
    Replayable(Body),
    OneShot,
}

impl fmt::Debug for RetryHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryHandler")
            .field("backoff", &self.backoff)
            .field("max_attempts", &self.max_attempts)
            .field("max_elapsed", &self.max_elapsed)
            .field("statuses", &self.statuses)
            .field("methods", &self.methods)
            .field("transport_errors", &self.transport_errors)
            .field("honor_retry_after", &self.honor_retry_after)
            .field("max_retry_after", &self.max_retry_after)
            .field("predicate", &self.predicate.as_ref().map(|_| "<fn>"))
            .field("decision", &self.decision.as_ref().map(|_| "<fn>"))
            .finish()
    }
}
