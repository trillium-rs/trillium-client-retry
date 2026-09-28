//! Automatic retry/backoff middleware for the [trillium](https://trillium.rs) HTTP client.
//!
//! [`RetryHandler`] is a [`ClientHandler`] that re-issues a request when it fails in a way that
//! is worth retrying — a transport-level error (connection refused, reset, timeout) or a
//! retryable response status (`429`, `503` by default) — spacing attempts out with a configurable
//! backoff schedule and honoring a server-advertised `Retry-After`.
//!
//! ```no_run
//! use std::time::Duration;
//! use trillium_client::Client;
//! use trillium_client_retry::RetryHandler;
//! use trillium_testing::client_config;
//!
//! let client = Client::new(client_config()).with_handler(
//!     RetryHandler::default()
//!         .with_exponential_backoff(Duration::from_millis(100))
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
//! TRACE). Within that gate, a request is retried when it fails with a transport error or returns
//! a status in the configured set. Adjust with [`with_all_methods`], [`with_statuses`], and
//! [`with_transport_errors`], or replace the whole decision with [`retry_when`] /
//! [`with_decision`].
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
//! `with_max_elapsed(None)` removes the budget: attempts are then bounded only by
//! [`with_max_attempts`], and each retry runs under the client's own timeout, if any. That is the
//! setting for requests whose time to first byte is long and legitimately variable — a retry cut
//! short by a clamped timeout fails for the clamp, not for the upstream.
//!
//! ## `Retry-After`
//!
//! When [`honor_retry_after`](RetryHandler::with_honor_retry_after) is set (the default) and the
//! response carries a `Retry-After` header in delta-seconds form, that delay takes precedence
//! over the computed backoff (clamped by [`with_max_retry_after`] if set, and always by the
//! elapsed budget). `Retry-After` HTTP-date values are not yet parsed and fall back to the
//! computed backoff.
//!
//! [`with_all_methods`]: RetryHandler::with_all_methods
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
use backoff::{Backoff, Kind};
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

/// Whether `method` is eligible for retry under the idempotent-only gate (GET, HEAD, PUT, DELETE,
/// OPTIONS, TRACE), per [RFC 9110 §9.2.2](https://www.rfc-editor.org/rfc/rfc9110#section-9.2.2).
/// Replaying a non-idempotent request (e.g. POST) risks a duplicate side effect.
fn is_idempotent(method: Method) -> bool {
    matches!(
        method,
        Method::Get | Method::Head | Method::Put | Method::Delete | Method::Options | Method::Trace
    )
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
    max_elapsed: Option<Duration>,
    statuses: Arc<[Status]>,
    all_methods: bool,
    transport_errors: bool,
    honor_retry_after: bool,
    max_retry_after: Option<Duration>,
    predicate: Option<Predicate>,
    decision: Option<Decision>,
}

impl Default for RetryHandler {
    fn default() -> Self {
        Self {
            backoff: Backoff::default(),
            max_attempts: 4,
            max_elapsed: Some(Duration::from_secs(30)),
            statuses: Arc::from([Status::TooManyRequests, Status::ServiceUnavailable].as_slice()),
            all_methods: false,
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

    /// Wait a fixed `delay` before every retry.
    ///
    /// One of four mutually exclusive backoff curves (the others are
    /// [`with_linear_backoff`](Self::with_linear_backoff),
    /// [`with_exponential_backoff`](Self::with_exponential_backoff), and
    /// [`with_custom_backoff`](Self::with_custom_backoff)); the last one set wins. The default
    /// curve is exponential from 100ms. [`with_max_delay`](Self::with_max_delay) and
    /// [`without_jitter`](Self::without_jitter) apply on top of whichever curve is chosen.
    #[must_use]
    pub fn with_constant_backoff(mut self, delay: Duration) -> Self {
        self.backoff.kind = Kind::Constant(delay);
        self
    }

    /// Grow the delay linearly: `step * retry_number` (the first retry waits `step`). See
    /// [`with_constant_backoff`](Self::with_constant_backoff) for how the curves combine.
    #[must_use]
    pub fn with_linear_backoff(mut self, step: Duration) -> Self {
        self.backoff.kind = Kind::Linear(step);
        self
    }

    /// Double the delay each retry: `base * 2^(retry_number - 1)` (the first retry waits `base`).
    /// This is the default curve, from 100ms. See
    /// [`with_constant_backoff`](Self::with_constant_backoff) for how the curves combine.
    #[must_use]
    pub fn with_exponential_backoff(mut self, base: Duration) -> Self {
        self.backoff.kind = Kind::Exponential(base);
        self
    }

    /// Compute the delay with a fully custom curve. The closure receives the 1-based retry number
    /// and the conn carrying the response or error being retried, and returns the base delay
    /// (before the [`with_max_delay`](Self::with_max_delay) cap and jitter). See
    /// [`with_constant_backoff`](Self::with_constant_backoff) for how the curves combine.
    #[must_use]
    pub fn with_custom_backoff(
        mut self,
        f: impl Fn(u32, &Conn) -> Duration + Send + Sync + 'static,
    ) -> Self {
        self.backoff.kind = Kind::Custom(Arc::new(f));
        self
    }

    /// Cap the computed backoff delay at `max`, applied before jitter. Defaults to uncapped. This
    /// caps *your* backoff curve; a server-advertised `Retry-After` is capped separately by
    /// [`with_max_retry_after`](Self::with_max_retry_after).
    #[must_use]
    pub fn with_max_delay(mut self, max: Duration) -> Self {
        self.backoff.max_delay = Some(max);
        self
    }

    /// Use the computed backoff delay exactly, with no randomization. By default, full jitter is
    /// applied — the actual delay is chosen uniformly at random from `0..=computed` — to spread
    /// retries from many clients across time and avoid a synchronized thundering herd.
    #[must_use]
    pub fn without_jitter(mut self) -> Self {
        self.backoff.jitter = backoff::Jitter::None;
        self
    }

    /// Set the maximum number of attempts, *including* the original request. Defaults to 4
    /// (the original plus up to 3 retries).
    #[must_use]
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set the total wall-clock budget across all attempts, or `None` for no budget. Defaults to
    /// 30 seconds. A budget is a hard ceiling: each retry's timeout is clamped to the time
    /// remaining. Without one, retries are bounded only by the attempt count, and each runs under
    /// the client's own timeout.
    #[must_use]
    pub fn with_max_elapsed(mut self, max_elapsed: impl Into<Option<Duration>>) -> Self {
        self.max_elapsed = max_elapsed.into();
        self
    }

    /// Replace the set of response statuses that trigger a retry. Defaults to `429` and `503`.
    #[must_use]
    pub fn with_statuses(mut self, statuses: impl IntoIterator<Item = Status>) -> Self {
        self.statuses = statuses.into_iter().collect();
        self
    }

    /// Retry regardless of request method, including POST and other non-idempotent requests. By
    /// default only idempotent methods (GET, HEAD, PUT, DELETE, OPTIONS, TRACE) are retried, since
    /// replaying a non-idempotent request risks a duplicate side effect. Enable this only when the
    /// endpoint is known to be safe to replay (e.g. it is idempotent in practice or guarded by an
    /// idempotency key).
    #[must_use]
    pub fn with_all_methods(mut self) -> Self {
        self.all_methods = true;
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
    /// set, and transport-error toggle. Timing still comes from the configured backoff schedule.
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
        if !self.all_methods && !is_idempotent(conn.method()) {
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

    fn build_followup(&self, conn: &Conn, state: RetryState, remaining: Option<Duration>) -> Conn {
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

        let timeout = match (conn.timeout(), remaining) {
            (Some(t), Some(r)) => Some(t.min(r)),
            (t, r) => t.or(r),
        };
        if let Some(timeout) = timeout {
            followup.set_timeout(timeout);
        }

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
                deadline: self.max_elapsed.map(|budget| Instant::now() + budget),
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
        if state
            .deadline
            .is_some_and(|deadline| Instant::now() + delay >= deadline)
        {
            return Ok(());
        }

        conn.client().connector().runtime().delay(delay).await;

        let remaining = state
            .deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()));
        if remaining.is_some_and(|r| r.is_zero()) {
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
    /// `None` when the handler has no elapsed budget.
    deadline: Option<Instant>,
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
            .field("all_methods", &self.all_methods)
            .field("transport_errors", &self.transport_errors)
            .field("honor_retry_after", &self.honor_retry_after)
            .field("max_retry_after", &self.max_retry_after)
            .field("predicate", &self.predicate.as_ref().map(|_| "<fn>"))
            .field("decision", &self.decision.as_ref().map(|_| "<fn>"))
            .finish()
    }
}
