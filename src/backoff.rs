use std::{fmt, sync::Arc, time::Duration};
use trillium_client::Conn;

/// How jitter is applied on top of a [`Backoff`] curve.
///
/// Jitter spreads retries from many clients across time so a recovering server isn't hit by a
/// synchronized thundering herd. It is orthogonal to the curve shape — it applies to any
/// [`Backoff`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Jitter {
    /// The computed delay is used exactly, with no randomization.
    None,
    /// Full jitter: the actual delay is chosen uniformly at random from `0..=computed`.
    #[default]
    Full,
}

type CustomFn = Arc<dyn Fn(u32, &Conn) -> Duration + Send + Sync>;

#[derive(Clone)]
enum Kind {
    Constant(Duration),
    Linear(Duration),
    Exponential(Duration),
    Custom(CustomFn),
}

/// A delay schedule for spacing out retry attempts.
///
/// A `Backoff` maps a 1-based retry number (the first retry is `1`) to a base delay, then
/// applies an optional [`max_delay`](Backoff::with_max_delay) cap and [`Jitter`]. It describes
/// *your client's* politeness curve; a server-advertised `Retry-After` is honored separately by
/// [`RetryHandler`](crate::RetryHandler) and takes precedence when present.
///
/// The default for [`RetryHandler`](crate::RetryHandler) is
/// `Backoff::exponential(Duration::from_millis(100))` with [`Jitter::Full`].
#[derive(Clone)]
pub struct Backoff {
    kind: Kind,
    max_delay: Option<Duration>,
    jitter: Jitter,
}

impl Backoff {
    fn from_kind(kind: Kind) -> Self {
        Self {
            kind,
            max_delay: None,
            jitter: Jitter::Full,
        }
    }

    /// A fixed delay before every retry.
    #[must_use]
    pub fn constant(delay: Duration) -> Self {
        Self::from_kind(Kind::Constant(delay))
    }

    /// A delay that grows linearly: `step * retry_number` (the first retry waits `step`).
    #[must_use]
    pub fn linear(step: Duration) -> Self {
        Self::from_kind(Kind::Linear(step))
    }

    /// A delay that doubles each retry: `base * 2^(retry_number - 1)` (the first retry waits
    /// `base`).
    #[must_use]
    pub fn exponential(base: Duration) -> Self {
        Self::from_kind(Kind::Exponential(base))
    }

    /// A fully custom curve. The closure receives the 1-based retry number and the conn carrying
    /// the response or error being retried, and returns the base delay (before jitter and the
    /// [`max_delay`](Backoff::with_max_delay) cap).
    #[must_use]
    pub fn custom(f: impl Fn(u32, &Conn) -> Duration + Send + Sync + 'static) -> Self {
        Self::from_kind(Kind::Custom(Arc::new(f)))
    }

    /// Cap the computed delay at `max`. Applied before jitter.
    #[must_use]
    pub fn with_max_delay(mut self, max: Duration) -> Self {
        self.max_delay = Some(max);
        self
    }

    /// Set the [`Jitter`] strategy. Defaults to [`Jitter::Full`].
    #[must_use]
    pub fn with_jitter(mut self, jitter: Jitter) -> Self {
        self.jitter = jitter;
        self
    }

    pub(crate) fn delay(&self, retry_number: u32, conn: &Conn) -> Duration {
        let base = match &self.kind {
            Kind::Constant(delay) => *delay,
            Kind::Linear(step) => step.saturating_mul(retry_number),
            Kind::Exponential(base) => {
                base.saturating_mul(2u32.saturating_pow(retry_number.saturating_sub(1)))
            }
            Kind::Custom(f) => f(retry_number, conn),
        };
        let capped = self.max_delay.map_or(base, |max| base.min(max));
        match self.jitter {
            Jitter::None => capped,
            Jitter::Full => full_jitter(capped),
        }
    }
}

fn full_jitter(max: Duration) -> Duration {
    let max_nanos = u64::try_from(max.as_nanos()).unwrap_or(u64::MAX);
    if max_nanos == 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos(fastrand::u64(0..=max_nanos))
    }
}

impl fmt::Debug for Backoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backoff")
            .field("kind", &self.kind)
            .field("max_delay", &self.max_delay)
            .field("jitter", &self.jitter)
            .finish()
    }
}

impl fmt::Debug for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Constant(d) => f.debug_tuple("Constant").field(d).finish(),
            Self::Linear(d) => f.debug_tuple("Linear").field(d).finish(),
            Self::Exponential(d) => f.debug_tuple("Exponential").field(d).finish(),
            Self::Custom(_) => f.debug_tuple("Custom").field(&"<fn>").finish(),
        }
    }
}
