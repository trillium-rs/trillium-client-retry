//! Internal backoff schedule. The curve, the optional cap, and the jitter strategy are all
//! configured through [`RetryHandler`](crate::RetryHandler)'s builders rather than exposed as
//! standalone types.

use std::{fmt, sync::Arc, time::Duration};
use trillium_client::Conn;

/// How jitter is applied on top of the backoff curve.
///
/// Jitter spreads retries from many clients across time so a recovering server isn't hit by a
/// synchronized thundering herd. It is orthogonal to the curve shape.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Jitter {
    /// The computed delay is used exactly, with no randomization.
    None,
    /// Full jitter: the actual delay is chosen uniformly at random from `0..=computed`.
    #[default]
    Full,
}

type CustomFn = Arc<dyn Fn(u32, &Conn) -> Duration + Send + Sync>;

/// The shape of the backoff curve.
#[derive(Clone)]
pub(crate) enum Kind {
    Constant(Duration),
    Linear(Duration),
    Exponential(Duration),
    Custom(CustomFn),
}

/// A delay schedule for spacing out retry attempts.
///
/// Maps a 1-based retry number (the first retry is `1`) to a base delay, then applies an optional
/// `max_delay` cap and [`Jitter`]. Configured through `RetryHandler`'s `with_*_backoff`,
/// `with_max_delay`, and `without_jitter` builders.
#[derive(Clone)]
pub(crate) struct Backoff {
    pub(crate) kind: Kind,
    pub(crate) max_delay: Option<Duration>,
    pub(crate) jitter: Jitter,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            kind: Kind::Exponential(Duration::from_millis(100)),
            max_delay: None,
            jitter: Jitter::Full,
        }
    }
}

impl Backoff {
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
