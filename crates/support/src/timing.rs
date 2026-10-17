use std::thread;
use std::time::{Duration, Instant};

use time::OffsetDateTime;

const MINIMUM_RETRY_DELAY_MILLISECONDS: u64 = 50;
const RETRY_JITTER_MILLISECONDS: u64 = 100;
#[cfg(feature = "test-fixtures")]
const TEST_POLL_INTERVAL_MILLISECONDS: &str = "UM_TEST_POLL_INTERVAL_MILLISECONDS";

#[expect(
    clippy::disallowed_methods,
    reason = "this module is the production boundary for monotonic clock reads"
)]
pub fn monotonic_now() -> Instant {
    Instant::now()
}

#[expect(
    clippy::disallowed_methods,
    reason = "this module is the production boundary for monotonic elapsed-time reads"
)]
pub fn elapsed(started_at: Instant) -> Duration {
    started_at.elapsed()
}

#[expect(
    clippy::disallowed_methods,
    reason = "this module is the production boundary for UTC wall-clock reads"
)]
pub fn utc_now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

#[expect(
    clippy::disallowed_methods,
    reason = "this module is the production boundary for blocking retry waits"
)]
pub fn sleep(duration: Duration) {
    thread::sleep(duration);
}

#[expect(
    clippy::disallowed_methods,
    reason = "this module is the production boundary for asynchronous deadline waits"
)]
pub async fn async_sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

/// Integration tests drive the built command binary and cannot inject a clock,
/// so builds with `test-fixtures` let them shorten a polling cadence through
/// the environment. Every other build polls at the production interval.
pub fn poll_interval(production: Duration) -> Duration {
    #[cfg(feature = "test-fixtures")]
    if let Some(milliseconds) = std::env::var_os(TEST_POLL_INTERVAL_MILLISECONDS)
        .and_then(|value| value.to_str()?.parse().ok())
    {
        return Duration::from_millis(milliseconds).min(production);
    }
    production
}

pub fn short_retry_delay() -> Duration {
    let mut bytes = [0_u8; 8];
    let random = if getrandom::fill(&mut bytes).is_ok() {
        u64::from_le_bytes(bytes)
    } else {
        RETRY_JITTER_MILLISECONDS / 2
    };
    retry_delay_from(random)
}

fn retry_delay_from(random: u64) -> Duration {
    Duration::from_millis(
        MINIMUM_RETRY_DELAY_MILLISECONDS + random % RETRY_JITTER_MILLISECONDS.saturating_add(1),
    )
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    reason = "timing unit tests use Rust equality assertions"
)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_has_a_nonzero_bounded_jitter_window() {
        assert_eq!(retry_delay_from(0), Duration::from_millis(50));
        assert_eq!(retry_delay_from(50), Duration::from_millis(100));
        assert_eq!(retry_delay_from(100), Duration::from_millis(150));
        assert_eq!(retry_delay_from(101), Duration::from_millis(50));
    }
}
