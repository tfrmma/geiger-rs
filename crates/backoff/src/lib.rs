use std::time::Duration;

/// Exponential backoff with a cap, no jitter library dependency, just
/// `fastrand`-free integer math off a per-instance counter. Reset on
/// every successful connect, each adapter owns one instance for its own
/// reconnect loop.
pub struct ExponentialBackoff {
    attempt: u32,
    base: Duration,
    max: Duration,
}

impl ExponentialBackoff {
    pub fn new(base: Duration, max: Duration) -> Self {
        ExponentialBackoff { attempt: 0, base, max }
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Doubles per call, capped at `max`. `2u32.saturating_pow` guards the
    /// shift from overflowing after a very long outage, at which point
    /// we're just sitting at `max` anyway.
    pub fn next_delay(&mut self) -> Duration {
        let mult = 2u32.saturating_pow(self.attempt.min(20));
        self.attempt += 1;
        self.base.saturating_mul(mult).min(self.max)
    }
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_each_attempt_until_capped() {
        let mut b = ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(5));
        assert_eq!(b.next_delay(), Duration::from_millis(100));
        assert_eq!(b.next_delay(), Duration::from_millis(200));
        assert_eq!(b.next_delay(), Duration::from_millis(400));
        assert_eq!(b.next_delay(), Duration::from_millis(800));
    }

    #[test]
    fn caps_at_max() {
        let mut b = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(3));
        for _ in 0..10 {
            assert!(b.next_delay() <= Duration::from_secs(3));
        }
    }

    #[test]
    fn reset_goes_back_to_base() {
        let mut b = ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(5));
        b.next_delay();
        b.next_delay();
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_millis(100));
    }

    #[test]
    fn never_panics_over_many_attempts() {
        let mut b = ExponentialBackoff::new(Duration::from_millis(50), Duration::from_secs(10));
        for _ in 0..10_000 {
            let d = b.next_delay();
            assert!(d <= Duration::from_secs(10));
        }
    }
}
