use std::time::Duration;

/// Reconnect backoff parameters. Exists separately from
/// `ExponentialBackoff` so callers (`trade-ingest`'s adapters,
/// `toxicity-client-rs`) can accept a config value instead of always
/// building the same hardcoded backoff internally, and so
/// `toxicity-service` can read per-stream overrides from its JSON config
/// instead of every stream being stuck with one hardcoded shared value.
///
/// The default (250ms base, doubling, 30s cap) reaches the cap in about
/// 7 attempts (~32s elapsed), settling to roughly 2 reconnect attempts
/// per minute during a sustained outage. Checked against each venue's
/// documented connection limits: Binance allows 300 connection attempts
/// per 5 minutes per IP, Bybit up to 500 per 5 minutes, Hyperliquid caps
/// at 100 concurrent WebSocket connections per IP (no separate
/// reconnect-rate figure documented). This default stays comfortably
/// under all three for a single stream. It does NOT account for many
/// concurrent streams on the same exchange sharing one IP all
/// reconnecting at once during a shared outage, that's a real gap, see
/// `services/toxicity-service`'s open items.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BackoffConfig {
    pub base: Duration,
    pub max: Duration,
}

impl BackoffConfig {
    pub fn build(&self) -> ExponentialBackoff {
        ExponentialBackoff::new(self.base, self.max)
    }
}

impl Default for BackoffConfig {
    fn default() -> Self {
        BackoffConfig { base: Duration::from_millis(250), max: Duration::from_secs(30) }
    }
}

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
        BackoffConfig::default().build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_config_default_matches_documented_safe_defaults() {
        let cfg = BackoffConfig::default();
        assert_eq!(cfg.base, Duration::from_millis(250));
        assert_eq!(cfg.max, Duration::from_secs(30));
    }

    #[test]
    fn backoff_config_build_produces_a_working_backoff() {
        let mut b = BackoffConfig { base: Duration::from_millis(10), max: Duration::from_millis(40) }.build();
        assert_eq!(b.next_delay(), Duration::from_millis(10));
        assert_eq!(b.next_delay(), Duration::from_millis(20));
        assert_eq!(b.next_delay(), Duration::from_millis(40));
        assert_eq!(b.next_delay(), Duration::from_millis(40)); // capped
    }

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
