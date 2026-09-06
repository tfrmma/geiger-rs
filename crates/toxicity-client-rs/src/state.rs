/// What a consumer (`boros-mm`'s quoting loop, or anything else) sees
/// when it asks "what's the toxicity right now."
///
/// Four states rather than a plain `Option<f64>`: `Stale` and
/// `Connecting` both mean "no trustworthy reading," which isn't the same
/// as "toxicity is zero." Collapsing them into `None` would make it easy
/// for a caller to treat "unknown" as "everything's fine"; silence isn't
/// the same as low toxicity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ToxicityState {
    /// No message received since the most recent (re)connect attempt,
    /// covers both "just started" and "just reconnected, haven't heard
    /// anything yet."
    Connecting,
    /// Connected and receiving messages, but VPIN itself isn't ready yet
    /// server-side (not enough buckets in `vpin-engine`'s warmup
    /// window). `trades_in_bucket` is `0` if this came from a heartbeat
    /// rather than an actual `Reading`, i.e. we know the connection is
    /// alive but haven't seen a bucket close yet either.
    Warming { trades_in_bucket: u32 },
    /// A real reading. `vpin_cdf` can still be `None` even here, the CDF
    /// transform has its own, possibly different, warmup window.
    Live { vpin: f64, vpin_cdf: Option<f64>, trades_in_bucket: u32 },
    /// No message (`Reading` or `Heartbeat`) within the configured
    /// staleness threshold. Could be a dead connection, a dead
    /// `toxicity-service`, or a network partition, this client can't
    /// tell which and doesn't try to. Treat this as "assume the worst."
    Stale,
}

impl ToxicityState {
    /// Convenience for the common case: "do I have a numeric score I can
    /// actually act on right now." `false` for every other state,
    /// including `Warming`, a bucket count isn't a toxicity score.
    pub fn is_live(&self) -> bool {
        matches!(self, ToxicityState::Live { .. })
    }

    /// `true` for `Stale` specifically, not `Connecting`. A caller doing
    /// "fall back to a conservative posture" on first-ever connect
    /// (before anything has had a chance to warm up) would spend its
    /// entire startup in a defensive crouch for no reason, that's a
    /// different situation than a feed that WAS working and went dark.
    pub fn is_stale(&self) -> bool {
        matches!(self, ToxicityState::Stale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_live_only_true_for_live() {
        assert!(ToxicityState::Live { vpin: 0.1, vpin_cdf: None, trades_in_bucket: 5 }.is_live());
        assert!(!ToxicityState::Warming { trades_in_bucket: 0 }.is_live());
        assert!(!ToxicityState::Connecting.is_live());
        assert!(!ToxicityState::Stale.is_live());
    }

    #[test]
    fn is_stale_only_true_for_stale_not_connecting() {
        assert!(ToxicityState::Stale.is_stale());
        assert!(!ToxicityState::Connecting.is_stale());
        assert!(!ToxicityState::Warming { trades_in_bucket: 0 }.is_stale());
    }
}
