//! Volume clock: buckets close on accumulated executed volume, not wall
//! time. Easley/Lopez de Prado/O'Hara (2012), "Flow Toxicity and Liquidity
//! in a High-Frequency World", section 2 — the whole point of VPIN is
//! sampling synchronized to trading activity instead of calendar time, so
//! this has to stay volume-driven even when trades are bursty.

use crate::VpinError;

/// One closed bucket, ready to feed into BVC classification.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClosedBucket {
    pub bucket_id: u64,
    /// Price change across the bucket: last trade price minus the last
    /// trade price of the *previous* bucket. Signed, this is what BVC's
    /// Φ(ΔP / σ) needs, not |ΔP|.
    pub delta_p: f64,
    /// Total volume in the bucket. Should equal `target_volume` except for
    /// the last trade that closes it, see `push`'s doc comment for the
    /// overshoot handling.
    pub volume: f64,
    pub last_price: f64,
    pub trade_count: u32,
    /// Timestamp of the trade that closed the bucket, exchange clock, ns.
    pub ts_close_ns: u64,
}

/// Accumulates trades into fixed-volume buckets. One instance per
/// (symbol, venue).
pub struct VolumeBucketer {
    target_volume: f64,
    bucket_id: u64,
    acc_volume: f64,
    last_price: f64,
    prev_bucket_close_price: Option<f64>,
    trade_count: u32,
    started: bool,
}

impl VolumeBucketer {
    /// `target_volume` is V in the BVC literature, base-asset units
    /// (contracts, coins, whatever the trade feed reports). Has to be
    /// positive, a zero or negative bucket size would either divide by
    /// zero downstream or never close.
    pub fn new(target_volume: f64) -> Result<Self, VpinError> {
        if !(target_volume.is_finite() && target_volume > 0.0) {
            return Err(VpinError::InvalidBucketVolume(target_volume));
        }
        Ok(VolumeBucketer {
            target_volume,
            bucket_id: 0,
            acc_volume: 0.0,
            last_price: 0.0,
            prev_bucket_close_price: None,
            trade_count: 0,
            started: false,
        })
    }

    /// Feed one trade in. Returns `Some(ClosedBucket)` if this trade tips
    /// the accumulator over `target_volume`, closing the bucket.
    ///
    /// Deliberately does NOT split the closing trade across two buckets.
    /// A 50-lot print against a 40-lot remaining bucket closes that
    /// bucket at 50 (not 40) and starts the next one empty, rather than
    /// carrying 10 lots forward. Splitting is what you'd want for exact
    /// volume-uniform buckets, but it also means one trade's direction
    /// gets counted as if it were two independent prints, which biases
    /// BVC's Φ(ΔP/σ) classification on exactly the large, informative
    /// trades VPIN cares most about. Overshoot is the standard tradeoff
    /// here (Easley et al. use it too, real trades don't divide evenly
    /// into any bucket size you pick).
    pub fn push(&mut self, price: f64, volume: f64, ts_ns: u64) -> Option<ClosedBucket> {
        debug_assert!(price.is_finite() && price > 0.0, "non-finite or non-positive trade price");
        debug_assert!(volume.is_finite() && volume > 0.0, "non-finite or non-positive trade volume");

        if !self.started {
            self.prev_bucket_close_price = Some(price);
            self.started = true;
        }

        self.acc_volume += volume;
        self.last_price = price;
        self.trade_count += 1;

        if self.acc_volume < self.target_volume {
            return None;
        }

        let closed = ClosedBucket {
            bucket_id: self.bucket_id,
            delta_p: price - self.prev_bucket_close_price.unwrap_or(price),
            volume: self.acc_volume,
            last_price: price,
            trade_count: self.trade_count,
            ts_close_ns: ts_ns,
        };

        self.bucket_id += 1;
        self.acc_volume = 0.0;
        self.trade_count = 0;
        self.prev_bucket_close_price = Some(price);

        Some(closed)
    }

    /// Volume accumulated in the bucket currently being filled. Exposed
    /// for metrics/dashboards, not used by the classification math itself.
    pub fn partial_volume(&self) -> f64 {
        self.acc_volume
    }

    pub fn current_bucket_id(&self) -> u64 {
        self.bucket_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_nonpositive_target_volume() {
        assert!(VolumeBucketer::new(0.0).is_err());
        assert!(VolumeBucketer::new(-1.0).is_err());
        assert!(VolumeBucketer::new(f64::NAN).is_err());
        assert!(VolumeBucketer::new(f64::INFINITY).is_err());
    }

    #[test]
    fn closes_exactly_at_target_volume() {
        let mut b = VolumeBucketer::new(100.0).unwrap();
        assert!(b.push(50.0, 60.0, 1).is_none());
        let closed = b.push(51.0, 40.0, 2).expect("should close at exactly 100");
        assert_eq!(closed.volume, 100.0);
        assert_eq!(closed.trade_count, 2);
        assert_eq!(closed.bucket_id, 0);
    }

    #[test]
    fn overshoot_closes_the_bucket_it_tips_over_not_the_next_one() {
        let mut b = VolumeBucketer::new(100.0).unwrap();
        assert!(b.push(50.0, 90.0, 1).is_none());
        // this trade alone is bigger than the whole remaining bucket
        let closed = b.push(51.0, 500.0, 2).expect("should close, overshooting");
        assert_eq!(closed.volume, 590.0);
        // next bucket starts clean, no 490 carried forward
        assert_eq!(b.partial_volume(), 0.0);
    }

    #[test]
    fn first_bucket_has_zero_delta_p_seed() {
        // nothing to diff against yet, delta_p for the very first bucket is
        // relative to its own first trade price, i.e. zero
        let mut b = VolumeBucketer::new(10.0).unwrap();
        let closed = b.push(100.0, 10.0, 1).unwrap();
        assert_eq!(closed.delta_p, 0.0);
    }

    #[test]
    fn delta_p_is_close_to_close_not_open_to_close() {
        let mut b = VolumeBucketer::new(10.0).unwrap();
        let first = b.push(100.0, 10.0, 1).unwrap();
        assert_eq!(first.last_price, 100.0);
        // second bucket's delta should be measured from the first bucket's
        // CLOSE (100), not from wherever the second bucket happens to open
        b.push(105.0, 4.0, 2);
        let second = b.push(110.0, 6.0, 3).unwrap();
        assert_eq!(second.delta_p, 10.0); // 110 - 100, not 110 - 105
    }

    #[test]
    fn bucket_id_increments_monotonically() {
        let mut b = VolumeBucketer::new(10.0).unwrap();
        for i in 0..5u64 {
            let closed = b.push(100.0 + i as f64, 10.0, i).unwrap();
            assert_eq!(closed.bucket_id, i);
        }
        assert_eq!(b.current_bucket_id(), 5);
    }
}
