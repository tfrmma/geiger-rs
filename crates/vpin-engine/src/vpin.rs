use std::collections::VecDeque;

use crate::bucket::{ClosedBucket, VolumeBucketer};
use crate::bvc;
use crate::VpinError;

/// Rolling standard deviation of bucket-to-bucket price changes (sigma_dP
/// in the BVC formula). Sample std (N-1 denominator), fixed-capacity
/// window, `None` until the window is completely full, matching the
/// standard `.rolling(window).std()` convention (NaN during warmup, not a
/// partial-window estimate).
///
/// Recomputes from the buffer on every push instead of maintaining
/// running sums. Windows here are realistically a few hundred to a few
/// thousand samples and this only runs once per closed bucket, not once
/// per trade, an O(window) recompute is microseconds, not worth the
/// numerical-stability headaches of incremental sliding-window variance.
pub struct RollingSigma {
    window: usize,
    buf: VecDeque<f64>,
    current: Option<f64>,
}

impl RollingSigma {
    pub fn new(window: usize) -> Result<Self, VpinError> {
        if window < 2 {
            return Err(VpinError::InvalidWindow { got: window, min: 2 });
        }
        Ok(RollingSigma { window, buf: VecDeque::with_capacity(window), current: None })
    }

    /// Estimate from buckets seen so far, `None` until the window fills.
    /// Read-only, `push` is what advances state, this lets a caller ask
    /// "what's sigma right now" before feeding in the sample that would
    /// shift it.
    pub fn current(&self) -> Option<f64> {
        self.current
    }

    pub fn push(&mut self, delta_p: f64) -> Option<f64> {
        if self.buf.len() == self.window {
            self.buf.pop_front();
        }
        self.buf.push_back(delta_p);

        self.current = if self.buf.len() < self.window {
            None
        } else {
            let mean = self.buf.iter().sum::<f64>() / self.window as f64;
            let var = self.buf.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (self.window - 1) as f64;
            Some(var.sqrt())
        };
        self.current
    }
}

/// Percentile rank of the latest value within a rolling window of past
/// values. This is what actually gets consumed downstream, not raw VPIN:
/// Easley/Lopez de Prado/O'Hara are explicit that VPIN's absolute level
/// isn't comparable across instruments (different bucket sizes, different
/// volume profiles), the CDF-transformed value is. Same fixed-window,
/// full-only-then convention as `RollingSigma`.
pub struct EmpiricalCdf {
    window: usize,
    buf: VecDeque<f64>,
}

impl EmpiricalCdf {
    pub fn new(window: usize) -> Result<Self, VpinError> {
        if window < 2 {
            return Err(VpinError::InvalidWindow { got: window, min: 2 });
        }
        Ok(EmpiricalCdf { window, buf: VecDeque::with_capacity(window) })
    }

    /// Pushes `value` in, returns its rank as a fraction in `[0, 1]` of
    /// how many buffered values (including itself) are `<= value`. `None`
    /// until the window fills. O(window) per call, see `RollingSigma`'s
    /// doc comment for why that's fine here.
    pub fn push(&mut self, value: f64) -> Option<f64> {
        if self.buf.len() == self.window {
            self.buf.pop_front();
        }
        self.buf.push_back(value);

        if self.buf.len() < self.window {
            return None;
        }

        let le_count = self.buf.iter().filter(|&&v| v <= value).count();
        Some(le_count as f64 / self.window as f64)
    }
}

/// Order imbalance for one closed bucket: `|V_buy - V_sell|`.
fn bucket_imbalance(bucket: &ClosedBucket, sigma: f64) -> Option<f64> {
    let (buy, sell) = bvc::classify(bucket.delta_p, sigma, bucket.volume)?;
    Some((buy - sell).abs())
}

/// One VPIN reading, emitted every time a bucket closes.
#[derive(Debug, Clone, Copy)]
pub struct VpinReading {
    pub bucket_id: u64,
    pub ts_close_ns: u64,
    pub trades_in_bucket: u32,
    /// `None` during warmup, until both the sigma window and the VPIN
    /// averaging window are full. `trades_in_bucket` still tells you the
    /// bucket closed, so you can tell "no data yet" apart from "flow is
    /// genuinely quiet" while waiting.
    pub vpin: Option<f64>,
    /// Percentile rank of `vpin` within its own rolling history, `None`
    /// under the same warmup condition as `vpin`, or if `vpin` itself is
    /// `None`. This, not `vpin`, is what's cross-instrument comparable.
    pub vpin_cdf: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
pub struct VpinEngineConfig {
    /// V: target volume per bucket, base-asset units. This is the
    /// bucket-closing threshold, not a hard cap, see `VolumeBucketer`'s
    /// overshoot doc comment, so realized bucket volume can run above
    /// this, VPIN's normalization accounts for that rather than assuming
    /// every bucket lands at exactly this value.
    pub bucket_volume: f64,
    /// Window for sigma_dP, in buckets. Many published calibrations use
    /// the same n for this and `vpin_window`, that's not enforced here,
    /// set them equal yourself if that's the convention you want.
    pub sigma_window: usize,
    /// n: number of trailing buckets averaged into the VPIN reading.
    pub vpin_window: usize,
    /// Window for the CDF transform (see `VpinReading::vpin_cdf`).
    /// `None` disables the CDF transform entirely, `vpin_cdf` will
    /// always be `None` on every reading.
    pub cdf_window: Option<usize>,
}

/// Ties `VolumeBucketer` + `RollingSigma` + BVC + a rolling imbalance
/// window into one streaming VPIN estimator. One instance per
/// (symbol, venue).
pub struct VpinEngine {
    bucketer: VolumeBucketer,
    sigma: RollingSigma,
    // (imbalance, actual bucket volume) pairs. Actual volume, not the
    // nominal target V: the overshoot behavior in `VolumeBucketer::push`
    // means a bucket's real volume can run above V, and normalizing by
    // n*V instead of the real total would let VPIN exceed 1.
    imbalances: VecDeque<(f64, f64)>,
    vpin_window: usize,
    cdf: Option<EmpiricalCdf>,
}

impl VpinEngine {
    pub fn new(cfg: VpinEngineConfig) -> Result<Self, VpinError> {
        if cfg.vpin_window < 1 {
            return Err(VpinError::InvalidWindow { got: cfg.vpin_window, min: 1 });
        }
        Ok(VpinEngine {
            bucketer: VolumeBucketer::new(cfg.bucket_volume)?,
            sigma: RollingSigma::new(cfg.sigma_window)?,
            imbalances: VecDeque::with_capacity(cfg.vpin_window),
            vpin_window: cfg.vpin_window,
            cdf: cfg.cdf_window.map(EmpiricalCdf::new).transpose()?,
        })
    }

    /// Feed one trade in. Returns `Some(VpinReading)` when this trade
    /// closes a bucket, `None` otherwise (most calls, buckets are made of
    /// many trades).
    pub fn push_trade(&mut self, price: f64, volume: f64, ts_ns: u64) -> Option<VpinReading> {
        let closed = self.bucketer.push(price, volume, ts_ns)?;

        // Classify THIS bucket with sigma estimated from buckets before
        // it, then advance sigma with this bucket's own delta_p, which
        // only affects the NEXT bucket's classification. Using sigma
        // that already includes this bucket's own delta_p would leak
        // this bucket's outcome into its own classification.
        let sigma_for_this_bucket = self.sigma.current();
        let vpin = self.update_vpin(&closed, sigma_for_this_bucket);
        self.sigma.push(closed.delta_p);

        let vpin_cdf = vpin.and_then(|v| self.cdf.as_mut().and_then(|c| c.push(v)));

        Some(VpinReading {
            bucket_id: closed.bucket_id,
            ts_close_ns: closed.ts_close_ns,
            trades_in_bucket: closed.trade_count,
            vpin,
            vpin_cdf,
        })
    }

    // VPIN = sum(|V_buy - V_sell|) / sum(actual bucket volume). Textbook
    // ELO uses n*V, which is only equivalent when every bucket lands at
    // exactly V. Since `VolumeBucketer` doesn't split the closing trade
    // (see its doc comment), real bucket volume can overshoot V, so this
    // normalizes by the realized total instead, keeping the result
    // exactly bounded in [0, 1]: |buy_i - sell_i| <= volume_i always.
    fn update_vpin(&mut self, closed: &ClosedBucket, sigma: Option<f64>) -> Option<f64> {
        let sigma = sigma?;
        let imbalance = bucket_imbalance(closed, sigma)?;

        if self.imbalances.len() == self.vpin_window {
            self.imbalances.pop_front();
        }
        self.imbalances.push_back((imbalance, closed.volume));

        if self.imbalances.len() < self.vpin_window {
            return None;
        }

        let (imb_sum, vol_sum) = self
            .imbalances
            .iter()
            .fold((0.0, 0.0), |(i, v), (bi, bv)| (i + bi, v + bv));
        Some(imb_sum / vol_sum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rolling_sigma_none_until_window_full() {
        let mut s = RollingSigma::new(5).unwrap();
        for i in 0..4 {
            assert!(s.push(i as f64).is_none());
        }
        assert!(s.push(4.0).is_some());
    }

    #[test]
    fn rolling_sigma_matches_known_sample_std() {
        // 2,4,4,4,5,5,7,9 -> sample std = 2.13809... (textbook example,
        // Wikipedia "Standard deviation" worked example)
        let mut s = RollingSigma::new(8).unwrap();
        let data = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let mut last = None;
        for x in data {
            last = s.push(x);
        }
        assert!((last.unwrap() - 2.138_089_935_299_395).abs() < 1e-9);
    }

    #[test]
    fn rejects_window_below_two() {
        assert!(RollingSigma::new(1).is_err());
        assert!(RollingSigma::new(0).is_err());
        assert!(EmpiricalCdf::new(1).is_err());
    }

    #[test]
    fn empirical_cdf_rank_of_max_is_one() {
        let mut c = EmpiricalCdf::new(4).unwrap();
        c.push(1.0);
        c.push(2.0);
        c.push(3.0);
        let rank = c.push(4.0).unwrap();
        assert!((rank - 1.0).abs() < 1e-9);
    }

    #[test]
    fn empirical_cdf_rank_of_min_is_one_over_window() {
        let mut c = EmpiricalCdf::new(4).unwrap();
        c.push(4.0);
        c.push(3.0);
        c.push(2.0);
        let rank = c.push(1.0).unwrap();
        assert!((rank - 0.25).abs() < 1e-9);
    }

    #[test]
    fn vpin_engine_none_during_warmup_then_produces_readings() {
        let mut engine = VpinEngine::new(VpinEngineConfig {
            bucket_volume: 10.0,
            sigma_window: 3,
            vpin_window: 3,
            cdf_window: None,
        })
        .unwrap();

        let mut saw_a_value = false;
        let mut price = 100.0;
        for i in 0..200u64 {
            // alternate up/down ticks so delta_p is never degenerately
            // constant, a flat price series gives sigma=0 which is
            // correctly undefined for BVC, that's not what this test is
            // checking
            price += if i % 2 == 0 { 0.5 } else { -0.3 };
            if let Some(reading) = engine.push_trade(price, 10.0, i) {
                if reading.vpin.is_some() {
                    saw_a_value = true;
                    let v = reading.vpin.unwrap();
                    assert!((0.0..=1.0).contains(&v), "VPIN out of [0,1]: {v}");
                }
            }
        }
        assert!(saw_a_value, "expected at least one non-warmup VPIN reading over 200 trades");
    }

    proptest::proptest! {
        // VPIN is a normalized imbalance fraction, it has to stay in
        // [0, 1] no matter what the trade tape looks like, wild price
        // jumps, tiny volumes, whatever. This is the property the manual
        // zigzag test above only spot-checks, running it over randomized
        // sequences catches edge cases a hand-picked one wouldn't.
        #[test]
        fn vpin_reading_always_in_unit_interval(
            prices in proptest::collection::vec(1.0f64..1_000_000.0, 300),
            volumes in proptest::collection::vec(0.001f64..500.0, 300),
        ) {
            let mut engine = VpinEngine::new(VpinEngineConfig {
                bucket_volume: 50.0,
                sigma_window: 10,
                vpin_window: 10,
                cdf_window: Some(20),
            }).unwrap();

            for (i, (p, v)) in prices.iter().zip(volumes.iter()).enumerate() {
                if let Some(reading) = engine.push_trade(*p, *v, i as u64) {
                    if let Some(vpin) = reading.vpin {
                        proptest::prop_assert!((0.0..=1.0).contains(&vpin), "vpin={vpin}");
                    }
                    if let Some(cdf) = reading.vpin_cdf {
                        proptest::prop_assert!((0.0..=1.0).contains(&cdf), "vpin_cdf={cdf}");
                    }
                }
            }
        }
    }

    #[test]
    fn vpin_engine_rejects_zero_vpin_window() {
        let res = VpinEngine::new(VpinEngineConfig {
            bucket_volume: 10.0,
            sigma_window: 3,
            vpin_window: 0,
            cdf_window: None,
        });
        assert!(res.is_err());
    }
}
