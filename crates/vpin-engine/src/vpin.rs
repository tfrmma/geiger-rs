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
            return Err(VpinError::InvalidWindow {
                got: window,
                min: 2,
            });
        }
        Ok(RollingSigma {
            window,
            buf: VecDeque::with_capacity(window),
            current: None,
        })
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
            let var =
                self.buf.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (self.window - 1) as f64;
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
            return Err(VpinError::InvalidWindow {
                got: window,
                min: 2,
            });
        }
        Ok(EmpiricalCdf {
            window,
            buf: VecDeque::with_capacity(window),
        })
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

/// Small, fast, deterministic PRNG (SplitMix64, Vigna) used only to
/// drive the bootstrap resampling below. Hand-rolled instead of adding
/// `rand`: this crate is deliberately dependency-light (`thiserror` and
/// `libm` are the only real dependencies, see the module doc comment),
/// and bootstrap resampling needs a fast, reproducible number source,
/// not cryptographic-quality randomness.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform index in `[0, n)`. Plain modulo, not bias-corrected: `n`
    /// here is `vpin_window`, realistically tens to low hundreds,
    /// against a 64-bit range that bias is unmeasurably small, and this
    /// is resampling for an operational confidence interval, not a
    /// cryptographic or adversarial context. `n` is always `>= 1` at
    /// every call site (see `bootstrap_ci`), so this never divides by
    /// zero.
    fn next_index(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Percentile bootstrap confidence interval for VPIN over the current
/// window. Resamples `(imbalance, volume)` PAIRS together, not each
/// independently: VPIN is `sum(imbalance) / sum(volume)`, a ratio of
/// sums rather than a simple mean, so a resample has to keep each
/// imbalance paired with the bucket volume it actually came from, or
/// the resampled ratio no longer estimates the same statistic. This is
/// the standard bootstrap treatment for ratio estimators (Efron &
/// Tibshirani, "An Introduction to the Bootstrap", 1993), which is also
/// why this is a bootstrap rather than a closed-form standard-error
/// interval: there's no simple closed form for the sampling variance of
/// a ratio-of-sums, but the bootstrap doesn't need one.
fn bootstrap_ci(
    window: &VecDeque<(f64, f64)>,
    cfg: &ConfidenceIntervalConfig,
    rng: &mut SplitMix64,
) -> (f64, f64) {
    let n = window.len();
    let pairs: Vec<(f64, f64)> = window.iter().copied().collect();

    let mut samples = Vec::with_capacity(cfg.bootstrap_samples);
    for _ in 0..cfg.bootstrap_samples {
        let mut imb_sum = 0.0;
        let mut vol_sum = 0.0;
        for _ in 0..n {
            let (imb, vol) = pairs[rng.next_index(n)];
            imb_sum += imb;
            vol_sum += vol;
        }
        // vol_sum is always > 0: every pair's volume is a closed
        // bucket's realized volume, which `VolumeBucketer` never closes
        // at zero, so this never divides by zero.
        samples.push(imb_sum / vol_sum);
    }

    samples.sort_by(f64::total_cmp);

    let alpha = 1.0 - cfg.confidence_level;
    let low = percentile(&samples, alpha / 2.0);
    let high = percentile(&samples, 1.0 - alpha / 2.0);
    (low, high)
}

/// Linear interpolation between the two nearest ranks (the common
/// "R-7"/NumPy-default percentile method). `sorted` must be sorted
/// ascending and non-empty, `p` in `[0, 1]`.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }
    let rank = p * (n - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = rank - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
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
    /// Percentile bootstrap confidence interval around `vpin`, `None`
    /// unless `VpinEngineConfig::confidence_interval` is set (it's
    /// opt-in: real compute cost per bucket close, not free like the
    /// other fields here) and under the same warmup condition as `vpin`.
    /// See `ConfidenceIntervalConfig` for the method and why it's a
    /// bootstrap rather than a closed-form interval.
    pub vpin_ci_low: Option<f64>,
    pub vpin_ci_high: Option<f64>,
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
    /// `None` disables the bootstrap confidence interval entirely,
    /// `vpin_ci_low`/`vpin_ci_high` will always be `None` on every
    /// reading. Opt-in rather than always-on: it's real extra compute
    /// per bucket close (`bootstrap_samples` resamples of the current
    /// `vpin_window`), not free like `vpin`/`vpin_cdf` themselves.
    pub confidence_interval: Option<ConfidenceIntervalConfig>,
}

/// Settings for the bootstrap confidence interval on `VpinReading::vpin`.
/// See `bootstrap_ci`'s doc comment for the method and why it's a
/// bootstrap rather than a closed-form standard-error interval.
#[derive(Debug, Clone, Copy)]
pub struct ConfidenceIntervalConfig {
    /// e.g. `0.95` for a 95% interval. Must be in `(0, 1)` exclusive.
    pub confidence_level: f64,
    /// Resamples drawn per reading. This runs once per bucket close,
    /// not once per trade, so even a few thousand is cheap; `1000` is a
    /// reasonable default trade-off between the interval's resolution
    /// and compute cost. Must be at least 2.
    pub bootstrap_samples: usize,
    /// Seed for the internal PRNG driving resampling. Fixed rather than
    /// time-based: replaying the same trade tape through the same
    /// config twice then produces bit-identical CI bounds both times,
    /// which matters for tests and for reproducing a `tools/calibrate`
    /// run.
    pub seed: u64,
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
    ci: Option<(ConfidenceIntervalConfig, SplitMix64)>,
}

impl VpinEngine {
    pub fn new(cfg: VpinEngineConfig) -> Result<Self, VpinError> {
        if cfg.vpin_window < 1 {
            return Err(VpinError::InvalidWindow {
                got: cfg.vpin_window,
                min: 1,
            });
        }
        let ci = match cfg.confidence_interval {
            None => None,
            Some(c) => {
                if !(c.confidence_level > 0.0 && c.confidence_level < 1.0) {
                    return Err(VpinError::InvalidConfidenceLevel(c.confidence_level));
                }
                if c.bootstrap_samples < 2 {
                    return Err(VpinError::InvalidBootstrapSamples {
                        got: c.bootstrap_samples,
                        min: 2,
                    });
                }
                Some((c, SplitMix64::new(c.seed)))
            }
        };

        Ok(VpinEngine {
            bucketer: VolumeBucketer::new(cfg.bucket_volume)?,
            sigma: RollingSigma::new(cfg.sigma_window)?,
            imbalances: VecDeque::with_capacity(cfg.vpin_window),
            vpin_window: cfg.vpin_window,
            cdf: cfg.cdf_window.map(EmpiricalCdf::new).transpose()?,
            ci,
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

        let (vpin_ci_low, vpin_ci_high) = match (vpin, self.ci.as_mut()) {
            (Some(_), Some((ci_cfg, rng))) => {
                let (low, high) = bootstrap_ci(&self.imbalances, ci_cfg, rng);
                (Some(low), Some(high))
            }
            _ => (None, None),
        };

        Some(VpinReading {
            bucket_id: closed.bucket_id,
            ts_close_ns: closed.ts_close_ns,
            trades_in_bucket: closed.trade_count,
            vpin,
            vpin_cdf,
            vpin_ci_low,
            vpin_ci_high,
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
            confidence_interval: None,
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
                if let Some(v) = reading.vpin {
                    saw_a_value = true;
                    assert!((0.0..=1.0).contains(&v), "VPIN out of [0,1]: {v}");
                }
            }
        }
        assert!(
            saw_a_value,
            "expected at least one non-warmup VPIN reading over 200 trades"
        );
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
                confidence_interval: Some(ConfidenceIntervalConfig {
                    confidence_level: 0.95,
                    bootstrap_samples: 200,
                    seed: 1,
                }),
            }).unwrap();

            for (i, (p, v)) in prices.iter().zip(volumes.iter()).enumerate() {
                if let Some(reading) = engine.push_trade(*p, *v, i as u64) {
                    if let Some(vpin) = reading.vpin {
                        proptest::prop_assert!((0.0..=1.0).contains(&vpin), "vpin={vpin}");
                        // The interval is only computed alongside a
                        // non-warmup vpin, so it must be present here.
                        // NOT asserted here: that it brackets `vpin`
                        // itself. A plain percentile bootstrap isn't
                        // guaranteed to (that's one of the textbook
                        // reasons BCa intervals exist), so claiming it
                        // always does would be an unproven invariant
                        // this proptest, which specifically hunts for
                        // edge cases, would eventually catch out.
                        let low = reading.vpin_ci_low.expect("ci enabled, vpin present");
                        let high = reading.vpin_ci_high.expect("ci enabled, vpin present");
                        proptest::prop_assert!((0.0..=1.0).contains(&low), "low={low}");
                        proptest::prop_assert!((0.0..=1.0).contains(&high), "high={high}");
                        proptest::prop_assert!(low <= high, "low={low} > high={high}");
                    } else {
                        proptest::prop_assert!(reading.vpin_ci_low.is_none());
                        proptest::prop_assert!(reading.vpin_ci_high.is_none());
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
            confidence_interval: None,
        });
        assert!(res.is_err());
    }

    #[test]
    fn splitmix64_is_deterministic_given_the_same_seed() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn splitmix64_different_seeds_diverge() {
        // Not a rigorous randomness test, just a sanity check that two
        // different seeds don't collapse to the same stream by some
        // trivial bug (e.g. state not actually depending on the seed).
        let mut a = SplitMix64::new(1);
        let mut b = SplitMix64::new(2);
        let seq_a: Vec<u64> = (0..20).map(|_| a.next_u64()).collect();
        let seq_b: Vec<u64> = (0..20).map(|_| b.next_u64()).collect();
        assert_ne!(seq_a, seq_b);
    }

    #[test]
    fn percentile_matches_hand_computed_values() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((percentile(&data, 0.0) - 1.0).abs() < 1e-12);
        assert!((percentile(&data, 1.0) - 5.0).abs() < 1e-12);
        assert!((percentile(&data, 0.5) - 3.0).abs() < 1e-12);
        // rank = 0.25 * 4 = 1.0 -> exactly data[1] = 2.0, no interpolation
        assert!((percentile(&data, 0.25) - 2.0).abs() < 1e-12);
        // rank = 0.1 * 4 = 0.4 -> 40% of the way from data[0]=1.0 to
        // data[1]=2.0
        assert!((percentile(&data, 0.1) - 1.4).abs() < 1e-9);
    }

    #[test]
    fn percentile_of_single_value_is_that_value() {
        assert_eq!(percentile(&[7.0], 0.0), 7.0);
        assert_eq!(percentile(&[7.0], 0.5), 7.0);
        assert_eq!(percentile(&[7.0], 1.0), 7.0);
    }

    #[test]
    fn rejects_confidence_level_outside_open_unit_interval() {
        let base = VpinEngineConfig {
            bucket_volume: 10.0,
            sigma_window: 3,
            vpin_window: 3,
            cdf_window: None,
            confidence_interval: None,
        };
        for bad_level in [0.0, 1.0, -0.5, 1.5, f64::NAN] {
            let cfg = VpinEngineConfig {
                confidence_interval: Some(ConfidenceIntervalConfig {
                    confidence_level: bad_level,
                    bootstrap_samples: 100,
                    seed: 1,
                }),
                ..base
            };
            assert!(
                VpinEngine::new(cfg).is_err(),
                "expected confidence_level={bad_level} to be rejected"
            );
        }
    }

    #[test]
    fn rejects_too_few_bootstrap_samples() {
        let base = VpinEngineConfig {
            bucket_volume: 10.0,
            sigma_window: 3,
            vpin_window: 3,
            cdf_window: None,
            confidence_interval: None,
        };
        for bad_n in [0, 1] {
            let cfg = VpinEngineConfig {
                confidence_interval: Some(ConfidenceIntervalConfig {
                    confidence_level: 0.95,
                    bootstrap_samples: bad_n,
                    seed: 1,
                }),
                ..base
            };
            assert!(
                VpinEngine::new(cfg).is_err(),
                "expected bootstrap_samples={bad_n} to be rejected"
            );
        }
    }

    #[test]
    fn ci_fields_stay_none_when_not_configured() {
        let mut engine = VpinEngine::new(VpinEngineConfig {
            bucket_volume: 10.0,
            sigma_window: 3,
            vpin_window: 3,
            cdf_window: None,
            confidence_interval: None,
        })
        .unwrap();

        let mut price = 100.0;
        let mut saw_a_vpin = false;
        for i in 0..100u64 {
            price += if i % 2 == 0 { 0.5 } else { -0.3 };
            if let Some(reading) = engine.push_trade(price, 10.0, i) {
                assert!(reading.vpin_ci_low.is_none());
                assert!(reading.vpin_ci_high.is_none());
                saw_a_vpin |= reading.vpin.is_some();
            }
        }
        assert!(
            saw_a_vpin,
            "test needs at least one non-warmup reading to be meaningful"
        );
    }

    #[test]
    fn ci_fields_track_vpin_warmup_exactly_when_configured() {
        let mut engine = VpinEngine::new(VpinEngineConfig {
            bucket_volume: 10.0,
            sigma_window: 3,
            vpin_window: 3,
            cdf_window: None,
            confidence_interval: Some(ConfidenceIntervalConfig {
                confidence_level: 0.95,
                bootstrap_samples: 100,
                seed: 3,
            }),
        })
        .unwrap();

        let mut price = 100.0;
        let mut saw_both_some = false;
        for i in 0..100u64 {
            price += if i % 2 == 0 { 0.5 } else { -0.3 };
            if let Some(reading) = engine.push_trade(price, 10.0, i) {
                assert_eq!(reading.vpin.is_some(), reading.vpin_ci_low.is_some());
                assert_eq!(reading.vpin.is_some(), reading.vpin_ci_high.is_some());
                saw_both_some |= reading.vpin.is_some();
            }
        }
        assert!(saw_both_some);
    }

    #[test]
    fn same_seed_gives_bit_identical_ci_bounds_across_runs() {
        fn run() -> Vec<(f64, f64)> {
            let mut engine = VpinEngine::new(VpinEngineConfig {
                bucket_volume: 10.0,
                sigma_window: 3,
                vpin_window: 5,
                cdf_window: None,
                confidence_interval: Some(ConfidenceIntervalConfig {
                    confidence_level: 0.95,
                    bootstrap_samples: 300,
                    seed: 99,
                }),
            })
            .unwrap();
            let mut out = Vec::new();
            let mut price = 100.0;
            for i in 0..150u64 {
                price += if i % 3 == 0 { 0.8 } else { -0.5 };
                if let Some(reading) = engine.push_trade(price, 10.0, i) {
                    if let (Some(low), Some(high)) = (reading.vpin_ci_low, reading.vpin_ci_high) {
                        out.push((low, high));
                    }
                }
            }
            out
        }

        let first = run();
        assert!(!first.is_empty(), "test needs at least one CI reading");
        assert_eq!(first, run());
    }

    #[test]
    fn higher_confidence_level_gives_a_wider_or_equal_interval() {
        // Same seed and bootstrap_samples on both, fed the identical
        // trade sequence: the underlying bootstrap resamples are then
        // bit-identical between the two runs (confidence_level only
        // affects which percentiles get read out of that same sample
        // set afterward, not how many draws happen or in what order),
        // so this is a genuine apples-to-apples comparison, not just
        // two unrelated random runs that happened to differ.
        fn last_ci(level: f64) -> (f64, f64) {
            let mut engine = VpinEngine::new(VpinEngineConfig {
                bucket_volume: 10.0,
                sigma_window: 3,
                vpin_window: 5,
                cdf_window: None,
                confidence_interval: Some(ConfidenceIntervalConfig {
                    confidence_level: level,
                    bootstrap_samples: 500,
                    seed: 7,
                }),
            })
            .unwrap();
            let mut last = None;
            let mut price = 100.0;
            for i in 0..100u64 {
                price += if i % 2 == 0 { 0.6 } else { -0.4 };
                if let Some(reading) = engine.push_trade(price, 10.0, i) {
                    if let (Some(low), Some(high)) = (reading.vpin_ci_low, reading.vpin_ci_high) {
                        last = Some((low, high));
                    }
                }
            }
            last.expect("expected at least one non-warmup reading")
        }

        let (low_80, high_80) = last_ci(0.80);
        let (low_99, high_99) = last_ci(0.99);
        assert!(
            (high_99 - low_99) >= (high_80 - low_80) - 1e-12,
            "99% CI [{low_99}, {high_99}] should be at least as wide as 80% CI [{low_80}, {high_80}]"
        );
    }
}
