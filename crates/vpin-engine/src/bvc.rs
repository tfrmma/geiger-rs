//! Bulk Volume Classification (BVC), Easley, Lopez de Prado & O'Hara (2012),
//! "Flow Toxicity and Liquidity in a High-Frequency World", eq. 5-6, also
//! restated in Prado (2018) "Advances in Financial Machine Learning" ch. 19.
//!
//! For a closed bucket with total volume V and price change delta_p over
//! the bucket:
//!
//!   V_buy  = V * Phi(delta_p / sigma)
//!   V_sell = V - V_buy
//!
//! Phi is the standard normal CDF, sigma is the rolling standard deviation
//! of bucket-to-bucket price changes (NOT of returns, and NOT of the raw
//! price series). ELO center the distribution at zero rather than at the
//! sample mean delta_p, on the grounds that in HFT data the mean price
//! change is negligible next to its standard deviation, this is what
//! `classify` below does, there's no mean-subtraction step.
//!
//! This crate operates on real tick-level trades (see `trade-ingest`), so
//! delta_p is bucket-close to bucket-close, one classification per closed
//! bucket. The bar-subdivided variant of BVC (classify each intra-bucket
//! bar separately, then sum) exists in the literature specifically for
//! when only aggregated OHLCV bars are available and true tick data isn't,
//! that's not our situation and this crate doesn't implement it.

use libm::erf;

/// Standard normal CDF via `erf`: Phi(x) = 0.5 * (1 + erf(x / sqrt(2))).
/// `libm::erf` is a full double-precision port of MUSL's implementation,
/// not a hand-rolled polynomial approximation, no accuracy tradeoff here.
#[inline]
fn standard_normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x * std::f64::consts::FRAC_1_SQRT_2))
}

/// Buy-side fraction of a bucket's volume, in `[0, 1]`.
///
/// Returns `None` when `sigma` isn't finite and strictly positive, delta_p
/// can't be standardized against a degenerate or not-yet-warmed-up sigma.
/// Callers should skip the bucket for VPIN purposes rather than substitute
/// a made-up value, this comes up during startup before enough buckets
/// exist to estimate sigma, see `RollingSigma` in `vpin.rs`.
pub fn buy_fraction(delta_p: f64, sigma: f64) -> Option<f64> {
    if !(sigma.is_finite() && sigma > 0.0) || !delta_p.is_finite() {
        return None;
    }
    Some(standard_normal_cdf(delta_p / sigma))
}

/// Splits `volume` into `(buy, sell)` using `buy_fraction`. `None` under
/// the same conditions as `buy_fraction`.
pub fn classify(delta_p: f64, sigma: f64, volume: f64) -> Option<(f64, f64)> {
    let frac = buy_fraction(delta_p, sigma)?;
    let buy = volume * frac;
    Some((buy, volume - buy))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_delta_p_splits_evenly() {
        let (buy, sell) = classify(0.0, 1.0, 100.0).unwrap();
        assert!((buy - 50.0).abs() < 1e-9);
        assert!((sell - 50.0).abs() < 1e-9);
    }

    #[test]
    fn large_positive_delta_p_goes_almost_entirely_to_buy() {
        // delta_p several sigma above zero, Phi saturates near 1
        let frac = buy_fraction(10.0, 1.0).unwrap();
        assert!(frac > 0.999_999);
    }

    #[test]
    fn large_negative_delta_p_goes_almost_entirely_to_sell() {
        let frac = buy_fraction(-10.0, 1.0).unwrap();
        assert!(frac < 0.000_001);
    }

    #[test]
    fn buy_and_sell_always_sum_to_total_volume() {
        for delta_p in [-5.0, -1.0, -0.01, 0.0, 0.01, 1.0, 5.0] {
            for sigma in [0.001, 0.5, 1.0, 3.7] {
                let (buy, sell) = classify(delta_p, sigma, 1234.5).unwrap();
                assert!((buy + sell - 1234.5).abs() < 1e-6, "delta_p={delta_p} sigma={sigma}");
            }
        }
    }

    #[test]
    fn rejects_nonpositive_or_nonfinite_sigma() {
        assert!(buy_fraction(1.0, 0.0).is_none());
        assert!(buy_fraction(1.0, -1.0).is_none());
        assert!(buy_fraction(1.0, f64::NAN).is_none());
        assert!(buy_fraction(1.0, f64::INFINITY).is_none());
    }

    #[test]
    fn rejects_nonfinite_delta_p() {
        assert!(buy_fraction(f64::NAN, 1.0).is_none());
        assert!(buy_fraction(f64::INFINITY, 1.0).is_none());
    }

    #[test]
    fn cdf_matches_known_values() {
        // standard normal CDF at 0, 1, -1, 1.96 (textbook reference points)
        assert!((standard_normal_cdf(0.0) - 0.5).abs() < 1e-9);
        assert!((standard_normal_cdf(1.0) - 0.8413447460685429).abs() < 1e-9);
        assert!((standard_normal_cdf(-1.0) - 0.15865525393145707).abs() < 1e-9);
        assert!((standard_normal_cdf(1.96) - 0.9750021048517796).abs() < 1e-8);
    }
}
