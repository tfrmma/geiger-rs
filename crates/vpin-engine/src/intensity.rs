use std::collections::VecDeque;

use crate::VpinError;

/// VPIN's percentile rank computed within a cohort of similarly-busy
/// buckets, rather than against all buckets regardless of how busy the
/// market was at the time. Andersen & Bondarenko (2014) found VPIN's
/// apparent predictive power is largely a mechanical byproduct of
/// trading intensity, so a spike during a genuinely busy period may not
/// mean anything, it's what you'd expect from busy-period buckets in
/// general.
///
/// Splits the rolling window into two cohorts by whether
/// `trades_in_bucket` is at or below the MEDIAN observed intensity so
/// far, self-calibrating rather than a fixed threshold picked in
/// advance, then ranks the current reading only against its own cohort.
/// This is a stratification, not a regression, it doesn't claim to be
/// the definitively correct correction, it's a starting point: does this
/// VPIN reading look unusual given how busy the market currently is, not
/// just in absolute terms.
pub struct IntensityAdjustedCdf {
    window: usize,
    // (vpin, trades_in_bucket) pairs, oldest first
    history: VecDeque<(f64, u32)>,
}

impl IntensityAdjustedCdf {
    /// `window` needs enough points for both a meaningful median split
    /// and a meaningful rank within each half, 4 is the practical floor,
    /// realistic use wants this much larger (same order as
    /// `vpin_engine::VpinEngineConfig::vpin_window`).
    pub fn new(window: usize) -> Result<Self, VpinError> {
        if window < 4 {
            return Err(VpinError::InvalidWindow { got: window, min: 4 });
        }
        Ok(IntensityAdjustedCdf { window, history: VecDeque::with_capacity(window) })
    }

    /// Pushes `(vpin, trades_in_bucket)` in, returns the rank of `vpin`
    /// within its intensity cohort as a fraction in `[0, 1]`, `None`
    /// until the window fills (same warmup convention as `EmpiricalCdf`).
    pub fn push(&mut self, vpin: f64, trades_in_bucket: u32) -> Option<f64> {
        if self.history.len() == self.window {
            self.history.pop_front();
        }
        self.history.push_back((vpin, trades_in_bucket));

        if self.history.len() < self.window {
            return None;
        }

        let median_intensity = median_u32(self.history.iter().map(|&(_, t)| t));
        // >=, not >: with few distinct intensity values (e.g. many
        // buckets closing with exactly the same trade count), the median
        // itself is often one of those repeated values. `>` would then
        // exclude every point at that value from BOTH cohorts (nothing
        // is strictly greater than a value that equals itself), silently
        // collapsing the split back to "everything." `>=` puts ties into
        // the upper cohort consistently instead.
        let this_cohort_is_busy = trades_in_bucket >= median_intensity;

        let cohort: Vec<f64> = self
            .history
            .iter()
            .filter(|&&(_, t)| (t >= median_intensity) == this_cohort_is_busy)
            .map(|&(v, _)| v)
            .collect();

        let le_count = cohort.iter().filter(|&&v| v <= vpin).count();
        Some(le_count as f64 / cohort.len() as f64)
    }
}

fn median_u32(values: impl Iterator<Item = u32>) -> u32 {
    let mut v: Vec<u32> = values.collect();
    v.sort_unstable();
    v[v.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_window_below_four() {
        assert!(IntensityAdjustedCdf::new(3).is_err());
        assert!(IntensityAdjustedCdf::new(0).is_err());
    }

    #[test]
    fn none_until_window_full() {
        let mut c = IntensityAdjustedCdf::new(4).unwrap();
        assert!(c.push(0.5, 10).is_none());
        assert!(c.push(0.5, 10).is_none());
        assert!(c.push(0.5, 10).is_none());
        assert!(c.push(0.5, 10).is_some());
    }

    /// The behavior this whole struct exists for: a VPIN reading that
    /// looks extreme globally can be completely unremarkable once you
    /// account for how busy the market was. Fill the window with two
    /// cohorts, quiet buckets always around 0.3, busy buckets spread
    /// 0.90-0.94 (this is the exact mechanical pattern Andersen &
    /// Bondarenko describe: VPIN reads high specifically when trading is
    /// intense). Then push one more busy-cohort reading at exactly the
    /// MIDDLE of that busy range, 0.92, its rank has to land mid-cohort,
    /// nowhere near the top, even though 0.92 is about as high as VPIN
    /// gets anywhere in this data set.
    #[test]
    fn high_vpin_during_busy_period_ranks_unremarkable_within_cohort() {
        let mut c = IntensityAdjustedCdf::new(40).unwrap();

        // 39 points, window (40) not yet full, every push still returns
        // None, this is purely building history for the real check below
        for i in 0..39u32 {
            let (vpin, trades) = if i % 2 == 0 {
                (0.3 + (i as f64 % 5.0) * 0.01, 5) // quiet cohort
            } else {
                (0.9 + (i as f64 % 5.0) * 0.01, 200) // busy cohort, spans 0.90-0.94
            };
            assert!(c.push(vpin, trades).is_none(), "window shouldn't be full yet");
        }

        // the 40th push: deliberately the midpoint of the busy cohort's
        // own 0.90-0.94 range, not an incidental value
        let rank = c.push(0.92, 200).unwrap();
        assert!(rank > 0.1 && rank < 0.9, "expected a mid-range rank within the busy cohort, got {rank}");
    }

    #[test]
    fn quiet_and_busy_cohorts_are_scored_independently() {
        // same VPIN value, different intensity: a 0.5 reading should not
        // necessarily rank the same in a cohort centered on 0.3 vs one
        // centered on 0.9
        let mut quiet_low = IntensityAdjustedCdf::new(10).unwrap();
        let mut busy_high = IntensityAdjustedCdf::new(10).unwrap();

        for i in 0..9u32 {
            quiet_low.push(0.2 + (i as f64) * 0.01, 5);
            busy_high.push(0.8 + (i as f64) * 0.01, 200);
        }

        let rank_in_quiet = quiet_low.push(0.5, 5).unwrap();
        let rank_in_busy = busy_high.push(0.5, 200).unwrap();

        // 0.5 is far above everything in the quiet cohort (near the top)
        // and far below everything in the busy cohort (near the bottom)
        assert!(rank_in_quiet > 0.8, "0.5 should rank high in a ~0.2-0.3 cohort, got {rank_in_quiet}");
        assert!(rank_in_busy < 0.2, "0.5 should rank low in a ~0.8-0.9 cohort, got {rank_in_busy}");
    }

    #[test]
    fn median_boundary_with_only_two_distinct_intensities_still_splits_correctly() {
        // Regression test: caught this exact case failing with a `>`
        // split instead of `>=`. With only two distinct trades_in_bucket
        // values (5 and 200), the median lands exactly on one of them,
        // and `>` put every point at that value into neither cohort,
        // silently collapsing the split back to "everything."
        let mut c = IntensityAdjustedCdf::new(40).unwrap();
        for i in 0..39u32 {
            let (vpin, trades) = if i % 2 == 0 { (0.3, 5) } else { (0.9 + (i as f64 % 5.0) * 0.01, 200) };
            c.push(vpin, trades);
        }
        // a busy-cohort (200) reading at the midpoint of that cohort's
        // own 0.90-0.94 spread has to rank mid-range, not at the extreme
        // of the full 40-point history
        let rank = c.push(0.92, 200).unwrap();
        assert!(rank > 0.05 && rank < 0.95, "expected the busy cohort to actually be isolated, got rank {rank}");
    }

    #[test]
    fn rank_stays_in_unit_interval() {
        let mut c = IntensityAdjustedCdf::new(20).unwrap();
        let mut state: u64 = 7;
        for _ in 0..500 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let vpin = ((state >> 40) as f64 / (1u64 << 24) as f64).min(1.0);
            let trades = ((state >> 20) & 0xff) as u32;
            if let Some(rank) = c.push(vpin, trades) {
                assert!((0.0..=1.0).contains(&rank), "rank out of bounds: {rank}");
            }
        }
    }
}
