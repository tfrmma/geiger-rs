//! Python bindings for `vpin-engine`, built with `maturin`. Import name
//! is `vpin` (see `[lib] name = "vpin"` in Cargo.toml), package name on
//! PyPI/in `pyproject.toml` is `py-vpin`, those are allowed to differ and
//! here they deliberately do, `import vpin` reads better than
//! `import py_vpin`.
//!
//! ```python
//! from vpin import VpinEngine
//!
//! engine = VpinEngine(bucket_volume=50.0, sigma_window=50, vpin_window=50, cdf_window=250)
//! reading = engine.push_trade(price=100.5, volume=2.3, ts_ns=1_700_000_000_000_000_000)
//! if reading is not None and reading.vpin is not None:
//!     print(reading.vpin, reading.vpin_cdf, reading.trades_in_bucket)
//! ```
//!
//! Thin wrapper, no logic of its own: every field and the bucket/sigma/
//! BVC/CDF math itself all live in `vpin-engine`, this crate only
//! translates types across the FFI boundary and turns `VpinError` into a
//! Python `ValueError`.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use vpin_engine::{VpinEngine, VpinEngineConfig, VpinReading};

/// One VPIN reading, mirrors `vpin_engine::VpinReading` field for field.
#[pyclass(name = "VpinReading", module = "vpin", frozen)]
struct PyVpinReading {
    #[pyo3(get)]
    bucket_id: u64,
    #[pyo3(get)]
    ts_close_ns: u64,
    #[pyo3(get)]
    trades_in_bucket: u32,
    /// `None` during warmup, see `vpin_engine::VpinReading`'s doc comment.
    #[pyo3(get)]
    vpin: Option<f64>,
    /// Percentile rank of `vpin` within its own rolling history, this is
    /// what's actually comparable across instruments, not `vpin` itself.
    #[pyo3(get)]
    vpin_cdf: Option<f64>,
}

impl From<VpinReading> for PyVpinReading {
    fn from(r: VpinReading) -> Self {
        PyVpinReading {
            bucket_id: r.bucket_id,
            ts_close_ns: r.ts_close_ns,
            trades_in_bucket: r.trades_in_bucket,
            vpin: r.vpin,
            vpin_cdf: r.vpin_cdf,
        }
    }
}

#[pymethods]
impl PyVpinReading {
    fn __repr__(&self) -> String {
        format!(
            "VpinReading(bucket_id={}, trades_in_bucket={}, vpin={}, vpin_cdf={})",
            self.bucket_id,
            self.trades_in_bucket,
            opt_repr(self.vpin),
            opt_repr(self.vpin_cdf),
        )
    }
}

fn opt_repr(v: Option<f64>) -> String {
    match v {
        Some(x) => format!("{x}"),
        None => "None".to_string(),
    }
}

/// Streaming VPIN estimator. See `vpin_engine::VpinEngine` for the actual
/// algorithm (BVC on a volume clock) and `vpin_engine::VpinEngineConfig`
/// for what each constructor argument means, this class is a direct
/// pass-through to both.
#[pyclass(name = "VpinEngine", module = "vpin")]
struct PyVpinEngine {
    inner: VpinEngine,
}

#[pymethods]
impl PyVpinEngine {
    #[new]
    #[pyo3(signature = (bucket_volume, sigma_window, vpin_window, cdf_window=None))]
    fn new(bucket_volume: f64, sigma_window: usize, vpin_window: usize, cdf_window: Option<usize>) -> PyResult<Self> {
        let cfg = VpinEngineConfig { bucket_volume, sigma_window, vpin_window, cdf_window };
        let inner = VpinEngine::new(cfg).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyVpinEngine { inner })
    }

    /// Feed one trade in. Returns a `VpinReading` if this trade closes a
    /// bucket, `None` otherwise (most calls, a bucket is many trades).
    fn push_trade(&mut self, price: f64, volume: f64, ts_ns: u64) -> Option<PyVpinReading> {
        self.inner.push_trade(price, volume, ts_ns).map(PyVpinReading::from)
    }
}

#[pymodule]
fn vpin(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyVpinEngine>()?;
    m.add_class::<PyVpinReading>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // These call straight into the #[pymethods] impls (not through the
    // Python interpreter, no .py file involved), which is enough to
    // catch FFI-boundary mistakes (wrong Option handling, panics
    // crossing into Python, wrong error type) without needing an actual
    // built wheel and a Python process. Import-level and .py-usage
    // testing (`import vpin`) needs the real `extension-module` build
    // via maturin, not exercised here, see the crate doc comment.

    #[test]
    fn construct_and_push_trade_via_pymethods() {
        let mut engine = PyVpinEngine::new(10.0, 5, 5, None).unwrap();
        let mut saw_reading = false;
        let mut price = 100.0;
        for i in 0..300u64 {
            price += if i % 2 == 0 { 0.5 } else { -0.3 };
            if let Some(reading) = engine.push_trade(price, 10.0, i) {
                saw_reading = true;
                assert!(reading.trades_in_bucket > 0);
                if let Some(v) = reading.vpin {
                    assert!((0.0..=1.0).contains(&v));
                }
            }
        }
        assert!(saw_reading);
    }

    #[test]
    fn invalid_config_becomes_value_error_not_a_panic() {
        let result = PyVpinEngine::new(-1.0, 5, 5, None);
        assert!(result.is_err());
    }

    #[test]
    fn repr_handles_none_fields_without_panicking() {
        let reading = PyVpinReading { bucket_id: 0, ts_close_ns: 0, trades_in_bucket: 3, vpin: None, vpin_cdf: None };
        assert_eq!(reading.__repr__(), "VpinReading(bucket_id=0, trades_in_bucket=3, vpin=None, vpin_cdf=None)");
    }

    #[test]
    fn repr_formats_some_values_as_numbers_not_debug_syntax() {
        let reading = PyVpinReading { bucket_id: 1, ts_close_ns: 0, trades_in_bucket: 3, vpin: Some(0.42), vpin_cdf: Some(0.9) };
        assert_eq!(reading.__repr__(), "VpinReading(bucket_id=1, trades_in_bucket=3, vpin=0.42, vpin_cdf=0.9)");
    }
}
