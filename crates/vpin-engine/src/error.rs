use thiserror::Error;

#[derive(Debug, Error, Clone, Copy, PartialEq)]
pub enum VpinError {
    #[error("bucket target volume must be finite and positive, got {0}")]
    InvalidBucketVolume(f64),

    #[error("window size must be at least {min}, got {got}")]
    InvalidWindow { got: usize, min: usize },
}
