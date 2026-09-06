use std::mem;

use feedhandler::{Exchange, Price, Qty, Symbol};

use crate::IngestError;

/// Ground-truth taker side as reported by the exchange itself. Not used
/// by `vpin-engine`'s BVC classification (that's the point of BVC, see
/// its module doc), kept here so `tools/calibrate` can check BVC's
/// probabilistic classification against what actually happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum TakerSide {
    #[default]
    Buy  = 0,
    Sell = 1,
}

/// One executed trade, normalized across venues. Reuses feedhandler's
/// `Price`/`Qty`/`Symbol`/`Exchange` so `toxicity-service` can correlate
/// this against `NormalizedTick` under the exact same identity types, no
/// conversion layer between the book side and the trade side.
///
/// Not chasing `NormalizedTick`'s 64B/cache-line discipline here, that
/// constraint exists there for a ~500k updates/sec book path, trades run
/// at a fraction of that rate and this struct doesn't sit on anything
/// that hot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct NormalizedTrade {
    pub price: Price,
    pub qty: Qty,
    pub ts_exchange_ns: u64,
    pub ts_recv_ns: u64,
    pub symbol: Symbol,
    pub exchange: Exchange,
    pub taker_side: TakerSide,
    /// Adapter-assigned, monotonic per (symbol, exchange) stream. NOT the
    /// exchange's own trade id, those don't share a type across venues
    /// (Bybit's is a UUID string, Binance's and Hyperliquid's are
    /// integers with different bit widths), this is just "the Nth trade
    /// this adapter has emitted for this stream," for gap detection and
    /// capture/replay ordering.
    pub sequence: u64,
}

const _: () = assert!(mem::size_of::<NormalizedTrade>() % 8 == 0);

/// Parses an exchange decimal string (e.g. `"16578.50"`) into `raw` fixed-
/// point units at `Price`/`Qty`'s 1e-8 scale.
///
/// Deliberately not `s.parse::<f64>()? * 1e8`, binary floats can't
/// represent most decimal fractions exactly, `"0.1"` isn't exactly
/// 0.1 in f64, and multiplying by 1e8 before rounding compounds that
/// error right where exactness matters most. This parses the integer and
/// fractional parts as strings and combines them with integer math
/// instead, so `"16578.50000000"` round-trips to exactly
/// `1_657_850_000_000` every time.
pub fn parse_fixed8(s: &str) -> Result<u64, IngestError> {
    let s = s.trim();
    if s.is_empty() || s.starts_with('-') {
        return Err(IngestError::BadDecimal(s.to_string()));
    }

    const SCALE: u64 = 100_000_000; // 1e8

    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };

    if frac_part.len() > 8 {
        // Exchanges we integrate with don't quote past 8 decimals, more
        // than that is either a different asset class than this format
        // supports or a wire-format surprise, either way silently
        // truncating financial precision is the wrong failure mode.
        return Err(IngestError::BadDecimal(s.to_string()));
    }

    let int_val: u64 = int_part.parse().map_err(|_| IngestError::BadDecimal(s.to_string()))?;
    let int_val = int_val.checked_mul(SCALE).ok_or_else(|| IngestError::BadDecimal(s.to_string()))?;

    if frac_part.is_empty() {
        return Ok(int_val);
    }

    let mut padded = frac_part.to_string();
    padded.push_str(&"0".repeat(8 - frac_part.len()));
    let frac_val: u64 = padded.parse().map_err(|_| IngestError::BadDecimal(s.to_string()))?;

    int_val.checked_add(frac_val).ok_or_else(|| IngestError::BadDecimal(s.to_string()))
}

pub fn parse_price(s: &str) -> Result<Price, IngestError> {
    parse_fixed8(s).map(Price::new)
}

pub fn parse_qty(s: &str) -> Result<Qty, IngestError> {
    parse_fixed8(s).map(Qty::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_whole_number() {
        assert_eq!(parse_fixed8("16578").unwrap(), 1_657_800_000_000);
    }

    #[test]
    fn parses_exact_8_decimals() {
        assert_eq!(parse_fixed8("0.00000001").unwrap(), 1);
    }

    #[test]
    fn parses_short_decimal_pads_with_zeros() {
        // "16578.50" -> 16578.50000000
        assert_eq!(parse_fixed8("16578.50").unwrap(), 1_657_850_000_000);
    }

    #[test]
    fn rejects_more_than_8_decimals() {
        assert!(parse_fixed8("1.123456789").is_err());
    }

    #[test]
    fn rejects_negative() {
        assert!(parse_fixed8("-1.5").is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_fixed8("").is_err());
        assert!(parse_fixed8("abc").is_err());
        assert!(parse_fixed8("1.2.3").is_err());
    }

    #[test]
    fn normalized_trade_size_is_8_byte_aligned() {
        // not chasing NormalizedTick's exact 64B, but repr(C) layouts
        // still shouldn't have surprise padding eating capture file space
        assert_eq!(mem::size_of::<NormalizedTrade>() % 8, 0);
    }

    proptest::proptest! {
        // Any raw fixed8 value, rendered back out as an 8-decimal string,
        // has to parse back to exactly the same raw value. This is the
        // property that actually matters, the hand-picked cases above
        // only spot-check a few points on this curve.
        #[test]
        fn parse_fixed8_round_trips_arbitrary_values(raw in 0u64..1_000_000_000_000_000u64) {
            let int_part = raw / 100_000_000;
            let frac_part = raw % 100_000_000;
            let s = format!("{int_part}.{frac_part:08}");
            let parsed = parse_fixed8(&s).unwrap();
            proptest::prop_assert_eq!(parsed, raw);
        }
    }
}
