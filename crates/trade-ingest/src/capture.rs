//! Bit-stable capture/replay for `NormalizedTrade` streams, so
//! `tools/calibrate` (and anyone else) can replay a real trade tape
//! instead of calibrating against synthetic data.
//!
//! This is NOT `feedhandler-core-rs`'s `TickRecorder`/`TickReader`
//! pattern (raw `repr(C)` memcpy of the whole struct via
//! `offset_of!`/`ptr::read_unaligned`). Two reasons: `offset_of!` needs
//! Rust 1.77+, which isn't the actual constraint here, and more to the
//! point, trade capture doesn't sit anywhere near the book's
//! ~500k-updates/sec hot path that raw-byte trick earns its complexity
//! for. A plain, explicit field-by-field encoding is `unsafe`-free,
//! doesn't depend on struct layout matching memory layout, and is just
//! as fast at trade rates. Same discipline where it actually matters
//! though: validate `Exchange`/`TakerSide` discriminants before
//! constructing the enum, a truncated trailing record is a surfaced
//! error, not a silent stop.
//!
//! No compression, and the framing is minimal: a 5-byte header (magic +
//! format version), not a schema. If `NormalizedTrade`'s fields change,
//! bump `FORMAT_VERSION` so old capture files fail to open loudly
//! instead of getting parsed against the wrong field layout in silence.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use feedhandler::{Exchange, Price, Qty, Symbol};

use crate::types::{NormalizedTrade, TakerSide};

const MAGIC: &[u8; 4] = b"GTRC";
const FORMAT_VERSION: u8 = 1;
const HEADER_SIZE: usize = 5; // magic(4) + version(1)

// price(8) + qty(8) + ts_exchange_ns(8) + ts_recv_ns(8) + symbol(16) +
// exchange(1) + taker_side(1) + sequence(8)
const RECORD_SIZE: usize = 58;

fn header_bytes() -> [u8; HEADER_SIZE] {
    let mut buf = [0u8; HEADER_SIZE];
    buf[0..4].copy_from_slice(MAGIC);
    buf[4] = FORMAT_VERSION;
    buf
}

fn validate_header(header: &[u8; HEADER_SIZE]) -> io::Result<()> {
    if &header[0..4] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "not a geiger-rs capture file: expected magic {MAGIC:?}, got {:?}",
                &header[0..4]
            ),
        ));
    }
    if header[4] != FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "capture file format version {} unsupported, this build reads version {FORMAT_VERSION}",
                header[4]
            ),
        ));
    }
    Ok(())
}

#[derive(Debug)]
pub struct TradeRecorder {
    out: BufWriter<File>,
}

impl TradeRecorder {
    /// Appends to `path`, creating it (and writing the format header) if
    /// it doesn't exist. Reopening an existing file validates its header
    /// against this build's `FORMAT_VERSION` before allowing further
    /// appends, so a capture started under an old build can't silently
    /// end up with old-layout and new-layout records mixed in one file.
    ///
    /// # Errors
    /// The header is invalid or the wrong version (existing file only),
    /// or whatever the underlying file operations return.
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        if file.metadata()?.len() == 0 {
            file.write_all(&header_bytes())?;
        } else {
            // Append mode always writes at EOF regardless of the
            // current cursor position (POSIX O_APPEND semantics), so
            // reading the header from the start here doesn't disturb
            // where subsequent `record` calls land.
            let mut header = [0u8; HEADER_SIZE];
            file.read_exact(&mut header).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("capture file too short to contain a header: {e}"),
                )
            })?;
            validate_header(&header)?;
        }

        Ok(TradeRecorder {
            out: BufWriter::new(file),
        })
    }

    /// # Errors
    /// Whatever the underlying `write_all` returns.
    pub fn record(&mut self, trade: &NormalizedTrade) -> io::Result<()> {
        let mut buf = [0u8; RECORD_SIZE];
        buf[0..8].copy_from_slice(&trade.price.raw().to_le_bytes());
        buf[8..16].copy_from_slice(&trade.qty.raw().to_le_bytes());
        buf[16..24].copy_from_slice(&trade.ts_exchange_ns.to_le_bytes());
        buf[24..32].copy_from_slice(&trade.ts_recv_ns.to_le_bytes());
        buf[32..48].copy_from_slice(trade.symbol.as_bytes());
        buf[48] = trade.exchange as u8;
        buf[49] = trade.taker_side as u8;
        buf[50..58].copy_from_slice(&trade.sequence.to_le_bytes());
        self.out.write_all(&buf)
    }

    /// `BufWriter` buffers internally, call this before your process
    /// exits or the tail of the session never makes it to disk.
    ///
    /// # Errors
    /// Whatever the underlying flush returns.
    pub fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}

#[derive(Debug)]
pub struct TradeReader {
    input: BufReader<File>,
}

impl TradeReader {
    /// # Errors
    /// The file is missing/unreadable, too short to contain a header,
    /// has the wrong magic, or was written by a build with a different
    /// `FORMAT_VERSION`.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut header = [0u8; HEADER_SIZE];
        file.read_exact(&mut header).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("capture file too short to contain a header: {e}"),
            )
        })?;
        validate_header(&header)?;
        Ok(TradeReader {
            input: BufReader::new(file),
        })
    }
}

impl Iterator for TradeReader {
    type Item = io::Result<NormalizedTrade>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = [0u8; RECORD_SIZE];
        let mut got = 0;

        loop {
            match self.input.read(&mut buf[got..]) {
                Ok(0) => break,
                Ok(n) => {
                    got += n;
                    if got == RECORD_SIZE {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Some(Err(e)),
            }
        }

        if got == 0 {
            return None; // clean end of file, no partial record left dangling
        }
        if got != RECORD_SIZE {
            return Some(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("capture file truncated mid-record: got {got} of {RECORD_SIZE} bytes"),
            )));
        }

        let exchange = match exchange_from_u8(buf[48]) {
            Ok(e) => e,
            Err(e) => return Some(Err(e)),
        };
        let taker_side = match taker_side_from_u8(buf[49]) {
            Ok(t) => t,
            Err(e) => return Some(Err(e)),
        };

        Some(Ok(NormalizedTrade {
            price: Price::new(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            qty: Qty::new(u64::from_le_bytes(buf[8..16].try_into().unwrap())),
            ts_exchange_ns: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            ts_recv_ns: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            symbol: Symbol::from_bytes(&buf[32..48]),
            exchange,
            taker_side,
            sequence: u64::from_le_bytes(buf[50..58].try_into().unwrap()),
        }))
    }
}

fn exchange_from_u8(b: u8) -> io::Result<Exchange> {
    match b {
        0 => Ok(Exchange::Binance),
        1 => Ok(Exchange::Bybit),
        2 => Ok(Exchange::Hyperliquid),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("corrupt capture record: exchange discriminant {b} is out of range"),
        )),
    }
}

fn taker_side_from_u8(b: u8) -> io::Result<TakerSide> {
    match b {
        0 => Ok(TakerSide::Buy),
        1 => Ok(TakerSide::Sell),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("corrupt capture record: taker_side discriminant {b} is out of range"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(seq: u64) -> NormalizedTrade {
        NormalizedTrade {
            price: Price::new(100 + seq),
            qty: Qty::new(1),
            ts_exchange_ns: seq * 1000,
            ts_recv_ns: seq * 1000 + 50,
            symbol: Symbol::from_bytes(b"BTCUSDT"),
            exchange: Exchange::Binance,
            taker_side: if seq % 2 == 0 {
                TakerSide::Buy
            } else {
                TakerSide::Sell
            },
            sequence: seq,
        }
    }

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("ti-capture-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("session.trades")
    }

    #[test]
    fn round_trips_a_session_bit_for_bit() {
        let path = temp_path("roundtrip");
        let written: Vec<NormalizedTrade> = (0..50).map(trade).collect();
        {
            let mut rec = TradeRecorder::create(&path).unwrap();
            for t in &written {
                rec.record(t).unwrap();
            }
            rec.flush().unwrap();
        }

        let read: Vec<NormalizedTrade> = TradeReader::open(&path)
            .unwrap()
            .collect::<io::Result<_>>()
            .unwrap();
        assert_eq!(written, read);
    }

    #[test]
    fn file_starts_with_the_format_header() {
        let path = temp_path("header");
        TradeRecorder::create(&path).unwrap().flush().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), HEADER_SIZE);
        assert_eq!(&bytes[0..4], MAGIC);
        assert_eq!(bytes[4], FORMAT_VERSION);
    }

    #[test]
    fn reopening_an_existing_file_validates_header_and_appends_after_it() {
        let path = temp_path("reopen");
        {
            let mut rec = TradeRecorder::create(&path).unwrap();
            rec.record(&trade(1)).unwrap();
            rec.flush().unwrap();
        }
        // Simulates a process restart: a fresh `TradeRecorder::create`
        // call against a file that already has a header and one record.
        {
            let mut rec = TradeRecorder::create(&path).unwrap();
            rec.record(&trade(2)).unwrap();
            rec.flush().unwrap();
        }

        let read: Vec<NormalizedTrade> = TradeReader::open(&path)
            .unwrap()
            .collect::<io::Result<_>>()
            .unwrap();
        assert_eq!(read, vec![trade(1), trade(2)]);
    }

    #[test]
    fn reader_rejects_wrong_magic() {
        let path = temp_path("bad-magic");
        std::fs::write(&path, b"NOPE\x01somejunkafterit").unwrap();
        let err = TradeReader::open(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn reader_rejects_unsupported_version() {
        let path = temp_path("bad-version");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(FORMAT_VERSION + 1);
        std::fs::write(&path, bytes).unwrap();
        let err = TradeReader::open(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn reader_rejects_file_too_short_for_a_header() {
        let path = temp_path("too-short");
        std::fs::write(&path, b"GT").unwrap();
        let err = TradeReader::open(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn recorder_refuses_to_reopen_a_file_with_wrong_version() {
        let path = temp_path("reopen-bad-version");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(FORMAT_VERSION + 1);
        std::fs::write(&path, bytes).unwrap();

        let err = TradeRecorder::create(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn truncated_trailing_record_is_an_error_not_a_silent_stop() {
        let path = temp_path("trunc");
        {
            let mut rec = TradeRecorder::create(&path).unwrap();
            rec.record(&trade(1)).unwrap();
            rec.record(&trade(2)).unwrap();
            rec.flush().unwrap();
        }
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len((HEADER_SIZE + RECORD_SIZE) as u64 + 10)
            .unwrap();
        drop(file);

        let mut reader = TradeReader::open(&path).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), trade(1));
        let second = reader
            .next()
            .expect("truncated record must surface, not be swallowed");
        assert!(second.is_err());
        assert!(
            reader.next().is_none(),
            "reader must stop cleanly after reporting the truncation"
        );
    }

    #[test]
    fn corrupt_exchange_discriminant_is_rejected() {
        use std::io::{Seek, SeekFrom};
        let path = temp_path("corrupt-exchange");
        {
            let mut rec = TradeRecorder::create(&path).unwrap();
            rec.record(&trade(1)).unwrap();
        }
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start((HEADER_SIZE + 48) as u64))
            .unwrap();
        file.write_all(&[200u8]).unwrap(); // no Exchange variant has this discriminant
        drop(file);

        let mut reader = TradeReader::open(&path).unwrap();
        assert!(reader.next().unwrap().is_err());
    }

    #[test]
    fn corrupt_taker_side_discriminant_is_rejected() {
        use std::io::{Seek, SeekFrom};
        let path = temp_path("corrupt-side");
        {
            let mut rec = TradeRecorder::create(&path).unwrap();
            rec.record(&trade(1)).unwrap();
        }
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start((HEADER_SIZE + 49) as u64))
            .unwrap();
        file.write_all(&[7u8]).unwrap(); // no TakerSide variant has this discriminant
        drop(file);

        let mut reader = TradeReader::open(&path).unwrap();
        assert!(reader.next().unwrap().is_err());
    }

    #[test]
    fn empty_file_yields_no_records() {
        let path = temp_path("empty");
        TradeRecorder::create(&path).unwrap().flush().unwrap();
        let mut reader = TradeReader::open(&path).unwrap();
        assert!(reader.next().is_none());
    }
}
