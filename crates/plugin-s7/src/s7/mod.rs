//! Vendored S7 client (rust7, MIT) plus the Lymon discover extension.
//!
//! - [`client`] — the vendored rust7 `S7Client` (ISO-on-TCP read/write), with two
//!   `// LYMON DELTA:` fixes and a `pub(crate)` `exchange` accessor.
//! - [`decode`] — S7 scalar types and big-endian byte → f64 decoders.
//! - [`blocks`] — the `list blocks of type` telegram that powers discover.

pub mod blocks;
// rust7 is vendored whole (kept verbatim for a clean upstream diff) but we use only a
// subset of its API (reads, not writes/extra connect helpers). Silence dead_code/unused
// and clippy on the vendored module so its third-party style doesn't fail `cargo clippy
// -D warnings`; client.rs is also excluded from `cargo fmt` via rustfmt.toml.
#[allow(dead_code, unused, clippy::all)]
pub mod client;
pub mod decode;
