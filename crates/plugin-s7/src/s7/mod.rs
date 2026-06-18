//! Vendored S7 client (rust7, MIT) plus the Lymon discover extension.
//!
//! - [`client`] — the vendored rust7 `S7Client` (ISO-on-TCP read/write), with two
//!   `// LYMON DELTA:` fixes and a `pub(crate)` `exchange` accessor.
//! - [`decode`] — S7 scalar types and big-endian byte → f64 decoders.
//! - [`blocks`] — the `list blocks of type` telegram that powers discover.

pub mod client;
pub mod decode;
