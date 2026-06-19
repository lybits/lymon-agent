//! Discover extension: enumerate the CPU's data blocks and read per-block metadata
//! via the S7 *block functions* userdata service.
//!
//! Two ops, both ported from snap7 (`src/core/s7_micro_client.cpp` + structs in
//! `s7_types.h`; function group `grBlocksInfo=0x43`):
//! - [`list_data_blocks`] — `opListBlocksOfType` (SubFun `SFun_ListBoT=0x02`,
//!   block type `Block_DB=0x41`) → the DB *numbers* present on the CPU.
//! - [`get_block_info`] — `opAgBlockInfo` (SubFun `SFun_BlkInfo=0x03`) → per-DB
//!   metadata: MC7 size, the 8-char block *name* (header field), and language.
//!
//! Coarse by design: S7comm exposes DB numbers, sizes, and the short header name —
//! **not** the field layout or variable names (those live in the offline project, or
//! over OPC-UA). The block name is reliably set on S7-300/400 and often blank on
//! S7-1200/1500 optimized blocks. The list op is single-slice (covers ~112 DBs for a
//! 480-byte PDU); the rare multi-slice case is left as a future enhancement.
//!
//! Both responses share the userdata reply framing: TS7ResHeader17 (10 bytes) +
//! TResFunGetBlockInfo params (12 bytes; ErrNo at [20..22)) + a data section at [22..).

use super::client::{S7Client, S7Error};

/// Shared response offsets (header 10 + params 12, then the data section at [22..)).
const ERRNO_OFF: usize = 20;
const RETVAL_OFF: usize = 22;

// ── List blocks of type (DB) ───────────────────────────────────────────────

/// "List Blocks of Type = DB", first slice. 31 bytes:
/// TPKT(4) + COTP(3) + S7 userdata header(10) + params(8) + data(6).
const LIST_DB_TELEGRAM: &[u8] = &[
    0x03, 0x00, 0x00, 0x1f, // TPKT, total length 31
    0x02, 0xf0, 0x80, // COTP DT
    0x32, 0x07, 0x00, 0x00, 0x05, 0x00, 0x00, 0x08, 0x00,
    0x06, // S7 userdata hdr: ParLen 8, DataLen 6
    0x00, 0x01, 0x12, 0x04, 0x11, 0x43, 0x02,
    0x00, // params: Tg=grBlocksInfo(0x43), SubFun=ListBoT(0x02)
    0xff, 0x09, 0x00, 0x02, 0x30,
    0x41, // data: RetVal FF, TSize 09, Len 0002, '0', BlkType DB(0x41)
];

// List response data section: RetVal[22], TSize[23], DataLen(BE u16)[24..26),
// then 4-byte records (BlockNum BE u16, unknown, block-language) from [26).
const DATALEN_OFF: usize = 24;
const ITEMS_START: usize = 26;

/// Enumerate the data-block numbers present on the CPU.
pub fn list_data_blocks(client: &mut S7Client) -> Result<Vec<u16>, S7Error> {
    let payload = client.exchange(LIST_DB_TELEGRAM)?;
    parse_block_list(&payload)
}

fn parse_block_list(payload: &[u8]) -> Result<Vec<u16>, S7Error> {
    if payload.len() < ITEMS_START {
        return Err(S7Error::Other(format!(
            "list-blocks response too short ({} bytes)",
            payload.len()
        )));
    }
    check_userdata_ok(payload, "list-blocks")?;
    let data_len = u16::from_be_bytes([payload[DATALEN_OFF], payload[DATALEN_OFF + 1]]) as usize;
    let avail = payload.len() - ITEMS_START;
    let count = data_len.min(avail) / 4;
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let o = ITEMS_START + i * 4;
        out.push(u16::from_be_bytes([payload[o], payload[o + 1]]));
    }
    Ok(out)
}

// ── Get AG block info (per DB) ──────────────────────────────────────────────

/// Per-block metadata from `get_block_info`. Every field is best-effort — an
/// optimized or legacy block may return a short record (or no name).
pub struct BlockInfo {
    /// MC7 (compiled body) size in bytes — the closest thing to "DB size".
    pub size: Option<u32>,
    /// The 8-char block header name (trimmed). Often blank on optimized S7-1500 DBs.
    pub name: Option<String>,
    /// Block language code (1=STL/AWL, 2=LAD, 3=FBD, 4=SCL, 7=DB, …).
    pub lang: Option<u8>,
}

// Block-info response field offsets within the S7 PDU payload (data section at 22):
//   RetVal[22], BlkLang[36], MC7Len(BE u16)[66..68), Header(8 ASCII)[84..92).
const BI_LANG_OFF: usize = 36;
const BI_MC7LEN_OFF: usize = 66;
const BI_HEADER_OFF: usize = 84;
const BI_HEADER_LEN: usize = 8;

/// "Get Block Info" for a DB. 37 bytes; the DB number is encoded as 5 ASCII digits.
#[rustfmt::skip]
fn block_info_telegram(db: u16) -> [u8; 37] {
    let n = db as u32;
    let digit = |div: u32| (((n / div) % 10) as u8) + b'0';
    [
        0x03, 0x00, 0x00, 0x25, // TPKT, total length 37
        0x02, 0xf0, 0x80,       // COTP DT
        0x32, 0x07, 0x00, 0x00, 0x05, 0x00, 0x00, 0x08, 0x00, 0x0c, // S7 hdr: ParLen 8, DataLen 12
        0x00, 0x01, 0x12, 0x04, 0x11, 0x43, 0x03, 0x00, // params: grBlocksInfo(0x43), SubFun=BlkInfo(0x03)
        0xff, 0x09, 0x00, 0x08, 0x30, 0x41, // data: RetVal FF, TSize 09, Len 0008, '0', BlkType DB
        digit(10000), digit(1000), digit(100), digit(10), digit(1), // AsciiBlk: 5-digit DB number
        0x41, // A = 'A'
    ]
}

/// Read per-block metadata for a DB (best-effort; see [`BlockInfo`]).
pub fn get_block_info(client: &mut S7Client, db: u16) -> Result<BlockInfo, S7Error> {
    let payload = client.exchange(&block_info_telegram(db))?;
    parse_block_info(&payload)
}

fn parse_block_info(payload: &[u8]) -> Result<BlockInfo, S7Error> {
    if payload.len() <= RETVAL_OFF {
        return Err(S7Error::Other(format!(
            "block-info response too short ({} bytes)",
            payload.len()
        )));
    }
    check_userdata_ok(payload, "block-info")?;
    let size = payload
        .get(BI_MC7LEN_OFF..BI_MC7LEN_OFF + 2)
        .map(|b| u16::from_be_bytes([b[0], b[1]]) as u32);
    let lang = payload.get(BI_LANG_OFF).copied();
    let name = payload
        .get(BI_HEADER_OFF..BI_HEADER_OFF + BI_HEADER_LEN)
        .and_then(parse_header_name);
    Ok(BlockInfo { size, name, lang })
}

/// Trim the 8-byte header name to printable ASCII; `None` if empty.
fn parse_header_name(header: &[u8]) -> Option<String> {
    let s: String = header
        .iter()
        .copied()
        .take_while(|&b| b != 0)
        .filter(|b| b.is_ascii_graphic() || *b == b' ')
        .map(|b| b as char)
        .collect();
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

/// Validate the shared userdata reply: params ErrNo == 0 and data RetVal == 0xFF.
fn check_userdata_ok(payload: &[u8], op: &str) -> Result<(), S7Error> {
    let err_no = u16::from_be_bytes([payload[ERRNO_OFF], payload[ERRNO_OFF + 1]]);
    if err_no != 0 {
        return Err(S7Error::Other(format!("{op}: CPU error 0x{err_no:04x}")));
    }
    if payload[RETVAL_OFF] != 0xFF {
        return Err(S7Error::Other(format!(
            "{op}: not available (retval 0x{:02x})",
            payload[RETVAL_OFF]
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_block_records() {
        // 22 zero bytes (response header + params, ErrNo=0), then the data section.
        let mut p = vec![0u8; ITEMS_START - 4];
        p.extend_from_slice(&[0xFF, 0x09, 0x00, 0x08]); // RetVal, TSize, DataLen=8 (2 items)
        p.extend_from_slice(&[0x00, 0x01, 0x00, 0x00]); // DB1
        p.extend_from_slice(&[0x00, 0x11, 0x00, 0x00]); // DB17
        assert_eq!(parse_block_list(&p).unwrap(), vec![1, 17]);
    }

    #[test]
    fn rejects_cpu_error() {
        let mut p = vec![0u8; ITEMS_START - 4];
        p[ERRNO_OFF] = 0x80; // ErrNo high byte set → CPU error
        p.extend_from_slice(&[0xFF, 0x09, 0x00, 0x00]);
        assert!(parse_block_list(&p).is_err());
    }

    #[test]
    fn rejects_short_response() {
        assert!(parse_block_list(&[0u8; 10]).is_err());
    }

    #[test]
    fn block_info_telegram_encodes_db_number() {
        let t = block_info_telegram(17);
        assert_eq!(t.len(), 37);
        assert_eq!(t[3], 0x25); // TPKT length 37
        assert_eq!(&t[21..24], &[0x11, 0x43, 0x03]); // Uk, grBlocksInfo, SFun_BlkInfo
        assert_eq!(&t[31..37], b"00017A"); // "00017" + 'A'
    }

    #[test]
    fn parses_block_info() {
        let mut p = vec![0u8; BI_HEADER_OFF + BI_HEADER_LEN]; // 92 bytes
        p[RETVAL_OFF] = 0xFF;
        p[BI_LANG_OFF] = 0x04; // SCL
        p[BI_MC7LEN_OFF] = 0x02; // MC7Len = 0x0200 = 512
        p[BI_MC7LEN_OFF + 1] = 0x00;
        p[BI_HEADER_OFF..BI_HEADER_OFF + 8].copy_from_slice(b"Motor   "); // trailing spaces
        let info = parse_block_info(&p).unwrap();
        assert_eq!(info.size, Some(512));
        assert_eq!(info.name.as_deref(), Some("Motor"));
        assert_eq!(info.lang, Some(4));
    }

    #[test]
    fn block_info_short_response_is_graceful() {
        // Valid header (RetVal ok) but truncated before the fields → all None, no error.
        let mut p = vec![0u8; RETVAL_OFF + 4];
        p[RETVAL_OFF] = 0xFF;
        let info = parse_block_info(&p).unwrap();
        assert!(info.size.is_none() && info.name.is_none());
    }
}
