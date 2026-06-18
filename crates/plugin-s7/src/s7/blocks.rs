//! Discover extension: enumerate the CPU's data blocks via the S7 "List Blocks of
//! Type" userdata service (block type = DB).
//!
//! The request telegram and the response offsets below are ported from snap7's
//! `TSnap7MicroClient::opListBlocksOfType` (src/core/s7_micro_client.cpp) and its
//! structs in s7_types.h (constants: grBlocksInfo=0x43, SFun_ListBoT=0x02,
//! TS_ResOctet=0x09, Block_DB=0x41; TS7ResHeader17=10 bytes; params=12 bytes).
//!
//! Coarse by design: S7comm exposes DB *numbers* (and, via a separate per-block
//! GetBlockInfo request we don't issue here, sizes) — not field layouts. A single
//! request covers up to ~(PDU-29)/4 blocks (~112 for a 480-byte PDU); the rare
//! multi-slice case (more DBs than fit one slice) is left as a future enhancement.

use super::client::{S7Client, S7Error};

/// A data block discovered on the CPU.
pub struct BlockRef {
    pub number: u16,
    /// MC7 size in bytes, if known. We don't issue per-block GetBlockInfo yet → None.
    pub size: Option<u32>,
}

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

// Response offsets into the S7 PDU payload returned by `S7Client::exchange`:
//   [0..10)  TS7ResHeader17 (userdata response header)
//   [10..22) TResFunGetBlockInfo params; ErrNo (BE u16) at [20..22)
//   [22..]   TDataFunGetBot: RetVal[22], TSize[23], DataLen(BE u16)[24..26), items[26..]
//   each item = 4 bytes: BlockNum(BE u16), unknown, block-language.
const ERRNO_OFF: usize = 20;
const RETVAL_OFF: usize = 22;
const DATALEN_OFF: usize = 24;
const ITEMS_START: usize = 26;

/// Enumerate the data blocks present on the CPU.
pub fn list_data_blocks(client: &mut S7Client) -> Result<Vec<BlockRef>, S7Error> {
    let payload = client.exchange(LIST_DB_TELEGRAM)?;
    parse_block_list(&payload)
}

fn parse_block_list(payload: &[u8]) -> Result<Vec<BlockRef>, S7Error> {
    if payload.len() < ITEMS_START {
        return Err(S7Error::Other(format!(
            "list-blocks response too short ({} bytes)",
            payload.len()
        )));
    }
    let err_no = u16::from_be_bytes([payload[ERRNO_OFF], payload[ERRNO_OFF + 1]]);
    if err_no != 0 {
        return Err(S7Error::Other(format!(
            "list-blocks: CPU error 0x{err_no:04x}"
        )));
    }
    if payload[RETVAL_OFF] != 0xFF {
        return Err(S7Error::Other(format!(
            "list-blocks: item not available (retval 0x{:02x})",
            payload[RETVAL_OFF]
        )));
    }
    let data_len = u16::from_be_bytes([payload[DATALEN_OFF], payload[DATALEN_OFF + 1]]) as usize;
    let avail = payload.len() - ITEMS_START;
    let count = data_len.min(avail) / 4;
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let o = ITEMS_START + i * 4;
        out.push(BlockRef {
            number: u16::from_be_bytes([payload[o], payload[o + 1]]),
            size: None,
        });
    }
    Ok(out)
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
        let blocks = parse_block_list(&p).unwrap();
        assert_eq!(
            blocks.iter().map(|b| b.number).collect::<Vec<_>>(),
            vec![1, 17]
        );
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
}
