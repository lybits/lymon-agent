//! S7 data types: parse the selection `type`, know each type's byte length, and
//! decode big-endian PLC bytes into the f64 the warehouse stores.

/// A readable S7 scalar type. `Bool` is read via the bit path; the rest read
/// `byte_len()` bytes and decode big-endian (S7 / "Motorola" byte order).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DType {
    Bool,
    SInt,  // i8
    USInt, // u8  (alias: byte)
    Int,   // i16
    UInt,  // u16 (alias: word)
    DInt,  // i32
    UDInt, // u32 (alias: dword)
    Real,  // f32
    LReal, // f64
}

impl DType {
    /// Parse the selection `type` string (case-insensitive), accepting TIA/IEC aliases.
    pub fn parse(s: &str) -> Result<DType, String> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "bool" | "bit" => DType::Bool,
            "sint" => DType::SInt,
            "usint" | "byte" => DType::USInt,
            "int" => DType::Int,
            "uint" | "word" => DType::UInt,
            "dint" => DType::DInt,
            "udint" | "dword" => DType::UDInt,
            "real" | "float" => DType::Real,
            "lreal" | "double" => DType::LReal,
            other => {
                return Err(format!(
                    "unknown s7 type {other:?} (expected bool/sint/usint/int/uint/dint/udint/real/lreal, \
                     or aliases byte/word/dword/float/double)"
                ))
            }
        })
    }

    /// Number of bytes a byte-addressed read needs. `Bool` reads via the bit path → 0.
    pub fn byte_len(self) -> usize {
        match self {
            DType::Bool => 0,
            DType::SInt | DType::USInt => 1,
            DType::Int | DType::UInt => 2,
            DType::DInt | DType::UDInt | DType::Real => 4,
            DType::LReal => 8,
        }
    }

    /// Decode big-endian `buf` (must be at least `byte_len()` bytes) into f64.
    /// Not valid for `Bool` (use the bit path).
    pub fn decode(self, buf: &[u8]) -> Result<f64, String> {
        let need = self.byte_len();
        if buf.len() < need {
            return Err(format!("s7 decode: need {need} bytes, got {}", buf.len()));
        }
        let b = buf;
        Ok(match self {
            DType::Bool => return Err("s7 decode() called on Bool — use the bit path".into()),
            DType::SInt => (b[0] as i8) as f64,
            DType::USInt => b[0] as f64,
            DType::Int => i16::from_be_bytes([b[0], b[1]]) as f64,
            DType::UInt => u16::from_be_bytes([b[0], b[1]]) as f64,
            DType::DInt => i32::from_be_bytes([b[0], b[1], b[2], b[3]]) as f64,
            DType::UDInt => u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as f64,
            DType::Real => f32::from_be_bytes([b[0], b[1], b[2], b[3]]) as f64,
            DType::LReal => {
                f64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_aliases_case_insensitive() {
        assert_eq!(DType::parse("REAL").unwrap(), DType::Real);
        assert_eq!(DType::parse("word").unwrap(), DType::UInt);
        assert_eq!(DType::parse("DWord").unwrap(), DType::UDInt);
        assert_eq!(DType::parse("byte").unwrap(), DType::USInt);
        assert_eq!(DType::parse("LReal").unwrap(), DType::LReal);
        assert!(DType::parse("string").is_err());
    }

    #[test]
    fn byte_lengths() {
        assert_eq!(DType::Bool.byte_len(), 0);
        assert_eq!(DType::USInt.byte_len(), 1);
        assert_eq!(DType::Int.byte_len(), 2);
        assert_eq!(DType::Real.byte_len(), 4);
        assert_eq!(DType::LReal.byte_len(), 8);
    }

    #[test]
    fn decodes_big_endian() {
        // REAL 3.14 = 0x4048F5C3 big-endian
        assert!((DType::Real.decode(&[0x40, 0x48, 0xF5, 0xC3]).unwrap() - 3.14).abs() < 1e-5);
        // INT -1 = 0xFFFF (signed)
        assert_eq!(DType::Int.decode(&[0xFF, 0xFF]).unwrap(), -1.0);
        // UINT 0xFFFF = 65535 (unsigned)
        assert_eq!(DType::UInt.decode(&[0xFF, 0xFF]).unwrap(), 65535.0);
        // DINT -2 = 0xFFFFFFFE
        assert_eq!(DType::DInt.decode(&[0xFF, 0xFF, 0xFF, 0xFE]).unwrap(), -2.0);
        // UDINT 0x00000100 = 256
        assert_eq!(DType::UDInt.decode(&[0x00, 0x00, 0x01, 0x00]).unwrap(), 256.0);
        // SINT -1 = 0xFF
        assert_eq!(DType::SInt.decode(&[0xFF]).unwrap(), -1.0);
        // USInt 200
        assert_eq!(DType::USInt.decode(&[200]).unwrap(), 200.0);
        // LREAL 1.0 = 0x3FF0000000000000
        assert_eq!(
            DType::LReal.decode(&[0x3F, 0xF0, 0, 0, 0, 0, 0, 0]).unwrap(),
            1.0
        );
    }

    #[test]
    fn decode_rejects_short_buffer() {
        assert!(DType::Real.decode(&[0x40, 0x48]).is_err());
        assert!(DType::LReal.decode(&[0u8; 4]).is_err());
    }
}
