use core::fmt;

/// Why a received packet was rejected. Cheap to construct, `Copy`, and
/// suitable for use as a counter key (one counter per variant).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DecodeError {
    /// Fewer bytes than a header.
    TooShort,
    /// More bytes than [`crate::MAX_PACKET_LEN`].
    TooLong,
    /// Magic number mismatch.
    BadMagic,
    /// Unsupported protocol version.
    BadVersion,
    /// Unknown packet type byte.
    BadType,
    /// A reserved or undefined flag bit is set.
    BadFlags,
    /// `count` is invalid for this packet type (e.g. 0 for DATA/NAK, non-0 for HEARTBEAT).
    BadCount,
    /// Buffer length does not equal header + count * message size.
    LengthMismatch,
    /// `seq + count` overflows u64.
    SeqOverflow,
    /// A message has an undefined enum value.
    BadMessage,
    /// A message has non-zero reserved bytes.
    BadReserved,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::TooShort => "packet shorter than header",
            Self::TooLong => "packet longer than max packet length",
            Self::BadMagic => "bad magic",
            Self::BadVersion => "unsupported version",
            Self::BadType => "unknown packet type",
            Self::BadFlags => "undefined flag bits set",
            Self::BadCount => "invalid message count for packet type",
            Self::LengthMismatch => "length does not match header and count",
            Self::SeqOverflow => "sequence number plus count overflows",
            Self::BadMessage => "invalid message field value",
            Self::BadReserved => "non-zero reserved bytes",
        };
        f.write_str(s)
    }
}

impl std::error::Error for DecodeError {}

/// Why an encode call failed. These are programming errors on the sender side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeError {
    /// Output buffer too small.
    BufferTooSmall,
    /// Too many (or zero) messages for one packet.
    BadCount,
    /// `seq + count` overflows u64.
    SeqOverflow,
    /// Flags contain undefined bits.
    BadFlags,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::BufferTooSmall => "output buffer too small",
            Self::BadCount => "message count must be 1..=MAX_MESSAGES_PER_PACKET",
            Self::SeqOverflow => "sequence number plus count overflows",
            Self::BadFlags => "undefined flag bits set",
        };
        f.write_str(s)
    }
}

impl std::error::Error for EncodeError {}
