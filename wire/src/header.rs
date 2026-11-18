use crate::{DecodeError, HEADER_LEN, MAGIC, VERSION};

/// Packet type byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PacketType {
    /// Sequenced market-data messages. `seq` is the first message's sequence number.
    Data = 1,
    /// Keep-alive with no messages. `seq` is the *next* sequence number the sender will assign,
    /// which lets a receiver detect a tail loss without waiting for more data.
    Heartbeat = 2,
    /// Receiver -> sender repair request for `count` messages starting at `seq`.
    Nak = 3,
}

impl PacketType {
    pub(crate) fn from_u8(b: u8) -> Result<Self, DecodeError> {
        match b {
            1 => Ok(Self::Data),
            2 => Ok(Self::Heartbeat),
            3 => Ok(Self::Nak),
            _ => Err(DecodeError::BadType),
        }
    }
}

/// Header flag bits. Undefined bits must be zero on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Flags(pub u16);

impl Flags {
    /// DATA packet is a repair, not live traffic.
    pub const RETRANSMIT: Flags = Flags(1 << 0);
    /// Packet was sent on line B (reserved for the A/B arbitration stretch goal).
    pub const LINE_B: Flags = Flags(1 << 1);
    /// Sender is ending the session cleanly.
    pub const END_OF_SESSION: Flags = Flags(1 << 2);
    /// All defined bits.
    pub const ALL: Flags = Flags(0b111);
    /// No flags.
    pub const NONE: Flags = Flags(0);

    /// True if every bit in `other` is set in `self`.
    pub const fn contains(self, other: Flags) -> bool {
        self.0 & other.0 == other.0
    }
    /// Bitwise or.
    pub const fn union(self, other: Flags) -> Flags {
        Flags(self.0 | other.0)
    }
    pub(crate) const fn is_valid(self) -> bool {
        self.0 & !Self::ALL.0 == 0
    }
}

/// Decoded packet header (32 bytes on the wire).
///
/// ```text
/// off size field
///  0   2   magic        u16  0x424D
///  2   1   version      u8   1
///  3   1   type         u8   1=DATA 2=HEARTBEAT 3=NAK
///  4   2   flags        u16
///  6   2   count        u16  messages (DATA) / requested messages (NAK) / 0 (HEARTBEAT)
///  8   8   session_id   u64  changes on sender restart
/// 16   8   seq          u64  see PacketType
/// 24   8   send_ts_ns   u64  sender CLOCK_MONOTONIC_RAW ns (0 on NAK)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Packet type.
    pub ptype: PacketType,
    /// Flag bits.
    pub flags: Flags,
    /// Message count (meaning depends on `ptype`).
    pub count: u16,
    /// Session identifier.
    pub session_id: u64,
    /// Sequence number (meaning depends on `ptype`).
    pub seq: u64,
    /// Sender timestamp in nanoseconds.
    pub send_ts_ns: u64,
}

impl Header {
    /// Decode and validate the header fields (not the body length).
    pub fn decode(buf: &[u8]) -> Result<Header, DecodeError> {
        let b: &[u8; HEADER_LEN] = buf
            .get(..HEADER_LEN)
            .and_then(|s| s.try_into().ok())
            .ok_or(DecodeError::TooShort)?;
        if u16::from_le_bytes([b[0], b[1]]) != MAGIC {
            return Err(DecodeError::BadMagic);
        }
        if b[2] != VERSION {
            return Err(DecodeError::BadVersion);
        }
        let ptype = PacketType::from_u8(b[3])?;
        let flags = Flags(u16::from_le_bytes([b[4], b[5]]));
        if !flags.is_valid() {
            return Err(DecodeError::BadFlags);
        }
        Ok(Header {
            ptype,
            flags,
            count: u16::from_le_bytes([b[6], b[7]]),
            session_id: u64::from_le_bytes(b[8..16].try_into().unwrap()),
            seq: u64::from_le_bytes(b[16..24].try_into().unwrap()),
            send_ts_ns: u64::from_le_bytes(b[24..32].try_into().unwrap()),
        })
    }

    /// Write the header into the first 32 bytes of `out`. Panics if `out` is shorter
    /// than [`HEADER_LEN`] (internal use; public encoders check the length first).
    pub(crate) fn write(&self, out: &mut [u8]) {
        out[0..2].copy_from_slice(&MAGIC.to_le_bytes());
        out[2] = VERSION;
        out[3] = self.ptype as u8;
        out[4..6].copy_from_slice(&self.flags.0.to_le_bytes());
        out[6..8].copy_from_slice(&self.count.to_le_bytes());
        out[8..16].copy_from_slice(&self.session_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.seq.to_le_bytes());
        out[24..32].copy_from_slice(&self.send_ts_ns.to_le_bytes());
    }
}
