//! Wire protocol for the reliable multicast market-data bus.
//!
//! Design rules:
//! * Fixed layout, little-endian, no text. See `docs/PROTOCOL.md`.
//! * Every packet is untrusted input. [`Packet::parse`] validates the *entire*
//!   packet up front, so everything you do with the returned view afterwards
//!   (iterating messages) is infallible and cannot panic.
//! * Zero-copy: [`Packet`] borrows the receive buffer; messages are decoded
//!   lazily from it. No allocation anywhere in this crate.
//! * Strict and canonical: reserved bits must be zero and lengths must match
//!   exactly, so `encode(parse(x)) == x` for every accepted `x` (the fuzz
//!   target checks this).
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
mod header;
mod message;
mod packet;

pub use error::{DecodeError, EncodeError};
pub use header::{Flags, Header, PacketType};
pub use message::{Action, L2Update, Side};
pub use packet::{encode_data, encode_heartbeat, encode_nak, Messages, Packet};

/// Magic number at the start of every packet ("MB" little-endian).
pub const MAGIC: u16 = 0x424D;
/// Wire protocol version.
pub const VERSION: u8 = 1;
/// Size of the fixed packet header in bytes.
pub const HEADER_LEN: usize = 32;
/// Size of one encoded [`L2Update`] in bytes.
pub const L2_UPDATE_LEN: usize = 24;
/// Max UDP payload without IP fragmentation (1500 MTU - 20 IP - 8 UDP).
pub const MAX_PACKET_LEN: usize = 1472;
/// Max messages in one DATA packet: `(1472 - 32) / 24 = 60`.
pub const MAX_MESSAGES_PER_PACKET: usize = (MAX_PACKET_LEN - HEADER_LEN) / L2_UPDATE_LEN;
