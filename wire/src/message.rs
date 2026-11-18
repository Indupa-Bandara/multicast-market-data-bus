use crate::{DecodeError, L2_UPDATE_LEN};

/// Book side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Side {
    /// Bid.
    Bid = 0,
    /// Ask.
    Ask = 1,
}

/// What to do to the price level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Action {
    /// Set the level's quantity to `qty` (insert if absent). Idempotent by design,
    /// which makes snapshots and replays easy.
    Set = 0,
    /// Remove the level. `qty` must be 0.
    Delete = 1,
}

/// Level-2 price-level update, 24 bytes on the wire.
///
/// ```text
/// off size field
///  0   4   instrument_id u32
///  4   1   side          u8   0=bid 1=ask
///  5   1   action        u8   0=set 1=delete
///  6   2   reserved      u16  must be 0
///  8   8   price         i64  fixed-point ticks
/// 16   8   qty           u64
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Update {
    /// Instrument identifier.
    pub instrument_id: u32,
    /// Book side.
    pub side: Side,
    /// Set or delete.
    pub action: Action,
    /// Price in fixed-point ticks.
    pub price: i64,
    /// Quantity at the level (0 for delete).
    pub qty: u64,
}

impl L2Update {
    /// Decode one message from exactly [`L2_UPDATE_LEN`] bytes, validating every field.
    pub fn decode(b: &[u8; L2_UPDATE_LEN]) -> Result<L2Update, DecodeError> {
        let side = match b[4] {
            0 => Side::Bid,
            1 => Side::Ask,
            _ => return Err(DecodeError::BadMessage),
        };
        let action = match b[5] {
            0 => Action::Set,
            1 => Action::Delete,
            _ => return Err(DecodeError::BadMessage),
        };
        if b[6] != 0 || b[7] != 0 {
            return Err(DecodeError::BadReserved);
        }
        let qty = u64::from_le_bytes(b[16..24].try_into().unwrap());
        if action == Action::Delete && qty != 0 {
            return Err(DecodeError::BadMessage);
        }
        Ok(L2Update {
            instrument_id: u32::from_le_bytes(b[0..4].try_into().unwrap()),
            side,
            action,
            price: i64::from_le_bytes(b[8..16].try_into().unwrap()),
            qty,
        })
    }

    /// Encode into exactly [`L2_UPDATE_LEN`] bytes.
    pub fn encode(&self, out: &mut [u8; L2_UPDATE_LEN]) {
        out[0..4].copy_from_slice(&self.instrument_id.to_le_bytes());
        out[4] = self.side as u8;
        out[5] = self.action as u8;
        out[6] = 0;
        out[7] = 0;
        out[8..16].copy_from_slice(&self.price.to_le_bytes());
        out[16..24].copy_from_slice(&self.qty.to_le_bytes());
    }
}
