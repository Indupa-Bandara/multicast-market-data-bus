use crate::{
    DecodeError, EncodeError, Flags, Header, L2Update, PacketType, HEADER_LEN, L2_UPDATE_LEN,
    MAX_MESSAGES_PER_PACKET, MAX_PACKET_LEN,
};

/// A fully validated packet borrowing the receive buffer (zero-copy).
#[derive(Debug, Clone, Copy)]
pub struct Packet<'a> {
    header: Header,
    body: &'a [u8],
}

impl<'a> Packet<'a> {
    /// Validate `buf` completely and return a view over it.
    ///
    /// After `Ok`, [`Packet::messages`] cannot fail or panic.
    pub fn parse(buf: &'a [u8]) -> Result<Packet<'a>, DecodeError> {
        if buf.len() > MAX_PACKET_LEN {
            return Err(DecodeError::TooLong);
        }
        let header = Header::decode(buf)?;
        let body = &buf[HEADER_LEN..];
        let n = header.count as usize;

        // seq + count must not overflow for packets that carry/identify a range.
        if header.ptype != PacketType::Heartbeat && header.seq.checked_add(n as u64).is_none() {
            return Err(DecodeError::SeqOverflow);
        }

        match header.ptype {
            PacketType::Data => {
                if n == 0 || n > MAX_MESSAGES_PER_PACKET {
                    return Err(DecodeError::BadCount);
                }
                if body.len() != n * L2_UPDATE_LEN {
                    return Err(DecodeError::LengthMismatch);
                }
                // Validate every message now so iteration is infallible.
                for i in 0..n {
                    let start = i * L2_UPDATE_LEN;
                    let arr: &[u8; L2_UPDATE_LEN] =
                        body[start..start + L2_UPDATE_LEN].try_into().unwrap();
                    L2Update::decode(arr)?;
                }
            }
            PacketType::Heartbeat => {
                if n != 0 {
                    return Err(DecodeError::BadCount);
                }
                if !body.is_empty() {
                    return Err(DecodeError::LengthMismatch);
                }
                if header.flags.contains(Flags::RETRANSMIT) {
                    return Err(DecodeError::BadFlags);
                }
            }
            PacketType::Nak => {
                if n == 0 {
                    return Err(DecodeError::BadCount);
                }
                if !body.is_empty() {
                    return Err(DecodeError::LengthMismatch);
                }
                // Canonical form: NAKs carry no timestamp.
                if header.send_ts_ns != 0 {
                    return Err(DecodeError::BadReserved);
                }
            }
        }
        // RETRANSMIT only makes sense on DATA.
        if header.ptype == PacketType::Nak && header.flags.contains(Flags::RETRANSMIT) {
            return Err(DecodeError::BadFlags);
        }
        Ok(Packet { header, body })
    }

    /// The decoded header.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Sequence number one past the last message in this packet (DATA/NAK range end).
    /// For a heartbeat this is the sender's next sequence number.
    pub fn next_seq(&self) -> u64 {
        // Validated in parse: cannot overflow for DATA/NAK, and count is 0 for heartbeat.
        self.header.seq + self.header.count as u64
    }

    /// True if this is a DATA packet flagged as a retransmission.
    pub fn is_retransmit(&self) -> bool {
        self.header.flags.contains(Flags::RETRANSMIT)
    }

    /// Iterate the messages (empty for HEARTBEAT and NAK). Lazily decodes from the buffer.
    pub fn messages(&self) -> Messages<'a> {
        Messages { rest: self.body }
    }
}

/// Iterator over the messages of a validated DATA packet.
#[derive(Debug, Clone)]
pub struct Messages<'a> {
    rest: &'a [u8],
}

impl Iterator for Messages<'_> {
    type Item = L2Update;

    fn next(&mut self) -> Option<L2Update> {
        if self.rest.len() < L2_UPDATE_LEN {
            return None;
        }
        let (head, tail) = self.rest.split_at(L2_UPDATE_LEN);
        self.rest = tail;
        // Validated by Packet::parse, so this cannot fail.
        L2Update::decode(head.try_into().ok()?).ok()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.rest.len() / L2_UPDATE_LEN;
        (n, Some(n))
    }
}

impl ExactSizeIterator for Messages<'_> {}

fn check_flags(flags: Flags) -> Result<(), EncodeError> {
    if flags.is_valid() {
        Ok(())
    } else {
        Err(EncodeError::BadFlags)
    }
}

/// Encode a DATA packet into `out`; returns the number of bytes written.
/// `first_seq` is the sequence number of `msgs[0]`.
pub fn encode_data(
    out: &mut [u8],
    session_id: u64,
    first_seq: u64,
    flags: Flags,
    send_ts_ns: u64,
    msgs: &[L2Update],
) -> Result<usize, EncodeError> {
    check_flags(flags)?;
    if msgs.is_empty() || msgs.len() > MAX_MESSAGES_PER_PACKET {
        return Err(EncodeError::BadCount);
    }
    first_seq
        .checked_add(msgs.len() as u64)
        .ok_or(EncodeError::SeqOverflow)?;
    let total = HEADER_LEN + msgs.len() * L2_UPDATE_LEN;
    if out.len() < total {
        return Err(EncodeError::BufferTooSmall);
    }
    Header {
        ptype: PacketType::Data,
        flags,
        count: msgs.len() as u16,
        session_id,
        seq: first_seq,
        send_ts_ns,
    }
    .write(out);
    for (i, m) in msgs.iter().enumerate() {
        let off = HEADER_LEN + i * L2_UPDATE_LEN;
        let slot: &mut [u8; L2_UPDATE_LEN] =
            (&mut out[off..off + L2_UPDATE_LEN]).try_into().unwrap();
        m.encode(slot);
    }
    Ok(total)
}

/// Encode a HEARTBEAT. `next_seq` is the next sequence number the sender will assign.
pub fn encode_heartbeat(
    out: &mut [u8],
    session_id: u64,
    next_seq: u64,
    flags: Flags,
    send_ts_ns: u64,
) -> Result<usize, EncodeError> {
    check_flags(flags)?;
    if flags.contains(Flags::RETRANSMIT) {
        return Err(EncodeError::BadFlags);
    }
    if out.len() < HEADER_LEN {
        return Err(EncodeError::BufferTooSmall);
    }
    Header {
        ptype: PacketType::Heartbeat,
        flags,
        count: 0,
        session_id,
        seq: next_seq,
        send_ts_ns,
    }
    .write(out);
    Ok(HEADER_LEN)
}

/// Encode a NAK asking for `count` messages starting at `from_seq`.
pub fn encode_nak(
    out: &mut [u8],
    session_id: u64,
    from_seq: u64,
    count: u16,
) -> Result<usize, EncodeError> {
    if count == 0 {
        return Err(EncodeError::BadCount);
    }
    from_seq
        .checked_add(count as u64)
        .ok_or(EncodeError::SeqOverflow)?;
    if out.len() < HEADER_LEN {
        return Err(EncodeError::BufferTooSmall);
    }
    Header {
        ptype: PacketType::Nak,
        flags: Flags::NONE,
        count,
        session_id,
        seq: from_seq,
        send_ts_ns: 0,
    }
    .write(out);
    Ok(HEADER_LEN)
}
