use proptest::prelude::*;
use wire::*;

fn upd(i: u32) -> L2Update {
    L2Update {
        instrument_id: i,
        side: if i.is_multiple_of(2) {
            Side::Bid
        } else {
            Side::Ask
        },
        action: Action::Set,
        price: 10_000 + i as i64,
        qty: 100 + i as u64,
    }
}

/// Pins the wire format. If this test changes, you changed the protocol: bump VERSION.
#[test]
fn golden_data_packet() {
    let mut buf = [0u8; MAX_PACKET_LEN];
    let msg = L2Update {
        instrument_id: 7,
        side: Side::Ask,
        action: Action::Set,
        price: -2,
        qty: 5,
    };
    let n = encode_data(
        &mut buf,
        0x1122334455667788,
        42,
        Flags::RETRANSMIT,
        99,
        &[msg],
    )
    .unwrap();
    assert_eq!(n, 32 + 24);
    let expected: Vec<u8> = vec![
        0x4D, 0x42, // magic "MB" (0x424D LE)
        0x01, // version
        0x01, // type DATA
        0x01, 0x00, // flags RETRANSMIT
        0x01, 0x00, // count 1
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, // session
        42, 0, 0, 0, 0, 0, 0, 0, // seq
        99, 0, 0, 0, 0, 0, 0, 0, // ts
        7, 0, 0, 0, // instrument
        1, // side ask
        0, // action set
        0, 0, // reserved
        0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // price -2
        5, 0, 0, 0, 0, 0, 0, 0, // qty
    ];
    assert_eq!(&buf[..n], &expected[..]);
}

#[test]
fn data_roundtrip_and_zero_copy_view() {
    let msgs: Vec<_> = (0..MAX_MESSAGES_PER_PACKET as u32).map(upd).collect();
    let mut buf = [0u8; MAX_PACKET_LEN];
    let n = encode_data(&mut buf, 1, 1000, Flags::NONE, 5, &msgs).unwrap();
    assert!(n <= MAX_PACKET_LEN);
    let p = Packet::parse(&buf[..n]).unwrap();
    assert_eq!(p.header().seq, 1000);
    assert_eq!(p.next_seq(), 1000 + 60);
    assert!(!p.is_retransmit());
    assert_eq!(p.messages().len(), 60);
    assert!(p.messages().eq(msgs.iter().copied()));
}

#[test]
fn heartbeat_and_nak_roundtrip() {
    let mut buf = [0u8; 64];
    let n = encode_heartbeat(&mut buf, 9, 500, Flags::NONE, 77).unwrap();
    let p = Packet::parse(&buf[..n]).unwrap();
    assert_eq!(p.header().ptype, PacketType::Heartbeat);
    assert_eq!(p.next_seq(), 500);
    assert_eq!(p.messages().count(), 0);

    let n = encode_nak(&mut buf, 9, 100, 5).unwrap();
    let p = Packet::parse(&buf[..n]).unwrap();
    assert_eq!(p.header().ptype, PacketType::Nak);
    assert_eq!((p.header().seq, p.header().count), (100, 5));
    assert_eq!(p.next_seq(), 105);
}

#[test]
fn every_truncation_is_rejected_without_panic() {
    let mut buf = [0u8; MAX_PACKET_LEN];
    let n = encode_data(&mut buf, 1, 1, Flags::NONE, 0, &[upd(1), upd(2), upd(3)]).unwrap();
    for len in 0..n {
        assert!(Packet::parse(&buf[..len]).is_err(), "len {len} accepted");
    }
    assert!(Packet::parse(&buf[..n]).is_ok());
}

#[test]
fn trailing_garbage_is_rejected() {
    let mut buf = [0u8; MAX_PACKET_LEN];
    let n = encode_data(&mut buf, 1, 1, Flags::NONE, 0, &[upd(1)]).unwrap();
    assert_eq!(
        Packet::parse(&buf[..n + 1]).unwrap_err(),
        DecodeError::LengthMismatch
    );
}

#[test]
fn rejects_bad_header_fields() {
    let mut buf = [0u8; 64];
    let n = encode_heartbeat(&mut buf, 1, 1, Flags::NONE, 0).unwrap();
    let good = buf;

    let mut b = good;
    b[0] ^= 1;
    assert_eq!(Packet::parse(&b[..n]).unwrap_err(), DecodeError::BadMagic);
    let mut b = good;
    b[2] = 2;
    assert_eq!(Packet::parse(&b[..n]).unwrap_err(), DecodeError::BadVersion);
    let mut b = good;
    b[3] = 9;
    assert_eq!(Packet::parse(&b[..n]).unwrap_err(), DecodeError::BadType);
    let mut b = good;
    b[5] = 0x80; // undefined flag bit
    assert_eq!(Packet::parse(&b[..n]).unwrap_err(), DecodeError::BadFlags);
    let mut b = good;
    b[6] = 1; // heartbeat with count 1
    assert_eq!(Packet::parse(&b[..n]).unwrap_err(), DecodeError::BadCount);
}

#[test]
fn rejects_seq_overflow() {
    let mut buf = [0u8; MAX_PACKET_LEN];
    assert_eq!(
        encode_data(&mut buf, 1, u64::MAX, Flags::NONE, 0, &[upd(1), upd(2)]),
        Err(EncodeError::SeqOverflow)
    );
    // Hand-craft a NAK that overflows.
    let n = encode_nak(&mut buf, 1, 10, 1).unwrap();
    buf[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
    assert_eq!(
        Packet::parse(&buf[..n]).unwrap_err(),
        DecodeError::SeqOverflow
    );
}

#[test]
fn rejects_bad_message_fields() {
    let mut buf = [0u8; MAX_PACKET_LEN];
    let n = encode_data(&mut buf, 1, 1, Flags::NONE, 0, &[upd(1)]).unwrap();
    let good = buf;
    let mut b = good;
    b[HEADER_LEN + 4] = 2; // bad side
    assert_eq!(Packet::parse(&b[..n]).unwrap_err(), DecodeError::BadMessage);
    let mut b = good;
    b[HEADER_LEN + 5] = 2; // bad action
    assert_eq!(Packet::parse(&b[..n]).unwrap_err(), DecodeError::BadMessage);
    let mut b = good;
    b[HEADER_LEN + 6] = 1; // reserved
    assert_eq!(
        Packet::parse(&b[..n]).unwrap_err(),
        DecodeError::BadReserved
    );
    let mut b = good;
    b[HEADER_LEN + 5] = 1; // delete with non-zero qty
    assert_eq!(Packet::parse(&b[..n]).unwrap_err(), DecodeError::BadMessage);
}

#[test]
fn encoder_limits() {
    let mut buf = [0u8; MAX_PACKET_LEN];
    let too_many: Vec<_> = (0..61).map(upd).collect();
    assert_eq!(
        encode_data(&mut buf, 1, 1, Flags::NONE, 0, &too_many),
        Err(EncodeError::BadCount)
    );
    assert_eq!(
        encode_data(&mut buf, 1, 1, Flags::NONE, 0, &[]),
        Err(EncodeError::BadCount)
    );
    assert_eq!(
        encode_data(&mut buf[..40], 1, 1, Flags::NONE, 0, &[upd(1)]),
        Err(EncodeError::BufferTooSmall)
    );
    assert_eq!(encode_nak(&mut buf, 1, 1, 0), Err(EncodeError::BadCount));
    assert_eq!(
        encode_data(&mut buf, 1, 1, Flags(0x8000), 0, &[upd(1)]),
        Err(EncodeError::BadFlags)
    );
}

fn arb_msg() -> impl Strategy<Value = L2Update> {
    (
        any::<u32>(),
        any::<bool>(),
        any::<bool>(),
        any::<i64>(),
        any::<u64>(),
    )
        .prop_map(|(instrument_id, bid, del, price, qty)| L2Update {
            instrument_id,
            side: if bid { Side::Bid } else { Side::Ask },
            action: if del { Action::Delete } else { Action::Set },
            price,
            qty: if del { 0 } else { qty },
        })
}

proptest! {
    #[test]
    fn data_roundtrip(
        msgs in prop::collection::vec(arb_msg(), 1..=MAX_MESSAGES_PER_PACKET),
        session in any::<u64>(),
        seq in 0u64..(u64::MAX - 100),
        ts in any::<u64>(),
        retx in any::<bool>(),
    ) {
        let flags = if retx { Flags::RETRANSMIT } else { Flags::NONE };
        let mut buf = [0u8; MAX_PACKET_LEN];
        let n = encode_data(&mut buf, session, seq, flags, ts, &msgs).unwrap();
        let p = Packet::parse(&buf[..n]).unwrap();
        prop_assert_eq!(p.header().session_id, session);
        prop_assert_eq!(p.header().seq, seq);
        prop_assert_eq!(p.header().send_ts_ns, ts);
        prop_assert_eq!(p.is_retransmit(), retx);
        prop_assert!(p.messages().eq(msgs.iter().copied()));
    }

    /// Arbitrary bytes never panic, and anything accepted re-encodes to identical bytes.
    #[test]
    fn arbitrary_bytes_never_panic_and_are_canonical(bytes in prop::collection::vec(any::<u8>(), 0..1600)) {
        if let Ok(p) = Packet::parse(&bytes) {
            let h = *p.header();
            let mut out = [0u8; MAX_PACKET_LEN];
            let n = match h.ptype {
                PacketType::Data => {
                    let m: Vec<_> = p.messages().collect();
                    encode_data(&mut out, h.session_id, h.seq, h.flags, h.send_ts_ns, &m).unwrap()
                }
                PacketType::Heartbeat => encode_heartbeat(&mut out, h.session_id, h.seq, h.flags, h.send_ts_ns).unwrap(),
                PacketType::Nak => encode_nak(&mut out, h.session_id, h.seq, h.count).unwrap(),
            };
            prop_assert_eq!(&out[..n], &bytes[..]);
        }
    }

    /// Mutating one byte of a valid packet never panics.
    #[test]
    fn single_byte_mutation_never_panics(idx in 0usize..56, val in any::<u8>()) {
        let mut buf = [0u8; MAX_PACKET_LEN];
        let n = encode_data(&mut buf, 1, 1, Flags::NONE, 0, &[upd(3)]).unwrap();
        buf[idx % n] = val;
        let _ = Packet::parse(&buf[..n]).map(|p| p.messages().count());
    }
}
