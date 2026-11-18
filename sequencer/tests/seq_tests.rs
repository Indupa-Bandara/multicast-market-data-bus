use proptest::prelude::*;
use sequencer::*;
use wire::{Action, L2Update, Packet, PacketType, Side, MAX_PACKET_LEN};

const MS: u64 = 1_000_000;

fn msg(i: u64) -> L2Update {
    L2Update {
        instrument_id: i as u32,
        side: Side::Bid,
        action: Action::Set,
        price: i as i64,
        qty: i,
    }
}

#[derive(Debug, PartialEq)]
enum Out {
    Data { first: u64, n: u64 },
    Heartbeat { next: u64 },
}

fn decode(b: &[u8]) -> Out {
    let p = Packet::parse(b).expect("sequencer emits valid packets");
    match p.header().ptype {
        PacketType::Data => Out::Data {
            first: p.header().seq,
            n: p.header().count as u64,
        },
        PacketType::Heartbeat => Out::Heartbeat {
            next: p.header().seq,
        },
        PacketType::Nak => panic!("sequencer never sends NAKs"),
    }
}

fn cfg(max_batch: usize, max_wait: u64, hb: u64) -> Config {
    Config {
        session_id: 5,
        max_batch,
        max_wait,
        heartbeat_interval: hb,
        log_capacity: 1000,
    }
}

#[test]
fn full_batch_flushes_immediately_with_gapless_numbering() {
    let mut s = Sequencer::new(cfg(3, 10 * MS, 1000 * MS), 0);
    let mut out = vec![];
    for i in 1..=7 {
        s.push(0, msg(i), |b| out.push(decode(b)));
    }
    assert_eq!(
        out,
        vec![Out::Data { first: 1, n: 3 }, Out::Data { first: 4, n: 3 }]
    );
    assert_eq!(
        s.next_seq(),
        7,
        "the 7th message is still pending, not yet sent"
    );
    assert_eq!(s.stats().full_flushes, 2);
}

#[test]
fn partial_batch_flushes_after_max_wait() {
    let mut s = Sequencer::new(cfg(60, 2 * MS, 1000 * MS), 0);
    let mut out = vec![];
    s.push(0, msg(1), |b| out.push(decode(b)));
    s.push(MS, msg(2), |b| out.push(decode(b)));
    s.tick(2 * MS - 1, |b| out.push(decode(b)));
    assert!(out.is_empty());
    assert_eq!(s.next_deadline(), Some(2 * MS));
    s.tick(2 * MS, |b| out.push(decode(b)));
    assert_eq!(out, vec![Out::Data { first: 1, n: 2 }]);
    assert_eq!(s.stats().timed_flushes, 1);
}

#[test]
fn idle_heartbeats_advertise_only_sent_sequence_numbers() {
    let mut s = Sequencer::new(cfg(60, 50 * MS, 10 * MS), 0);
    let mut out = vec![];
    s.tick(10 * MS - 1, |b| out.push(decode(b)));
    assert!(out.is_empty());
    s.tick(10 * MS, |b| out.push(decode(b)));
    assert_eq!(out, vec![Out::Heartbeat { next: 1 }]);

    // Two messages are pending (not yet sent). A heartbeat must not claim them.
    out.clear();
    s.push(11 * MS, msg(1), |_| panic!());
    s.push(11 * MS, msg(2), |_| panic!());
    s.tick(20 * MS, |b| out.push(decode(b)));
    assert_eq!(out, vec![Out::Heartbeat { next: 1 }]);

    // Once flushed, the next heartbeat covers them. Sending data counts as activity.
    out.clear();
    s.flush(21 * MS, |b| out.push(decode(b)));
    s.tick(30 * MS, |b| out.push(decode(b)));
    assert_eq!(out, vec![Out::Data { first: 1, n: 2 }]);
    s.tick(31 * MS, |b| out.push(decode(b)));
    assert_eq!(out[1], Out::Heartbeat { next: 3 });
}

#[test]
fn log_holds_exactly_what_was_sent() {
    let mut s = Sequencer::new(cfg(2, 1000 * MS, 1000 * MS), 0);
    for i in 1..=5 {
        s.push(0, msg(i), |_| {});
    }
    assert_eq!(s.log().next_seq(), 5);
    assert_eq!(s.log().get(4), Some(msg(4)));
    assert_eq!(
        s.log().get(5),
        None,
        "pending message is not in the log yet"
    );
}

#[test]
fn batch_size_is_capped_at_protocol_maximum() {
    let mut s = Sequencer::new(cfg(10_000, 1000 * MS, 1000 * MS), 0);
    let mut sizes = vec![];
    for i in 1..=130 {
        s.push(0, msg(i), |b| {
            assert!(b.len() <= MAX_PACKET_LEN);
            if let Out::Data { n, .. } = decode(b) {
                sizes.push(n)
            }
        });
    }
    assert_eq!(sizes, vec![60, 60]);
}

proptest! {
    /// Any push/tick pattern yields gapless seqs 1.. with intact content in order.
    #[test]
    fn output_is_gapless_ordered_and_complete(
        n in 1u64..300,
        max_batch in 1usize..=60,
        max_wait in 1u64..3 * MS,
        deltas in prop::collection::vec(0u64..2 * MS, 300),
    ) {
        let mut s = Sequencer::new(cfg(max_batch, max_wait, 5 * MS), 0);
        let mut now = 0u64;
        let mut got: Vec<(u64, L2Update)> = vec![];
        let mut hb_next = vec![];
        let mut collect = |b: &[u8], got: &mut Vec<(u64, L2Update)>| {
            let p = Packet::parse(b).unwrap();
            if p.header().ptype == PacketType::Data {
                for (i, m) in p.messages().enumerate() { got.push((p.header().seq + i as u64, m)); }
            } else {
                hb_next.push((p.header().seq, got.len() as u64));
            }
        };
        for i in 1..=n {
            now += deltas[(i - 1) as usize];
            let mut buf: Vec<Vec<u8>> = vec![];
            s.push(now, msg(i), |b| buf.push(b.to_vec()));
            s.tick(now, |b| buf.push(b.to_vec()));
            for b in buf { collect(&b, &mut got); }
        }
        let mut buf: Vec<Vec<u8>> = vec![];
        s.flush(now, |b| buf.push(b.to_vec()));
        for b in buf { collect(&b, &mut got); }

        prop_assert_eq!(got.len() as u64, n);
        for (i, (seq, m)) in got.iter().enumerate() {
            prop_assert_eq!(*seq, i as u64 + 1);
            prop_assert_eq!(*m, msg(i as u64 + 1));
        }
        // A heartbeat never advertises more than had been sent when it was emitted.
        for (next, sent_so_far) in hb_next {
            prop_assert_eq!(next, sent_so_far + 1);
        }
    }
}
