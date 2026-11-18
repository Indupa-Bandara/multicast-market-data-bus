use proptest::prelude::*;
use recovery::*;
use wire::{
    encode_data, encode_heartbeat, encode_nak, Action as Op, Flags, L2Update, Side, MAX_PACKET_LEN,
};

const MS: u64 = 1_000_000;
const SESSION: u64 = 42;

fn msg(seq: u64) -> L2Update {
    L2Update {
        instrument_id: seq as u32,
        side: Side::Bid,
        action: Op::Set,
        price: seq as i64,
        qty: seq,
    }
}

fn data(session: u64, first: u64, n: u64, retx: bool) -> Vec<u8> {
    let msgs: Vec<_> = (first..first + n).map(msg).collect();
    let mut buf = vec![0u8; MAX_PACKET_LEN];
    let flags = if retx { Flags::RETRANSMIT } else { Flags::NONE };
    let len = encode_data(&mut buf, session, first, flags, 0, &msgs).unwrap();
    buf.truncate(len);
    buf
}

fn heartbeat(session: u64, next_seq: u64) -> Vec<u8> {
    let mut buf = vec![0u8; 64];
    let len = encode_heartbeat(&mut buf, session, next_seq, Flags::NONE, 0).unwrap();
    buf.truncate(len);
    buf
}

/// Wraps a Receiver and records everything it asks for.
struct H {
    rx: Receiver,
    out: Vec<Action>,
    delivered: Vec<u64>,
    naks: Vec<(u64, u16)>,
    resyncs: Vec<ResyncReason>,
    stale: u32,
}

impl H {
    fn new(cfg: Config) -> Self {
        H {
            rx: Receiver::new(cfg),
            out: Vec::new(),
            delivered: Vec::new(),
            naks: Vec::new(),
            resyncs: Vec::new(),
            stale: 0,
        }
    }
    fn absorb(&mut self) {
        for a in self.out.drain(..) {
            match a {
                Action::Deliver { seq, msg: m } => {
                    assert_eq!(m, msg(seq), "payload must match its sequence number");
                    self.delivered.push(seq)
                }
                Action::SendNak {
                    from_seq,
                    count,
                    session_id,
                } => {
                    assert_eq!(session_id, SESSION);
                    self.naks.push((from_seq, count))
                }
                Action::Resync(r) => self.resyncs.push(r),
                Action::Stale => self.stale += 1,
            }
        }
    }
    fn feed(&mut self, now: u64, bytes: &[u8]) {
        self.rx.on_datagram(now, bytes, &mut self.out);
        self.absorb();
    }
    fn tick(&mut self, now: u64) {
        self.rx.on_tick(now, &mut self.out);
        self.absorb();
    }
}

#[test]
fn in_order_delivery() {
    let mut h = H::new(Config::default());
    h.feed(0, &data(SESSION, 1, 3, false));
    h.feed(10, &data(SESSION, 4, 2, false));
    assert_eq!(h.delivered, vec![1, 2, 3, 4, 5]);
    assert_eq!(h.rx.state(), State::Live);
    assert_eq!(h.rx.expected_seq(), Some(6));
    assert!(h.naks.is_empty());
}

#[test]
fn gap_holds_delivery_then_naks_after_tolerance_then_repair_fills() {
    let mut h = H::new(Config::default());
    h.feed(0, &data(SESSION, 1, 3, false));
    h.feed(100, &data(SESSION, 7, 3, false)); // 4..=6 missing
    assert_eq!(
        h.delivered,
        vec![1, 2, 3],
        "must not deliver beyond the gap"
    );
    assert_eq!(h.rx.state(), State::GapRecovery);

    h.tick(MS - 1);
    assert!(h.naks.is_empty(), "no NAK inside the reorder tolerance");
    h.tick(100 + MS);
    assert_eq!(h.naks, vec![(4, 3)]);

    h.feed(100 + MS + 10, &data(SESSION, 4, 3, true));
    assert_eq!(h.delivered, (1..=9).collect::<Vec<_>>());
    assert_eq!(h.rx.state(), State::Live);
    assert_eq!(h.rx.stats().repaired, 3);
    h.tick(10 * MS * 1000);
    assert_eq!(h.naks.len(), 1, "no further NAKs once repaired");
}

#[test]
fn reordering_within_tolerance_causes_no_nak() {
    let mut h = H::new(Config::default());
    h.feed(0, &data(SESSION, 4, 3, false)); // arrives first; join expects seq 1
    h.feed(50_000, &data(SESSION, 1, 3, false));
    assert_eq!(h.delivered, (1..=6).collect::<Vec<_>>());
    h.tick(5 * MS);
    assert!(h.naks.is_empty());
    assert_eq!(h.rx.state(), State::Live);
}

#[test]
fn duplicates_are_dropped() {
    let mut h = H::new(Config::default());
    h.feed(0, &data(SESSION, 1, 3, false));
    h.feed(1, &data(SESSION, 1, 3, false));
    h.feed(2, &data(SESSION, 2, 3, false)); // overlaps 2,3, new 4
    assert_eq!(h.delivered, vec![1, 2, 3, 4]);
    assert_eq!(h.rx.stats().duplicates, 5);
    // duplicate of a buffered out-of-order message
    h.feed(3, &data(SESSION, 8, 1, false));
    h.feed(4, &data(SESSION, 8, 1, false));
    assert_eq!(h.rx.stats().duplicates, 6);
}

#[test]
fn heartbeat_reveals_tail_loss() {
    let mut h = H::new(Config::default());
    h.feed(0, &data(SESSION, 1, 3, false));
    h.feed(500, &heartbeat(SESSION, 6)); // sender is at 6, we have up to 3
    assert_eq!(h.rx.state(), State::GapRecovery);
    h.tick(500 + MS);
    assert_eq!(h.naks, vec![(4, 2)]);
}

#[test]
fn nak_backoff_doubles_and_retries_exhaust_into_resync() {
    let cfg = Config {
        max_nak_retries: 3,
        nak_initial_backoff: 5 * MS,
        nak_max_backoff: 15 * MS,
        ..Config::default()
    };
    let mut h = H::new(cfg);
    h.feed(0, &data(SESSION, 1, 1, false));
    h.feed(0, &data(SESSION, 5, 1, false)); // 2..=4 missing
    let mut now = 0;
    let mut times = vec![];
    while h.resyncs.is_empty() && now < 10_000 * MS {
        let before = h.naks.len();
        now = h.rx.next_deadline().unwrap();
        h.tick(now);
        if h.naks.len() > before {
            times.push(now);
        }
    }
    assert_eq!(h.naks, vec![(2, 3), (2, 3), (2, 3)]);
    // first at tolerance (1ms), then +5ms*2=10ms, then +min(20,15)=15ms
    assert_eq!(times, vec![MS, MS + 10 * MS, MS + 10 * MS + 15 * MS]);
    assert_eq!(h.resyncs, vec![ResyncReason::RetriesExhausted]);
    assert_eq!(h.rx.state(), State::Init);

    // After a resync the receiver rejoins at the first sequence it sees.
    h.feed(now + 1, &data(SESSION, 100, 2, false));
    assert_eq!(h.delivered, vec![1, 100, 101]);
    assert_eq!(h.rx.state(), State::Live);
}

#[test]
fn partial_repair_counts_as_progress_and_resets_retries() {
    let cfg = Config {
        max_nak_retries: 2,
        ..Config::default()
    };
    let mut h = H::new(cfg);
    h.feed(0, &data(SESSION, 1, 1, false));
    h.feed(0, &data(SESSION, 10, 1, false)); // 2..=9 missing
    let mut now = h.rx.next_deadline().unwrap();
    h.tick(now); // round 1
                 // repair only 2..=3 (progress), then wait: retries must have reset
    h.feed(now + 1, &data(SESSION, 2, 2, true));
    for _ in 0..2 {
        now = h.rx.next_deadline().unwrap();
        h.tick(now);
    }
    assert!(h.resyncs.is_empty(), "progress must reset the retry budget");
    assert_eq!(*h.naks.last().unwrap(), (4, 6));
}

#[test]
fn session_change_forces_resync_and_restarts_numbering() {
    let mut h = H::new(Config::default());
    h.feed(0, &data(SESSION, 1, 3, false));
    h.feed(1, &data(SESSION, 6, 1, false)); // buffered behind a gap
    h.feed(2, &data(SESSION + 1, 1, 2, false)); // sender restarted
    assert_eq!(h.resyncs, vec![ResyncReason::SessionChange]);
    assert_eq!(h.delivered, vec![1, 2, 3, 1, 2]);
    assert_eq!(h.rx.state(), State::Live);
}

#[test]
fn stale_alarm_fires_once_and_clears_on_traffic() {
    let mut h = H::new(Config::default());
    h.feed(0, &data(SESSION, 1, 1, false));
    h.tick(2_999 * MS);
    assert_eq!(h.stale, 0);
    h.tick(3_000 * MS);
    h.tick(4_000 * MS);
    assert_eq!(h.stale, 1, "alarm is edge-triggered");
    assert_eq!(h.rx.state(), State::Stale);
    h.feed(5_000 * MS, &heartbeat(SESSION, 2));
    assert_eq!(h.rx.state(), State::Live);
}

#[test]
fn buffer_overflow_and_gap_too_large_resync() {
    let cfg = Config {
        max_buffered_messages: 3,
        ..Config::default()
    };
    let mut h = H::new(cfg);
    h.feed(0, &data(SESSION, 1, 1, false));
    h.feed(1, &data(SESSION, 10, 5, false)); // 5 buffered > 3
    assert_eq!(h.resyncs, vec![ResyncReason::BufferOverflow]);

    let cfg = Config {
        max_gap: 10,
        ..Config::default()
    };
    let mut h = H::new(cfg);
    h.feed(0, &data(SESSION, 1, 1, false));
    h.feed(1, &heartbeat(SESSION, 1000));
    assert_eq!(h.resyncs, vec![ResyncReason::GapTooLarge]);
}

#[test]
fn large_gap_naks_are_chunked_and_rate_limited() {
    let cfg = Config {
        max_nak_count: 100,
        max_naks_per_round: 2,
        ..Config::default()
    };
    let mut h = H::new(cfg);
    h.feed(0, &data(SESSION, 1, 1, false));
    h.feed(0, &heartbeat(SESSION, 1000));
    h.tick(MS);
    assert_eq!(h.naks, vec![(2, 100), (102, 100)]);
}

#[test]
fn join_first_seen_starts_at_first_packet() {
    let cfg = Config {
        join: JoinPolicy::FirstSeen,
        ..Config::default()
    };
    let mut h = H::new(cfg);
    h.feed(0, &data(SESSION, 500, 2, false));
    assert_eq!(h.delivered, vec![500, 501]);
    assert!(h.naks.is_empty());
}

#[test]
fn join_from_seq_one_naks_for_history() {
    let mut h = H::new(Config::default());
    h.feed(0, &data(SESSION, 5, 2, false));
    assert!(h.delivered.is_empty());
    h.tick(MS);
    assert_eq!(h.naks, vec![(1, 4)]);
}

#[test]
fn malformed_and_stray_packets_are_counted_not_fatal() {
    let mut h = H::new(Config::default());
    h.feed(0, &[1, 2, 3]);
    h.feed(0, &[]);
    assert_eq!(h.rx.stats().decode_errors, 2);
    let mut buf = [0u8; 64];
    let n = encode_nak(&mut buf, SESSION, 1, 1).unwrap();
    h.feed(0, &buf[..n]);
    assert_eq!(h.rx.stats().naks_ignored, 1);
    assert_eq!(h.rx.state(), State::Init);
}

// ---------------------------------------------------------------------------
// Deterministic simulation: lossy, reordering, duplicating network + a sender
// that answers NAKs. Safety: delivered is exactly 1..=N (a prefix at every
// point, no dups, no gaps, in order). Liveness: it all arrives.
// ---------------------------------------------------------------------------

fn run_sim(
    sizes: &[u64],
    drops: &[bool],
    keys: &[u32],
    dups: &[u8],
) -> (Vec<u64>, Vec<ResyncReason>) {
    // Build the original packet stream.
    let mut pkts: Vec<(u64, u64)> = vec![];
    let mut next = 1;
    for &s in sizes {
        pkts.push((next, s));
        next += s;
    }
    let total = next - 1;

    // Network: drop, shuffle, duplicate.
    let mut order: Vec<usize> = (0..pkts.len())
        .filter(|&i| !drops[i % drops.len()])
        .collect();
    order.sort_by_key(|&i| keys[i % keys.len()]);

    let mut h = H::new(Config::default());
    let mut now = 0u64;
    for i in order {
        let (first, n) = pkts[i];
        for _ in 0..=dups[i % dups.len()] {
            h.feed(now, &data(SESSION, first, n, false));
            now += 1_000;
        }
    }
    // Sender heartbeat announces the tail (assumed to get through).
    h.feed(now, &heartbeat(SESSION, total + 1));

    // Drive timers; the sender answers every NAK immediately and reliably.
    for _ in 0..100_000 {
        if h.delivered.len() as u64 == total {
            break;
        }
        let Some(deadline) = h.rx.next_deadline() else {
            break;
        };
        now = now.max(deadline);
        h.tick(now);
        let naks = std::mem::take(&mut h.naks);
        for (from, count) in naks {
            let mut s = from;
            let end = (from + u64::from(count)).min(total + 1);
            while s < end {
                let n = (end - s).min(50);
                h.feed(now, &data(SESSION, s, n, true));
                s += n;
            }
        }
    }
    (h.delivered, h.resyncs)
}

proptest! {
    #[test]
    fn lossy_reordering_network_still_delivers_everything_in_order(
        sizes in prop::collection::vec(1u64..=10, 1..30),
        drops in prop::collection::vec(any::<bool>(), 30),
        keys in prop::collection::vec(any::<u32>(), 30),
        dups in prop::collection::vec(0u8..3, 30),
    ) {
        let total: u64 = sizes.iter().sum();
        let (delivered, resyncs) = run_sim(&sizes, &drops, &keys, &dups);
        prop_assert!(resyncs.is_empty());
        prop_assert_eq!(delivered, (1..=total).collect::<Vec<_>>());
    }

    /// Whatever bytes arrive, nothing panics and delivery stays strictly increasing.
    #[test]
    fn garbage_never_breaks_ordering(
        chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..80), 0..40),
    ) {
        let mut rx = Receiver::new(Config::default());
        let mut out = Vec::new();
        let mut last = 0u64;
        for (i, c) in chunks.iter().enumerate() {
            rx.on_datagram(i as u64 * 1000, c, &mut out);
            rx.on_tick(i as u64 * 1000, &mut out);
            for a in out.drain(..) {
                match a {
                    Action::Deliver { seq, .. } => { prop_assert!(seq > last || last == 0); last = seq; }
                    Action::Resync(_) => last = 0,
                    _ => {}
                }
            }
        }
    }
}
