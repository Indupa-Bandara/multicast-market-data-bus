use retx_log::*;
use wire::{Action, L2Update, Packet, PacketType, Side};

const SESSION: u64 = 9;

fn msg(seq: u64) -> L2Update {
    L2Update {
        instrument_id: seq as u32,
        side: Side::Ask,
        action: Action::Set,
        price: seq as i64,
        qty: seq,
    }
}

fn filled(cap: usize, n: u64) -> RetxLog {
    let mut log = RetxLog::new(cap);
    for s in 1..=n {
        assert_eq!(log.append(msg(s)), s);
    }
    log
}

/// Run a NAK and return (first_seq, seqs, retransmit_flag) per emitted packet.
fn nak(
    srv: &mut RetransmitServer,
    now: u64,
    session: u64,
    from: u64,
    count: u64,
    log: &RetxLog,
) -> Vec<(u64, Vec<u64>, bool)> {
    let mut out = vec![];
    srv.on_nak(now, session, from, count, log, |b| {
        let p = Packet::parse(b).expect("server emits valid packets");
        assert_eq!(p.header().ptype, PacketType::Data);
        assert_eq!(p.header().session_id, SESSION);
        let ms: Vec<L2Update> = p.messages().collect();
        for (i, m) in ms.iter().enumerate() {
            assert_eq!(
                *m,
                msg(p.header().seq + i as u64),
                "repair content must match the original"
            );
        }
        out.push((
            p.header().seq,
            (p.header().seq..p.next_seq()).collect(),
            p.is_retransmit(),
        ));
    });
    out
}

#[test]
fn log_window_slides() {
    let log = filled(4, 3);
    assert_eq!((log.oldest_seq(), log.next_seq(), log.len()), (1, 4, 3));
    assert_eq!(log.get(1), Some(msg(1)));
    assert_eq!(log.get(4), None);

    let log = filled(4, 10);
    assert_eq!((log.oldest_seq(), log.next_seq(), log.len()), (7, 11, 4));
    assert_eq!(log.get(6), None);
    for s in 7..=10 {
        assert_eq!(log.get(s), Some(msg(s)));
    }
    assert!(!log.is_empty());
    assert!(RetxLog::new(0).is_empty());
}

#[test]
fn serves_requested_range_in_flagged_batches() {
    let log = filled(1000, 200);
    let cfg = RetransmitConfig {
        max_batch: 50,
        ..Default::default()
    };
    let mut srv = RetransmitServer::new(cfg, SESSION);
    let pkts = nak(&mut srv, 0, SESSION, 11, 120, &log);
    assert_eq!(
        pkts.iter().map(|p| p.0).collect::<Vec<_>>(),
        vec![11, 61, 111]
    );
    assert!(
        pkts.iter().all(|p| p.2),
        "repairs carry the RETRANSMIT flag"
    );
    let all: Vec<u64> = pkts.into_iter().flat_map(|p| p.1).collect();
    assert_eq!(all, (11..131).collect::<Vec<_>>());
    assert_eq!(srv.stats().served_msgs, 120);
}

#[test]
fn range_is_clamped_to_what_was_sent() {
    let log = filled(1000, 20);
    let mut srv = RetransmitServer::new(RetransmitConfig::default(), SESSION);
    let all: Vec<u64> = nak(&mut srv, 0, SESSION, 15, 100, &log)
        .into_iter()
        .flat_map(|p| p.1)
        .collect();
    assert_eq!(all, (15..21).collect::<Vec<_>>());
}

#[test]
fn out_of_window_future_and_wrong_session_naks_get_no_reply() {
    let log = filled(10, 100); // holds 91..=100
    let mut srv = RetransmitServer::new(RetransmitConfig::default(), SESSION);
    assert!(nak(&mut srv, 0, SESSION, 5, 10, &log).is_empty());
    assert!(nak(&mut srv, 0, SESSION, 101, 10, &log).is_empty());
    assert!(nak(&mut srv, 0, SESSION + 1, 95, 3, &log).is_empty());
    let s = srv.stats();
    assert_eq!(
        (s.unavailable, s.future, s.wrong_session, s.naks),
        (1, 1, 1, 3)
    );
    // A request starting inside the window is served.
    assert_eq!(nak(&mut srv, 0, SESSION, 95, 3, &log).len(), 1);
}

#[test]
fn token_bucket_limits_repair_bandwidth_and_refills() {
    let log = filled(10_000, 5_000);
    let cfg = RetransmitConfig {
        rate_msgs_per_sec: 1_000,
        burst_msgs: 100,
        max_batch: 50,
    };
    let mut srv = RetransmitServer::new(cfg, SESSION);

    // Burst: only 100 of 1000 requested messages are served, lowest sequence numbers first.
    let all: Vec<u64> = nak(&mut srv, 0, SESSION, 1, 1000, &log)
        .into_iter()
        .flat_map(|p| p.1)
        .collect();
    assert_eq!(all, (1..=100).collect::<Vec<_>>());
    assert_eq!(srv.stats().throttled_msgs, 900);

    // Immediately again: bucket is empty.
    assert!(nak(&mut srv, 0, SESSION, 101, 50, &log).is_empty());

    // 50 ms later at 1000 msg/s: 50 tokens.
    let all: Vec<u64> = nak(&mut srv, 50_000_000, SESSION, 101, 500, &log)
        .into_iter()
        .flat_map(|p| p.1)
        .collect();
    assert_eq!(all, (101..=150).collect::<Vec<_>>());

    // A long pause refills only up to the burst size, not beyond.
    let all: Vec<u64> = nak(&mut srv, 100_000_000_000, SESSION, 151, 1000, &log)
        .into_iter()
        .flat_map(|p| p.1)
        .collect();
    assert_eq!(all.len(), 100);
}
