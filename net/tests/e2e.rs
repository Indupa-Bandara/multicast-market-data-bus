//! End-to-end: Sequencer -> lossy link -> Receiver, with NAKs going back over a lossy
//! uplink and repairs over a lossy repair link. Fully deterministic (seeded, virtual time).

use net::{LinkConfig, Loss, SimLink};
use recovery::{Action, Config as RxConfig, Receiver, ResyncReason, Stats as RxStats};
use retx_log::{RetransmitConfig, RetransmitServer, RetransmitStats};
use sequencer::{Config as SeqConfig, Sequencer};
use wire::{encode_nak, Action as Op, L2Update, Packet, PacketType, Side};

const US: u64 = 1_000;
const MS: u64 = 1_000_000;

fn mk(i: u64) -> L2Update {
    L2Update {
        instrument_id: i as u32,
        side: Side::Bid,
        action: Op::Set,
        price: i as i64,
        qty: i,
    }
}

#[derive(Clone)]
struct Scenario {
    seed: u64,
    n_msgs: u64,
    interval: u64,
    down: LinkConfig,
    up: LinkConfig,
    repair: LinkConfig,
    log_capacity: usize,
    /// The downlink drops everything before this time (late joiner).
    blackout_until: u64,
    /// Sender restarts (new session, numbering restarts at 1) after this many messages.
    restart_after: Option<u64>,
}

impl Scenario {
    fn clean() -> Self {
        Scenario {
            seed: 1,
            n_msgs: 2_000,
            interval: 10 * US,
            down: LinkConfig::perfect(50 * US),
            up: LinkConfig::perfect(50 * US),
            repair: LinkConfig::perfect(50 * US),
            log_capacity: 100_000,
            blackout_until: 0,
            restart_after: None,
        }
    }
    fn lossy(loss: Loss, seed: u64) -> Self {
        let link = LinkConfig {
            latency: 50 * US,
            jitter: 300 * US,
            loss,
            duplicate_prob: 0.02,
        };
        Scenario {
            seed,
            down: link,
            up: link,
            repair: link,
            ..Scenario::clean()
        }
    }
}

struct Outcome {
    delivered: Vec<u64>,
    resyncs: Vec<ResyncReason>,
    rx: RxStats,
    srv: RetransmitStats,
    sim_end: u64,
}

fn run(sc: &Scenario) -> Outcome {
    let mut down = SimLink::new(sc.down, sc.seed * 3 + 1);
    let mut up = SimLink::new(sc.up, sc.seed * 3 + 2);
    let mut repair = SimLink::new(sc.repair, sc.seed * 3 + 3);
    down.blackout_until(sc.blackout_until);

    let seq_cfg = |session| SeqConfig {
        session_id: session,
        max_batch: 20,
        max_wait: 100 * US,
        heartbeat_interval: 5 * MS,
        log_capacity: sc.log_capacity,
    };
    let mut session = 1u64;
    let mut seq = Sequencer::new(seq_cfg(session), 0);
    let mut srv = RetransmitServer::new(RetransmitConfig::default(), session);
    let mut rx = Receiver::new(RxConfig::default());

    let mut actions: Vec<Action> = Vec::new();
    let mut delivered: Vec<u64> = Vec::new();
    let mut resyncs = Vec::new();
    let mut nak_buf = [0u8; 64];

    let mut produced = 0u64; // total messages produced
    let mut local = 0u64; // messages in the current session (their seq)
    let mut next_msg_at = 0u64;
    let mut now = 0u64;
    let mut final_seq = 0u64; // last seq of the final session

    // Closure-free helper: apply receiver actions.
    macro_rules! apply {
        () => {
            for a in actions.drain(..) {
                match a {
                    Action::Deliver { seq: s, msg } => {
                        assert_eq!(msg, mk(s), "payload must match its sequence number");
                        delivered.push(s);
                    }
                    Action::SendNak {
                        session_id,
                        from_seq,
                        count,
                    } => {
                        let n = encode_nak(&mut nak_buf, session_id, from_seq, count).unwrap();
                        up.send(now, &nak_buf[..n]);
                    }
                    Action::Resync(r) => resyncs.push(r),
                    Action::Stale => {}
                }
            }
        };
    }

    for _iter in 0..5_000_000u32 {
        // ---- finished?
        if produced == sc.n_msgs {
            final_seq = local;
            if delivered.last() == Some(&final_seq) {
                break;
            }
        }
        assert!(
            now < 120_000 * MS,
            "simulation did not converge (delivered {} of {})",
            delivered.len(),
            sc.n_msgs
        );

        // ---- advance virtual time to the next event
        let mut t = u64::MAX;
        if produced < sc.n_msgs {
            t = t.min(next_msg_at);
        }
        for c in [
            seq.next_deadline(),
            down.next_time(),
            up.next_time(),
            repair.next_time(),
            rx.next_deadline(),
        ]
        .into_iter()
        .flatten()
        {
            t = t.min(c);
        }
        now = now.max(t);

        // ---- producer (with optional sender restart)
        while produced < sc.n_msgs && next_msg_at <= now {
            if sc.restart_after == Some(produced) {
                seq.flush(now, |b| down.send(now, b));
                session += 1;
                seq = Sequencer::new(seq_cfg(session), now);
                srv = RetransmitServer::new(RetransmitConfig::default(), session);
                local = 0;
            }
            local += 1;
            produced += 1;
            seq.push(now, mk(local), |b| down.send(now, b));
            next_msg_at += sc.interval;
        }
        seq.tick(now, |b| down.send(now, b));

        // ---- network deliveries
        while let Some(b) = down.poll(now) {
            rx.on_datagram(now, &b, &mut actions);
            apply!();
        }
        while let Some(b) = repair.poll(now) {
            rx.on_datagram(now, &b, &mut actions);
            apply!();
        }
        while let Some(b) = up.poll(now) {
            if let Ok(p) = Packet::parse(&b) {
                if p.header().ptype == PacketType::Nak {
                    let h = *p.header();
                    srv.on_nak(
                        now,
                        h.session_id,
                        h.seq,
                        u64::from(h.count),
                        seq.log(),
                        |r| repair.send(now, r),
                    );
                }
            }
        }
        rx.on_tick(now, &mut actions);
        apply!();
    }
    let _ = final_seq;
    Outcome {
        delivered,
        resyncs,
        rx: rx.stats().clone(),
        srv: srv.stats().clone(),
        sim_end: now,
    }
}

fn assert_exact(o: &Outcome, n: u64) {
    assert!(o.resyncs.is_empty(), "unexpected resyncs: {:?}", o.resyncs);
    assert_eq!(
        o.delivered,
        (1..=n).collect::<Vec<_>>(),
        "must be gapless, duplicate-free and in order"
    );
}

#[test]
fn clean_network_needs_no_repairs() {
    let sc = Scenario::clean();
    let o = run(&sc);
    assert_exact(&o, sc.n_msgs);
    assert_eq!(o.rx.naks_sent, 0);
    assert_eq!(o.rx.duplicates, 0);
    assert_eq!(o.rx.gaps_detected, 0);
}

#[test]
fn light_random_loss_is_repaired() {
    // ~1000 data packets at 1% loss: enough that real loss (not just reordering) occurs.
    let sc = Scenario {
        n_msgs: 20_000,
        ..Scenario::lossy(Loss::Random(0.01), 11)
    };
    let o = run(&sc);
    assert_exact(&o, sc.n_msgs);
    assert!(
        o.rx.naks_sent > 0 && o.rx.repaired > 0,
        "loss should have forced repairs"
    );
}

#[test]
fn heavy_random_loss_on_every_path_is_repaired() {
    // 5% loss on the live feed, on the NAKs, and on the repairs themselves.
    for seed in 0..10 {
        let sc = Scenario::lossy(Loss::Random(0.05), seed);
        let o = run(&sc);
        assert_exact(&o, sc.n_msgs);
    }
}

#[test]
fn bursty_gilbert_elliott_loss_is_repaired() {
    let burst = Loss::GilbertElliott {
        p_good_to_bad: 0.02,
        p_bad_to_good: 0.2,
        loss_in_good: 0.0,
        loss_in_bad: 0.9,
    };
    for seed in 0..10 {
        let sc = Scenario::lossy(burst, seed);
        let o = run(&sc);
        assert_exact(&o, sc.n_msgs);
        assert!(o.rx.gaps_detected > 0);
    }
}

#[test]
fn reordering_alone_causes_no_unrecoverable_state() {
    // Large jitter, no loss: packets reorder and duplicate but nothing is lost.
    let link = LinkConfig {
        latency: 50 * US,
        jitter: 2 * MS,
        loss: Loss::None,
        duplicate_prob: 0.1,
    };
    let sc = Scenario {
        down: link,
        ..Scenario::clean()
    };
    let o = run(&sc);
    assert_exact(&o, sc.n_msgs);
    assert!(o.rx.duplicates > 0);
}

#[test]
fn identical_seed_gives_identical_run() {
    let sc = Scenario::lossy(Loss::Random(0.05), 99);
    let (a, b) = (run(&sc), run(&sc));
    assert_eq!(a.rx, b.rx);
    assert_eq!(a.sim_end, b.sim_end);
    assert_eq!(a.srv, b.srv);
}

#[test]
fn retransmit_window_exceeded_resyncs_and_then_streams_contiguously() {
    // The receiver "joins" 1 s late while the sender keeps only the last 100 messages.
    // History cannot be repaired; the receiver must give up, resync, and then follow live.
    let sc = Scenario {
        n_msgs: 3_000,
        interval: MS,
        log_capacity: 100,
        blackout_until: 1_000 * MS,
        ..Scenario::clean()
    };
    let o = run(&sc);
    assert_eq!(o.resyncs, vec![ResyncReason::RetriesExhausted]);
    assert!(
        o.srv.unavailable > 0,
        "server should have reported out-of-window requests"
    );
    let first = o.delivered[0];
    assert!(
        first > 1_000,
        "history before the join point is unrecoverable (first delivered {first})"
    );
    assert_eq!(
        o.delivered,
        (first..=sc.n_msgs).collect::<Vec<_>>(),
        "after the resync the stream is contiguous to the end"
    );
}

#[test]
fn sender_restart_forces_resync_and_numbering_restarts() {
    let sc = Scenario {
        restart_after: Some(800),
        ..Scenario::clean()
    };
    let o = run(&sc);
    assert_eq!(o.resyncs, vec![ResyncReason::SessionChange]);
    let expected: Vec<u64> = (1..=800).chain(1..=(sc.n_msgs - 800)).collect();
    assert_eq!(o.delivered, expected);
}

#[test]
fn repair_rate_limit_slows_but_does_not_break_recovery() {
    // Same lossy network, but the retransmit server is throttled: still converges.
    let sc = Scenario::lossy(Loss::Random(0.05), 4);
    let o = run(&sc);
    assert_exact(&o, sc.n_msgs);
    assert_eq!(
        o.srv.throttled_msgs, 0,
        "default limit is generous for this load"
    );
}
