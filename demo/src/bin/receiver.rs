//! Subscriber: UDP multicast -> recovery state machine -> checked, in-order delivery.
//! NAKs go out over unicast; repairs come back on the same socket.
//!
//! cargo run --release -p demo --bin receiver -- --iface 172.20.1.5 --count 100000
//!
//! Flags (all optional):
//!   --group 239.255.42.1:5000   multicast group and port
//!   --iface 127.0.0.1           local interface IP to join on
//!   --nak-to 127.0.0.1:5001     the sender's NAK/repair address
//!   --count 0                   exit successfully after this many deliveries (0 = run to timeout)
//!   --timeout-s 60              give up after this long
//!   --rcvbuf 4194304            requested SO_RCVBUF in bytes
//!
//! Exit status: 0 if every delivery was contiguous and matched its expected payload, else 1.
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::{Duration, Instant};

use demo::{mk, Args};
use net::udp::{Datagram, UdpEndpoint};
use recovery::{Action, Config, Receiver};
use wire::encode_nak;

#[derive(Default)]
struct Tally {
    delivered: u64,
    last_seq: u64,
    /// True until the first delivery after start or after a resync.
    fresh: bool,
    order_violations: u64,
    payload_mismatches: u64,
    resyncs: u64,
    naks_sent: u64,
    stale_events: u64,
}

fn apply(actions: &mut Vec<Action>, t: &mut Tally, nak: &mut UdpEndpoint, nak_buf: &mut [u8; 64]) {
    for a in actions.drain(..) {
        match a {
            Action::Deliver { seq, msg } => {
                if !t.fresh && seq != t.last_seq + 1 {
                    t.order_violations += 1;
                }
                if msg != mk(seq) {
                    t.payload_mismatches += 1;
                }
                t.fresh = false;
                t.last_seq = seq;
                t.delivered += 1;
            }
            Action::SendNak {
                session_id,
                from_seq,
                count,
            } => {
                if let Ok(n) = encode_nak(nak_buf, session_id, from_seq, count) {
                    if nak.send(&nak_buf[..n]).is_ok() {
                        t.naks_sent += 1;
                    }
                }
            }
            Action::Resync(r) => {
                eprintln!("receiver: RESYNC {r:?} (consumer must discard its book)");
                t.resyncs += 1;
                t.fresh = true;
            }
            Action::Stale => {
                eprintln!("receiver: STALE, no packets or heartbeats");
                t.stale_events += 1;
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    let a = Args::parse();
    let group: SocketAddrV4 = a.get("group", "239.255.42.1:5000".parse().unwrap());
    let iface: Ipv4Addr = a.get("iface", Ipv4Addr::LOCALHOST);
    let nak_to: SocketAddr = a.get("nak-to", "127.0.0.1:5001".parse().unwrap());
    let count: u64 = a.get("count", 0);
    let timeout_ns: u64 = a.get("timeout-s", 60u64) * 1_000_000_000;
    let rcvbuf: usize = a.get("rcvbuf", 4 << 20);

    let start = Instant::now();
    let now_ns = || start.elapsed().as_nanos() as u64;

    let mut mc = UdpEndpoint::multicast_receiver(group, iface, Some(rcvbuf))?;
    let mut nak = UdpEndpoint::unicast("0.0.0.0:0".parse().unwrap(), Some(nak_to))?;
    eprintln!(
        "receiver: group={group} iface={iface} nak_to={nak_to} rcvbuf requested={rcvbuf} effective={}",
        mc.recv_buffer_size()?
    );

    let mut rx = Receiver::new(Config::default());
    let mut tally = Tally {
        fresh: true,
        ..Tally::default()
    };
    let mut actions: Vec<Action> = Vec::with_capacity(256);
    let mut nak_buf = [0u8; 64];
    let mut buf = [0u8; 2048];
    let mut last_report = 0u64;

    loop {
        let now = now_ns();
        let mut got = false;

        // Live multicast feed, then repairs arriving on the unicast socket.
        while let Some(n) = mc.recv(&mut buf)? {
            rx.on_datagram(now, &buf[..n], &mut actions);
            apply(&mut actions, &mut tally, &mut nak, &mut nak_buf);
            got = true;
        }
        while let Some(n) = nak.recv(&mut buf)? {
            rx.on_datagram(now, &buf[..n], &mut actions);
            apply(&mut actions, &mut tally, &mut nak, &mut nak_buf);
            got = true;
        }
        if rx.next_deadline().is_some_and(|d| now >= d) {
            rx.on_tick(now, &mut actions);
            apply(&mut actions, &mut tally, &mut nak, &mut nak_buf);
        }

        if now - last_report >= 1_000_000_000 {
            last_report = now;
            eprintln!(
                "receiver: t={}s state={:?} delivered={} last_seq={} naks={} repaired={}",
                now / 1_000_000_000,
                rx.state(),
                tally.delivered,
                tally.last_seq,
                tally.naks_sent,
                rx.stats().repaired
            );
        }
        if count > 0 && tally.delivered >= count {
            break;
        }
        if now >= timeout_ns {
            eprintln!("receiver: timed out");
            break;
        }
        if !got {
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    let s = rx.stats();
    eprintln!("receiver: stats {s:?}");
    eprintln!(
        "receiver: delivered={} order_violations={} payload_mismatches={} resyncs={} naks_sent={} stale={}",
        tally.delivered,
        tally.order_violations,
        tally.payload_mismatches,
        tally.resyncs,
        tally.naks_sent,
        tally.stale_events
    );
    let ok = tally.order_violations == 0
        && tally.payload_mismatches == 0
        && (count == 0 || tally.delivered >= count);
    println!("{}", if ok { "PASS" } else { "FAIL" });
    std::process::exit(if ok { 0 } else { 1 });
}
