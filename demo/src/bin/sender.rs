//! Publisher: sequencer -> UDP multicast, plus a unicast NAK/repair server.
//!
//! cargo run --release -p demo --bin sender -- --iface 172.20.1.5 --count 100000 --rate 20000
//!
//! Flags (all optional):
//!   --group 239.255.42.1:5000   multicast group and port
//!   --iface 127.0.0.1           local interface IP to send from (see harness/WSL2.md)
//!   --nak-bind 0.0.0.0:5001     where NAKs arrive and repairs leave from
//!   --count 100000              messages to send
//!   --rate 10000                messages per second
//!   --batch 20                  max messages per packet (1..=60)
//!   --max-wait-us 200           flush a partial batch after this long
//!   --drop-pct 0                drop this % of live packets before sending (loss injection)
//!   --seed 1                    seed for the loss injector
//!   --linger-s 3                keep serving NAKs this long after the last message
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use demo::{mk, Args};
use net::udp::{Datagram, UdpEndpoint};
use net::Rng;
use retx_log::{RetransmitConfig, RetransmitServer};
use sequencer::{Config, Sequencer};
use wire::{Packet, PacketType};

fn main() -> std::io::Result<()> {
    let a = Args::parse();
    let group: SocketAddrV4 = a.get("group", "239.255.42.1:5000".parse().unwrap());
    let iface: Ipv4Addr = a.get("iface", Ipv4Addr::LOCALHOST);
    let nak_bind: SocketAddr = a.get("nak-bind", "0.0.0.0:5001".parse().unwrap());
    let count: u64 = a.get("count", 100_000);
    let rate: u64 = a.get("rate", 10_000u64).max(1);
    let batch: usize = a.get("batch", 20);
    let max_wait_us: u64 = a.get("max-wait-us", 200);
    let drop_pct: f64 = a.get("drop-pct", 0.0);
    let seed: u64 = a.get("seed", 1);
    let linger_ns: u64 = a.get("linger-s", 3u64) * 1_000_000_000;

    // A new session id on every start, so receivers detect a restart.
    let session = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |d| d.as_nanos() as u64);

    let start = Instant::now();
    let now_ns = || start.elapsed().as_nanos() as u64;

    let mut tx = UdpEndpoint::multicast_sender(group, iface, 1, true)?;
    let nak_sock = UdpEndpoint::unicast(nak_bind, None)?;
    let cfg = Config {
        session_id: session,
        max_batch: batch,
        max_wait: max_wait_us * 1_000,
        ..Config::default()
    };
    let mut seq = Sequencer::new(cfg, now_ns());
    let mut srv = RetransmitServer::new(RetransmitConfig::default(), session);
    let mut rng = Rng::new(seed);
    let (mut injected_drops, mut send_errors) = (0u64, 0u64);

    eprintln!(
        "sender: session={session} group={group} iface={iface} naks={} count={count} rate={rate}/s batch={batch} drop={drop_pct}%",
        nak_sock.local_addr()?
    );

    let interval = 1_000_000_000 / rate;
    let mut produced = 0u64;
    let mut next_at = now_ns();
    let mut done_at: Option<u64> = None;
    let mut buf = [0u8; 2048];

    let mut send_live = |b: &[u8]| {
        if drop_pct > 0.0 && rng.next_f64() * 100.0 < drop_pct {
            injected_drops += 1;
            return;
        }
        if tx.send(b).is_err() {
            send_errors += 1;
        }
    };

    loop {
        let now = now_ns();

        while produced < count && next_at <= now {
            produced += 1; // message i gets sequence number i
            seq.push(now, mk(produced), &mut send_live);
            next_at += interval;
        }
        seq.tick(now, &mut send_live);
        if produced == count && done_at.is_none() {
            seq.flush(now, &mut send_live);
            done_at = Some(now);
            eprintln!(
                "sender: all {count} messages sent, serving NAKs for {}s",
                linger_ns / 1_000_000_000
            );
        }

        // Answer NAKs with unicast repairs to whoever asked.
        while let Some((n, from)) = nak_sock.recv_from(&mut buf)? {
            if let Ok(p) = Packet::parse(&buf[..n]) {
                let h = p.header();
                if h.ptype == PacketType::Nak {
                    srv.on_nak(
                        now,
                        h.session_id,
                        h.seq,
                        u64::from(h.count),
                        seq.log(),
                        |r| {
                            let _ = nak_sock.send_to(r, from);
                        },
                    );
                }
            }
        }

        if let Some(t) = done_at {
            if now.saturating_sub(t) >= linger_ns {
                break;
            }
        }
        if produced == count || next_at.saturating_sub(now) > 200_000 {
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    eprintln!("sender: sequencer {:?}", seq.stats());
    eprintln!("sender: retransmit {:?}", srv.stats());
    eprintln!("sender: injected_drops={injected_drops} send_errors={send_errors}");
    Ok(())
}
