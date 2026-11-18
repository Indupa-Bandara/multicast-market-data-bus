//! Network layer.
//!
//! Today this holds [`SimLink`], a deterministic simulated one-way link with
//! latency, jitter (which causes reordering), random or bursty loss and
//! duplication, all driven by a seed so any failure replays exactly.
//! [`udp`] holds the real non-blocking UDP endpoints (unicast and IPv4 multicast) behind the
//! small [`udp::Datagram`] trait.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod udp;

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

/// Time in nanoseconds.
pub type Nanos = u64;

/// SplitMix64: tiny, fast, and identical on every platform.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// Seeded generator.
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }
    /// Next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform in [0, 1).
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// Uniform in [0, n] inclusive.
    pub fn up_to(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % (n + 1)
        }
    }
}

/// Packet loss model.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Loss {
    /// Nothing is lost.
    None,
    /// Each packet is lost independently with this probability.
    Random(f64),
    /// Two-state Gilbert-Elliott model: loss arrives in bursts, as on real networks.
    GilbertElliott {
        /// Probability of moving from the good to the bad state per packet.
        p_good_to_bad: f64,
        /// Probability of moving from the bad to the good state per packet.
        p_bad_to_good: f64,
        /// Loss probability while in the good state.
        loss_in_good: f64,
        /// Loss probability while in the bad state.
        loss_in_bad: f64,
    },
}

/// Link behaviour.
#[derive(Debug, Clone, Copy)]
pub struct LinkConfig {
    /// Fixed one-way delay.
    pub latency: Nanos,
    /// Extra random delay in `[0, jitter]`; anything above the packet spacing reorders.
    pub jitter: Nanos,
    /// Loss model.
    pub loss: Loss,
    /// Probability a delivered packet is delivered twice.
    pub duplicate_prob: f64,
}

impl LinkConfig {
    /// Perfect link with the given latency.
    pub fn perfect(latency: Nanos) -> Self {
        LinkConfig {
            latency,
            jitter: 0,
            loss: Loss::None,
            duplicate_prob: 0.0,
        }
    }
}

/// Link counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinkStats {
    /// Packets handed to `send`.
    pub sent: u64,
    /// Packets dropped (loss model or blackout).
    pub dropped: u64,
    /// Extra copies created.
    pub duplicated: u64,
    /// Packets returned by `poll`.
    pub delivered: u64,
}

#[derive(Debug)]
struct Item {
    at: Nanos,
    id: u64,
    data: Vec<u8>,
}
impl PartialEq for Item {
    fn eq(&self, o: &Self) -> bool {
        (self.at, self.id) == (o.at, o.id)
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Item {
    fn cmp(&self, o: &Self) -> Ordering {
        (self.at, self.id).cmp(&(o.at, o.id))
    }
}

/// Deterministic simulated one-way link.
#[derive(Debug)]
pub struct SimLink {
    cfg: LinkConfig,
    rng: Rng,
    in_bad_state: bool,
    blackout_until: Nanos,
    next_id: u64,
    queue: BinaryHeap<Reverse<Item>>,
    stats: LinkStats,
}

impl SimLink {
    /// Create a link. The same `(cfg, seed, inputs)` always produce the same output.
    pub fn new(cfg: LinkConfig, seed: u64) -> Self {
        SimLink {
            cfg,
            rng: Rng::new(seed),
            in_bad_state: false,
            blackout_until: 0,
            next_id: 0,
            queue: BinaryHeap::new(),
            stats: LinkStats::default(),
        }
    }

    /// Drop everything sent before time `t` (models a receiver that joins late).
    pub fn blackout_until(&mut self, t: Nanos) {
        self.blackout_until = t;
    }

    /// Counters.
    pub fn stats(&self) -> &LinkStats {
        &self.stats
    }

    /// Offer a packet to the link at time `now`.
    pub fn send(&mut self, now: Nanos, bytes: &[u8]) {
        self.stats.sent += 1;
        if now < self.blackout_until || self.is_lost() {
            self.stats.dropped += 1;
            return;
        }
        self.enqueue(now, bytes);
        if self.cfg.duplicate_prob > 0.0 && self.rng.next_f64() < self.cfg.duplicate_prob {
            self.stats.duplicated += 1;
            self.enqueue(now, bytes);
        }
    }

    /// Next packet whose delivery time is `<= now`, in delivery-time order.
    pub fn poll(&mut self, now: Nanos) -> Option<Vec<u8>> {
        match self.queue.peek() {
            Some(Reverse(it)) if it.at <= now => {
                self.stats.delivered += 1;
                self.queue.pop().map(|Reverse(it)| it.data)
            }
            _ => None,
        }
    }

    /// Delivery time of the earliest packet in flight.
    pub fn next_time(&self) -> Option<Nanos> {
        self.queue.peek().map(|Reverse(it)| it.at)
    }

    fn enqueue(&mut self, now: Nanos, bytes: &[u8]) {
        let at = now + self.cfg.latency + self.rng.up_to(self.cfg.jitter);
        let id = self.next_id;
        self.next_id += 1;
        self.queue.push(Reverse(Item {
            at,
            id,
            data: bytes.to_vec(),
        }));
    }

    fn is_lost(&mut self) -> bool {
        match self.cfg.loss {
            Loss::None => false,
            Loss::Random(p) => self.rng.next_f64() < p,
            Loss::GilbertElliott {
                p_good_to_bad,
                p_bad_to_good,
                loss_in_good,
                loss_in_bad,
            } => {
                let f = self.rng.next_f64();
                if self.in_bad_state {
                    if f < p_bad_to_good {
                        self.in_bad_state = false;
                    }
                } else if f < p_good_to_bad {
                    self.in_bad_state = true;
                }
                let p = if self.in_bad_state {
                    loss_in_bad
                } else {
                    loss_in_good
                };
                self.rng.next_f64() < p
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(link: &mut SimLink, upto: Nanos) -> Vec<Vec<u8>> {
        let mut v = vec![];
        while let Some(p) = link.poll(upto) {
            v.push(p);
        }
        v
    }

    #[test]
    fn perfect_link_delivers_in_order_after_latency() {
        let mut l = SimLink::new(LinkConfig::perfect(100), 1);
        for i in 0..5u8 {
            l.send(i as u64, &[i]);
        }
        assert!(l.poll(99).is_none());
        assert_eq!(l.next_time(), Some(100));
        let got = drain(&mut l, 1_000);
        assert_eq!(got, (0..5u8).map(|i| vec![i]).collect::<Vec<_>>());
    }

    #[test]
    fn same_seed_same_behaviour() {
        let cfg = LinkConfig {
            latency: 10,
            jitter: 50,
            loss: Loss::Random(0.3),
            duplicate_prob: 0.2,
        };
        let run = |seed| {
            let mut l = SimLink::new(cfg, seed);
            for i in 0..200u8 {
                l.send(i as u64, &[i]);
            }
            drain(&mut l, u64::MAX)
        };
        assert_eq!(run(7), run(7));
        assert_ne!(run(7), run(8));
    }

    #[test]
    fn jitter_reorders_and_loss_and_dup_happen() {
        let cfg = LinkConfig {
            latency: 0,
            jitter: 1_000,
            loss: Loss::Random(0.1),
            duplicate_prob: 0.1,
        };
        let mut l = SimLink::new(cfg, 3);
        for i in 0..1000u32 {
            l.send(i as u64 * 10, &i.to_le_bytes());
        }
        let got: Vec<u32> = drain(&mut l, u64::MAX)
            .iter()
            .map(|b| u32::from_le_bytes(b[..4].try_into().unwrap()))
            .collect();
        assert!(got.windows(2).any(|w| w[1] < w[0]), "expected reordering");
        assert!(l.stats().dropped > 50 && l.stats().dropped < 200);
        assert!(l.stats().duplicated > 50);
    }

    #[test]
    fn gilbert_elliott_loss_is_bursty() {
        // Same ~10% average loss, but GE concentrates it into runs.
        fn longest_run(cfg: LinkConfig) -> usize {
            let mut l = SimLink::new(cfg, 5);
            let (mut best, mut cur) = (0, 0);
            for i in 0..20_000u64 {
                let before = l.stats().dropped;
                l.send(i, &[0]);
                if l.stats().dropped > before {
                    cur += 1;
                    best = best.max(cur);
                } else {
                    cur = 0;
                }
            }
            best
        }
        let random = LinkConfig {
            loss: Loss::Random(0.10),
            ..LinkConfig::perfect(0)
        };
        let bursty = LinkConfig {
            loss: Loss::GilbertElliott {
                p_good_to_bad: 0.01,
                p_bad_to_good: 0.09,
                loss_in_good: 0.0,
                loss_in_bad: 1.0,
            },
            ..LinkConfig::perfect(0)
        };
        assert!(longest_run(bursty) > 2 * longest_run(random));
    }

    #[test]
    fn blackout_drops_early_sends() {
        let mut l = SimLink::new(LinkConfig::perfect(0), 1);
        l.blackout_until(100);
        l.send(99, &[1]);
        l.send(100, &[2]);
        assert_eq!(drain(&mut l, 1000), vec![vec![2]]);
    }
}
