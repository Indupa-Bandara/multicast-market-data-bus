//! Sans-IO receiver state machine for the reliable multicast market-data bus.
//!
//! The [`Receiver`] owns no sockets, threads or clocks. You feed it inputs
//! (a parsed packet or a raw datagram, plus the current time) and it appends
//! [`Action`]s to a `Vec` you provide (reused between calls, so no per-call
//! allocation once warmed up). Because time is an argument, every behaviour is
//! deterministic and reproducible from a seed in tests.
//!
//! Guarantee: the sequence of `Action::Deliver` between two `Action::Resync`
//! actions is gap-free, duplicate-free and strictly in order, starting at the
//! sequence number the receiver joined at. A book builder must reset its state
//! on every `Resync`.
//!
//! ```
//! use recovery::{Action, Config, Receiver};
//! use wire::{encode_data, Flags, L2Update, Side, Action as Op, MAX_PACKET_LEN};
//!
//! let mut rx = Receiver::new(Config::default());
//! let mut out = Vec::new();
//! let msg = L2Update { instrument_id: 1, side: Side::Bid, action: Op::Set, price: 100, qty: 5 };
//! let mut buf = [0u8; MAX_PACKET_LEN];
//! let n = encode_data(&mut buf, 7, 1, Flags::NONE, 0, &[msg]).unwrap();
//! rx.on_datagram(1_000, &buf[..n], &mut out);
//! assert!(matches!(out[0], Action::Deliver { seq: 1, .. }));
//! ```
#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeMap;
use wire::{DecodeError, L2Update, Packet, PacketType};

/// Time in nanoseconds on any monotonic clock the caller chooses.
pub type Nanos = u64;

const MS: Nanos = 1_000_000;

/// Where a receiver starts when it first sees a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinPolicy {
    /// Expect delivery to begin at this sequence number (normally 1) and NAK
    /// for everything before the first packet seen. Gives a complete stream if
    /// the sender's retransmit log still holds it.
    FromSeq(u64),
    /// Start at the first sequence number seen. Gives a partial stream
    /// (a late joiner would normally take a snapshot; that is a stretch goal).
    FirstSeen,
}

/// Tunables. All durations are nanoseconds.
#[derive(Debug, Clone)]
pub struct Config {
    /// How long to wait after noticing a gap before the first NAK
    /// (many gaps are just reordering).
    pub reorder_tolerance: Nanos,
    /// Delay before re-NAKing after the first NAK; doubles each round.
    pub nak_initial_backoff: Nanos,
    /// Upper bound for the NAK backoff.
    pub nak_max_backoff: Nanos,
    /// NAK rounds without any progress before giving up and resyncing.
    pub max_nak_retries: u32,
    /// Max out-of-order messages held while waiting for a gap to fill.
    pub max_buffered_messages: usize,
    /// Max messages requested by a single NAK packet.
    pub max_nak_count: u16,
    /// Max NAK packets emitted per NAK round (storm control).
    pub max_naks_per_round: usize,
    /// If the missing span exceeds this many messages, resync instead of NAKing.
    pub max_gap: u64,
    /// No packet (data or heartbeat) for this long raises [`Action::Stale`].
    pub stale_timeout: Nanos,
    /// Where to begin on first contact.
    pub join: JoinPolicy,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            reorder_tolerance: MS,
            nak_initial_backoff: 5 * MS,
            nak_max_backoff: 200 * MS,
            max_nak_retries: 8,
            max_buffered_messages: 65_536,
            max_nak_count: 256,
            max_naks_per_round: 4,
            max_gap: 100_000,
            stale_timeout: 3_000 * MS,
            join: JoinPolicy::FromSeq(1),
        }
    }
}

/// Why the receiver abandoned the stream and restarted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResyncReason {
    /// The sender restarted (new session id); sequence numbers restart.
    SessionChange,
    /// A gap was not repaired after `max_nak_retries` rounds (e.g. the data
    /// has left the sender's retransmit window).
    RetriesExhausted,
    /// Too many out-of-order messages buffered.
    BufferOverflow,
    /// The missing span exceeded `max_gap`.
    GapTooLarge,
}

/// Something the caller must do.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Action {
    /// Hand this message to the consumer. Strictly increasing, gap-free `seq`.
    Deliver {
        /// Sequence number of the message.
        seq: u64,
        /// The message.
        msg: L2Update,
    },
    /// Send a NAK (unicast to the retransmit server) for `[from_seq, from_seq + count)`.
    SendNak {
        /// Session the request belongs to.
        session_id: u64,
        /// First missing sequence number.
        from_seq: u64,
        /// Number of messages requested.
        count: u16,
    },
    /// The stream was abandoned. The consumer must discard its derived state
    /// (e.g. the order book). Delivery restarts from a new sequence number.
    Resync(ResyncReason),
    /// No traffic within `stale_timeout`: raise an alarm and rejoin the multicast
    /// group. Emitted once per silence; the next packet clears it.
    Stale,
}

/// Coarse receiver state (derived, for monitoring and tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Nothing joined yet (start, or just after a resync).
    Init,
    /// In sync, delivering as packets arrive.
    Live,
    /// Holding messages behind a gap and requesting repairs.
    GapRecovery,
    /// No traffic within the stale timeout.
    Stale,
}

/// Counters. Read with [`Receiver::stats`]; aggregate off the hot path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    /// Valid packets of any type processed.
    pub packets: u64,
    /// Messages delivered to the consumer.
    pub delivered: u64,
    /// Duplicate messages dropped.
    pub duplicates: u64,
    /// Gap episodes detected.
    pub gaps_detected: u64,
    /// NAK packets emitted.
    pub naks_sent: u64,
    /// Useful messages that arrived in RETRANSMIT packets.
    pub repaired: u64,
    /// Resyncs triggered.
    pub resyncs: u64,
    /// Stale alarms raised.
    pub stale_events: u64,
    /// Datagrams rejected by the decoder.
    pub decode_errors: u64,
    /// NAK packets received by mistake (receivers ignore them).
    pub naks_ignored: u64,
    /// Heartbeats whose sequence number was behind what we already delivered.
    pub heartbeats_behind: u64,
}

#[derive(Debug, Clone, Copy)]
struct Gap {
    next_nak_at: Nanos,
    backoff: Nanos,
    retries: u32,
}

/// The receiver state machine. See the crate docs.
#[derive(Debug)]
pub struct Receiver {
    cfg: Config,
    session: Option<u64>,
    joined: bool,
    /// After a non-session resync, rejoin at the first seq seen (FromSeq already failed).
    first_seen_next: bool,
    /// Next sequence number to deliver.
    expected: u64,
    /// One past the highest sequence number known to exist.
    high: u64,
    /// Out-of-order messages awaiting delivery, keyed by sequence number.
    buf: BTreeMap<u64, L2Update>,
    gap: Option<Gap>,
    last_rx: Option<Nanos>,
    stale: bool,
    stats: Stats,
}

impl Receiver {
    /// Create a receiver in the `Init` state.
    pub fn new(cfg: Config) -> Self {
        Receiver {
            cfg,
            session: None,
            joined: false,
            first_seen_next: false,
            expected: 0,
            high: 0,
            buf: BTreeMap::new(),
            gap: None,
            last_rx: None,
            stale: false,
            stats: Stats::default(),
        }
    }

    /// Current coarse state.
    pub fn state(&self) -> State {
        if !self.joined {
            State::Init
        } else if self.stale {
            State::Stale
        } else if self.gap.is_some() {
            State::GapRecovery
        } else {
            State::Live
        }
    }

    /// Next sequence number that will be delivered (None before joining).
    pub fn expected_seq(&self) -> Option<u64> {
        self.joined.then_some(self.expected)
    }

    /// Counters.
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Earliest time at which [`Receiver::on_tick`] has work to do, so the I/O
    /// layer can sleep precisely instead of polling.
    pub fn next_deadline(&self) -> Option<Nanos> {
        let mut d = self.gap.map(|g| g.next_nak_at);
        if !self.stale {
            if let Some(last) = self.last_rx {
                let t = last.saturating_add(self.cfg.stale_timeout);
                d = Some(d.map_or(t, |x| x.min(t)));
            }
        }
        d
    }

    /// Feed a raw datagram. Malformed input is counted and dropped; it never panics.
    pub fn on_datagram(&mut self, now: Nanos, bytes: &[u8], out: &mut Vec<Action>) {
        match Packet::parse(bytes) {
            Ok(p) => self.on_packet(now, &p, out),
            Err(_e) => self.stats.decode_errors += 1,
        }
    }

    /// Like [`Receiver::on_datagram`] but reports why a datagram was rejected.
    pub fn try_on_datagram(
        &mut self,
        now: Nanos,
        bytes: &[u8],
        out: &mut Vec<Action>,
    ) -> Result<(), DecodeError> {
        match Packet::parse(bytes) {
            Ok(p) => {
                self.on_packet(now, &p, out);
                Ok(())
            }
            Err(e) => {
                self.stats.decode_errors += 1;
                Err(e)
            }
        }
    }

    /// Feed a validated packet.
    pub fn on_packet(&mut self, now: Nanos, pkt: &Packet<'_>, out: &mut Vec<Action>) {
        let h = *pkt.header();
        if h.ptype == PacketType::Nak {
            self.stats.naks_ignored += 1;
            return;
        }
        self.stats.packets += 1;

        match self.session {
            Some(s) if s == h.session_id => {}
            Some(_) => {
                self.resync(ResyncReason::SessionChange, out);
                self.session = Some(h.session_id);
            }
            None => self.session = Some(h.session_id),
        }

        self.last_rx = Some(now);
        self.stale = false;

        if !self.joined {
            let start = match (self.first_seen_next, self.cfg.join) {
                (false, JoinPolicy::FromSeq(n)) => n,
                _ => h.seq,
            };
            self.expected = start;
            self.high = start;
            self.joined = true;
            self.first_seen_next = false;
        }

        let before = self.expected;
        match h.ptype {
            PacketType::Data => {
                if !self.on_data(pkt, out) {
                    return;
                }
            }
            PacketType::Heartbeat => {
                if h.seq < self.expected {
                    self.stats.heartbeats_behind += 1;
                } else {
                    self.high = self.high.max(h.seq);
                }
            }
            PacketType::Nak => return,
        }
        self.after_update(now, before, out);
    }

    /// Advance timers: stale detection and NAK scheduling. Call when
    /// `now >= next_deadline()` (calling more often is harmless).
    pub fn on_tick(&mut self, now: Nanos, out: &mut Vec<Action>) {
        if let Some(last) = self.last_rx {
            if !self.stale && now.saturating_sub(last) >= self.cfg.stale_timeout {
                self.stale = true;
                self.stats.stale_events += 1;
                out.push(Action::Stale);
            }
        }

        let (Some(g), Some(session_id)) = (self.gap, self.session) else {
            return;
        };
        if now < g.next_nak_at {
            return;
        }
        if g.retries >= self.cfg.max_nak_retries {
            self.resync(ResyncReason::RetriesExhausted, out);
            return;
        }
        let max_count = u64::from(self.cfg.max_nak_count.max(1));
        for (from_seq, count) in self.nak_ranges(max_count, self.cfg.max_naks_per_round.max(1)) {
            out.push(Action::SendNak {
                session_id,
                from_seq,
                count,
            });
            self.stats.naks_sent += 1;
        }
        let backoff = g.backoff.saturating_mul(2).min(self.cfg.nak_max_backoff);
        self.gap = Some(Gap {
            next_nak_at: now.saturating_add(backoff),
            backoff,
            retries: g.retries + 1,
        });
    }

    // ---- internals -------------------------------------------------------

    /// Returns false if the receiver resynced while processing.
    fn on_data(&mut self, pkt: &Packet<'_>, out: &mut Vec<Action>) -> bool {
        let retx = pkt.is_retransmit();
        for (seq, msg) in (pkt.header().seq..).zip(pkt.messages()) {
            if seq < self.expected {
                self.stats.duplicates += 1;
            } else if seq == self.expected {
                self.deliver(msg, out);
                if retx {
                    self.stats.repaired += 1;
                }
                self.drain(out);
            } else if self.buf.insert(seq, msg).is_some() {
                self.stats.duplicates += 1;
            } else {
                if retx {
                    self.stats.repaired += 1;
                }
                if self.buf.len() > self.cfg.max_buffered_messages {
                    self.resync(ResyncReason::BufferOverflow, out);
                    return false;
                }
            }
        }
        self.high = self.high.max(pkt.next_seq());
        true
    }

    fn deliver(&mut self, msg: L2Update, out: &mut Vec<Action>) {
        out.push(Action::Deliver {
            seq: self.expected,
            msg,
        });
        self.expected += 1;
        self.stats.delivered += 1;
    }

    /// Deliver buffered messages that are now contiguous with `expected`.
    fn drain(&mut self, out: &mut Vec<Action>) {
        while let Some(e) = self.buf.first_entry() {
            if *e.key() != self.expected {
                break;
            }
            let msg = e.remove();
            self.deliver(msg, out);
        }
    }

    fn after_update(&mut self, now: Nanos, expected_before: u64, out: &mut Vec<Action>) {
        if self.expected >= self.high {
            self.gap = None;
            return;
        }
        if self.high - self.expected > self.cfg.max_gap {
            self.resync(ResyncReason::GapTooLarge, out);
            return;
        }
        let progressed = self.expected != expected_before;
        let initial = self.cfg.nak_initial_backoff;
        match &mut self.gap {
            None => {
                self.stats.gaps_detected += 1;
                self.gap = Some(Gap {
                    next_nak_at: now.saturating_add(self.cfg.reorder_tolerance),
                    backoff: initial,
                    retries: 0,
                });
            }
            Some(g) if progressed => {
                g.retries = 0;
                g.backoff = initial;
                g.next_nak_at = now.saturating_add(initial);
            }
            Some(_) => {}
        }
    }

    fn resync(&mut self, reason: ResyncReason, out: &mut Vec<Action>) {
        self.stats.resyncs += 1;
        out.push(Action::Resync(reason));
        self.joined = false;
        self.buf.clear();
        self.gap = None;
        self.expected = 0;
        self.high = 0;
        // A new session restarts numbering, so the join policy still applies;
        // otherwise FromSeq just failed, so take whatever comes next.
        self.first_seen_next = reason != ResyncReason::SessionChange;
    }

    /// Missing ranges `[from, from+count)` in ascending order, chunked.
    fn nak_ranges(&self, max_count: u64, max_naks: usize) -> Vec<(u64, u16)> {
        fn push(v: &mut Vec<(u64, u16)>, mut s: u64, end: u64, max_count: u64, max: usize) {
            while s < end && v.len() < max {
                let n = (end - s).min(max_count).min(u64::from(u16::MAX));
                v.push((s, n as u16));
                s += n;
            }
        }
        let mut v = Vec::new();
        let mut cursor = self.expected;
        for &k in self.buf.keys() {
            if v.len() >= max_naks {
                return v;
            }
            if k > cursor {
                push(&mut v, cursor, k, max_count, max_naks);
            }
            cursor = k + 1;
        }
        if cursor < self.high {
            push(&mut v, cursor, self.high, max_count, max_naks);
        }
        v
    }
}
