//! Sender-side repair: a bounded log of everything sent, and a rate-limited
//! server that answers NAKs from it.
//!
//! * [`RetxLog`]: ring indexed by sequence number. In memory for now; an
//!   mmap-backed ring with the same API is a later optimisation.
//! * [`RetransmitServer`]: sans-IO. Given a NAK it emits unicast DATA packets
//!   with the RETRANSMIT flag. A token bucket caps repair bandwidth so a burst
//!   of NAKs cannot starve the live feed. A NAK for data that has left the ring
//!   gets no reply (wire v1 has no "unavailable" packet); the receiver learns
//!   that by exhausting its NAK retries and resyncing.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

use wire::{encode_data, Action, Flags, L2Update, Side, MAX_MESSAGES_PER_PACKET, MAX_PACKET_LEN};

/// Time in nanoseconds on any monotonic clock the caller chooses.
pub type Nanos = u64;

const NANO: u128 = 1_000_000_000;
const DUMMY: L2Update = L2Update {
    instrument_id: 0,
    side: Side::Bid,
    action: Action::Set,
    price: 0,
    qty: 0,
};

/// Bounded log of sent messages, indexed by sequence number (first message is seq 1).
#[derive(Debug, Clone)]
pub struct RetxLog {
    buf: Vec<L2Update>,
    cap: u64,
    first: u64,
    next: u64,
}

impl RetxLog {
    /// Create a log holding the most recent `capacity` messages (at least 1).
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        RetxLog {
            buf: vec![DUMMY; cap],
            cap: cap as u64,
            first: 1,
            next: 1,
        }
    }

    /// Append the next message; returns its sequence number.
    pub fn append(&mut self, msg: L2Update) -> u64 {
        let seq = self.next;
        self.buf[(seq % self.cap) as usize] = msg;
        self.next += 1;
        if self.next - self.first > self.cap {
            self.first += 1;
        }
        seq
    }

    /// Message with this sequence number, if it is still in the window.
    pub fn get(&self, seq: u64) -> Option<L2Update> {
        if seq < self.first || seq >= self.next {
            None
        } else {
            Some(self.buf[(seq % self.cap) as usize])
        }
    }

    /// Oldest sequence number still held.
    pub fn oldest_seq(&self) -> u64 {
        self.first
    }

    /// One past the newest sequence number (the next one that will be assigned).
    pub fn next_seq(&self) -> u64 {
        self.next
    }

    /// Number of messages currently held.
    pub fn len(&self) -> usize {
        (self.next - self.first) as usize
    }

    /// True if nothing has been appended.
    pub fn is_empty(&self) -> bool {
        self.next == self.first
    }
}

/// Retransmit server tunables.
#[derive(Debug, Clone)]
pub struct RetransmitConfig {
    /// Sustained repair rate in messages per second.
    pub rate_msgs_per_sec: u64,
    /// Bucket size: how many messages can be repaired in a burst.
    pub burst_msgs: u64,
    /// Messages per repair packet (capped at the protocol maximum of 60).
    pub max_batch: usize,
}

impl Default for RetransmitConfig {
    fn default() -> Self {
        RetransmitConfig {
            rate_msgs_per_sec: 100_000,
            burst_msgs: 10_000,
            max_batch: 50,
        }
    }
}

/// Retransmit server counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetransmitStats {
    /// NAKs received.
    pub naks: u64,
    /// NAKs for a different session (stale, ignored).
    pub wrong_session: u64,
    /// NAKs for data older than the log window (no reply).
    pub unavailable: u64,
    /// NAKs starting at a sequence number not yet sent (ignored).
    pub future: u64,
    /// Messages sent as repairs.
    pub served_msgs: u64,
    /// Requested messages withheld by the rate limiter.
    pub throttled_msgs: u64,
    /// Repair packets emitted.
    pub repair_packets: u64,
}

/// Answers NAKs from a [`RetxLog`], rate limited by a token bucket.
#[derive(Debug)]
pub struct RetransmitServer {
    cfg: RetransmitConfig,
    session_id: u64,
    /// Tokens scaled by 1e9 so refill is exact integer math.
    tokens: u128,
    last: Option<Nanos>,
    scratch: Vec<L2Update>,
    buf: [u8; MAX_PACKET_LEN],
    stats: RetransmitStats,
}

impl RetransmitServer {
    /// Create a server for the given sender session. The bucket starts full.
    pub fn new(cfg: RetransmitConfig, session_id: u64) -> Self {
        let tokens = u128::from(cfg.burst_msgs) * NANO;
        RetransmitServer {
            cfg,
            session_id,
            tokens,
            last: None,
            scratch: Vec::with_capacity(MAX_MESSAGES_PER_PACKET),
            buf: [0; MAX_PACKET_LEN],
            stats: RetransmitStats::default(),
        }
    }

    /// Counters.
    pub fn stats(&self) -> &RetransmitStats {
        &self.stats
    }

    /// Handle a NAK for `[from_seq, from_seq + count)`. Each repair packet is
    /// passed to `emit` (send it unicast to the requester). When the rate
    /// limiter runs dry the lowest sequence numbers are served first, so the
    /// receiver still makes contiguous progress and re-NAKs the rest.
    pub fn on_nak<F: FnMut(&[u8])>(
        &mut self,
        now: Nanos,
        session_id: u64,
        from_seq: u64,
        count: u64,
        log: &RetxLog,
        mut emit: F,
    ) {
        self.stats.naks += 1;
        if session_id != self.session_id {
            self.stats.wrong_session += 1;
            return;
        }
        self.refill(now);
        if from_seq < log.oldest_seq() {
            self.stats.unavailable += 1;
            return;
        }
        if from_seq >= log.next_seq() {
            self.stats.future += 1;
            return;
        }
        let end = from_seq.saturating_add(count).min(log.next_seq());
        let want = end - from_seq;
        let allow = (self.tokens / NANO).min(u128::from(want)) as u64;
        self.stats.throttled_msgs += want - allow;
        self.tokens -= u128::from(allow) * NANO;

        let batch = self.cfg.max_batch.clamp(1, MAX_MESSAGES_PER_PACKET) as u64;
        let stop = from_seq + allow;
        let mut s = from_seq;
        while s < stop {
            let n = (stop - s).min(batch);
            self.scratch.clear();
            for q in s..s + n {
                self.scratch
                    .push(log.get(q).expect("range checked against the window"));
            }
            let len = encode_data(
                &mut self.buf,
                self.session_id,
                s,
                Flags::RETRANSMIT,
                now,
                &self.scratch,
            )
            .expect("batch is within packet limits");
            emit(&self.buf[..len]);
            self.stats.repair_packets += 1;
            self.stats.served_msgs += n;
            s += n;
        }
    }

    fn refill(&mut self, now: Nanos) {
        let burst = u128::from(self.cfg.burst_msgs) * NANO;
        if let Some(last) = self.last {
            let elapsed = u128::from(now.saturating_sub(last));
            self.tokens =
                (self.tokens + elapsed * u128::from(self.cfg.rate_msgs_per_sec)).min(burst);
        }
        self.last = Some(now);
    }
}
