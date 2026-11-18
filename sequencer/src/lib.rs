//! Sender-side sequencer: gives messages gap-free sequence numbers (starting at 1),
//! packs them into packets, emits heartbeats when idle, and records everything sent
//! in a [`RetxLog`] so NAKs can be answered.
//!
//! Sans-IO like the receiver: you pass the time in and receive finished datagrams
//! through a callback, so there is no allocation per packet and everything is
//! deterministic in tests.
//!
//! Batching policy (configurable, and worth reporting in benchmarks): a packet is
//! flushed when it holds `max_batch` messages or when the oldest pending message has
//! waited `max_wait`. Larger batches mean fewer packets per second at the cost of latency.
//!
//! Messages are added to the log when their packet is *sent*, so the log is exactly
//! what receivers could have seen, and heartbeats advertise only sent sequence numbers
//! (never messages still waiting in the batch).
#![forbid(unsafe_code)]
#![deny(missing_docs)]

use retx_log::RetxLog;
use wire::{
    encode_data, encode_heartbeat, Flags, L2Update, MAX_MESSAGES_PER_PACKET, MAX_PACKET_LEN,
};

/// Time in nanoseconds on any monotonic clock the caller chooses.
pub type Nanos = u64;

/// Sequencer tunables.
#[derive(Debug, Clone)]
pub struct Config {
    /// Session id stamped on every packet. Must change on sender restart.
    pub session_id: u64,
    /// Flush when this many messages are pending (capped at the protocol max of 60).
    pub max_batch: usize,
    /// Flush when the oldest pending message has waited this long.
    pub max_wait: Nanos,
    /// Send a heartbeat if nothing has been sent for this long.
    pub heartbeat_interval: Nanos,
    /// How many recent messages the retransmit log keeps.
    pub log_capacity: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            session_id: 1,
            max_batch: MAX_MESSAGES_PER_PACKET,
            max_wait: 100_000, // 100 us
            heartbeat_interval: 1_000_000_000,
            log_capacity: 1 << 20,
        }
    }
}

/// Counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    /// DATA packets sent.
    pub data_packets: u64,
    /// Messages sent.
    pub messages: u64,
    /// Heartbeats sent.
    pub heartbeats: u64,
    /// Packets flushed because the batch filled.
    pub full_flushes: u64,
    /// Packets flushed because `max_wait` elapsed (or an explicit flush).
    pub timed_flushes: u64,
}

/// The sequencer. See the crate docs.
#[derive(Debug)]
pub struct Sequencer {
    cfg: Config,
    log: RetxLog,
    pending: Vec<L2Update>,
    first_pending_at: Option<Nanos>,
    last_send: Nanos,
    buf: [u8; MAX_PACKET_LEN],
    stats: Stats,
}

impl Sequencer {
    /// Create a sequencer. `now` starts the heartbeat clock.
    pub fn new(mut cfg: Config, now: Nanos) -> Self {
        cfg.max_batch = cfg.max_batch.clamp(1, MAX_MESSAGES_PER_PACKET);
        Sequencer {
            log: RetxLog::new(cfg.log_capacity),
            pending: Vec::with_capacity(cfg.max_batch),
            first_pending_at: None,
            last_send: now,
            buf: [0; MAX_PACKET_LEN],
            stats: Stats::default(),
            cfg,
        }
    }

    /// Session id.
    pub fn session_id(&self) -> u64 {
        self.cfg.session_id
    }

    /// The retransmit log (hand this to the retransmit server).
    pub fn log(&self) -> &RetxLog {
        &self.log
    }

    /// Next sequence number that will be put on the wire.
    pub fn next_seq(&self) -> u64 {
        self.log.next_seq()
    }

    /// Counters.
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Queue a message. May emit a packet if the batch is now full.
    pub fn push<F: FnMut(&[u8])>(&mut self, now: Nanos, msg: L2Update, mut emit: F) {
        if self.pending.is_empty() {
            self.first_pending_at = Some(now);
        }
        self.pending.push(msg);
        if self.pending.len() >= self.cfg.max_batch {
            self.stats.full_flushes += 1;
            self.flush_inner(now, &mut emit);
        }
    }

    /// Send whatever is pending right now.
    pub fn flush<F: FnMut(&[u8])>(&mut self, now: Nanos, mut emit: F) {
        if !self.pending.is_empty() {
            self.stats.timed_flushes += 1;
        }
        self.flush_inner(now, &mut emit);
    }

    /// Advance timers: flush an overdue batch, then send a heartbeat if idle.
    pub fn tick<F: FnMut(&[u8])>(&mut self, now: Nanos, mut emit: F) {
        if let Some(t) = self.first_pending_at {
            if now.saturating_sub(t) >= self.cfg.max_wait {
                self.stats.timed_flushes += 1;
                self.flush_inner(now, &mut emit);
            }
        }
        if now.saturating_sub(self.last_send) >= self.cfg.heartbeat_interval {
            let n = encode_heartbeat(
                &mut self.buf,
                self.cfg.session_id,
                self.log.next_seq(),
                Flags::NONE,
                now,
            )
            .expect("buffer holds a header");
            self.last_send = now;
            self.stats.heartbeats += 1;
            emit(&self.buf[..n]);
        }
    }

    /// Earliest time `tick` has work to do.
    pub fn next_deadline(&self) -> Option<Nanos> {
        let hb = self.last_send.saturating_add(self.cfg.heartbeat_interval);
        let batch = self
            .first_pending_at
            .map(|t| t.saturating_add(self.cfg.max_wait));
        Some(batch.map_or(hb, |b| b.min(hb)))
    }

    fn flush_inner(&mut self, now: Nanos, emit: &mut dyn FnMut(&[u8])) {
        if self.pending.is_empty() {
            return;
        }
        let first_seq = self.log.next_seq();
        let n = encode_data(
            &mut self.buf,
            self.cfg.session_id,
            first_seq,
            Flags::NONE,
            now,
            &self.pending,
        )
        .expect("batch is within packet limits");
        for m in &self.pending {
            self.log.append(*m);
        }
        self.stats.data_packets += 1;
        self.stats.messages += self.pending.len() as u64;
        self.pending.clear();
        self.first_pending_at = None;
        self.last_send = now;
        emit(&self.buf[..n]);
    }
}
