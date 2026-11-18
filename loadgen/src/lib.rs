//! Open-loop load generator (fixed send schedule, latency measured from scheduled time).
//!
//! The schedule never catches up by emitting a burst after a slow poll. If work misses
//! its deadline, it remains one missed sample and the next send is at the following
//! fixed scheduled time. This avoids coordinated omission in latency measurements.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeMap;

/// Time in nanoseconds.
pub type Nanos = u64;

/// Fixed-schedule open-loop load configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadConfig {
    /// Number of messages to schedule.
    pub count: u64,
    /// Messages per second.
    pub rate: u64,
}

impl LoadConfig {
    /// Validate and return the interval between scheduled sends.
    pub fn interval_ns(&self) -> Option<Nanos> {
        if self.count == 0 || self.rate == 0 {
            return None;
        }
        Some(1_000_000_000 / self.rate)
    }
}

/// One scheduled message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scheduled {
    /// Sequence number, starting at 1.
    pub seq: u64,
    /// Fixed scheduled send time.
    pub scheduled_at: Nanos,
}

/// Open-loop send schedule.
#[derive(Debug, Clone)]
pub struct OpenLoop {
    config: LoadConfig,
    start: Nanos,
    interval: Nanos,
    next_index: u64,
    next_at: Nanos,
}

impl OpenLoop {
    /// Create a schedule at `start`.
    pub fn new(start: Nanos, config: LoadConfig) -> Option<Self> {
        let interval = config.interval_ns()?;
        Some(OpenLoop {
            config,
            start,
            interval,
            next_index: 0,
            next_at: start,
        })
    }

    /// Configuration.
    pub fn config(&self) -> LoadConfig {
        self.config
    }

    /// Number of messages already emitted.
    pub fn sent(&self) -> u64 {
        self.next_index
    }

    /// Next scheduled deadline, if the configured count has not been exhausted.
    pub fn next_deadline(&self) -> Option<Nanos> {
        (self.next_index < self.config.count).then_some(self.next_at)
    }

    /// Return the next scheduled sample whose deadline has arrived.
    ///
    /// The returned sample is advanced once only. A caller that is late therefore misses
    /// intervening deadlines instead of generating a catch-up burst.
    pub fn poll(&mut self, now: Nanos) -> Option<Scheduled> {
        if self.next_index >= self.config.count || now < self.next_at {
            return None;
        }
        let sample = Scheduled {
            seq: self.next_index + 1,
            scheduled_at: self.next_at,
        };
        self.next_index += 1;
        self.next_at = self
            .start
            .saturating_add(self.next_index.saturating_mul(self.interval));
        Some(sample)
    }
}

/// Latency histogram in nanoseconds, with exact counts for percentile reporting.
#[derive(Debug, Clone, Default)]
pub struct LatencyHistogram {
    buckets: BTreeMap<Nanos, u64>,
    count: u64,
    sum: u128,
    min: Option<Nanos>,
    max: Option<Nanos>,
}

impl LatencyHistogram {
    /// Create an empty histogram.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one observed latency.
    pub fn record(&mut self, latency: Nanos) {
        *self.buckets.entry(latency).or_insert(0) += 1;
        self.count = self.count.saturating_add(1);
        self.sum = self.sum.saturating_add(latency as u128);
        self.min = Some(self.min.map_or(latency, |v| v.min(latency)));
        self.max = Some(self.max.map_or(latency, |v| v.max(latency)));
    }

    /// Record end time minus scheduled send time.
    pub fn record_scheduled(&mut self, scheduled: Nanos, arrived: Nanos) {
        self.record(arrived.saturating_sub(scheduled));
    }

    /// Number of samples.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Minimum latency.
    pub fn min(&self) -> Option<Nanos> {
        self.min
    }

    /// Maximum latency.
    pub fn max(&self) -> Option<Nanos> {
        self.max
    }

    /// Mean latency.
    pub fn mean(&self) -> Option<f64> {
        (self.count != 0).then(|| self.sum as f64 / self.count as f64)
    }

    /// Approximate percentile using a nearest-rank selection.
    pub fn percentile(&self, percentile: u32) -> Option<Nanos> {
        if self.count == 0 || percentile == 0 || percentile > 100 {
            return None;
        }
        let rank = ((self.count as u128 * percentile as u128).div_ceil(100)).max(1);
        let mut seen = 0u128;
        for (&latency, &bucket_count) in &self.buckets {
            seen += bucket_count as u128;
            if seen >= rank {
                return Some(latency);
            }
        }
        self.max
    }

    /// p50 latency.
    pub fn p50(&self) -> Option<Nanos> {
        self.percentile(50)
    }
    /// p99 latency.
    pub fn p99(&self) -> Option<Nanos> {
        self.percentile(99)
    }
    /// p99.9 latency, rounded to the nearest percent-unit as supported by this API.
    pub fn p999(&self) -> Option<Nanos> {
        self.percentile_for_thousandths(999)
    }
    /// p99.99 latency, rounded to the nearest percent-unit as supported by this API.
    pub fn p9999(&self) -> Option<Nanos> {
        self.percentile_for_thousandths(9999)
    }

    fn percentile_for_thousandths(&self, percentile_thousandths: u32) -> Option<Nanos> {
        if self.count == 0 {
            return None;
        }
        let denominator = if percentile_thousandths == 999 {
            1_000
        } else {
            10_000
        };
        let rank = (self.count as u128 * percentile_thousandths as u128)
            .div_ceil(denominator)
            .max(1);
        let mut seen = 0u128;
        for (&latency, &bucket_count) in &self.buckets {
            seen += bucket_count as u128;
            if seen >= rank {
                return Some(latency);
            }
        }
        self.max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_loop_advances_without_catch_up_burst() {
        let mut load = OpenLoop::new(100, LoadConfig { count: 3, rate: 10 }).unwrap();
        assert_eq!(load.poll(99), None);
        assert_eq!(
            load.poll(100),
            Some(Scheduled {
                seq: 1,
                scheduled_at: 100
            })
        );
        assert_eq!(load.poll(100), None);
        assert_eq!(
            load.poll(300_000_000),
            Some(Scheduled {
                seq: 2,
                scheduled_at: 100_000_100
            })
        );
        assert_eq!(load.sent(), 2);
    }

    #[test]
    fn invalid_load_config_is_rejected() {
        assert!(OpenLoop::new(0, LoadConfig { count: 0, rate: 1 }).is_none());
        assert!(OpenLoop::new(0, LoadConfig { count: 1, rate: 0 }).is_none());
    }

    #[test]
    fn latency_percentiles_report_samples() {
        let mut h = LatencyHistogram::new();
        h.record_scheduled(100, 110);
        h.record_scheduled(200, 205);
        h.record_scheduled(300, 330);
        h.record_scheduled(400, 440);
        assert_eq!(h.count(), 4);
        assert_eq!(h.min(), Some(5));
        assert_eq!(h.max(), Some(40));
        assert_eq!(h.p50(), Some(10));
        assert_eq!(h.p99(), Some(40));
        assert_eq!(h.p999(), Some(40));
        assert_eq!(h.p9999(), Some(40));
    }
}
