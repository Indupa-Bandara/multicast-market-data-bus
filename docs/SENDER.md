# Sender side and the simulated network

## `sequencer`

Gives messages gap-free sequence numbers starting at 1, packs them into DATA packets, sends
heartbeats when idle, and records what it sent in the retransmit log. Sans-IO: pass `now` in,
finished datagrams come out through a callback (no per-packet allocation).

Batching policy (report it with every benchmark): flush when `max_batch` messages are pending
(max 60) or the oldest pending message has waited `max_wait`. Bigger batches = fewer packets per
second, more latency.

Two invariants the tests enforce:
* The log holds exactly what was *sent*. Messages still waiting in a batch are not in it.
* A heartbeat's `seq` is the next *sent* sequence number, never counting pending messages;
  otherwise receivers would NAK for data that does not exist yet.

## `retx-log`

* `RetxLog`: ring indexed by sequence number (in memory now; mmap later with the same API).
* `RetransmitServer`: answers a NAK with unicast DATA packets flagged RETRANSMIT, in batches.
  A token bucket (`rate_msgs_per_sec`, `burst_msgs`) caps repair bandwidth so NAK bursts cannot
  starve the live feed; when throttled it serves the lowest sequence numbers first so the receiver
  still makes contiguous progress.
* NAKs for the wrong session, for not-yet-sent data, or for data older than the ring get no reply.
  The receiver finds out by exhausting its retries and resyncing (an explicit UNAVAILABLE packet
  would make that faster; it is not in wire v1).

## `net::SimLink`

Deterministic one-way link: latency, jitter (reorders when larger than packet spacing), random or
Gilbert-Elliott burst loss, duplication, and a blackout window (late joiner). Everything is driven
by a seed (SplitMix64), so any failing run replays exactly. Real sockets live in `net::udp`
(`UdpEndpoint`: non-blocking unicast and IPv4 multicast, `SO_RCVBUF`, MTU-sized send check,
`Datagram` trait). The multicast test is `#[ignore]` because it needs a multicast-capable interface.

## End-to-end tests (`net/tests/e2e.rs`)

Virtual-time simulation: Sequencer -> downlink -> Receiver, NAKs over a lossy uplink, repairs over
a lossy repair link. Scenarios: clean, 1% loss, 5% loss on *every* path (10 seeds), bursty loss
(10 seeds), reordering/duplication only, determinism, retransmit window exceeded (late joiner ->
resync -> contiguous), sender restart (session change). Core assertion in the loss scenarios:
delivered == 1..=N exactly (no gaps, no duplicates, strict order, no resyncs).

## Not yet covered

* Threads, SPSC ring, `recvmmsg`/io_uring, busy polling (needs Linux for most of it).
* Loadgen and latency measurement; netem scenarios.
* Snapshots, checkpoints, A/B arbitration (stretch).
