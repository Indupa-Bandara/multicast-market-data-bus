# Receiver state machine (`recovery` crate)

Sans-IO: no sockets, threads or clocks inside. The caller passes the current time with every
call and executes the returned `Action`s.

```
on_datagram(now, bytes, &mut Vec<Action>)   // or on_packet with a parsed Packet
on_tick(now, &mut Vec<Action>)              // call when now >= next_deadline()
next_deadline() -> Option<Nanos>            // when the next tick has work to do
```

Actions: `Deliver{seq,msg}`, `SendNak{session_id,from_seq,count}`, `Resync(reason)`, `Stale`.

## States (derived, `Receiver::state()`)

| state       | meaning |
|-------------|---------|
| Init        | nothing joined yet, or just resynced; next packet joins |
| Live        | `expected` seq arrives, delivered immediately |
| GapRecovery | something is missing; later messages are buffered, **not delivered** (hold-at-gap) |
| Stale       | no packet or heartbeat for `stale_timeout`; `Action::Stale` fires once |

RESYNC from the architecture review is modelled as an *event* (`Action::Resync`) that returns the
receiver to Init, because there is no snapshot to wait for in the MVP.

## Rules

* **Join:** `JoinPolicy::FromSeq(1)` (default) starts delivery at seq 1 and NAKs for anything
  before the first packet seen. `FirstSeen` starts wherever the first packet is.
* **Delivery:** a message is delivered only when its seq equals `expected`; buffered messages
  drain as soon as they become contiguous. Messages below `expected` are duplicates and dropped.
* **Gap detection:** `high` = one past the highest seq known to exist (from DATA ends and
  HEARTBEAT `seq`). A gap exists while `expected < high`. A heartbeat ahead of `expected`
  therefore exposes tail loss.
* **NAK timing:** first NAK after `reorder_tolerance` (default 1 ms). Then rounds back off
  5 ms, 10 ms, 20 ms ... capped at `nak_max_backoff`. Each round requests only still-missing
  ranges, at most `max_nak_count` messages per NAK and `max_naks_per_round` NAKs per round.
* **Progress resets the retry budget.** Any increase of `expected` resets retries and backoff,
  so a long repair that keeps advancing never gives up.
* **Resync reasons:** `SessionChange` (sender restarted), `RetriesExhausted` (no progress after
  `max_nak_retries` rounds, e.g. data left the retransmit window), `BufferOverflow`,
  `GapTooLarge`. After any resync except `SessionChange` the receiver rejoins at the first
  sequence number it sees.
* **Consumer contract:** between two `Resync` actions, delivered seqs are consecutive. On
  `Resync` the consumer must discard derived state (the book). Without snapshots (stretch goal)
  levels that were never updated again stay stale, so a resynced book is only trustworthy
  once snapshots exist.

## Known simplifications (candidates for later)

* The reorder buffer is a `BTreeMap` (allocates per out-of-order message). A preallocated
  ring + bitmap is the hot-path version; the existing tests and the simulation are the oracle
  for swapping it.
* One NAK schedule per gap *episode*, not per gap. A second gap appearing mid-recovery can be
  NAKed slightly before its own reorder tolerance has elapsed (harmless extra NAK).
* No explicit "unavailable" reply exists in wire v1. The sender's answer to an out-of-window NAK
  is silence, and the receiver learns that via `RetriesExhausted`. Adding an UNAVAILABLE packet
  type would make this faster.

## Tests

`recovery/tests/recovery_tests.rs`: scenario tests for each rule above, plus a deterministic
simulation (proptest) with random loss, reordering and duplication where a sender answers NAKs.
It asserts delivered == 1..=N exactly (safety and liveness). Deliberately breaking the drain
or hold-at-gap logic makes several of these tests fail.
