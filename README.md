# mdbus: reliable multicast market-data bus (Rust)

One publisher, many subscribers, UDP multicast, NAK-based repair, strict in-order delivery.
Design: see the architecture review. Wire format: `docs/PROTOCOL.md`.

## Status (MVP, ~3 weeks)

| crate      | state |
|------------|-------|
| `wire`     | done: encode/decode, golden test, proptests, fuzz target |
| `recovery` | done: sans-IO receiver state machine (see `docs/RECOVERY.md`), 16 tests + simulation |
| `sequencer`, `retx-log` | done: batching, heartbeats, retransmit log, rate-limited NAK server (see `docs/SENDER.md`) |
| `net` | done for MVP: deterministic `SimLink` + e2e simulation tests; real non-blocking UDP unicast/multicast (`net::udp`). Threads, SPSC ring, `recvmmsg` not yet |
| `demo` | runnable `sender` and `receiver` binaries over real UDP multicast with NAK repair; see `harness/WSL2.md` |
| `loadgen` | done: fixed-schedule open-loop schedule and latency histograms |
| `spec/`    | TLA+ model (stretch) |

## Developing on macOS/Windows

Multicast, netem and isolcpus need Linux. `wire` and the sans-IO `recovery` crate are portable
and run natively. For anything with sockets or netem use a Linux VM (Lima/Multipass on macOS,
WSL2 on Windows), and label every benchmark number with the VM environment.

```sh
cargo test                       # everything portable
cargo clippy --all-targets -- -D warnings
cargo install cargo-fuzz         # needs nightly
cd wire && cargo +nightly fuzz run decode
```

## Prior art to cite

Nasdaq MoldUDP64, CME MDP 3.0, Cboe PITCH, Aeron, PGM (RFC 3208), NORM (RFC 5740).

## Limits (be honest in the write-up)

Loopback/veth numbers are not wire numbers. TLA+ checks the protocol logic, not the Rust code.
