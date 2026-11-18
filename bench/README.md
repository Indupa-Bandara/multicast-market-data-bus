## Benchmark methodology

Use an open-loop fixed schedule. The `loadgen` crate intentionally does not catch up with a
burst after the runner stalls; missed deadlines remain visible in latency data instead of
coordinated omission hiding slow periods.

Run warm-ups, repeated runs, and report variance. Record CPU model, kernel version, CPU
governor, isolation settings, and whether the environment is a VM, veth, loopback, or real
NIC. Loopback and veth numbers are not wire numbers.

Sweep:

- loss: 0, 0.1, 1, and 5% (random and Gilbert-Elliott burst loss)
- message rate and message size
- batching delay
- blocking receive versus later `recvmmsg`/io_uring implementations

Report throughput, p50/p99/p99.9/p99.99/max latency, recovery time, NAK count, retransmit
bandwidth, CPU use, memory use, decode errors, ring overruns, and receiver resync count.
