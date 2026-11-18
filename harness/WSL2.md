# Running real multicast on WSL2

Goal: a sender and one or more receivers exchanging real UDP multicast packets on your
laptop, with NAK repair, inside one WSL2 Linux VM.

## 1. One-time setup

In **PowerShell (admin)**:

```powershell
wsl --install -d Ubuntu-24.04     # reboot if asked, then create your Linux user
wsl --update
wsl --status                      # "Default Version: 2"
```

Inside **Ubuntu**:

```sh
sudo apt update && sudo apt install -y build-essential iproute2 iputils-ping tcpdump
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh   # accept defaults
source ~/.cargo/env
```

Keep the repo on the Linux filesystem (`~/mdbus`), not under `/mnt/c`, for build speed.

```sh
cd ~/mdbus && cargo test --workspace      # portable tests first
cargo build --release -p demo
```

## 2. Find the interface to use

WSL2's `lo` has no MULTICAST flag by default, so use `eth0`:

```sh
ip -br link show eth0            # flags must include MULTICAST
ip -4 -br addr show eth0         # note the IP, e.g. 172.20.1.5
```

Put it in a variable (replace with yours):

```sh
export IF=172.20.1.5
```

## 3. Run it (three terminals, all inside WSL)

Start receivers **first** (a multicast join only sees packets sent after it):

```sh
# terminal 1
./target/release/receiver --iface $IF --nak-to 127.0.0.1:5001 --count 100000
# terminal 2 (optional second subscriber; the NAK addr is the same, repairs go to whoever asked)
./target/release/receiver --iface $IF --nak-to 127.0.0.1:5001 --count 100000
# terminal 3
./target/release/sender --iface $IF --count 100000 --rate 20000
```

Clean run: each receiver ends with `order_violations=0 payload_mismatches=0` and prints `PASS`.

Make it lossy to see recovery happen (the sender drops 2% of live packets before they hit the
wire, and the receivers must NAK for them):

```sh
./target/release/sender --iface $IF --count 100000 --rate 20000 --drop-pct 2
```

Expected: receivers show `naks_sent > 0`, `repaired > 0`, final `PASS`; the sender prints
`injected_drops=...` and retransmit stats with `served_msgs > 0`.

Watch the packets: `sudo tcpdump -ni eth0 'udp and (port 5000 or port 5001)'`.

## 4. If nothing arrives

| symptom | check |
|---|---|
| `join_multicast_v4` error | wrong `--iface` (must be a local IP), or interface lacks MULTICAST |
| receivers see nothing | started after the sender? same `--group` on both? `ip maddr show eth0` should list 239.255.42.1 |
| gets packets but never NAKs get answered | `--nak-to` must reach the sender's `--nak-bind` port (default 5001) |
| drops under high `--rate` | raise `sudo sysctl -w net.core.rmem_max=8388608`, then check `effective=` in the receiver's first line; `nstat -az UdpRcvbufErrors` shows kernel drops |

## 5. What this does and does not prove

* **Real:** kernel UDP stack, real multicast join/send/receive, real socket buffers, real NAK and
  repair datagrams.
* **Not a wire test:** everything is inside one VM on a virtual NIC, so numbers are VM numbers.
  Label any measurement "WSL2 / eth0, same VM".
* **netem will not impair this setup.** For same-host receivers the kernel loops multicast back
  internally before the packet reaches the qdisc, so `tc qdisc ... netem` on `eth0` does not touch
  the looped copy. That is why the sender has `--drop-pct`. Real netem needs the sender and
  receiver in separate network namespaces joined by a bridge (the next `harness/` step).
* **Windows <-> WSL2 multicast** is not covered. In default NAT mode WSL2 sits behind a virtual
  switch that does not forward multicast to the Windows host or LAN. Keep sender and receivers
  inside WSL.
* Sender and receiver clocks are the same host's, so one-way latency would be measurable later.
