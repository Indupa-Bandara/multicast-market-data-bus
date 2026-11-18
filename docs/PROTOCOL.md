# Wire protocol v1

Little-endian, fixed layout. One UDP datagram = one packet, at most 1472 bytes of payload
(1500 MTU - 20 IP - 8 UDP) so nothing is ever fragmented.

## Header (32 bytes)

| off | size | field       | notes |
|----:|-----:|-------------|-------|
| 0   | 2    | magic       | `0x424D` (bytes `4D 42`) |
| 2   | 1    | version     | `1` |
| 3   | 1    | type        | 1 DATA, 2 HEARTBEAT, 3 NAK |
| 4   | 2    | flags       | bit0 RETRANSMIT, bit1 LINE_B (reserved), bit2 END_OF_SESSION; other bits must be 0 |
| 6   | 2    | count       | DATA: messages in packet (1..=60). HEARTBEAT: 0. NAK: messages requested (>=1) |
| 8   | 8    | session_id  | changes on sender restart; receivers must resync when it changes |
| 16  | 8    | seq         | DATA: seq of first message. HEARTBEAT: *next* seq the sender will assign. NAK: first missing seq |
| 24  | 8    | send_ts_ns  | sender monotonic ns. NAK: must be 0 |

## Packet types

* **DATA**: header + `count` x 24-byte `L2Update`. Message `i` has sequence number `seq + i`.
  A retransmission is a DATA packet with the RETRANSMIT flag. Sequence numbers are per
  message, not per packet, so a repair may re-pack the range differently from the original.
* **HEARTBEAT**: header only. Lets a receiver distinguish "quiet" from "dead" and detect
  tail loss (`seq` > receiver's expected seq means the last packets were lost).
* **NAK**: header only, receiver -> sender (unicast). Requests `[seq, seq + count)`.
  RETRANSMIT is not allowed on NAK or HEARTBEAT.

## L2Update (24 bytes)

| off | size | field         | notes |
|----:|-----:|---------------|-------|
| 0   | 4    | instrument_id | u32 |
| 4   | 1    | side          | 0 bid, 1 ask |
| 5   | 1    | action        | 0 set level, 1 delete level |
| 6   | 2    | reserved      | must be 0 |
| 8   | 8    | price         | i64 fixed-point ticks |
| 16  | 8    | qty           | u64; must be 0 when action = delete |

## Validation (all enforced by `Packet::parse`)

Reject if: length < 32 or > 1472; bad magic/version/type; undefined flag bits; count invalid
for the type; length != 32 + count*24 (DATA) or != 32 (others); `seq + count` overflows;
any message has an undefined enum value or non-zero reserved bytes; NAK timestamp != 0.
The encoding is canonical: every accepted packet re-encodes to the same bytes.

## Compatibility

Any change to the layout bumps `VERSION`. `golden_data_packet` in `wire/tests` pins the bytes.
Sequence numbers are u64, so no wraparound handling is needed.
