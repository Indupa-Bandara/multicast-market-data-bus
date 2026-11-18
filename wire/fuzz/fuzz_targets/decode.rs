#![no_main]
use libfuzzer_sys::fuzz_target;
use wire::*;

// Properties: (1) parse never panics on any input; (2) iterating an accepted packet never
// panics; (3) anything accepted re-encodes to identical bytes (canonical encoding).
fuzz_target!(|data: &[u8]| {
    let Ok(p) = Packet::parse(data) else { return };
    let h = *p.header();
    let mut out = [0u8; MAX_PACKET_LEN];
    let n = match h.ptype {
        PacketType::Data => {
            let msgs: Vec<_> = p.messages().collect();
            encode_data(&mut out, h.session_id, h.seq, h.flags, h.send_ts_ns, &msgs).unwrap()
        }
        PacketType::Heartbeat => {
            encode_heartbeat(&mut out, h.session_id, h.seq, h.flags, h.send_ts_ns).unwrap()
        }
        PacketType::Nak => encode_nak(&mut out, h.session_id, h.seq, h.count).unwrap(),
    };
    assert_eq!(&out[..n], data);
});
