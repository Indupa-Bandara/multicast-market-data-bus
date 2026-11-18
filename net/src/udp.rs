//! Real UDP endpoints: unicast (NAK / repair path) and IPv4 multicast (live feed).
//!
//! All sockets are non-blocking: `recv` returns `Ok(None)` when nothing is waiting, so the
//! caller owns the polling policy (spin, sleep, epoll, later io_uring). Nothing here blocks
//! the hot path.
//!
//! Multicast needs a multicast-capable interface. On Linux loopback that usually means
//! `ip link set lo multicast on` plus a route, or use a veth/bridge (see `harness/`).

use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};

use socket2::{Domain, Protocol, Socket, Type};

/// Largest payload that avoids IP fragmentation on a 1500-byte MTU (1500 - 20 IP - 8 UDP).
pub const MAX_DATAGRAM: usize = 1472;

/// Minimal datagram interface shared by real and (later) simulated transports.
pub trait Datagram {
    /// Send one datagram to the configured peer.
    fn send(&mut self, bytes: &[u8]) -> io::Result<()>;
    /// Receive one datagram into `buf`; `Ok(None)` if none is waiting.
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>>;
}

/// A non-blocking UDP socket with an optional default peer.
#[derive(Debug)]
pub struct UdpEndpoint {
    sock: UdpSocket,
    peer: Option<SocketAddr>,
}

impl UdpEndpoint {
    /// Unicast socket bound to `bind`, sending to `peer` when `send` is used.
    pub fn unicast(bind: SocketAddr, peer: Option<SocketAddr>) -> io::Result<Self> {
        let sock = UdpSocket::bind(bind)?;
        sock.set_nonblocking(true)?;
        Ok(UdpEndpoint { sock, peer })
    }

    /// Multicast sender for `group`, leaving through interface `iface`.
    ///
    /// `ttl` bounds how many routers the feed may cross; `loopback` lets receivers on the
    /// same host see it (needed for single-host tests).
    pub fn multicast_sender(
        group: SocketAddrV4,
        iface: Ipv4Addr,
        ttl: u32,
        loopback: bool,
    ) -> io::Result<Self> {
        let s = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        s.bind(&SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)).into())?;
        s.set_multicast_if_v4(&iface)?;
        s.set_multicast_ttl_v4(ttl)?;
        s.set_multicast_loop_v4(loopback)?;
        s.set_nonblocking(true)?;
        Ok(UdpEndpoint {
            sock: s.into(),
            peer: Some(SocketAddr::V4(group)),
        })
    }

    /// Multicast receiver joined to `group` on interface `iface`.
    ///
    /// `rcvbuf` requests the kernel receive buffer in bytes (`SO_RCVBUF`). The kernel clamps it
    /// to `net.core.rmem_max`; read the real value back with [`Self::recv_buffer_size`].
    /// Overflow here is the most common real source of UDP loss.
    pub fn multicast_receiver(
        group: SocketAddrV4,
        iface: Ipv4Addr,
        rcvbuf: Option<usize>,
    ) -> io::Result<Self> {
        let s = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        s.set_reuse_address(true)?; // several receivers on one host may share the port
        if let Some(n) = rcvbuf {
            s.set_recv_buffer_size(n)?;
        }
        // Linux/BSD: binding to the group address filters out other traffic on this port.
        // Windows cannot bind to a multicast address, so use the wildcard there.
        let bind_ip = if cfg!(windows) {
            Ipv4Addr::UNSPECIFIED
        } else {
            *group.ip()
        };
        s.bind(&SocketAddr::V4(SocketAddrV4::new(bind_ip, group.port())).into())?;
        s.join_multicast_v4(group.ip(), &iface)?;
        s.set_nonblocking(true)?;
        Ok(UdpEndpoint {
            sock: s.into(),
            peer: None,
        })
    }

    /// Address this socket is bound to (useful after binding port 0).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Effective kernel receive buffer in bytes (Linux reports double the requested value).
    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        socket2::SockRef::from(&self.sock).recv_buffer_size()
    }

    /// Send to an explicit address (repair replies go to the NAK's source).
    pub fn send_to(&self, bytes: &[u8], to: SocketAddr) -> io::Result<()> {
        self.check_len(bytes)?;
        self.sock.send_to(bytes, to).map(|_| ())
    }

    /// Receive one datagram and its source; `Ok(None)` if none is waiting.
    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(usize, SocketAddr)>> {
        match self.sock.recv_from(buf) {
            Ok(x) => Ok(Some(x)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn check_len(&self, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() > MAX_DATAGRAM {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "datagram exceeds MAX_DATAGRAM (would fragment)",
            ));
        }
        Ok(())
    }
}

impl Datagram for UdpEndpoint {
    fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        let to = self
            .peer
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "no peer configured"))?;
        self.send_to(bytes, to)
    }

    fn recv(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>> {
        Ok(self.recv_from(buf)?.map(|(n, _)| n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn lo() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    /// Poll until a datagram arrives or 2 s pass.
    fn wait_recv(ep: &mut UdpEndpoint, buf: &mut [u8]) -> Option<usize> {
        let end = Instant::now() + Duration::from_secs(2);
        while Instant::now() < end {
            if let Some(n) = ep.recv(buf).unwrap() {
                return Some(n);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        None
    }

    #[test]
    fn recv_is_nonblocking_when_empty() {
        let mut a = UdpEndpoint::unicast(lo(), None).unwrap();
        let mut buf = [0u8; 64];
        assert_eq!(a.recv(&mut buf).unwrap(), None);
    }

    #[test]
    fn unicast_roundtrip_and_reply_to_source() {
        let server = UdpEndpoint::unicast(lo(), None).unwrap();
        let saddr = server.local_addr().unwrap();
        let mut client = UdpEndpoint::unicast(lo(), Some(saddr)).unwrap();

        client.send(b"nak").unwrap();
        let mut buf = [0u8; 64];
        let end = Instant::now() + Duration::from_secs(2);
        let (n, from) = loop {
            if let Some(x) = server.recv_from(&mut buf).unwrap() {
                break x;
            }
            assert!(Instant::now() < end, "timed out");
            std::thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(&buf[..n], b"nak");
        server.send_to(b"repair", from).unwrap();
        let n = wait_recv(&mut client, &mut buf).expect("reply");
        assert_eq!(&buf[..n], b"repair");
    }

    #[test]
    fn oversize_datagram_is_rejected_before_sending() {
        let mut c = UdpEndpoint::unicast(lo(), Some("127.0.0.1:9".parse().unwrap())).unwrap();
        let big = vec![0u8; MAX_DATAGRAM + 1];
        assert_eq!(
            c.send(&big).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(c.send(&vec![0u8; MAX_DATAGRAM]).is_ok());
    }

    #[test]
    fn send_without_peer_errors() {
        let mut c = UdpEndpoint::unicast(lo(), None).unwrap();
        assert_eq!(
            c.send(b"x").unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
    }

    /// Needs a multicast-capable interface, so it is opt-in: `cargo test -p net -- --ignored`.
    #[test]
    #[ignore]
    fn multicast_two_receivers_both_get_the_packet() {
        let group = SocketAddrV4::new(Ipv4Addr::new(239, 255, 42, 1), 45_678);
        // Set MDBUS_IFACE to your eth0 IP on WSL2 (see harness/WSL2.md); lo needs `multicast on`.
        let iface: Ipv4Addr = std::env::var("MDBUS_IFACE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(Ipv4Addr::LOCALHOST);
        let mut r1 = UdpEndpoint::multicast_receiver(group, iface, Some(1 << 20)).unwrap();
        let mut r2 = UdpEndpoint::multicast_receiver(group, iface, None).unwrap();
        let mut tx = UdpEndpoint::multicast_sender(group, iface, 1, true).unwrap();
        assert!(r1.recv_buffer_size().unwrap() > 0);
        tx.send(b"tick").unwrap();
        let mut buf = [0u8; 64];
        for r in [&mut r1, &mut r2] {
            let n = wait_recv(r, &mut buf).expect("multicast delivery");
            assert_eq!(&buf[..n], b"tick");
        }
    }
}
