//! `server healthcheck`: a minimal HTTP client for the Docker healthcheck, so
//! the runtime image needs no curl or wget.

use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpStream},
    time::Duration,
};

/// Requests `/readyz` from the local server. True only for a 200.
pub fn check(bind_addr: SocketAddr) -> bool {
    // The server may listen on 0.0.0.0; the check always goes to loopback.
    let target = SocketAddr::from((Ipv4Addr::LOCALHOST, bind_addr.port()));
    let attempt = || -> std::io::Result<bool> {
        let timeout = Duration::from_secs(2);
        let mut stream = TcpStream::connect_timeout(&target, timeout)?;
        stream.set_read_timeout(Some(timeout))?;
        stream
            .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
        let mut status_line = [0u8; 12];
        stream.read_exact(&mut status_line)?;
        // "HTTP/1.1 200"
        Ok(status_line.ends_with(b" 200"))
    };
    attempt().unwrap_or(false)
}
