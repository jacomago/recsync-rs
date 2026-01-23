// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::net::UdpSocket;
use tokio::time::{sleep, Duration};
use std::io;

use wire::SERVER_ANNOUNCEMENT_UDP_PORT;

/// Announcer broadcasts the presence of the recceiver server via UDP.
pub struct Announcer {
    socket: UdpSocket,
    server_addr: Ipv4Addr,
    server_port: u16,
    server_key: u32,
}

impl Announcer {
    /// Creates a new Announcer.
    ///
    /// Binds to `0.0.0.0:SERVER_ANNOUNCEMENT_UDP_PORT` to listen for broadcast messages.
    ///
    /// # Arguments
    ///
    /// * `server_addr` - The IP address of the recceiver server.
    /// * `server_port` - The TCP port of the recceiver server.
    /// * `server_key` - A unique key identifying the recceiver server.
    ///
    pub async fn new(
        server_addr: Ipv4Addr,
        server_port: u16,
        server_key: u32,
    ) -> io::Result<Self> {
        let broadcast_addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0);
        let socket = UdpSocket::bind(broadcast_addr).await?;
        socket.set_broadcast(true)?;

        Ok(Self {
            socket,
            server_addr,
            server_port,
            server_key,
        })
    }

    /// Starts broadcasting the server's presence.
    ///
    /// This method runs indefinitely, sending UDP broadcast messages every 5 seconds.
    pub async fn start_broadcasting(&self) -> io::Result<()> {
        let broadcast_target = SocketAddrV4::new(Ipv4Addr::new(255, 255, 255, 255), SERVER_ANNOUNCEMENT_UDP_PORT);
        let announcement_id: u16 = 0x5243; // "RC" in ASCII

        loop {
            let mut buf = Vec::new();
            buf.extend_from_slice(&announcement_id.to_be_bytes()); // ID (H)
            buf.extend_from_slice(&0u16.to_be_bytes()); // Padding (H)
            buf.extend_from_slice(&self.server_addr.octets()); // Server IP (4s)
            buf.extend_from_slice(&self.server_port.to_be_bytes()); // Server Port (H)
            buf.extend_from_slice(&0u16.to_be_bytes()); // Padding (H)
            buf.extend_from_slice(&self.server_key.to_be_bytes()); // Server Key (I)

            self.socket.send_to(&buf, broadcast_target).await?;
            sleep(Duration::from_secs(5)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_announcer_broadcast() {
        let server_addr = Ipv4Addr::new(127, 0, 0, 1);
        let server_port = 12345;
        let server_key = 54321;

        let announcer = Announcer::new(server_addr, server_port, server_key)
            .await
            .unwrap();

        let listener = UdpSocket::bind(format!("0.0.0.0:{}", SERVER_ANNOUNCEMENT_UDP_PORT))
            .await
            .unwrap();
        listener.set_broadcast(true).unwrap();

        tokio::spawn(async move {
            announcer.start_broadcasting().await.unwrap();
        });

        let mut recv_buf = [0u8; 1024];
        let (_len, _) = timeout(Duration::from_secs(6), listener.recv_from(&mut recv_buf))
            .await
            .unwrap()
            .unwrap();

        let expected_id: u16 = 0x5243;
        let announcement_id = u16::from_be_bytes([recv_buf[0], recv_buf[1]]);
        assert_eq!(announcement_id, expected_id);

        // Skip 2 bytes for the first padding (H)
        let announced_addr = Ipv4Addr::new(recv_buf[4], recv_buf[5], recv_buf[6], recv_buf[7]);
        assert_eq!(announced_addr, server_addr);

        let announced_port = u16::from_be_bytes([recv_buf[8], recv_buf[9]]);
        assert_eq!(announced_port, server_port);

        // Skip 2 bytes for the second padding (H)
        let announced_key = u32::from_be_bytes([recv_buf[12], recv_buf[13], recv_buf[14], recv_buf[15]]);
        assert_eq!(announced_key, server_key);
    }
}
