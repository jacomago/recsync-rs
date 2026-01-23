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
        let broadcast_addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), SERVER_ANNOUNCEMENT_UDP_PORT);
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
            buf.extend_from_slice(&announcement_id.to_be_bytes()); // ID
            buf.extend_from_slice(&self.server_addr.octets()); // Server IP (4 bytes)
            buf.extend_from_slice(&self.server_port.to_be_bytes()); // Server Port (2 bytes)
            buf.extend_from_slice(&self.server_key.to_be_bytes()); // Server Key (4 bytes)
            buf.extend_from_slice(&[0, 0]); // Padding (2 bytes) - Python struct has H for this

            self.socket.send_to(&buf, broadcast_target).await?;
            sleep(Duration::from_secs(5)).await;
        }
    }
}
