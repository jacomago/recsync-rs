// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use wire::{MessageCodec, Message, ClientGreet, ServerGreet, Ping, Pong};
use tracing::{debug, info, error};
use std::io;
use std::net::SocketAddr;
use futures_util::stream::StreamExt;
use futures_util::sink::SinkExt; 

/// Defines the different states of a client session.
#[derive(Debug, PartialEq)]
enum SessionState {
    Greeting,
    Upload,
    PingPong,
}

/// Session represents a single client connection and manages its state.
pub struct Session<S> {
    framed_stream: Framed<S, MessageCodec>,
    state: SessionState,
    client_key: Option<u32>,
    peer_addr: SocketAddr,
    // TODO: Add store/aggregation of IOC data here
}

impl<S> Session<S> 
where 
    S: AsyncRead + AsyncWrite + Unpin
{
    /// Creates a new session from a stream (e.g., TcpStream).
    pub fn new(stream: S, peer_addr: SocketAddr) -> Self {
        let codec = MessageCodec;
        let framed_stream = Framed::new(stream, codec);
        Self { 
            framed_stream,
            state: SessionState::Greeting,
            client_key: None,
            peer_addr,
        }
    }

    /// Runs the session, handling incoming messages and managing the protocol state.
    pub async fn run(&mut self) -> io::Result<()> {
        info!("Session started for {:?}", self.peer_addr);

        // Send ServerGreet immediately upon connection
        if let Err(e) = self.framed_stream.send(Message::ServerGreet(ServerGreet)).await {
            error!("Failed to send ServerGreet to {:?}: {:?}", self.peer_addr, e);
            return Err(io::Error::other("Failed to send ServerGreet"));
        }
        info!("Sent ServerGreet to {:?}", self.peer_addr);

        while let Some(message_result) = self.framed_stream.next().await {
            match message_result {
                Ok(message) => {
                    if let Err(e) = self.handle_message(message).await {
                        error!("Error handling message from {:?}: {:?}", self.peer_addr, e);
                        break; // Break the loop on message handling error
                    }
                }
                Err(e) => {
                    error!("Error receiving message from {:?}: {:?}", self.peer_addr, e);
                    break;
                }
            }
        }

        info!("Session ended for {:?}", self.peer_addr);
        Ok(())
    }

    async fn handle_message(&mut self, message: Message) -> io::Result<()> {
        debug!("Received message in state {:?}: {:?}", self.state, message);
        match self.state {
            SessionState::Greeting => {
                match message {
                    Message::ClientGreet(ClientGreet { serv_key }) => {
                        info!("Received ClientGreet with key: {}", serv_key);
                        self.client_key = Some(serv_key);
                        self.state = SessionState::Upload;
                        Ok(())
                    }
                    _ => {
                        error!("Unexpected message in Greeting state: {:?}", message);
                        Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected message in Greeting state"))
                    }
                }
            }
            SessionState::Upload => {
                match message {
                    Message::AddRecord(_) => {
                        info!("Received AddRecord message.");
                        // TODO: Process AddRecord
                        Ok(())
                    }
                    Message::DelRecord(_) => {
                        info!("Received DelRecord message.");
                        // TODO: Process DelRecord
                        Ok(())
                    }
                    Message::AddInfo(_) => {
                        info!("Received AddInfo message.");
                        // TODO: Process AddInfo
                        Ok(())
                    }
                    Message::UploadDone(_) => {
                        info!("Received UploadDone message.");
                        self.state = SessionState::PingPong;
                        Ok(())
                    }
                    _ => {
                        error!("Unexpected message in Upload state: {:?}", message);
                        Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected message in Upload state"))
                    }
                }
            }
            SessionState::PingPong => {
                match message {
                    Message::Ping(Ping { nonce }) => {
                        info!("Received Ping with nonce: {}", nonce);
                        if let Err(e) = self.framed_stream.send(Message::Pong(Pong { nonce })).await {
                            error!("Failed to send Pong: {:?}", e);
                            return Err(io::Error::other("Failed to send Pong"));
                        }
                        info!("Sent Pong with nonce: {}", nonce);
                        Ok(())
                    }
                    _ => {
                        error!("Unexpected message in PingPong state: {:?}", message);
                        Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected message in PingPong state"))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test::io::Builder;
    use wire::MessageCodec;
    use bytes::BytesMut;
    use tokio_util::codec::Encoder;
    use std::net::{IpAddr, Ipv4Addr};

    // Helper to encode a Message using MessageCodec
    fn encode_message(message: Message) -> BytesMut {
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();
        codec.encode(message, &mut buf).unwrap();
        buf
    }

    #[tokio::test]
    async fn test_session_greeting() -> io::Result<()> {
        let client_key = 0xbeef;
        
        // Mock the I/O for the session.
        // The session first sends a ServerGreet, then expects a ClientGreet.
        let mock_io = Builder::new()
            .write(&encode_message(Message::ServerGreet(ServerGreet))) // Session sends ServerGreet
            .read(&encode_message(Message::ClientGreet(ClientGreet { serv_key: client_key }))) // Session receives ClientGreet
            .build();
        
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);

        let mut session = Session::new(mock_io, peer_addr);

        // Run the session until it processes the ClientGreet
        session.run().await?;

        // Assertions
        assert_eq!(session.state, SessionState::Upload);
        assert_eq!(session.client_key, Some(client_key));

        Ok(())
    }
}

