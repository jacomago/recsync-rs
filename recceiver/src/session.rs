// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use wire::{MessageCodec, Message, ClientGreet, ServerGreet, Ping, Pong, AddRecordType};
use tracing::{debug, info, error};
use std::io;
use std::net::SocketAddr;
use futures_util::stream::StreamExt;
use futures_util::sink::SinkExt; 
use crate::backend::Transaction;
use tokio::sync::mpsc::Sender;
use std::collections::HashMap;

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
    
    // Accumulate updates here
    tx_builder: Transaction,
    // Channel to send committed transactions to the Synchronizer
    sync_tx: Sender<Transaction>,
    
    // Maintain mapping from ID to Name for this session
    id_to_name: HashMap<u32, String>,
}

impl<S> Session<S> 
where 
    S: AsyncRead + AsyncWrite + Unpin
{
    /// Creates a new session from a stream (e.g., TcpStream).
    pub fn new(stream: S, peer_addr: SocketAddr, sync_tx: Sender<Transaction>) -> Self {
        let codec = MessageCodec;
        let framed_stream = Framed::new(stream, codec);
        Self { 
            framed_stream,
            state: SessionState::Greeting,
            client_key: None,
            peer_addr,
            tx_builder: Transaction::new(),
            sync_tx,
            id_to_name: HashMap::new(),
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
                        // Mark transaction as initial if needed (not tracked here yet)
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
                    Message::AddRecord(rec) => {
                        let update = self.tx_builder.updates.entry(rec.recid).or_default();
                        if rec.atype == (AddRecordType::Record as u8) {
                            self.id_to_name.insert(rec.recid, rec.rname.clone());
                            update.name = Some(rec.rname);
                            update.rtype = Some(rec.rtype);
                        } else if rec.atype == (AddRecordType::Alias as u8) {
                            // For alias, rname is the alias
                            update.aliases.push(rec.rname);
                            // Ensure name is set if we know it
                            if update.name.is_none() {
                                if let Some(name) = self.id_to_name.get(&rec.recid) {
                                    update.name = Some(name.clone());
                                }
                            }
                        }
                        Ok(())
                    }
                    Message::DelRecord(rec) => {
                        if let Some(name) = self.id_to_name.get(&rec.recid) {
                            self.tx_builder.records_to_delete.insert(name.clone());
                        } else {
                            // If we don't know the name, we can't tell the backend what to delete by name.
                            // This might happen if the record was never sent in this session.
                            // But usually DelRecord follows a previous session.
                            // If we can't resolve it, we might skip it or warn.
                            // For now, let's warn.
                            tracing::warn!("Received DelRecord for unknown ID: {}", rec.recid);
                        }
                        Ok(())
                    }
                    Message::AddInfo(info) => {
                        if info.recid == 0 {
                            self.tx_builder.client_infos.insert(info.key, info.value);
                        } else {
                            let update = self.tx_builder.updates.entry(info.recid).or_default();
                            update.properties.insert(info.key, info.value);
                            // Ensure name is set if we know it
                            if update.name.is_none() {
                                if let Some(name) = self.id_to_name.get(&info.recid) {
                                    update.name = Some(name.clone());
                                }
                            }
                        }
                        Ok(())
                    }
                    Message::UploadDone(_) => {
                        info!("Received UploadDone message. Committing transaction.");
                        self.tx_builder.source_address = Some(self.peer_addr);
                        self.tx_builder.connected = true;
                        
                        // Send the transaction
                        let tx = std::mem::replace(&mut self.tx_builder, Transaction::new());
                        
                        if let Err(e) = self.sync_tx.send(tx).await {
                             error!("Failed to send transaction to Synchronizer: {:?}", e);
                             return Err(io::Error::other("Synchronizer channel closed"));
                        }

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
                        // info!("Received Ping with nonce: {}", nonce);
                        if let Err(e) = self.framed_stream.send(Message::Pong(Pong { nonce })).await {
                            error!("Failed to send Pong: {:?}", e);
                            return Err(io::Error::other("Failed to send Pong"));
                        }
                        // info!("Sent Pong with nonce: {}", nonce);
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
    use tokio::sync::mpsc;

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
        let mock_io = Builder::new()
            .write(&encode_message(Message::ServerGreet(ServerGreet))) 
            .read(&encode_message(Message::ClientGreet(ClientGreet { serv_key: client_key }))) 
            .build();
        
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        let (tx, _rx) = mpsc::channel(10);

        let mut session = Session::new(mock_io, peer_addr, tx);

        // Run the session until it processes the ClientGreet
        session.run().await?;

        // Assertions
        assert_eq!(session.state, SessionState::Upload);
        assert_eq!(session.client_key, Some(client_key));

        Ok(())
    }
}

