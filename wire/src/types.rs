// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

use std::net::Ipv4Addr;
use crate::error::ProtocolError;

/// Wire ID newtype - represents a message ID on the wire
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WireId(pub u16);

/// Server key newtype - represents a server's key/identifier
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServerKey(pub u32);

impl From<u16> for WireId {
    fn from(value: u16) -> Self {
        WireId(value)
    }
}

impl From<WireId> for u16 {
    fn from(id: WireId) -> u16 {
        id.0
    }
}

impl From<u32> for ServerKey {
    fn from(value: u32) -> Self {
        ServerKey(value)
    }
}

impl From<ServerKey> for u32 {
    fn from(key: ServerKey) -> u32 {
        key.0
    }
}

/// AddRecord message type
pub enum AddRecordType {
    Record = 0,
    Alias = 1,
}

/// UDP Announcement message structure
#[derive(Debug)]
pub struct Announcement {
    pub server_addr: Ipv4Addr,
    pub server_port: u16,
    pub server_key: ServerKey,
}

/// Messages ID
#[derive(Copy, Clone)]
#[repr(u16)]
pub enum MessageID {
    ServerGreet = 0x8001,
    ClientGreet = 0x0001,
    Ping = 0x8002,
    Pong = 0x0002,
    AddRecord = 0x0003,
    DelRecord = 0x0004,
    UploadDone = 0x0005,
    AddInfo = 0x0006,
}

impl TryFrom<u16> for MessageID {
    type Error = ProtocolError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0x8001 => Ok(MessageID::ServerGreet),
            0x0001 => Ok(MessageID::ClientGreet),
            0x8002 => Ok(MessageID::Ping),
            0x0002 => Ok(MessageID::Pong),
            0x0003 => Ok(MessageID::AddRecord),
            0x0004 => Ok(MessageID::DelRecord),
            0x0005 => Ok(MessageID::UploadDone),
            0x0006 => Ok(MessageID::AddInfo),
            _ => Err(ProtocolError::UnknownMessageId(value)),
        }
    }
}

impl From<MessageID> for u16 {
    fn from(msg_id: MessageID) -> u16 {
        match msg_id {
            MessageID::ServerGreet => 0x8001,
            MessageID::ClientGreet => 0x0001,
            MessageID::Ping => 0x8002,
            MessageID::Pong => 0x0002,
            MessageID::AddRecord => 0x0003,
            MessageID::DelRecord => 0x0004,
            MessageID::UploadDone => 0x0005,
            MessageID::AddInfo => 0x0006,
        }
    }
}

// Define all the message structs and enums here

#[derive(Debug, Clone, PartialEq)]
pub struct ServerGreet;

#[derive(Debug, Clone, PartialEq)]
pub struct Ping {
    pub nonce: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientGreet {
    pub serv_key: ServerKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pong {
    pub nonce: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AddRecord {
    pub recid: u32,
    pub atype: u8,
    pub rtype: String,
    pub rname: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DelRecord {
    pub recid: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UploadDone;

#[derive(Debug, Clone, PartialEq)]
pub struct AddInfo {
    pub recid: u32,
    pub key: String,
    pub value: String,
}

/// Messages sent from the server to the client
#[derive(Debug, Clone, PartialEq)]
pub enum ServerMessage {
    ServerGreet(ServerGreet),
    Ping(Ping),
}

/// Messages sent from the client to the server
#[derive(Debug, Clone, PartialEq)]
pub enum ClientMessage {
    ClientGreet(ClientGreet),
    Pong(Pong),
    AddRecord(AddRecord),
    DelRecord(DelRecord),
    UploadDone(UploadDone),
    AddInfo(AddInfo),
}

/// Generic message that can be either from server or client
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Server(ServerMessage),
    Client(ClientMessage),
}
