// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

use bytes::{Buf, BufMut, BytesMut};
use std::{io, mem::size_of, convert::TryFrom};
use tokio_util::codec::{Decoder, Encoder};

use crate::{header::MessageHeader, ClientGreet, ClientMessage, Message, MessageID, Ping, Pong, ServerGreet, ServerMessage};

/// UDP broadcast port
pub const SERVER_ANNOUNCEMENT_UDP_PORT: u16 = 5049;

/// Message ID Magic number (ascii "RC")
pub const MSG_MAGIC_ID: u16 = 0x5243;

/// Encoders and Decoders for Messages
pub struct MessageCodec;

impl Encoder<Message> for MessageCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match msg {
            Message::Client(client_msg) => self.encode_client(client_msg, dst),
            Message::Server(server_msg) => self.encode_server(server_msg, dst),
        }
    }
}

impl MessageCodec {
    fn encode_client(&mut self, msg: ClientMessage, dst: &mut BytesMut) -> Result<(), io::Error> {
        match msg {
            ClientMessage::ClientGreet(msg) => {
                let header = MessageHeader::new(MessageID::ClientGreet.into(), (size_of::<u32>() + size_of::<ClientGreet>())as u32);
                dst.put(header.as_bytes());
                dst.put_u32(0); // Padding
                dst.put_u32(msg.serv_key.into());
                Ok(())
            },
            ClientMessage::Pong(msg) => {
                let header = MessageHeader::new(MessageID::Pong as u16, size_of::<Pong>() as u32);
                dst.put(header.as_bytes());
                dst.put_u32(msg.nonce);
                Ok(())
            },
            ClientMessage::AddRecord(msg) => {
                let len = (size_of::<u32>() + size_of::<u8>() + size_of::<u8>() + size_of::<u16>() + msg.rtype.len() + msg.rname.len()) as u32;
                let header = MessageHeader::new(MessageID::AddRecord.into(), len);
                dst.put_u16(header.id);
                dst.put_u16(header.msg_id);
                dst.put_u32(header.len);
                dst.put_u32(msg.recid);
                dst.put_u8(msg.atype);
                dst.put_u8(msg.rtype.len() as u8);
                dst.put_u16(msg.rname.len() as u16);
                dst.put_slice(msg.rtype.as_bytes());
                dst.put_slice(msg.rname.as_bytes());
                Ok(())
            },
            ClientMessage::DelRecord(_) => todo!(),
            ClientMessage::AddInfo(msg) => {
                let len = (size_of::<u32>() + size_of::<u8>() + size_of::<u8>() + size_of::<u16>() + msg.key.len() + msg.value.len()) as u32;
                let header = MessageHeader::new(MessageID::AddInfo.into(), len);
                dst.put_u16(header.id);
                dst.put_u16(header.msg_id);
                dst.put_u32(header.len);
                dst.put_u32(msg.recid);
                dst.put_u8(msg.key.len() as u8);
                dst.put_u8(0); // Padding
                dst.put_u16(msg.value.len() as u16);
                dst.put_slice(msg.key.as_bytes());
                dst.put_slice(msg.value.as_bytes());
                Ok(())
            },
            ClientMessage::UploadDone(_) => {
                let header = MessageHeader::new(MessageID::UploadDone.into(), size_of::<u32>() as u32);
                dst.put(header.as_bytes());
                dst.put_u32(0);
                Ok(())
            },
        }
    }

    fn encode_server(&mut self, msg: ServerMessage, _dst: &mut BytesMut) -> Result<(), io::Error> {
        match msg {
            ServerMessage::Ping(_) => unimplemented!("Sender related messages are not implemented yet."),
            ServerMessage::ServerGreet(_) => unimplemented!("Sender related messages are not implemented yet."),
        }
    }
}

/// Codec for encoding/decoding client messages
pub struct ClientCodec;

impl Encoder<ClientMessage> for ClientCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: ClientMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut codec = MessageCodec;
        codec.encode_client(msg, dst)
    }
}

/// Codec for decoding server messages
pub struct ServerCodec;

impl Decoder for ServerCodec {
    type Item = ServerMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 8 {
            // Not enough data to read header
            return Ok(None);
        }

        // Peek at header without consuming
        let id = u16::from_be_bytes([src[0], src[1]]);
        let msg_id = u16::from_be_bytes([src[2], src[3]]);
        let len = u32::from_be_bytes([src[4], src[5], src[6], src[7]]) as usize;

        // Checking if the ID is 'RC'
        if id != MSG_MAGIC_ID {
            return Ok(None);
        }

        if src.len() < 8 + len {
            // Not enough data to read the full message
            return Ok(None);
        }

        // Now consume the bytes
        src.advance(8);

        // Match based on `msg_id` and parse accordingly
        match MessageID::try_from(msg_id) {
            Ok(MessageID::ServerGreet) => {
                let _placeholder = src.get_u8();
                Ok(Some(ServerMessage::ServerGreet(ServerGreet)))
            }
            Ok(MessageID::Ping) => {
                let nonce = src.get_u32();
                Ok(Some(ServerMessage::Ping(Ping { nonce })))
            },
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected message type for server codec")),
        }
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 8 {
            // Not enough data to read header
            return Ok(None);
        }

        // Peek at header without consuming
        let id = u16::from_be_bytes([src[0], src[1]]);
        let msg_id = u16::from_be_bytes([src[2], src[3]]);
        let len = u32::from_be_bytes([src[4], src[5], src[6], src[7]]) as usize;

        // Checking if the ID is 'RC'
        if id != MSG_MAGIC_ID {
            return Ok(None);
        }

        if src.len() < 8 + len {
            // Not enough data to read the full message
            return Ok(None);
        }

        // Now consume the bytes
        src.advance(8);

        // Match based on `msg_id` and parse accordingly
        match MessageID::try_from(msg_id) {
            Ok(MessageID::ServerGreet) => {
                let _placeholder = src.get_u8();
                Ok(Some(Message::Server(ServerMessage::ServerGreet(ServerGreet))))
            }
            Ok(MessageID::Ping) => {
                let nonce = src.get_u32();
                Ok(Some(Message::Server(ServerMessage::Ping(Ping { nonce }))))
            },
            Ok(MessageID::ClientGreet) => unimplemented!("Receiver related messages are not implemented yet."),
            Ok(MessageID::Pong) => unimplemented!("Receiver related messages are not implemented yet."),
            Ok(MessageID::AddRecord) => unimplemented!("Receiver related messages are not implemented yet."),
            Ok(MessageID::DelRecord) => unimplemented!("Receiver related messages are not implemented yet."),
            Ok(MessageID::UploadDone) => unimplemented!("Receiver related messages are not implemented yet."),
            Ok(MessageID::AddInfo) => unimplemented!("Receiver related messages are not implemented yet."),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        }
    }
}
