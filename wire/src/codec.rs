// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

use bytes::{Buf, BufMut, BytesMut};
use std::{io, mem::size_of};
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    header::MessageHeader, AddInfo, AddRecord, ClientGreet, DelRecord, Message, MessageID, Ping,
    Pong, ServerGreet, UploadDone,
};

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
            Message::ClientGreet(msg) => {
                let header = MessageHeader::new(MessageID::ClientGreet.into(), 8);
                dst.put(header.as_bytes());
                dst.put_u32(0); // Padding
                dst.put_u32(msg.serv_key);
                Ok(())
            }
            Message::Pong(msg) => {
                let header = MessageHeader::new(MessageID::Pong as u16, size_of::<Pong>() as u32);
                dst.put(header.as_bytes());
                dst.put_u32(msg.nonce);
                Ok(())
            }
            Message::AddRecord(msg) => {
                let rtlen = msg.rtype.len() as u8;
                let rnlen = msg.rname.len() as u16;
                let len = (size_of::<u32>()
                    + size_of::<u8>()
                    + size_of::<u8>()
                    + size_of::<u16>()
                    + rtlen as usize
                    + rnlen as usize) as u32;
                let header = MessageHeader::new(MessageID::AddRecord.into(), len);
                dst.put(header.as_bytes());
                dst.put_u32(msg.recid);
                dst.put_u8(msg.atype);
                dst.put_u8(rtlen);
                dst.put_u16(rnlen);
                dst.put_slice(msg.rtype.as_bytes());
                dst.put_slice(msg.rname.as_bytes());
                Ok(())
            }
            Message::DelRecord(msg) => {
                let header = MessageHeader::new(MessageID::DelRecord.into(), size_of::<u32>() as u32);
                dst.put(header.as_bytes());
                dst.put_u32(msg.recid);
                Ok(())
            }
            Message::AddInfo(msg) => {
                let keylen = msg.key.len() as u8;
                let valen = msg.value.len() as u16;
                let len = (size_of::<u32>()
                    + size_of::<u8>()
                    + size_of::<u8>()
                    + size_of::<u16>()
                    + keylen as usize
                    + valen as usize) as u32;
                let header = MessageHeader::new(MessageID::AddInfo.into(), len);
                dst.put(header.as_bytes());
                dst.put_u32(msg.recid);
                dst.put_u8(keylen);
                dst.put_u8(0); // Padding
                dst.put_u16(valen);
                dst.put_slice(msg.key.as_bytes());
                dst.put_slice(msg.value.as_bytes());
                Ok(())
            }
            Message::UploadDone(_) => {
                let header =
                    MessageHeader::new(MessageID::UploadDone.into(), size_of::<u32>() as u32);
                dst.put(header.as_bytes());
                dst.put_u32(0);
                Ok(())
            }
            Message::Ping(msg) => {
                let header = MessageHeader::new(MessageID::Ping.into(), size_of::<Ping>() as u32);
                dst.put(header.as_bytes());
                dst.put_u32(msg.nonce);
                Ok(())
            }
            Message::ServerGreet(_) => {
                let header = MessageHeader::new(MessageID::ServerGreet.into(), 4);
                dst.put(header.as_bytes());
                dst.put_u8(2); // Version
                dst.put_u8(0); // Padding
                dst.put_u16(0); // Padding
                Ok(())
            }
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

        let mut buf = &src[..];
        // Read header
        let id = buf.get_u16();
        let msg_id = buf.get_u16();
        let len = buf.get_u32() as usize;

        // Checking if the ID is 'RC'
        if id != MSG_MAGIC_ID {
            // TODO: How to handle this without just returning Ok(None)
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid magic ID: {}", id),
            ));
        }

        if src.len() < len + 8 {
            // Not enough data to read the body
            return Ok(None);
        }

        // Consume the header
        src.advance(8);
        let mut body = src.split_to(len);

        // Match based on `msg_id` and parse accordingly
        match msg_id.into() {
            MessageID::ServerGreet => {
                let _version = body.get_u8();
                // We can ignore the padding
                Ok(Some(Message::ServerGreet(ServerGreet)))
            }
            MessageID::Ping => {
                let nonce = body.get_u32();
                Ok(Some(Message::Ping(Ping { nonce })))
            }
            MessageID::ClientGreet => {
                body.get_u32(); // Discard padding
                let serv_key = body.get_u32();
                Ok(Some(Message::ClientGreet(ClientGreet { serv_key })))
            }
            MessageID::Pong => {
                let nonce = body.get_u32();
                Ok(Some(Message::Pong(Pong { nonce })))
            }

            MessageID::AddRecord => {
                let recid = body.get_u32();
                let atype = body.get_u8();
                let rtlen = body.get_u8();
                let rnlen = body.get_u16();
                let rtype =
                    String::from_utf8(body.copy_to_bytes(rtlen as usize).to_vec()).unwrap();
                let rname =
                    String::from_utf8(body.copy_to_bytes(rnlen as usize).to_vec()).unwrap();
                Ok(Some(Message::AddRecord(AddRecord {
                    recid,
                    atype,
                    rtype,
                    rname,
                })))
            }

            MessageID::DelRecord => {
                let recid = body.get_u32();
                Ok(Some(Message::DelRecord(DelRecord { recid })))
            }
            MessageID::UploadDone => {
                body.get_u32(); // Discard padding
                Ok(Some(Message::UploadDone(UploadDone)))
            }
            MessageID::AddInfo => {
                let recid = body.get_u32();
                let keylen = body.get_u8();
                body.get_u8(); // Discard padding
                let valen = body.get_u16();
                let key = String::from_utf8(body.copy_to_bytes(keylen as usize).to_vec()).unwrap();
                let value = String::from_utf8(body.copy_to_bytes(valen as usize).to_vec()).unwrap();
                Ok(Some(Message::AddInfo(AddInfo {
                    recid,
                    key,
                    value,
                })))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_server_greet_round_trip() {
        let msg = Message::ServerGreet(ServerGreet);
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded_msg = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(msg, decoded_msg);
    }

    #[test]
    fn test_ping_round_trip() {
        let msg = Message::Ping(Ping { nonce: 12345 });
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded_msg = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(msg, decoded_msg);
    }

    #[test]
    fn test_client_greet_round_trip() {
        let msg = Message::ClientGreet(ClientGreet { serv_key: 12345 });
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded_msg = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(msg, decoded_msg);
    }

    #[test]
    fn test_pong_round_trip() {
        let msg = Message::Pong(Pong { nonce: 12345 });
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded_msg = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(msg, decoded_msg);
    }

    #[test]
    fn test_add_record_round_trip() {
        let msg = Message::AddRecord(AddRecord {
            recid: 1,
            atype: 0,
            rtype: "ai".to_string(),
            rname: "test-record".to_string(),
        });
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded_msg = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(msg, decoded_msg);
    }

    #[test]
    fn test_del_record_round_trip() {
        let msg = Message::DelRecord(DelRecord { recid: 1 });
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded_msg = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(msg, decoded_msg);
    }

    #[test]
    fn test_upload_done_round_trip() {
        let msg = Message::UploadDone(UploadDone);
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded_msg = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(msg, decoded_msg);
    }

    #[test]
    fn test_add_info_round_trip() {
        let msg = Message::AddInfo(AddInfo {
            recid: 1,
            key: "key".to_string(),
            value: "value".to_string(),
        });
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded_msg = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(msg, decoded_msg);
    }
}
