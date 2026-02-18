// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

mod error;
mod header;
mod codec;
mod types;

pub use error::*;
pub use types::*;
pub use codec::*;
pub use header::*;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use std::convert::TryFrom;
    use tokio_util::codec::Encoder;

    #[test]
    fn test_wire_id_creation() {
        let id = WireId(0x5243);
        assert_eq!(id.0, 0x5243);

        let id2: WireId = 0x5243.into();
        assert_eq!(id, id2);

        let val: u16 = id.into();
        assert_eq!(val, 0x5243);
    }

    #[test]
    fn test_server_key_creation() {
        let key = ServerKey(12345);
        assert_eq!(key.0, 12345);

        let key2: ServerKey = 12345u32.into();
        assert_eq!(key, key2);

        let val: u32 = key.into();
        assert_eq!(val, 12345);
    }

    #[test]
    fn test_message_id_try_from_valid() {
        assert_eq!(MessageID::try_from(0x8001).unwrap(), MessageID::ServerGreet);
        assert_eq!(MessageID::try_from(0x0001).unwrap(), MessageID::ClientGreet);
        assert_eq!(MessageID::try_from(0x8002).unwrap(), MessageID::Ping);
        assert_eq!(MessageID::try_from(0x0002).unwrap(), MessageID::Pong);
        assert_eq!(MessageID::try_from(0x0003).unwrap(), MessageID::AddRecord);
        assert_eq!(MessageID::try_from(0x0004).unwrap(), MessageID::DelRecord);
        assert_eq!(MessageID::try_from(0x0005).unwrap(), MessageID::UploadDone);
        assert_eq!(MessageID::try_from(0x0006).unwrap(), MessageID::AddInfo);
    }

    #[test]
    fn test_message_id_try_from_invalid() {
        let result = MessageID::try_from(0x9999);
        assert!(result.is_err());
        match result {
            Err(ProtocolError::UnknownMessageId(id)) => assert_eq!(id, 0x9999),
            _ => panic!("Expected UnknownMessageId error"),
        }
    }

    #[test]
    fn test_message_id_to_u16() {
        assert_eq!(u16::from(MessageID::ServerGreet), 0x8001);
        assert_eq!(u16::from(MessageID::Ping), 0x8002);
        assert_eq!(u16::from(MessageID::ClientGreet), 0x0001);
    }

    #[test]
    fn test_client_greet_message_creation() {
        let key = ServerKey(42);
        let msg = ClientGreet { serv_key: key };
        assert_eq!(msg.serv_key.0, 42);
    }

    #[test]
    fn test_ping_message_creation() {
        let nonce = 0xdeadbeef;
        let msg = Ping { nonce };
        assert_eq!(msg.nonce, nonce);
    }

    #[test]
    fn test_add_record_message_creation() {
        let msg = AddRecord {
            recid: 123,
            atype: 0,
            rtype: "PV".to_string(),
            rname: "TEST:Value".to_string(),
        };
        assert_eq!(msg.recid, 123);
        assert_eq!(msg.atype, 0);
        assert_eq!(msg.rtype, "PV");
        assert_eq!(msg.rname, "TEST:Value");
    }

    #[test]
    fn test_announcement_creation() {
        use std::net::Ipv4Addr;
        let addr = Ipv4Addr::new(192, 168, 1, 1);
        let announcement = Announcement {
            server_addr: addr,
            server_port: 5064,
            server_key: ServerKey(999),
        };
        assert_eq!(announcement.server_addr, addr);
        assert_eq!(announcement.server_port, 5064);
        assert_eq!(announcement.server_key.0, 999);
    }

    #[test]
    fn test_client_message_enum() {
        let msg1 = ClientMessage::Pong(Pong { nonce: 123 });
        let msg2 = ClientMessage::ClientGreet(ClientGreet { serv_key: ServerKey(456) });

        match msg1 {
            ClientMessage::Pong(p) => assert_eq!(p.nonce, 123),
            _ => panic!("Expected Pong"),
        }

        match msg2 {
            ClientMessage::ClientGreet(g) => assert_eq!(g.serv_key.0, 456),
            _ => panic!("Expected ClientGreet"),
        }
    }

    #[test]
    fn test_server_message_enum() {
        let msg1 = ServerMessage::Ping(Ping { nonce: 789 });
        let msg2 = ServerMessage::ServerGreet(ServerGreet);

        match msg1 {
            ServerMessage::Ping(p) => assert_eq!(p.nonce, 789),
            _ => panic!("Expected Ping"),
        }

        match msg2 {
            ServerMessage::ServerGreet(_) => (),
            _ => panic!("Expected ServerGreet"),
        }
    }

    #[test]
    fn test_message_wrapper_server() {
        let inner = ServerMessage::Ping(Ping { nonce: 555 });
        let msg = Message::Server(inner);

        match msg {
            Message::Server(ServerMessage::Ping(p)) => assert_eq!(p.nonce, 555),
            _ => panic!("Expected Message::Server(Ping)"),
        }
    }

    #[test]
    fn test_message_wrapper_client() {
        let inner = ClientMessage::Pong(Pong { nonce: 666 });
        let msg = Message::Client(inner);

        match msg {
            Message::Client(ClientMessage::Pong(p)) => assert_eq!(p.nonce, 666),
            _ => panic!("Expected Message::Client(Pong)"),
        }
    }

    #[test]
    fn test_encoder_client_greet() {
        let mut codec = MessageCodec;
        let mut dst = BytesMut::new();
        let msg = Message::Client(ClientMessage::ClientGreet(ClientGreet {
            serv_key: ServerKey(0x12345678),
        }));

        codec.encode(msg, &mut dst).expect("encode failed");
        assert!(!dst.is_empty());
    }

    #[test]
    fn test_protocol_error_display() {
        let err = ProtocolError::UnknownMessageId(0x9999);
        let msg = format!("{}", err);
        assert!(msg.contains("9999"));
    }

    #[test]
    fn test_protocol_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "test");
        let _err: ProtocolError = io_err.into();
    }
}
