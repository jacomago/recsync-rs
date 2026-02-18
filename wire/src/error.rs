// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

use thiserror::Error;

/// Protocol-level errors that can occur during wire protocol communication
#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Unknown message ID: {0:#06x}")]
    UnknownMessageId(u16),

    #[error("Invalid wire ID")]
    InvalidWireId,

    #[error("Insufficient data: expected {expected} bytes, got {got}")]
    InsufficientData { expected: usize, got: usize },

    #[error("Invalid message format: {0}")]
    InvalidMessageFormat(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
