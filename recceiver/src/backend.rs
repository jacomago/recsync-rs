// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use std::error::Error;
use wire::WireId;

/// Aggregated updates for a single record ID.
#[derive(Debug, Clone, Default)]
pub struct RecordUpdate {
    /// The primary name of the record (if provided/updated).
    pub name: Option<String>,
    /// The record type (if provided/updated).
    pub rtype: Option<String>,
    /// List of aliases associated with this record.
    pub aliases: Vec<String>,
    /// Key-value properties associated with this record.
    pub properties: HashMap<String, String>,
}

/// Data accumulated from a client session to be synced.
#[derive(Debug, Clone)]
pub struct Transaction {
    pub source_address: Option<SocketAddr>,
    pub client_infos: HashMap<String, String>,
    
    /// Updates grouped by record ID.
    pub updates: HashMap<WireId, RecordUpdate>,
    
    /// Set of record names to delete.
    pub records_to_delete: HashSet<String>,
    
    pub initial: bool,
    pub connected: bool,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            source_address: None,
            client_infos: HashMap::new(),
            updates: HashMap::new(),
            records_to_delete: HashSet::new(),
            initial: false,
            connected: true,
        }
    }
}

/// Abstract backend for storing/syncing channel data.
#[async_trait]
pub trait InventoryBackend: Send + Sync {
    /// Commit a transaction to the backend.
    async fn commit(&self, tx: Transaction) -> Result<(), Box<dyn Error + Send + Sync>>;
}

/// A Mock backend that stores transactions in memory.
#[derive(Debug, Default)]
pub struct MockBackend {
    pub transactions: Arc<Mutex<Vec<Transaction>>>,
}

impl MockBackend {
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl InventoryBackend for MockBackend {
    async fn commit(&self, tx: Transaction) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.transactions.lock().unwrap().push(tx);
        Ok(())
    }
}