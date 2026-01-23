// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

use crate::backend::{InventoryBackend, Transaction};
use tokio::sync::mpsc;
use std::sync::Arc;
use tracing::{info, error};

pub struct Synchronizer {
    backend: Arc<dyn InventoryBackend>,
    receiver: mpsc::Receiver<Transaction>,
}

impl Synchronizer {
    pub fn new(backend: Arc<dyn InventoryBackend>, receiver: mpsc::Receiver<Transaction>) -> Self {
        Self { backend, receiver }
    }

    pub async fn run(mut self) {
        info!("Synchronizer started");
        while let Some(tx) = self.receiver.recv().await {
            info!("Received transaction from {:?}", tx.source_address);
            if let Err(e) = self.backend.commit(tx).await {
                error!("Failed to commit transaction: {}", e);
            }
        }
        info!("Synchronizer stopped");
    }
}
