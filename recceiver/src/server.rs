// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

use tokio::net::TcpListener;
use tracing::{error, info};
use std::io;
use tokio::sync::mpsc::Sender;

use crate::session::Session;
use crate::backend::Transaction;

/// Server listens for incoming TCP connections from reccaster clients.
pub struct Server {
    listener: TcpListener,
    sync_tx: Sender<Transaction>,
}

impl Server {
    /// Creates a new Server instance, binding to the specified address.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address (e.g., "0.0.0.0:5051") to bind the TCP listener to.
    /// * `sync_tx` - The channel to send transactions to the Synchronizer.
    pub async fn new(addr: &str, sync_tx: Sender<Transaction>) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        info!("Listening on {}", addr);
        Ok(Self { listener, sync_tx })
    }

    /// Starts accepting incoming connections and spawns a new task for each.
    pub async fn run(&self) {
        loop {
            match self.listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("Accepted connection from: {}", peer_addr);
                    let tx = self.sync_tx.clone();
                    tokio::spawn(async move {
                        let _ = Session::new(stream, peer_addr, tx).run().await;
                    });
                }
                Err(e) => error!("Failed to accept connection: {}", e),
            }
        }
    }
}
