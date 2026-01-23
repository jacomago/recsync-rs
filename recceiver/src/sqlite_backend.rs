// This file is part of Recsync-rs.
// Copyright (c) 2024 UK Research and Innovation, Science and Technology Facilities Council
//
// This project is licensed under both the MIT License and the BSD 3-Clause License.
// You must comply with both licenses to use, modify, or distribute this software.
// See the LICENSE file for details.

use crate::backend::{InventoryBackend, Transaction};
use sqlx::{SqlitePool, Row, sqlite::SqliteConnectOptions, Acquire};
use async_trait::async_trait;
use std::error::Error;
use std::str::FromStr;
use tracing::{info, warn};

pub struct SqliteBackend {
    pool: SqlitePool,
}

impl SqliteBackend {
    pub async fn new(db_url: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let options = SqliteConnectOptions::from_str(db_url)?
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;
        
        // Initialize Schema
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS iocs (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                address TEXT,
                client_key INTEGER
            );
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY,
                ioc_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                rtype TEXT,
                UNIQUE(ioc_id, name),
                FOREIGN KEY(ioc_id) REFERENCES iocs(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS aliases (
                id INTEGER PRIMARY KEY,
                record_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                FOREIGN KEY(record_id) REFERENCES records(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS properties (
                id INTEGER PRIMARY KEY,
                record_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                UNIQUE(record_id, key),
                FOREIGN KEY(record_id) REFERENCES records(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS client_infos (
                id INTEGER PRIMARY KEY,
                ioc_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                UNIQUE(ioc_id, key),
                FOREIGN KEY(ioc_id) REFERENCES iocs(id) ON DELETE CASCADE
            );
            "#
        ).execute(&pool).await?;
        
        Ok(Self { pool })
    }
}

#[async_trait]
impl InventoryBackend for SqliteBackend {
    async fn commit(&self, tx: Transaction) -> Result<(), Box<dyn Error + Send + Sync>> {
        let ioc_name = match tx.source_address {
            Some(addr) => addr.to_string(),
            None => {
                warn!("Transaction without source address, ignoring");
                return Ok(());
            }
        };

        // Acquire a connection and start a SQL transaction
        let mut conn = self.pool.acquire().await?;
        let mut db_tx = conn.begin().await?;

        // 1. Upsert IOC
        let ioc_id: i64 = sqlx::query(
            "INSERT INTO iocs (name, address) VALUES (?, ?) 
             ON CONFLICT(name) DO UPDATE SET address=excluded.address
             RETURNING id"
        )
        .bind(&ioc_name)
        .bind(&ioc_name)
        .fetch_one(&mut *db_tx).await?
        .get::<i64, _>(0);

        // 2. Client Infos
        for (k, v) in &tx.client_infos {
            sqlx::query(
                "INSERT INTO client_infos (ioc_id, key, value) VALUES (?, ?, ?)
                 ON CONFLICT(ioc_id, key) DO UPDATE SET value=excluded.value"
            )
            .bind(ioc_id)
            .bind(k)
            .bind(v)
            .execute(&mut *db_tx).await?;
        }

        // 3. Deletions
        for name in &tx.records_to_delete {
            sqlx::query("DELETE FROM records WHERE ioc_id = ? AND name = ?")
                .bind(ioc_id)
                .bind(name)
                .execute(&mut *db_tx).await?;
        }

        // 4. Updates
        for update in tx.updates.values() {
             if let Some(name) = &update.name {
                 // Upsert Record
                 let rtype = update.rtype.as_deref().unwrap_or("");
                 let record_id: i64 = sqlx::query(
                     "INSERT INTO records (ioc_id, name, rtype) VALUES (?, ?, ?)
                      ON CONFLICT(ioc_id, name) DO UPDATE SET rtype=excluded.rtype
                      RETURNING id"
                 )
                 .bind(ioc_id)
                 .bind(name)
                 .bind(rtype)
                 .fetch_one(&mut *db_tx).await?
                 .get::<i64, _>(0);

                 // Properties
                 for (k, v) in &update.properties {
                     sqlx::query(
                         "INSERT INTO properties (record_id, key, value) VALUES (?, ?, ?)
                          ON CONFLICT(record_id, key) DO UPDATE SET value=excluded.value"
                     )
                     .bind(record_id)
                     .bind(k)
                     .bind(v)
                     .execute(&mut *db_tx).await?;
                 }

                 // Aliases
                 for alias in &update.aliases {
                     sqlx::query(
                         "INSERT OR IGNORE INTO aliases (record_id, name) VALUES (?, ?)"
                     )
                     .bind(record_id)
                     .bind(alias)
                     .execute(&mut *db_tx).await?;
                 }
             }
        }

        // Commit the SQL transaction
        db_tx.commit().await?;
        
        info!("Committed transaction for {} with {} updates", ioc_name, tx.updates.len());
        Ok(())
    }
}
