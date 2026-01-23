use crate::backend::{InventoryBackend, Transaction};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::error::Error;
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CFProperty {
    pub name: String,
    pub owner: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CFChannel {
    pub name: String,
    pub owner: String,
    pub properties: Vec<CFProperty>,
}

pub struct ChannelFinderBackend {
    client: Client,
    base_url: String,
    username: String,
}

impl ChannelFinderBackend {
    pub fn new(base_url: String, username: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
            username,
        }
    }

    async fn send_channels(&self, channels: Vec<CFChannel>) -> Result<(), Box<dyn Error + Send + Sync>> {
        if channels.is_empty() {
            return Ok(());
        }
        let url = format!("{}/resources/channels", self.base_url);
        // Chunking could be added here
        let res = self.client.post(&url)
            .json(&channels)
            .send()
            .await?;
        
        if !res.status().is_success() {
             let status = res.status();
             let text = res.text().await.unwrap_or_default();
             return Err(format!("CF API error: {} - {}", status, text).into());
        }
        info!("Successfully sent {} channels to ChannelFinder", channels.len());
        Ok(())
    }
}

#[async_trait]
impl InventoryBackend for ChannelFinderBackend {
    async fn commit(&self, tx: Transaction) -> Result<(), Box<dyn Error + Send + Sync>> {
        let source = match tx.source_address {
            Some(s) => s,
            None => {
                warn!("Transaction has no source address, skipping CF sync");
                return Ok(());
            }
        };

        let hostname = tx.client_infos.get("HOSTNAME").cloned().unwrap_or_else(|| source.ip().to_string());
        let ioc_name = tx.client_infos.get("IOCNAME").cloned().unwrap_or_else(|| source.port().to_string());
        // iocid logic from cfstore.py: host:port
        let iocid = format!("{}:{}", source.ip(), source.port());
        let ioc_ip = source.ip().to_string();
        let timestamp = chrono::Local::now().to_rfc3339();
        let owner = self.username.clone();

        let mut channels_to_send = Vec::new();

        // 1. Handle Records to Add/Update
        for update in tx.updates.values() {
            if let Some(name) = &update.name {
                let mut props = Vec::new();
                
                // Standard Properties
                props.push(CFProperty { name: "hostName".to_string(), owner: owner.clone(), value: hostname.clone() });
                props.push(CFProperty { name: "iocName".to_string(), owner: owner.clone(), value: ioc_name.clone() });
                props.push(CFProperty { name: "iocid".to_string(), owner: owner.clone(), value: iocid.clone() });
                props.push(CFProperty { name: "iocIP".to_string(), owner: owner.clone(), value: ioc_ip.clone() });
                props.push(CFProperty { name: "pvStatus".to_string(), owner: owner.clone(), value: "Active".to_string() });
                props.push(CFProperty { name: "time".to_string(), owner: owner.clone(), value: timestamp.clone() });
                
                // Record Type
                if let Some(rtype) = &update.rtype {
                    props.push(CFProperty { name: "recordType".to_string(), owner: owner.clone(), value: rtype.clone() });
                }

                // Aliases (as properties? CF usually handles aliases differently or as a property named 'alias')
                // cfstore.py uses a property "alias" on the RECORD, but also creates separate CHANNELS for aliases?
                // cfstore.py:
                // > if alias exists but not part of old list
                // > CFProperty.alias(..., cf_channel.name)
                // > channels.append(CFChannel(alias_name, ..., aprops))
                
                // So Aliases are separate channels in CF, pointing to the original record via 'alias' property?
                // Or just a property on the original record?
                // Looking at cfstore.py:
                // It creates a new CFChannel for the alias.
                // And adds a property 'alias' to it, with value = record_name.
                
                for alias in &update.aliases {
                    let mut alias_props = props.clone();
                    alias_props.push(CFProperty { name: "alias".to_string(), owner: owner.clone(), value: name.clone() });
                     channels_to_send.push(CFChannel {
                        name: alias.clone(),
                        owner: owner.clone(),
                        properties: alias_props,
                    });
                }

                // Client Info properties (Env vars mapped)
                // For now just add them if they are in client_infos? 
                // In cfstore.py it maps specific env vars.
                // We'll skip complex mapping for now and just add update.properties
                
                for (k, v) in &update.properties {
                    props.push(CFProperty { name: k.clone(), owner: owner.clone(), value: v.clone() });
                }

                channels_to_send.push(CFChannel {
                    name: name.clone(),
                    owner: owner.clone(),
                    properties: props,
                });
            }
        }

        // 2. Handle Deletions
        // Mark as Inactive
        for name in &tx.records_to_delete {
             let props = vec![
                CFProperty { name: "pvStatus".to_string(), owner: owner.clone(), value: "Inactive".to_string() },
                CFProperty { name: "time".to_string(), owner: owner.clone(), value: timestamp.clone() },
            ];
            channels_to_send.push(CFChannel {
                name: name.clone(),
                owner: owner.clone(),
                properties: props,
            });
        }
        
        self.send_channels(channels_to_send).await?;

        Ok(())
    }
}
