use recceiver::announcer::Announcer;
use recceiver::server::Server;
use recceiver::sqlite_backend::SqliteBackend;
use recceiver::channelfinder_backend::ChannelFinderBackend;
use recceiver::synchronizer::Synchronizer;
use recceiver::config::{Config, BackendConfig};
use recceiver::backend::{InventoryBackend, MockBackend};

use std::error::Error;
use tracing::{info, error};
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::sync::mpsc;
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    info!("Starting recceiver...");
    
    // Load Config
    // Check if a config file is provided as argument, otherwise default to demo.conf
    let args: Vec<String> = std::env::args().collect();
    let config_path = if args.len() > 1 { &args[1] } else { "demo.conf" };
    
    let config = Config::load(config_path);
    // info!("Loaded configuration: {:?}", config); // Config might not implement Debug

    // Start UDP Announcer
    let server_ip = Ipv4Addr::from_str(&config.server_ip).unwrap_or(Ipv4Addr::new(0, 0, 0, 0));
    let server_port_tcp = config.server_port;
    let server_key = config.server_key;

    let announcer = Announcer::new(server_ip, server_port_tcp, server_key).await?;
    tokio::spawn(async move {
        if let Err(e) = announcer.start_broadcasting().await {
            error!("Announcer failed: {}", e);
        }
    });
    info!("UDP Announcer started");

    // Initialize Backend
    let backend: Arc<dyn InventoryBackend> = match config.backend {
        BackendConfig::Sqlite { db_url } => {
            info!("Using SqliteBackend: {}", db_url);
            Arc::new(SqliteBackend::new(&db_url).await?)
        },
        BackendConfig::ChannelFinder { base_url, username } => {
             info!("Using ChannelFinderBackend: {} ({})", base_url, username);
             Arc::new(ChannelFinderBackend::new(base_url, username))
        },
        BackendConfig::Mock => {
            info!("Using MockBackend");
            Arc::new(MockBackend::new())
        }
    };
    
    let (sync_tx, sync_rx) = mpsc::channel(100);
    let synchronizer = Synchronizer::new(backend, sync_rx);
    tokio::spawn(async move {
        synchronizer.run().await;
    });

    // Start TCP Server
    let tcp_server_addr = format!("{}:{}", config.server_ip, config.server_port);
    info!("TCP Server listening on {}", tcp_server_addr);
    let server = Server::new(&tcp_server_addr, sync_tx).await?;
    server.run().await;

    Ok(())
}
