mod announcer;
mod server;
mod session;

use announcer::Announcer;
use server::Server;

use std::error::Error;
use tracing::{info, error};
use std::net::Ipv4Addr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    info!("Starting recceiver...");

    // Configuration for Announcer (UDP)
    let server_ip = Ipv4Addr::new(127, 0, 0, 1); // Localhost for testing
    let server_port_tcp = 5051; // The port the TCP server will listen on
    let server_key = 12345; // A unique key for this server

    // Start UDP Announcer
    let announcer = Announcer::new(server_ip, server_port_tcp, server_key).await?;
    tokio::spawn(async move {
        if let Err(e) = announcer.start_broadcasting().await {
            error!("Announcer failed: {}", e);
        }
    });
    info!("UDP Announcer started on port 5049");

    // Start TCP Server
    let tcp_server_addr = format!("0.0.0.0:{}", server_port_tcp);
    let server = Server::new(&tcp_server_addr).await?;
    server.run().await;

    Ok(())
}
