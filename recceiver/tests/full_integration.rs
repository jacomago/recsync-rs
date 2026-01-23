use recceiver::announcer::Announcer;
use recceiver::server::Server;
use recceiver::synchronizer::Synchronizer;
use recceiver::backend::MockBackend;
use wire::ServerKey;
use reccaster::{Reccaster, Record};
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_full_integration() {
    // 1. Setup Backend
    let backend = Arc::new(MockBackend::new());
    let backend_ref = backend.clone();

    // 2. Setup Synchronizer
    let (sync_tx, sync_rx) = mpsc::channel(100);
    let synchronizer = Synchronizer::new(backend, sync_rx);
    tokio::spawn(async move {
        synchronizer.run().await;
    });

    // 3. Start TCP Server
    // Use a random port or fixed port
    let server_port_tcp = 5052;
    let tcp_server_addr = format!("127.0.0.1:{}", server_port_tcp);
    let server = Server::new(&tcp_server_addr, sync_tx).await.expect("Failed to start TCP server");
    tokio::spawn(async move {
        server.run().await;
    });

    // 4. Start Announcer
    let server_ip = Ipv4Addr::new(127, 0, 0, 1);
    let server_key = ServerKey(9999);
    let announcer = Announcer::new(server_ip, server_port_tcp, server_key).await.expect("Failed to create Announcer");
    
    // Announcer broadcasts to 255.255.255.255:5049. 
    // We need to ensure reccaster is listening on 5049.
    // Reccaster::new() binds to 0.0.0.0:5049.
    
    tokio::spawn(async move {
        announcer.start_broadcasting().await.expect("Announcer failed");
    });

    // 5. Setup Reccaster (Client)
    let records = vec![
        Record {
            name: "TEST:REC:1".to_string(),
            r#type: "ai".to_string(),
            alias: Some("TEST:ALIAS:1".to_string()),
            properties: HashMap::from([
                ("desc".to_string(), "Description 1".to_string())
            ]),
        }
    ];
    let client_props = HashMap::from([
        ("IOCNAME".to_string(), "test-ioc-integration".to_string())
    ]);

    let mut reccaster = Reccaster::new(records, Some(client_props)).await;
    
    // Spawn Reccaster
    tokio::spawn(async move {
        reccaster.run().await;
    });

    // 6. Verification
    // Poll the backend until data arrives
    let start = tokio::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(10) {
            panic!("Timeout waiting for data sync");
        }

        let transactions = backend_ref.transactions.lock().unwrap();
        if !transactions.is_empty() {
             let tx = &transactions[0];
             let updates = &tx.updates;
             
             if updates.len() == 1 {
                 // Found it
                 let update = updates.values().next().unwrap();
                 assert_eq!(update.name.as_deref(), Some("TEST:REC:1"));
                 assert!(update.aliases.contains(&"TEST:ALIAS:1".to_string()));
                 assert_eq!(update.properties.get("desc"), Some(&"Description 1".to_string()));
                 
                 // Check client infos
                 assert_eq!(tx.client_infos.get("IOCNAME"), Some(&"test-ioc-integration".to_string()));
                 break;
             }
        }
        drop(transactions);
        sleep(Duration::from_millis(100)).await;
    }
}
