use recceiver::channelfinder_backend::ChannelFinderBackend;
use recceiver::backend::{InventoryBackend, Transaction, RecordUpdate};
use wire::WireId;
use wiremock::{MockServer, Mock, ResponseTemplate};
use wiremock::matchers::{method, path};

#[tokio::test]
async fn test_channelfinder_backend_integration() {
    // 1. Start Mock Server
    let mock_server = MockServer::start().await;
    
    // 2. Setup Expectation
    Mock::given(method("POST"))
        .and(path("/resources/channels"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    // 3. Initialize Backend
    let backend = ChannelFinderBackend::new(mock_server.uri(), "cfstore".to_string());

    // 4. Create Transaction
    let mut tx = Transaction::new();
    tx.source_address = Some("127.0.0.1:5050".parse().unwrap());
    tx.client_infos.insert("HOSTNAME".to_string(), "localhost".to_string());
    tx.client_infos.insert("IOCNAME".to_string(), "test-ioc".to_string());
    
    let wire_id = WireId(10);
    let mut update = RecordUpdate::default();
    update.name = Some("TEST:PV".to_string());
    update.rtype = Some("ai".to_string());
    tx.updates.insert(wire_id, update);

    // 5. Commit
    backend.commit(tx).await.expect("Failed to commit to CF");
}
