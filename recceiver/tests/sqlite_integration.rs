use recceiver::sqlite_backend::SqliteBackend;
use recceiver::backend::{InventoryBackend, Transaction, RecordUpdate};
use wire::WireId;
use sqlx::{SqlitePool, Row};

#[tokio::test]
async fn test_sqlite_backend_integration() {
    // 1. Setup
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let backend = SqliteBackend::new(&db_url).await.expect("Failed to create SqliteBackend");

    // 2. Create a Transaction
    let mut tx = Transaction::new();
    tx.source_address = Some("127.0.0.1:12345".parse().unwrap());
    tx.client_infos.insert("os".to_string(), "linux".to_string());
    
    let wire_id = WireId(100);
    let mut update = RecordUpdate::default();
    update.name = Some("TEST:RECORD".to_string());
    update.rtype = Some("ai".to_string());
    update.aliases = vec!["TEST:ALIAS".to_string()];
    update.properties.insert("desc".to_string(), "Test Description".to_string());
    
    tx.updates.insert(wire_id, update);

    // 3. Commit
    backend.commit(tx).await.expect("Failed to commit transaction");

    // 4. Verification
    // Connect separately to verify persistence
    let pool = SqlitePool::connect(&db_url).await.expect("Failed to connect to verification DB");
    
    // Check IOC
    let row = sqlx::query("SELECT id, name, address FROM iocs WHERE name = ?")
        .bind("127.0.0.1:12345")
        .fetch_one(&pool)
        .await
        .expect("IOC not found");
    let ioc_id: i64 = row.get("id");
    assert_eq!(row.get::<String, _>("address"), "127.0.0.1:12345");

    // Check Record
    let row = sqlx::query("SELECT id, name, rtype FROM records WHERE ioc_id = ? AND name = ?")
        .bind(ioc_id)
        .bind("TEST:RECORD")
        .fetch_one(&pool)
        .await
        .expect("Record not found");
    let record_id: i64 = row.get("id");
    assert_eq!(row.get::<String, _>("rtype"), "ai");

    // Check Property
    let row = sqlx::query("SELECT value FROM properties WHERE record_id = ? AND key = ?")
        .bind(record_id)
        .bind("desc")
        .fetch_one(&pool)
        .await
        .expect("Property not found");
    assert_eq!(row.get::<String, _>("value"), "Test Description");

     // Check Alias
    let row = sqlx::query("SELECT name FROM aliases WHERE record_id = ? AND name = ?")
        .bind(record_id)
        .bind("TEST:ALIAS")
        .fetch_one(&pool)
        .await
        .expect("Alias not found");
    assert_eq!(row.get::<String, _>("name"), "TEST:ALIAS");

    // Check Client Info
    let row = sqlx::query("SELECT value FROM client_infos WHERE ioc_id = ? AND key = ?")
        .bind(ioc_id)
        .bind("os")
        .fetch_one(&pool)
        .await
        .expect("Client Info not found");
    assert_eq!(row.get::<String, _>("value"), "linux");

}
