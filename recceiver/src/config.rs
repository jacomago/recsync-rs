use ini::Ini;
use wire::ServerKey;

#[derive(Debug, Clone)]
pub enum BackendConfig {
    Sqlite { db_url: String },
    ChannelFinder { base_url: String, username: String },
    Mock,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub server_ip: String,
    pub server_port: u16,
    pub server_key: ServerKey,
    pub backend: BackendConfig,
}

impl Config {
    pub fn load(path: &str) -> Self {
        let conf = Ini::load_from_file(path).unwrap_or_else(|_| Ini::new());
        let recceiver = conf.section(Some("recceiver"));
        
        // Parse bind address
        let bind = recceiver.and_then(|s| s.get("bind")).unwrap_or("0.0.0.0:5051");
        let parts: Vec<&str> = bind.split(':').collect();
        let server_ip = parts.get(0).unwrap_or(&"0.0.0.0").to_string();
        let server_port = parts.get(1).unwrap_or(&"5051").parse().unwrap_or(5051);

        // Parse key
        // In python recceiver, key might be generated or config.
        // We'll use a fixed key or env var if needed.
        let server_key = ServerKey(12345); 

        // Parse procs
        let procs = recceiver.and_then(|s| s.get("procs")).unwrap_or("");
        
        let backend = if procs.contains("db") {
             let db_section_name = "lite"; 
             let db_section = conf.section(Some(db_section_name));
             let db_name = db_section.and_then(|s| s.get("dbname")).unwrap_or("recceiver.db");
             let db_url = format!("sqlite://{}", db_name);
             BackendConfig::Sqlite { db_url }
        } else if procs.contains("cf") {
             // For CF, we expect a [cf] section or env vars.
             // We'll hardcode defaults for now as demo.conf doesn't specify URL
             let base_url = std::env::var("CF_URL").unwrap_or("http://localhost:8080/ChannelFinder".to_string());
             let username = std::env::var("CF_USER").unwrap_or("cfstore".to_string());
             BackendConfig::ChannelFinder { base_url, username }
        } else {
             // Default to Sqlite if nothing specified? Or Mock?
             // Let's default to Mock if nothing.
             if procs.is_empty() {
                 BackendConfig::Mock
             } else {
                 BackendConfig::Mock
             }
        };

        Self {
            server_ip,
            server_port,
            server_key,
            backend,
        }
    }
}
