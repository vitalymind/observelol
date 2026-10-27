pub const EVENT_BUFFER_CAPACITY: usize = 32;

#[derive(Clone)]
pub struct DispatcherConfig {
    pub redis_url: String,
    pub bind_addr: String,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub nginx_upstream_url: String,
    pub otel_endpoint: String,
    pub redis_event_ttl_sec: u64,
    pub game_end_grace_ms: u64,
}

impl DispatcherConfig {
    pub fn from_env() -> Self {
        let redis_url =
            std::env::var("REDIS_URL").expect("Missing required environment variable: REDIS_URL");
        let bind_addr =
            std::env::var("DISPATCHER_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:443".to_string());
        let tls_cert_path = std::env::var("TLS_CERT_PATH")
            .expect("Missing required environment variable: TLS_CERT_PATH");
        let tls_key_path = std::env::var("TLS_KEY_PATH")
            .expect("Missing required environment variable: TLS_KEY_PATH");
        let nginx_upstream_url = std::env::var("NGINX_UPSTREAM_URL")
            .expect("Missing required environment variable: NGINX_UPSTREAM_URL");
        let otel_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .expect("Missing required environment variable: OTEL_EXPORTER_OTLP_ENDPOINT");
        let redis_event_ttl_sec = std::env::var("REDIS_EVENT_TTL_SEC")
            .expect("Missing required environment variable: REDIS_EVENT_TTL_SEC")
            .parse::<u64>()
            .expect("REDIS_EVENT_TTL_SEC must be a valid u64 integer");
        let game_end_grace_ms = std::env::var("GAME_END_GRACE_MS")
            .expect("Missing required environment variable: GAME_END_GRACE_MS")
            .parse::<u64>()
            .expect("GAME_END_GRACE_MS must be a valid u64 integer");

        Self {
            redis_url,
            bind_addr,
            tls_cert_path,
            tls_key_path,
            nginx_upstream_url,
            otel_endpoint,
            redis_event_ttl_sec,
            game_end_grace_ms,
        }
    }
}
