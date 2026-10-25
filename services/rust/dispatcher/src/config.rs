pub const EVENT_BUFFER_CAPACITY: usize = 4096;

#[derive(Clone)]
pub struct DispatcherConfig {
    pub redis_url: String,
    pub socket_path: String,
    pub otel_endpoint: String,
    pub redis_event_ttl_sec: u64,
    pub game_end_grace_ms: u64,
}

impl DispatcherConfig {
    pub fn from_env() -> Self {
        let redis_url =
            std::env::var("REDIS_URL").expect("Missing required environment variable: REDIS_URL");
        let socket_path = std::env::var("DISPATCHER_SOCKET_PATH")
            .expect("Missing required environment variable: DISPATCHER_SOCKET_PATH");
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
            socket_path,
            otel_endpoint,
            redis_event_ttl_sec,
            game_end_grace_ms,
        }
    }
}
