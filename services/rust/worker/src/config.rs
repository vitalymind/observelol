use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbMode {
    Drop,
    Postgres,
}

impl FromStr for DbMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "drop" => Ok(Self::Drop),
            "postgres" => Ok(Self::Postgres),
            other => Err(format!(
                "Invalid DB_MODE '{other}': expected 'drop' or 'postgres'"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub redis_url: String,
    pub otel_endpoint: String,
    pub db_mode: DbMode,
    pub database_url: Option<String>,
    pub consumer_name: String,
    pub batch_size: usize,
    pub block_ms: usize,
    pub autoclaim_threshold_ms: u64,
    pub compression_level: i32,
}

impl WorkerConfig {
    pub fn from_env() -> Self {
        let redis_url =
            std::env::var("REDIS_URL").expect("Missing required environment variable: REDIS_URL");
        let otel_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .expect("Missing required environment variable: OTEL_EXPORTER_OTLP_ENDPOINT");

        let db_mode_raw = std::env::var("DB_MODE").unwrap_or_else(|_| "drop".to_string());
        let db_mode = DbMode::from_str(&db_mode_raw).expect("Invalid DB_MODE configuration");

        let database_url = match db_mode {
            DbMode::Drop => std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()),
            DbMode::Postgres => {
                let url = std::env::var("DATABASE_URL").expect(
                    "Missing required environment variable: DATABASE_URL when DB_MODE=postgres",
                );
                assert!(
                    !url.trim().is_empty(),
                    "DATABASE_URL cannot be empty when DB_MODE=postgres"
                );
                Some(url)
            }
        };

        let consumer_name = std::env::var("WORKER_CONSUMER_NAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("worker-{}", Uuid::now_v7()));

        let batch_size = std::env::var("WORKER_BATCH_SIZE")
            .ok()
            .map(|v| {
                v.parse::<usize>()
                    .expect("WORKER_BATCH_SIZE must be a valid usize")
            })
            .unwrap_or(16);

        let block_ms = std::env::var("WORKER_BLOCK_MS")
            .ok()
            .map(|v| {
                v.parse::<usize>()
                    .expect("WORKER_BLOCK_MS must be a valid usize")
            })
            .unwrap_or(5_000);

        let autoclaim_threshold_ms = std::env::var("WORKER_AUTOCLAIM_THRESHOLD_MS")
            .ok()
            .map(|v| {
                v.parse::<u64>()
                    .expect("WORKER_AUTOCLAIM_THRESHOLD_MS must be a valid u64")
            })
            .unwrap_or(30_000);

        let compression_level = std::env::var("WORKER_ZSTD_LEVEL")
            .ok()
            .map(|v| {
                v.parse::<i32>()
                    .expect("WORKER_ZSTD_LEVEL must be a valid i32")
            })
            .unwrap_or(3);

        Self {
            redis_url,
            otel_endpoint,
            db_mode,
            database_url,
            consumer_name,
            batch_size,
            block_ms,
            autoclaim_threshold_ms,
            compression_level,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_db_mode_correctly() {
        assert_eq!(DbMode::from_str("drop").unwrap(), DbMode::Drop);
        assert_eq!(DbMode::from_str("DROP").unwrap(), DbMode::Drop);
        assert_eq!(DbMode::from_str("postgres").unwrap(), DbMode::Postgres);
        assert_eq!(DbMode::from_str("POSTGRES").unwrap(), DbMode::Postgres);
        assert!(DbMode::from_str("invalid").is_err());
    }
}
