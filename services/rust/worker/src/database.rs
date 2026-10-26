use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::{DbMode, WorkerConfig};

// Currently a stub, think of a use case
pub const REPLAY_FORMAT_VERSION: i32 = 1;

pub trait Database: Clone + Send + Sync + 'static {
    fn save_replay(
        &self,
        game_id: &Uuid,
        version: i32,
        events_data: &[u8],
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;

    fn save_error(
        &self,
        game_id: &Uuid,
        error_message: &str,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;

    fn ping(&self) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

/// `DB_MODE=drop`: discards compiled replays and error reports
/// after in-memory processing so the ingestion & worker pipeline can be stress-tested without CONTORL.
#[derive(Debug, Clone, Default)]
pub struct DropDatabase;

impl Database for DropDatabase {
    async fn save_replay(
        &self,
        _game_id: &Uuid,
        _version: i32,
        _events_data: &[u8],
    ) -> Result<(), String> {
        Ok(())
    }

    async fn save_error(&self, _game_id: &Uuid, _error_message: &str) -> Result<(), String> {
        Ok(())
    }

    async fn ping(&self) -> Result<(), String> {
        Ok(())
    }
}

///`DB_MODE=postgres`: persists compiled replays and validation errors
/// to PostgreSQL on CONTORL using a connection pool with idempotent `ON CONFLICT DO NOTHING` queries.
#[derive(Clone)]
pub struct PostgresDatabase {
    pool: Pool,
}

impl PostgresDatabase {
    pub fn from_url(database_url: &str) -> Result<Self, String> {
        let pg_config = database_url
            .parse::<tokio_postgres::Config>()
            .map_err(|e| format!("Invalid DATABASE_URL: {e}"))?;

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
        let pool = Pool::builder(mgr)
            .max_size(2)
            .build()
            .map_err(|e| format!("Failed to build PostgreSQL connection pool: {e}"))?;

        Ok(Self { pool })
    }

    pub async fn connect_with_retry(database_url: &str) -> Self {
        let db = Self::from_url(database_url).expect("Invalid PostgreSQL configuration");
        let retry_delay = Duration::from_secs(2);

        loop {
            match db.ping().await {
                Ok(()) => {
                    info!("Connected to PostgreSQL database pool");
                    return db;
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        "PostgreSQL unavailable during startup; retrying in 2s"
                    );
                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
    }
}

impl Database for PostgresDatabase {
    async fn save_replay(
        &self,
        game_id: &Uuid,
        version: i32,
        events_data: &[u8],
    ) -> Result<(), String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        client
            .execute(
                "INSERT INTO replays (game_id, version, events_data) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (game_id) DO NOTHING",
                &[game_id, &version, &events_data],
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn save_error(&self, game_id: &Uuid, error_message: &str) -> Result<(), String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        client
            .execute(
                "INSERT INTO replay_errors (game_id, error_message) \
                 VALUES ($1, $2) \
                 ON CONFLICT (game_id) DO NOTHING",
                &[game_id, &error_message],
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn ping(&self) -> Result<(), String> {
        let client = self.pool.get().await.map_err(|e| e.to_string())?;
        client
            .simple_query("SELECT 1")
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[derive(Clone)]
pub enum WorkerDatabase {
    Drop(DropDatabase),
    Postgres(PostgresDatabase),
}

impl WorkerDatabase {
    pub async fn connect(config: &WorkerConfig) -> Self {
        match config.db_mode {
            DbMode::Drop => {
                info!("Worker running in DB_MODE=drop");
                Self::Drop(DropDatabase)
            }
            DbMode::Postgres => {
                let db_url = config
                    .database_url
                    .as_deref()
                    .expect("DATABASE_URL required in postgres mode");
                Self::Postgres(PostgresDatabase::connect_with_retry(db_url).await)
            }
        }
    }
}

impl Database for WorkerDatabase {
    async fn save_replay(
        &self,
        game_id: &Uuid,
        version: i32,
        events_data: &[u8],
    ) -> Result<(), String> {
        match self {
            Self::Drop(db) => db.save_replay(game_id, version, events_data).await,
            Self::Postgres(db) => db.save_replay(game_id, version, events_data).await,
        }
    }

    async fn save_error(&self, game_id: &Uuid, error_message: &str) -> Result<(), String> {
        match self {
            Self::Drop(db) => db.save_error(game_id, error_message).await,
            Self::Postgres(db) => db.save_error(game_id, error_message).await,
        }
    }

    async fn ping(&self) -> Result<(), String> {
        match self {
            Self::Drop(db) => db.ping().await,
            Self::Postgres(db) => db.ping().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drop_database_succeeds_without_external_connection() {
        let db = WorkerDatabase::Drop(DropDatabase);
        let game_id = Uuid::now_v7();

        assert!(db.ping().await.is_ok());
        assert!(
            db.save_replay(&game_id, REPLAY_FORMAT_VERSION, &[1, 2, 3])
                .await
                .is_ok()
        );
        assert!(db.save_error(&game_id, "some error").await.is_ok());
    }

    #[tokio::test]
    async fn postgres_database_parses_valid_connection_string() {
        let res =
            PostgresDatabase::from_url("postgres://postgres:postgres@127.0.0.1:5432/observelol");
        assert!(res.is_ok());

        let invalid = PostgresDatabase::from_url("not_a_valid_url://");
        assert!(invalid.is_err());
    }
}
