//! Durable PostgreSQL implementation of Ledgence's atomic storage ports.
//!
//! Migrations are explicit. Successful mutations commit before returning; a
//! connection failure during commit must be reconciled with the same command.

mod codec;
mod delivery;
mod persistence;
mod storage;
mod transaction;

use ledgence_orchestration_api::*;
use ledgence_worker_api::{NoopTraceBridge, TraceBridge};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{future::Future, sync::Arc, time::Duration};
use transaction::TransactionConnection;

/// Versioned embedded migrations. Running them is an explicit deployment action.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Database resource controls, independent of a worker's consumer/process N.
#[derive(Debug, Clone)]
pub struct PostgresOptions {
    pub max_connections: u32,
    pub acquire_timeout: Duration,
    pub statement_timeout: Duration,
    pub operation_timeout: Duration,
}
impl Default for PostgresOptions {
    fn default() -> Self {
        Self {
            max_connections: 8,
            acquire_timeout: Duration::from_secs(5),
            statement_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS),
        }
    }
}

#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    operation_timeout: Duration,
    trace_bridge: Arc<dyn TraceBridge>,
}
impl PostgresStore {
    /// Connect without changing the schema. A fresh database needs `migrate`.
    pub async fn connect(url: &str, options: PostgresOptions) -> Result<Self> {
        if options.max_connections == 0
            || options.acquire_timeout.is_zero()
            || options.statement_timeout.is_zero()
            || options.operation_timeout.is_zero()
            || options.statement_timeout.as_millis() > i32::MAX as u128
        {
            return Err(ContractError::InvalidInput(
                "invalid PostgreSQL resource limits".into(),
            ));
        }
        let statement_ms = options.statement_timeout.as_millis().max(1).to_string();
        let pool = PgPoolOptions::new()
            .max_connections(options.max_connections)
            .acquire_timeout(options.acquire_timeout)
            .after_connect(move |connection, _| {
                let statement_ms = statement_ms.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('statement_timeout',$1,false), set_config('lock_timeout',$1,false), set_config('synchronous_commit','on',false), set_config('application_name','ledgence',false)")
                        .bind(statement_ms).execute(connection).await?;
                    Ok(())
                })
            })
            .connect(url).await.map_err(database_error)?;
        let version: String = sqlx::query_scalar("SHOW server_version_num")
            .fetch_one(&pool)
            .await
            .map_err(database_error)?;
        if !(180_000..190_000).contains(&version.parse::<u32>().unwrap_or_default()) {
            pool.close().await;
            return Err(ContractError::Unavailable(
                "this adapter supports PostgreSQL 18".into(),
            ));
        }
        Ok(Self {
            pool,
            operation_timeout: options.operation_timeout,
            trace_bridge: Arc::new(NoopTraceBridge),
        })
    }

    /// Instrument genuinely new invocation allocations. Durable replays retain
    /// their original event context and do not create another producer span.
    pub fn with_trace_bridge(mut self, bridge: Arc<dyn TraceBridge>) -> Self {
        self.trace_bridge = bridge;
        self
    }

    /// Apply checksum-verified migrations under SQLx's migration lock.
    pub async fn migrate(&self) -> Result<()> {
        let work = async {
            // SQLx migration errors can leave a session advisory lock held.
            // Never return this connection to the pool, including cancellation.
            let mut connection = self.pool.acquire().await.map_err(database_error)?;
            connection.close_on_drop();
            let result = MIGRATOR.run(&mut *connection).await.map_err(|error| {
                tracing::error!(error = %error, "PostgreSQL migration failed");
                ContractError::Unavailable("PostgreSQL migration failed".into())
            });
            connection.close().await.map_err(database_error)?;
            result
        };
        tokio::time::timeout(self.operation_timeout, work)
            .await
            .unwrap_or_else(|_| {
                Err(ContractError::Unavailable(
                    "PostgreSQL migration timed out".into(),
                ))
            })
    }

    /// Verify the exact embedded migration set without applying any changes.
    /// Missing, dirty, newer, or checksum-mismatched schema versions cannot serve.
    pub async fn verify_schema(&self) -> Result<()> {
        let work = async {
            let applied: Vec<(i64, bool, Vec<u8>)> = sqlx::query_as(
                "SELECT version, success, checksum FROM _sqlx_migrations ORDER BY version",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|_| {
                ContractError::Unavailable(
                    "schema verification failed; run ledgence-orchestrator migrate explicitly"
                        .into(),
                )
            })?;
            let expected: Vec<_> = MIGRATOR
                .iter()
                .filter(|migration| migration.migration_type.is_up_migration())
                .collect();
            if applied.len() != expected.len()
                || applied.iter().zip(expected).any(|(applied, expected)| {
                    applied.0 != expected.version
                        || !applied.1
                        || applied.2.as_slice() != expected.checksum.as_ref()
                })
            {
                return Err(ContractError::Unavailable(
                    "database schema does not match this orchestrator's migrations".into(),
                ));
            }
            Ok(())
        };
        tokio::time::timeout(self.operation_timeout, work)
            .await
            .unwrap_or_else(|_| {
                Err(ContractError::Unavailable(
                    "schema verification timed out".into(),
                ))
            })
    }

    /// Check database availability within the configured operation budget.
    pub async fn check_connection(&self) -> Result<()> {
        tokio::time::timeout(self.operation_timeout, async {
            sqlx::query("SELECT 1")
                .execute(&self.pool)
                .await
                .map_err(database_error)?;
            Ok(())
        })
        .await
        .unwrap_or_else(|_| {
            Err(ContractError::Unavailable(
                "database availability check timed out".into(),
            ))
        })
    }

    /// Close this pool after callers have stopped submitting operations.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    async fn transaction_connection(&self) -> StoreResult<TransactionConnection> {
        Ok(TransactionConnection::new(self.pool.acquire().await?))
    }

    async fn run<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = StoreResult<T>>,
    {
        let work = async {
            for retry in 0..3 {
                match operation().await {
                    Ok(value) => return Ok(value),
                    Err(StoreError::Database(error)) if retry < 2 && retryable(&error) => {
                        tokio::time::sleep(Duration::from_millis(10 * (retry + 1))).await;
                    }
                    Err(error) => return Err(error.into_contract()),
                }
            }
            unreachable!("retry loop always returns")
        };
        tokio::time::timeout(self.operation_timeout, work)
            .await
            .unwrap_or_else(|_| {
                Err(ContractError::Unavailable(
                    "database operation timed out; reconcile using the same command".into(),
                ))
            })
    }
}

type StoreResult<T> = std::result::Result<T, StoreError>;
#[derive(Debug)]
enum StoreError {
    Contract(ContractError),
    Database(sqlx::Error),
}
impl From<ContractError> for StoreError {
    fn from(e: ContractError) -> Self {
        Self::Contract(e)
    }
}
impl From<sqlx::Error> for StoreError {
    fn from(e: sqlx::Error) -> Self {
        Self::Database(e)
    }
}
impl StoreError {
    fn into_contract(self) -> ContractError {
        match self {
            Self::Contract(e) => e,
            Self::Database(e) => database_error(e),
        }
    }
}
fn database_error(error: sqlx::Error) -> ContractError {
    // Do not include SQL detail (which can contain application values) in replies.
    tracing::warn!(
        sqlstate = error.as_database_error().and_then(|e| e.code()).as_deref(),
        "PostgreSQL operation failed"
    );
    ContractError::Unavailable("database operation failed; reconcile using the same command".into())
}
fn retryable(error: &sqlx::Error) -> bool {
    let Some(error) = error.as_database_error() else {
        return false;
    };
    match error.code().as_deref() {
        Some("40001" | "40P01") => true,
        Some("23505") => matches!(
            error.constraint(),
            Some(
                "tasks_pkey"
                    | "tasks_run_id_key"
                    | "attempts_pkey"
                    | "attempts_lease_id_key"
                    | "attempts_event_source_event_id_key"
                    | "worker_sessions_pkey"
            )
        ),
        _ => false,
    }
}

#[cfg(test)]
mod codec_db_tests;
#[cfg(test)]
mod expiry_db_tests;
#[cfg(test)]
mod http_delivery_tests;
#[cfg(test)]
mod restart_db_tests;
#[cfg(test)]
mod schema_db_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transaction_db_tests;
#[cfg(test)]
mod worker_delivery_tests;

#[cfg(test)]
mod observability_db_tests;
