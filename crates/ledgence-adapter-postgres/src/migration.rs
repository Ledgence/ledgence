use crate::{MIGRATOR, PostgresStore, database_error};
use ledgence_orchestration_api::{ContractError, Result};
use sqlx::migrate::Migrator;
use std::time::Duration;
use tokio::time::Instant;

/// A separate deployment budget; ordinary database request limits are unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationOptions {
    /// Total time for pool acquisition, lock waiting, migrations, and connection
    /// closure. Must be a positive whole number of milliseconds up to i32::MAX.
    /// Pool acquisition also retains its independently configured shorter limit.
    pub timeout: Duration,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(600),
        }
    }
}

impl MigrationOptions {
    pub fn validate(&self) -> Result<()> {
        if self.timeout.is_zero()
            || !self.timeout.subsec_nanos().is_multiple_of(1_000_000)
            || self.timeout.as_millis() > i32::MAX as u128
        {
            return Err(ContractError::InvalidInput(
                "migration timeout must be whole milliseconds between 1 and 2147483647".into(),
            ));
        }
        Ok(())
    }
}

impl PostgresStore {
    /// Apply checksum-verified migrations under SQLx's migration lock with the
    /// default ten-minute deployment budget, independent of request timeouts.
    pub async fn migrate(&self) -> Result<()> {
        self.migrate_with_options(MigrationOptions::default()).await
    }

    /// Apply the embedded migrations with a bounded deployment budget. Completed
    /// earlier migrations remain committed if a later migration fails or times
    /// out; each transactional migration rolls back its own incomplete changes.
    pub async fn migrate_with_options(&self, options: MigrationOptions) -> Result<()> {
        self.migrate_with_migrator(options, &MIGRATOR).await
    }

    pub(crate) async fn migrate_with_migrator(
        &self,
        options: MigrationOptions,
        migrator: &Migrator,
    ) -> Result<()> {
        options.validate()?;
        let deadline = Instant::now() + options.timeout;
        let work = async {
            let mut connection = self.pool.acquire().await.map_err(database_error)?;
            // Both SQLx's advisory lock and the longer session limits must never
            // escape into ordinary pooled requests, including on cancellation.
            connection.close_on_drop();
            let remaining_ms = deadline
                .saturating_duration_since(Instant::now())
                .as_millis();
            if remaining_ms == 0 {
                return Err(timed_out());
            }
            sqlx::query(
                "SELECT set_config('statement_timeout',$1,false), set_config('lock_timeout',$1,false)",
            )
            .bind(remaining_ms.to_string())
            .execute(&mut *connection)
            .await
            .map_err(database_error)?;
            // The connection is already acquired. SQLx exposes run_direct to
            // avoid its generic Acquire future losing Send on spawned callers.
            let result = migrator
                .run_direct(None, &mut *connection, false)
                .await
                .map_err(|error| {
                    tracing::error!(error = %error, "PostgreSQL migration failed");
                    ContractError::Unavailable("PostgreSQL migration failed".into())
                });
            let closed = connection.close().await.map_err(database_error);
            result.and(closed)
        };
        let result = tokio::time::timeout_at(deadline, work)
            .await
            .unwrap_or_else(|_| Err(timed_out()));
        // Tokio can poll an already-ready operation before its deadline timer.
        // Never report a late completion as meeting the deployment budget.
        if Instant::now() >= deadline {
            return Err(timed_out());
        }
        result
    }
}

fn timed_out() -> ContractError {
    ContractError::Unavailable("PostgreSQL migration timed out".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_budget_requires_supported_whole_milliseconds() {
        for timeout in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_micros(1_001),
            Duration::from_millis(i32::MAX as u64 + 1),
            Duration::MAX,
        ] {
            assert!(matches!(
                MigrationOptions { timeout }.validate(),
                Err(ContractError::InvalidInput(_))
            ));
        }
        for timeout in [
            Duration::from_millis(1),
            MigrationOptions::default().timeout,
            Duration::from_millis(i32::MAX as u64),
        ] {
            MigrationOptions { timeout }.validate().unwrap();
        }
    }
}
