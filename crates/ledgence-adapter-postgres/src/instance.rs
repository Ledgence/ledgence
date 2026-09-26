//! Persistent single-instance binding. Legacy unbound databases remain usable.
use crate::*;
use sqlx::{AssertSqlSafe, Row};

const SCOPED_TABLES: &[&str] = &[
    "tasks",
    "workflow_runs",
    "worker_sessions",
    "dispatch_routes",
    "completion_destinations",
    "completion_subscriptions",
    "retention_scans",
    "retention_turn",
    "console_programs",
    "console_program_versions",
];

/// Each non-null byte-collated scope lies between these indexed extremes.
/// Inspect at most two rows per table, including expired and retiring records.
pub(super) fn scope_audit_query() -> String {
    let mut extremes = Vec::with_capacity(SCOPED_TABLES.len() * 2);
    for table in SCOPED_TABLES {
        for order in ["ASC", "DESC"] {
            extremes.push(format!(
                "(SELECT tenant_id,namespace FROM {table} ORDER BY tenant_id {order},namespace {order} LIMIT 1)"
            ));
        }
    }
    format!(
        "SELECT EXISTS(SELECT 1 FROM ({}) scopes WHERE tenant_id<>$1 OR namespace<>$2)",
        extremes.join(" UNION ALL ")
    )
}

impl PostgresStore {
    /// Check every server startup, even when no configuration or assets were
    /// supplied. Binding must complete before any background coordinator starts.
    pub async fn initialize_instance(
        &self,
        config: Option<&SelfHostedInstanceConfig>,
    ) -> Result<Option<SelfHostedInstanceContext>> {
        if let Some(config) = config {
            config.validate()?;
        }
        self.run(|| async {
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_write().await?;
            // Locking the table also serializes initialization while it is empty.
            // This transaction is startup-only; execution never holds this lock.
            sqlx::query("LOCK TABLE self_hosted_instance IN EXCLUSIVE MODE").execute(&mut *tx).await?;
            let existing: Option<(String,String,String)> = sqlx::query_as("SELECT instance_id,tenant_id,namespace FROM self_hosted_instance WHERE singleton")
                .fetch_optional(&mut *tx).await?;
            let Some(config) = config else {
                if existing.is_some() { return Err(ContractError::InvalidInput("this database is bound to an instance; --instance-config is required, including headless operation".into()).into()); }
                tx.commit().await?;
                return Ok(None);
            };
            if let Some((id,tenant,namespace)) = existing
                && (id != config.instance_id || tenant != config.scope.tenant_id || namespace != config.scope.namespace) {
                return Err(ContractError::InvalidInput("instance configuration does not match the database binding".into()).into());
            }
            self.audit_instance_scopes(&mut tx, &config.scope).await?;
            sqlx::query("INSERT INTO self_hosted_instance(singleton,instance_id,tenant_id,namespace) VALUES(true,$1,$2,$3) ON CONFLICT(singleton) DO NOTHING")
                .bind(&config.instance_id).bind(&config.scope.tenant_id).bind(&config.scope.namespace).execute(&mut *tx).await?;
            tx.commit().await?;
            self.remember_scope(&config.scope)?;
            Ok(Some(config.context()))
        }).await
    }

    /// Refresh a binding for non-serving tools after migration. Retention may
    /// operate with its existing explicit scope, but cannot cross this binding.
    pub async fn load_instance_binding(&self) -> Result<()> {
        self.run(|| async {
            let exists: bool =
                sqlx::query_scalar("SELECT to_regclass('self_hosted_instance') IS NOT NULL")
                    .fetch_one(&self.pool)
                    .await?;
            if exists {
                let row = sqlx::query(
                    "SELECT tenant_id,namespace FROM self_hosted_instance WHERE singleton",
                )
                .fetch_optional(&self.pool)
                .await?;
                if let Some(row) = row {
                    self.remember_scope(&Scope {
                        tenant_id: row.try_get("tenant_id")?,
                        namespace: row.try_get("namespace")?,
                    })?;
                }
            }
            Ok(())
        })
        .await
    }

    fn remember_scope(&self, scope: &Scope) -> Result<()> {
        scope.validate()?;
        if self.instance_scope.set(scope.clone()).is_err()
            && self.instance_scope.get() != Some(scope)
        {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
    pub(crate) fn require_scope(&self, scope: &Scope) -> Result<()> {
        scope.validate()?;
        if self
            .instance_scope
            .get()
            .is_some_and(|bound| bound != scope)
        {
            return Err(ContractError::NotFound);
        }
        Ok(())
    }
    pub(crate) fn require_session_scope(&self, scope: &Scope) -> Result<()> {
        self.require_scope(scope)
            .map_err(|_| ContractError::UnknownSession)
    }
    pub(crate) fn require_submission_scope(&self, command: &SubmitCommand) -> Result<()> {
        self.require_scope(&Scope {
            tenant_id: command.input.tenant_id.clone(),
            namespace: command.input.namespace.clone(),
        })
    }
    async fn audit_instance_scopes(
        &self,
        tx: &mut sqlx::PgConnection,
        scope: &Scope,
    ) -> StoreResult<()> {
        // Identifiers in this query come exclusively from SCOPED_TABLES.
        let other: bool = sqlx::query_scalar(AssertSqlSafe(scope_audit_query()))
            .bind(&scope.tenant_id)
            .bind(&scope.namespace)
            .fetch_one(tx)
            .await?;
        if other {
            return Err(ContractError::InvalidInput("database contains another binding; stop all writers and migrate offline or use a dedicated database".into()).into());
        }
        Ok(())
    }
}
