//! Workflow mutations use workflow -> work/task row ordering. Ordinary task
//! settlement only inserts terminal obligations; it never takes a workflow lock.
mod data;
mod execution;
#[cfg(test)]
mod tests;
mod work;

use crate::{persistence as db, *};
use data::*;
use ledgence_orchestration_core as core;
use ledgence_worker_api::ProgramDescriptor;
use serde_json::Value;
use sqlx::{PgConnection, Row, postgres::PgRow};
use std::collections::BTreeMap;

impl WorkflowStore for PostgresStore {
    fn lookup_workflow_submission<'a>(
        &'a self,
        scope: &'a Scope,
        key: &'a str,
    ) -> ContractFuture<'a, Option<WorkflowSnapshot>> {
        Box::pin(self.run(move || async move {
            scope.validate()?;
            validate_text(key, 255)?;
            let row = sqlx::query("SELECT workflow_id,tenant_id,namespace,state,trunc(revision)::text AS revision_text,current_activation_id,submitted_at_ms,terminal_at_ms,correlation_key FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND idempotency_key=$3")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(key).fetch_optional(&self.pool).await?;
            row.as_ref().map(status_record).transpose()
        }))
    }

    fn replay_workflow_submission<'a>(
        &'a self,
        command: &'a SubmitCommand,
    ) -> ContractFuture<'a, Option<WorkflowSnapshot>> {
        Box::pin(self.run(move || async move {
            core::validate_submission(command)?;
            let row = sqlx::query("SELECT *,trunc(revision)::text AS revision_text FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND idempotency_key=$3")
                .bind(&command.input.tenant_id).bind(&command.input.namespace).bind(&command.idempotency_key).fetch_optional(&self.pool).await?;
            let Some(row) = row else { return Ok(None); };
            let run = run_record(&row)?;
            replay_submission(&run, command)?;
            Ok(Some(run.snapshot))
        }))
    }

    fn accept_resolved_workflow<'a>(
        &'a self,
        command: &'a SubmitCommand,
        controller: &'a ProgramDescriptor,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(self.run(move || async move {
            core::validate_submission(command)?;
            controller.validate().map_err(ContractError::from)?;
            if controller.program != command.input.program { return Err(ContractError::Conflict.into()); }
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_write().await?;
            let id = db::id(&mut tx, "wf").await?;
            let now = db::now(&mut tx).await?;
            let inserted = sqlx::query("INSERT INTO workflow_runs(workflow_id,tenant_id,namespace,idempotency_key,submission_bytes,controller_bytes,state,continuation,checkpoint_bytes,submitted_at_ms,correlation_key) VALUES($1,$2,$3,$4,$5,$6,'running','start',$7,$8,$9) ON CONFLICT(tenant_id,namespace,idempotency_key) DO NOTHING")
                .bind(&id).bind(&command.input.tenant_id).bind(&command.input.namespace).bind(&command.idempotency_key)
                .bind(codec::encode(command)?).bind(codec::encode(controller)?).bind(codec::encode(&Value::Null)?).bind(codec::ms(now)?).bind(&command.input.correlation_key)
                .execute(&mut *tx).await?.rows_affected();
            let mut run = load_run(&mut tx, &command_scope(command), if inserted == 1 { Some(&id) } else { None }, Some(&command.idempotency_key), true).await?;
            replay_submission(&run, command)?;
            let mut wakes = Vec::new();
            if inserted == 1 {
                wakes.push(schedule_activation(&mut tx, &mut run, BTreeMap::new(), now).await?);
                record_history(&mut tx, &run.snapshot.workflow_id, run.snapshot.activation_id.as_deref(), now, "started").await?;
            }
            tx.commit().await?;
            self.workflow_wakes(&wakes);
            Ok(run.snapshot)
        }))
    }

    fn workflow_status<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(self.run(move || async move {
            scope.validate()?;
            validate_text(id, 128)?;
            let row = sqlx::query("SELECT workflow_id,tenant_id,namespace,state,trunc(revision)::text AS revision_text,current_activation_id,submitted_at_ms,terminal_at_ms,correlation_key FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(id).fetch_optional(&self.pool).await?.ok_or(ContractError::NotFound)?;
            status_record(&row)
        }))
    }

    fn workflow_result<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowResult> {
        Box::pin(self.run(move || async move {
            scope.validate()?;
            validate_text(id, 128)?;
            let row = sqlx::query("SELECT workflow_id,tenant_id,namespace,state,trunc(revision)::text AS revision_text,current_activation_id,submitted_at_ms,terminal_at_ms,correlation_key,CASE WHEN terminal_at_ms IS NOT NULL THEN outcome_bytes ELSE NULL END AS outcome_bytes FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(id).fetch_optional(&self.pool).await?.ok_or(ContractError::NotFound)?;
            let result = WorkflowResult { workflow: status_record(&row)?, outcome: row.try_get::<Option<Vec<u8>>, _>("outcome_bytes")?.map(|b| codec::decode(&b)).transpose()? };
            result.validate().map_err(|_| corrupt("result"))?;
            Ok(result)
        }))
    }

    fn activation_context<'a>(
        &'a self,
        owner: &'a LeaseOwner,
    ) -> ContractFuture<'a, WorkflowActivationContext> {
        Box::pin(self.run(move || self.activation_context_once(owner)))
    }
    fn record_local_result<'a>(
        &'a self,
        command: &'a LocalResultCommand,
    ) -> ContractFuture<'a, LocalResultReceipt> {
        Box::pin(self.run(move || self.record_local_result_once(command)))
    }
    fn claim_work(&self, limit: u32) -> ContractFuture<'_, Vec<WorkflowWork>> {
        Box::pin(self.run(move || self.claim_work_once(limit)))
    }
    fn apply_work<'a>(
        &'a self,
        work: &'a WorkflowWork,
        resolved: &'a [ResolvedWorkflowChild],
    ) -> ContractFuture<'a, WorkflowProgress> {
        Box::pin(self.run(move || self.apply_work_once(work, resolved)))
    }
    fn retry_work<'a>(&'a self, work: &'a WorkflowWork, reason: &'a str) -> ContractFuture<'a, ()> {
        Box::pin(self.run(move || self.retry_work_once(work, reason)))
    }
    fn reject_work<'a>(
        &'a self,
        work: &'a WorkflowWork,
        error: &'a ApplicationError,
    ) -> ContractFuture<'a, ()> {
        Box::pin(self.run(move || self.reject_work_once(work, error)))
    }
    fn cancel_workflow<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(self.run(move || async move {
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_write().await?;
            let mut run = load_run(&mut tx, scope, Some(id), None, true).await?;
            if !run.snapshot.state.is_terminal() && run.snapshot.state != WorkflowState::Cancelling
            {
                let now = db::now(&mut tx).await?;
                run.snapshot.state = WorkflowState::Cancelling;
                run.outcome = Some(WorkflowOutcome::Cancelled {});
                save_run(&mut tx, &run).await?;
                enqueue_drain(&mut tx, &run, now).await?;
                record_history(
                    &mut tx,
                    id,
                    run.snapshot.activation_id.as_deref(),
                    now,
                    "cancel_requested",
                )
                .await?;
            }
            tx.commit().await?;
            Ok(run.snapshot)
        }))
    }
}

impl PostgresStore {
    fn workflow_wakes(&self, tasks: &[TaskSnapshot]) {
        for task in tasks {
            self.acquisition_wake
                .publish(AcquisitionHint::QueueChanged(AcquisitionQueue {
                    scope: task.scope(),
                    queue: task.input.queue.clone(),
                }));
        }
    }
}

/// Called only for a newly recorded terminal transition, with its task locked.
/// Reading immutable membership does not take a workflow lock. FK checks use
/// KEY SHARE, compatible with the workflow's NO KEY UPDATE mutation lock.
pub(crate) async fn terminal_obligation(
    connection: &mut PgConnection,
    task: &TaskSnapshot,
) -> StoreResult<()> {
    if task.workflow_id.is_none() {
        return Ok(());
    }
    let at = codec::ms(
        task.terminal_at
            .ok_or_else(|| corrupt("terminal timestamp"))?,
    )?;
    sqlx::query("WITH ended AS (UPDATE workflow_task_links SET terminal=true WHERE task_id=$1 RETURNING task_id,workflow_id) INSERT INTO workflow_work(id,workflow_id,task_id,available_at_ms,created_at_ms) SELECT 'terminal:'||task_id,workflow_id,task_id,$2,$2 FROM ended ON CONFLICT DO NOTHING")
        .bind(&task.task_id).bind(at).execute(connection).await?;
    Ok(())
}
