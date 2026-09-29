//! A fork is an interactive durable mutation, not a controller decision. Its
//! transaction owns the parent and task locks without advancing either state.
use super::*;
use execution::{check_live, lock_activation};

impl PostgresStore {
    pub(super) async fn fork_workflow_once(
        &self,
        command: &WorkflowForkCommand,
    ) -> StoreResult<WorkflowForkReceipt> {
        command.validate()?;
        let owner = &command.owner;
        self.require_scope(&owner.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let (workflow, authority) = lock_activation(&mut tx, owner).await?;
        let prior: Option<(Vec<u8>, String)> = sqlx::query_as(
            "SELECT request_bytes,accepting_attempt_id FROM workflow_forks WHERE workflow_id=$1 AND fork_key=$2",
        )
        .bind(&workflow.workflow_id).bind(&command.fork.key)
        .fetch_optional(&mut *tx).await?;
        if let Some((bytes, accepting_attempt)) = prior {
            let original: WorkflowForkRequest = codec::decode(&bytes)?;
            if !original.matches(&command.fork)? {
                return Err(ContractError::Conflict.into());
            }
            if accepting_attempt != owner.attempt_id {
                check_live(&workflow, &authority, owner, db::now(&mut tx).await?, true)?;
            }
            // Exact original-owner replay remains valid after cancellation,
            // attempt replacement and continuation. Another owner must be live.
            tx.commit().await?;
            return Ok(receipt(&original, true));
        }
        let now = db::now(&mut tx).await?;
        check_live(&workflow, &authority, owner, now, true)?;
        let bytes = codec::encode(&command.fork)?;
        let (count, total): (i64, i64) = sqlx::query_as(
            "SELECT count(*)::bigint,coalesce(sum(octet_length(request_bytes)),0)::bigint FROM workflow_forks WHERE accepting_activation_id=$1",
        ).bind(&owner.task_id).fetch_one(&mut *tx).await?;
        if count >= WORKFLOW_MAX_FORKS as i64
            || total + bytes.len() as i64 > WORKFLOW_FORK_LEDGER_MAX_BYTES as i64
        {
            return Err(invalid("workflow fork ledger exceeds supported limit").into());
        }
        let registered =
            registered_children(&mut tx, &workflow.workflow_id, &owner.task_id).await?;
        check_child_budget(registered, command.fork.branches.len())?;
        let keys: Vec<&str> = command
            .fork
            .branches
            .iter()
            .map(|branch| branch.key.as_str())
            .collect();
        let collision: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM workflow_task_links WHERE workflow_id=$1 AND NOT is_activation AND command_key=ANY($2)) OR EXISTS(SELECT 1 FROM owned_workflow_links WHERE parent_workflow_id=$1 AND command_key=ANY($2))",
        ).bind(&workflow.workflow_id).bind(&keys).fetch_one(&mut *tx).await?;
        // A new fork never adopts preexisting tasks, owned workflows or branches
        // of another fork, even if a subset of their submissions looks equal.
        if collision {
            return Err(ContractError::Conflict.into());
        }
        let parent = load_run(
            &mut tx,
            &owner.scope,
            Some(&workflow.workflow_id),
            None,
            false,
        )
        .await?;
        sqlx::query("INSERT INTO workflow_forks(workflow_id,fork_key,request_bytes,accepting_activation_id,accepting_attempt_id,accepted_at_ms) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(&workflow.workflow_id).bind(&command.fork.key).bind(bytes)
            .bind(&owner.task_id).bind(&owner.attempt_id).bind(codec::ms(now)?)
            .execute(&mut *tx).await?;
        let origin_trace = command
            .processing_trace
            .as_ref()
            .or(parent.submission.origin_trace.as_ref());
        let mut wakes = Vec::with_capacity(command.fork.branches.len());
        for branch in &command.fork.branches {
            let child = WorkflowChildCommand {
                kind: WorkflowChildKind::Workflow,
                key: branch.key.clone(),
                program: parent.controller.program.clone(),
                queue: branch.queue.clone(),
                data: branch.data.clone(),
                retry_policy: branch.retry_policy.clone(),
                attempt_timeout_ms: branch.attempt_timeout_ms,
            };
            let start = owned::ChildStart {
                descriptor: &parent.controller,
                entrypoint: &branch.entrypoint,
                fork_key: Some(&command.fork.key),
                origin_trace,
            };
            wakes.push(
                owned::create_child_at(&mut tx, &parent, &owner.task_id, &child, start, now)
                    .await?,
            );
        }
        explorer::insert(
            &mut tx,
            &owner.task_id,
            &ledgence_orchestration_api::console::ConsoleExplorerData::Fork {
                key: command.fork.key.clone(),
                branch_keys: command
                    .fork
                    .branches
                    .iter()
                    .map(|branch| branch.key.clone())
                    .collect(),
                accepted_at: now,
                accepting_attempt_id: owner.attempt_id.clone(),
            },
        )
        .await?;
        record_history(
            &mut tx,
            &workflow.workflow_id,
            Some(&owner.task_id),
            now,
            "fork_accepted",
        )
        .await?;
        tx.commit().await?;
        self.workflow_wakes(&wakes);
        Ok(receipt(&command.fork, false))
    }
}

fn receipt(fork: &WorkflowForkRequest, already_accepted: bool) -> WorkflowForkReceipt {
    WorkflowForkReceipt {
        key: fork.key.clone(),
        branch_keys: fork
            .branches
            .iter()
            .map(|branch| branch.key.clone())
            .collect(),
        already_accepted,
    }
}

/// Forks and later staged decisions share a single registration bound. Existing
/// child-key replays do not spend the current activation's child budget.
pub(super) async fn registered_children(
    connection: &mut PgConnection,
    workflow: &str,
    activation: &str,
) -> StoreResult<usize> {
    let count: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM workflow_task_links WHERE workflow_id=$1 AND activation_id=$2 AND NOT is_activation) + (SELECT count(*) FROM owned_workflow_links WHERE parent_workflow_id=$1 AND creating_activation_id=$2)",
    ).bind(workflow).bind(activation).fetch_one(connection).await?;
    usize::try_from(count).map_err(|_| corrupt("child registration count").into())
}

pub(super) fn check_child_budget(registered: usize, additional: usize) -> StoreResult<()> {
    if registered.saturating_add(additional) > WORKFLOW_MAX_COMMANDS {
        return Err(
            invalid("workflow activation child registrations exceed supported limit").into(),
        );
    }
    Ok(())
}
