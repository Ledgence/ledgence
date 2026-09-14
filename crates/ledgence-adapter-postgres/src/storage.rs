use crate::{persistence as db, *};
use ledgence_orchestration_core as core;
use ledgence_worker_api::ProgramDescriptor;

impl TaskStore for PostgresStore {
    fn lookup_submission<'a>(
        &'a self,
        scope: &'a Scope,
        key: &'a str,
    ) -> ContractFuture<'a, Option<TaskSnapshot>> {
        Box::pin(self.run(move || async move {
            scope.validate()?;
            validate_text(key, 255)?;
            let row = sqlx::query(
                "SELECT * FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND idempotency_key=$3",
            )
            .bind(&scope.tenant_id)
            .bind(&scope.namespace)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row.as_ref().map(codec::task).transpose()?)
        }))
    }
    fn accept_resolved_submission<'a>(
        &'a self,
        command: &'a SubmitCommand,
        descriptor: &'a ProgramDescriptor,
    ) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(self.run(move || async move {
            core::validate_submission(command)?;
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_write().await?;
            let task_id = db::id(&mut tx,"task").await?;
            let run_id = db::id(&mut tx,"run").await?;
            let transition = core::submit(command,descriptor,&task_id,&run_id,db::now(&mut tx).await?)?;
            let t = &transition.task;
            let input = codec::encode(&t.input)?;
            let descriptor = codec::encode(&t.descriptor)?;
            let trace = t.origin_trace.as_ref().map(codec::encode).transpose()?;
            let at = codec::ms(t.submitted_at)?;
            let inserted = sqlx::query!("INSERT INTO tasks(task_id,run_id,tenant_id,namespace,queue,idempotency_key,correlation_key,input_bytes,descriptor_bytes,origin_trace_bytes,state,submitted_at_ms,available_at_ms,attempt_count) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'queued',$11,$11,0) ON CONFLICT(tenant_id,namespace,idempotency_key) DO NOTHING",t.task_id,t.run_id,t.input.tenant_id,t.input.namespace,t.input.queue,t.idempotency_key,t.input.correlation_key,input,descriptor,trace,at)
                .execute(&mut *tx).await?.rows_affected();
            let task = if inserted == 1 {
                db::history(&mut tx,&transition.history).await?;
                transition.task
            } else {
                // A fresh statement sees the winner after the unique-index wait.
                let row = sqlx::query("SELECT * FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND idempotency_key=$3")
                    .bind(&command.input.tenant_id).bind(&command.input.namespace).bind(&command.idempotency_key).fetch_one(&mut *tx).await?;
                let task = codec::task(&row)?;
                core::replay_submission(&task,command)?;
                task
            };
            tx.commit().await?;
            if inserted == 1 {
                self.acquisition_wake.publish(AcquisitionHint::QueueChanged(AcquisitionQueue {
                    scope: task.scope(), queue: task.input.queue.clone(),
                }));
            }
            Ok(task)
        }))
    }
    fn open_session<'a>(
        &'a self,
        scope: &'a Scope,
        queue: &'a str,
        concurrency: u32,
    ) -> ContractFuture<'a, WorkerSession> {
        Box::pin(self.run(move || async move {
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_write().await?;
            let id = db::id(&mut tx,"ws").await?;
            let session = core::open_session(&id,scope.clone(),queue,concurrency,db::now(&mut tx).await?)?;
            let n = i64::from(concurrency); let at = codec::ms(session.expires_at)?;
            sqlx::query!("INSERT INTO worker_sessions(session_id,tenant_id,namespace,queue,concurrency,expires_at_ms) VALUES($1,$2,$3,$4,$5,$6)",id,scope.tenant_id,scope.namespace,queue,n,at).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(session)
        }))
    }
    fn extend_session<'a>(&'a self, id: &'a str) -> ContractFuture<'a, WorkerSession> {
        Box::pin(self.run(move || async move {
            validate_text(id, 128)?;
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_write().await?;
            let row = sqlx::query("SELECT * FROM worker_sessions WHERE session_id=$1 FOR UPDATE")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(ContractError::UnknownSession)?;
            let current = codec::session(&row)?;
            let session = core::extend_session(&current, db::now(&mut tx).await?)?;
            let at = codec::ms(session.expires_at)?;
            sqlx::query!(
                "UPDATE worker_sessions SET expires_at_ms=$2 WHERE session_id=$1",
                id,
                at
            )
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(session)
        }))
    }
    fn list_tasks<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a TaskListQuery,
    ) -> ContractFuture<'a, TaskPage> {
        Box::pin(self.run(move || self.list_tasks_once(scope, query)))
    }

    fn status<'a>(&'a self, scope: &'a Scope, id: &'a str) -> ContractFuture<'a, TaskStatus> {
        Box::pin(self.run(move || async move {
            scope.validate()?;
            validate_text(id, 128)?;
            let row = sqlx::query(include_str!("../queries/task_status.sql"))
                .bind(&scope.tenant_id)
                .bind(&scope.namespace)
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
                .ok_or(ContractError::NotFound)?;
            Ok(codec::status(&row)?)
        }))
    }

    fn result<'a>(&'a self, scope: &'a Scope, id: &'a str) -> ContractFuture<'a, TaskResult> {
        Box::pin(self.run(move || async move {
            scope.validate()?;
            validate_text(id, 128)?;
            // One statement snapshot binds scheduling state to the latest attempt
            // and its immutable report even while another transaction finalizes.
            let row = sqlx::query(include_str!("../queries/task_result.sql"))
                .bind(&scope.tenant_id)
                .bind(&scope.namespace)
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
                .ok_or(ContractError::NotFound)?;
            let task = codec::task(&row)?;
            let attempt = if task.attempt_count == 0 {
                None
            } else {
                Some(codec::attempt_prefixed(&row, &task, "a_")?)
            };
            Ok(core::task_result(&task, attempt.as_ref())?)
        }))
    }

    fn inspect<'a>(&'a self, scope: &'a Scope, id: &'a str) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(self.run(move || async move {
            scope.validate()?;
            validate_text(id, 128)?;
            let mut connection = self.pool.acquire().await?;
            db::load_task(&mut connection, scope, id, false).await
        }))
    }
    fn inspect_attempt<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
        attempt_id: &'a str,
    ) -> ContractFuture<'a, AttemptSnapshot> {
        Box::pin(self.run(move || async move {
            scope.validate()?;
            validate_text(task_id, 128)?;
            validate_text(attempt_id, 128)?;
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_read().await?;
            let task = db::load_task(&mut tx, scope, task_id, false).await?;
            let attempt = db::load_attempt(&mut tx, &task, attempt_id).await?;
            tx.commit().await?;
            Ok(attempt)
        }))
    }
    fn history<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
        after: u64,
    ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>> {
        Box::pin(self.run(move || async move {
            scope.validate()?; validate_text(task_id,128)?;
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_read().await?;
            db::load_task(&mut tx,scope,task_id,false).await?;
            let rows = sqlx::query("SELECT *,trunc(sequence)::text AS sequence_text FROM task_history WHERE task_id=$1 AND sequence>($2::text)::ldg_u64 ORDER BY sequence LIMIT 100")
                .bind(task_id).bind(after.to_string()).fetch_all(&mut *tx).await?;
            let events = rows.iter().map(codec::history).collect::<Result<Vec<_>>>()?;
            tx.commit().await?;
            Ok(events)
        }))
    }
    fn probe_acquisition<'a>(
        &'a self,
        command: &'a AcquireCommand,
        finish_empty: bool,
        deadline: Instant,
    ) -> ContractFuture<'a, AcquisitionProbe> {
        Box::pin(self.run_until(deadline, move || {
            self.probe_acquisition_once(command, finish_empty)
        }))
    }
    fn renew<'a>(&'a self, command: &'a RenewCommand) -> ContractFuture<'a, Authority> {
        Box::pin(self.run(move || self.renew_once(command)))
    }
    fn settle<'a>(&'a self, command: &'a SettleCommand) -> ContractFuture<'a, SettleReply> {
        Box::pin(async move {
            let result = self.run(|| self.settle_once(command)).await;
            if let Err(error) = &result {
                // Truncate identifiers to keep malformed direct callers bounded.
                tracing::warn!(tenant=%short(&command.owner.scope.tenant_id), namespace=%short(&command.owner.scope.namespace), task=%short(&command.owner.task_id), attempt=%short(&command.owner.attempt_id), operation=%short(&command.operation_id), reason=?error_kind(error), "settlement rejected or unavailable");
            }
            result
        })
    }
    fn confirm_quiescence<'a>(&'a self, owner: &'a LeaseOwner) -> ContractFuture<'a, TaskState> {
        Box::pin(self.run(move || async move {
            owner.scope.validate()?;
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_write().await?;
            let task = db::load_task(&mut tx, &owner.scope, &owner.task_id, true).await?;
            let attempt = db::load_attempt(&mut tx, &task, &owner.attempt_id).await?;
            let transition =
                core::confirm_quiescence(&task, &attempt, owner, db::now(&mut tx).await?)?;
            db::apply(&mut tx, &transition).await?;
            tx.commit().await?;
            self.wake_queued_transition(&transition);
            Ok(transition.reply)
        }))
    }
    fn cancel<'a>(&'a self, scope: &'a Scope, id: &'a str) -> ContractFuture<'a, TaskState> {
        Box::pin(self.run(move || async move {
            scope.validate()?;
            validate_text(id, 128)?;
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_write().await?;
            let task = db::load_task(&mut tx, scope, id, true).await?;
            let attempt = match &task.current_attempt_id {
                Some(id) => Some(db::load_attempt(&mut tx, &task, id).await?),
                None => None,
            };
            let transition = core::cancel(&task, attempt.as_ref(), db::now(&mut tx).await?)?;
            db::apply(&mut tx, &transition).await?;
            tx.commit().await?;
            Ok(transition.reply)
        }))
    }
}
fn short(s: &str) -> String {
    s.chars().take(128).collect()
}
fn error_kind(error: &ContractError) -> &'static str {
    match error {
        ContractError::InvalidInput(_) => "invalid_input",
        ContractError::Conflict => "conflict",
        ContractError::OwnershipLost => "ownership_lost",
        ContractError::NotFound => "not_found",
        ContractError::Unavailable(_) => "unavailable",
        _ => "invalid_operation",
    }
}

impl PostgresStore {
    pub(crate) fn wake_queued_transition<R>(&self, transition: &core::Transition<R>) {
        if transition.task.state == TaskState::Queued && !transition.history.is_empty() {
            self.acquisition_wake
                .publish(AcquisitionHint::QueueChanged(AcquisitionQueue {
                    scope: transition.task.scope(),
                    queue: transition.task.input.queue.clone(),
                }));
        }
    }
}
