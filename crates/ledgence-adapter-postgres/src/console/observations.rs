use super::*;
use ledgence_worker_api::{INVOCATION_OBSERVATIONS_MAX_BYTES, InvocationObservations};

pub(super) async fn attempt(
    connection: &mut PgConnection,
    scope: &Scope,
    id: &str,
    observed_at: Timestamp,
) -> StoreResult<ConsoleAttemptObservations> {
    let row = sqlx::query("SELECT a.task_id,s.observations_bytes FROM attempts a JOIN tasks t ON t.task_id=a.task_id LEFT JOIN accepted_settlements s ON s.attempt_id=a.attempt_id WHERE a.attempt_id=$1 AND t.tenant_id=$2 AND t.namespace=$3 AND t.retiring_at_ms IS NULL")
        .bind(id).bind(&scope.tenant_id).bind(&scope.namespace)
        .fetch_optional(connection).await?.ok_or(ContractError::NotFound)?;
    let observations = row
        .try_get::<Option<Vec<u8>>, _>("observations_bytes")?
        .map(|bytes| -> StoreResult<ConsoleInvocationObservations> {
            let value: InvocationObservations =
                decode_unique_json(&bytes, INVOCATION_OBSERVATIONS_MAX_BYTES)
                    .map_err(|_| corrupt("invocation observations"))?;
            value
                .validate()
                .map_err(|_| corrupt("invocation observations"))?;
            Ok(value.into())
        })
        .transpose()?;
    Ok(ConsoleAttemptObservations {
        task_id: row.try_get("task_id")?,
        attempt_id: id.to_owned(),
        observations,
        observed_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{TestDb, claimed, completed, scope};

    #[tokio::test]
    #[ignore = "requires PostgreSQL 18"]
    async fn measurements_are_scoped_optional_and_do_not_decode_application_output() {
        let db = TestDb::new().await;
        let (task, _, assigned) = claimed(&db.store).await;
        let query = ConsoleQuery::AttemptObservations {
            attempt_id: assigned.lease.owner.attempt_id.clone(),
        };
        let ConsoleQueryReply::AttemptObservations(before) =
            db.store.query_console(&scope(), &query).await.unwrap()
        else {
            panic!("measurements")
        };
        assert!(before.observations.is_none());
        db.store
            .renew(&RenewCommand {
                owner: assigned.lease.owner.clone(),
                sequence: 1,
                intent: RenewIntent::Dispatch,
            })
            .await
            .unwrap();
        let mut command = completed(
            &assigned,
            Quiescence::Confirmed,
            serde_json::json!({"private":"output"}),
        );
        let observations = InvocationObservations {
            runtime_started_at_ms: 100,
            runtime_elapsed_us: 25,
            process_cpu_user_us: Some(5),
            process_cpu_system_us: None,
            process_lifetime_peak_rss_bytes: Some(1024),
            local_steps: vec![],
            local_steps_truncated: false,
        };
        if let AttemptReport::Completed(report) = &mut command.report {
            report.observations = Some(Box::new(observations));
        }
        db.store.settle(&command).await.unwrap();
        db.store.settle(&command).await.unwrap();
        // Reading resource metadata is independent of both application and report
        // payloads and exposes neither owner/lease nor application output.
        sqlx::query("UPDATE accepted_settlements SET accepted_command=decode('00','hex') WHERE attempt_id=$1")
            .bind(&assigned.lease.owner.attempt_id).execute(&db.store.pool).await.unwrap();
        let ConsoleQueryReply::AttemptObservations(after) =
            db.store.query_console(&scope(), &query).await.unwrap()
        else {
            panic!("measurements")
        };
        assert_eq!(after.task_id, task.task_id);
        assert_eq!(after.observations.unwrap().runtime_elapsed_us.0, 25);
        let foreign = Scope {
            namespace: "other".into(),
            ..scope()
        };
        assert!(matches!(
            db.store.query_console(&foreign, &query).await,
            Err(ContractError::NotFound)
        ));
        sqlx::query("UPDATE tasks SET retiring_at_ms=100 WHERE task_id=$1")
            .bind(&task.task_id)
            .execute(&db.store.pool)
            .await
            .unwrap();
        assert!(matches!(
            db.store.query_console(&scope(), &query).await,
            Err(ContractError::NotFound)
        ));
        db.finish().await;
    }
}
