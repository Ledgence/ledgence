use crate::{persistence as db, *};
use sqlx::Row;

const PUBLICATION_LEASE_MS: u64 = 30_000;
const PUBLICATION_RETRY_MS: u64 = 1_000;
const PUBLICATION_REPAIR_MS: u64 = 60_000;

impl DispatchIntentStore for PostgresStore {
    fn configure_route<'a>(&'a self, route: &'a DispatchRoute) -> ContractFuture<'a, ()> {
        Box::pin(self.run(move || self.configure_route_once(route)))
    }

    fn lease_publications<'a>(
        &'a self,
        destination: &'a str,
        limit: u32,
        deadline: Instant,
    ) -> ContractFuture<'a, Vec<PublicationLease>> {
        Box::pin(self.run_until(deadline, move || {
            self.lease_publications_once(destination, limit)
        }))
    }

    fn complete_publications<'a>(
        &'a self,
        completions: &'a [PublicationCompletion],
        deadline: Instant,
    ) -> ContractFuture<'a, ()> {
        Box::pin(self.run_until(deadline, move || {
            self.complete_publications_once(completions)
        }))
    }
}

impl PostgresStore {
    async fn configure_route_once(&self, route: &DispatchRoute) -> StoreResult<()> {
        route.validate()?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        sqlx::query("INSERT INTO dispatch_routes(tenant_id,namespace,queue) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
            .bind(&route.scope.tenant_id).bind(&route.scope.namespace).bind(&route.queue)
            .execute(&mut *tx).await?;
        let current: Option<String> = sqlx::query_scalar("SELECT destination FROM dispatch_routes WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 FOR UPDATE")
            .bind(&route.scope.tenant_id).bind(&route.scope.namespace).bind(&route.queue)
            .fetch_one(&mut *tx).await?;
        match current {
            Some(destination) if destination == route.destination => {}
            Some(_) => return Err(ContractError::Conflict.into()),
            None => {
                // Submission holds this route for sharing until its task commits.
                // Existing active/queued tasks must be drained before activation.
                let populated: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 AND state IN ('queued','active'))")
                    .bind(&route.scope.tenant_id).bind(&route.scope.namespace).bind(&route.queue)
                    .fetch_one(&mut *tx).await?;
                if populated {
                    return Err(ContractError::InvalidInput(
                        "external routing requires an empty logical queue".into(),
                    )
                    .into());
                }
                sqlx::query("UPDATE dispatch_routes SET destination=$4 WHERE tenant_id=$1 AND namespace=$2 AND queue=$3")
                    .bind(&route.scope.tenant_id).bind(&route.scope.namespace).bind(&route.queue).bind(&route.destination)
                    .execute(&mut *tx).await.map_err(|error| {
                        if error.as_database_error().is_some_and(|error| error.code().as_deref() == Some("23505")) {
                            StoreError::Contract(ContractError::Conflict)
                        } else { StoreError::Database(error) }
                    })?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn lease_publications_once(
        &self,
        destination: &str,
        limit: u32,
    ) -> StoreResult<Vec<PublicationLease>> {
        validate_text(destination, 128)?;
        if !(1..=MAX_PUBLICATION_BATCH).contains(&limit) {
            return Err(
                ContractError::InvalidInput("invalid publication batch limit".into()).into(),
            );
        }
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let now = db::now(&mut tx).await?;
        let now_ms = codec::ms(now)?;
        let until =
            codec::ms(now.checked_add(PUBLICATION_LEASE_MS).ok_or_else(|| {
                ContractError::Unavailable("publication deadline overflow".into())
            })?)?;
        // Lock only intents. Task transitions lock task then intent; publication
        // must never acquire task locks after leasing an intent.
        let rows = sqlx::query("WITH due AS (SELECT task_id FROM dispatch_intents WHERE destination=$1 AND available_at_ms <= $2 AND next_publish_at_ms <= $2 AND (lease_until_ms IS NULL OR lease_until_ms <= $2) ORDER BY next_publish_at_ms,task_id LIMIT $3 FOR UPDATE SKIP LOCKED), leased AS (UPDATE dispatch_intents i SET publication_epoch=CASE WHEN last_confirmed_at_ms IS NULL THEN publication_epoch ELSE publication_epoch+1 END, publication_id=CASE WHEN last_confirmed_at_ms IS NULL THEN publication_id ELSE 'pub_'||gen_random_uuid()::text END,last_confirmed_at_ms=NULL,lease_token='publease_'||gen_random_uuid()::text,lease_until_ms=$4,next_publish_at_ms=$4 FROM due WHERE i.task_id=due.task_id RETURNING i.*) SELECT leased.*,t.tenant_id,t.namespace,t.queue FROM leased JOIN tasks t USING(task_id)")
            .bind(destination).bind(now_ms).bind(i64::from(limit)).bind(until)
            .fetch_all(&mut *tx).await?;
        let mut leases = Vec::with_capacity(rows.len());
        for row in rows {
            let generation: i64 = row.try_get("generation")?;
            let lease = PublicationLease {
                record: PublishedDispatch {
                    dispatch: DispatchRef {
                        scope: Scope {
                            tenant_id: row.try_get("tenant_id")?,
                            namespace: row.try_get("namespace")?,
                        },
                        queue: row.try_get("queue")?,
                        task_id: row.try_get("task_id")?,
                        generation: u32::try_from(generation).map_err(|_| {
                            ContractError::Unavailable("invalid dispatch generation".into())
                        })?,
                    },
                    publication_id: row.try_get("publication_id")?,
                },
                destination: row.try_get("destination")?,
                lease_token: row.try_get("lease_token")?,
            };
            lease.validate()?;
            leases.push(lease);
        }
        tx.commit().await?;
        Ok(leases)
    }

    async fn complete_publications_once(
        &self,
        completions: &[PublicationCompletion],
    ) -> StoreResult<()> {
        if completions.len() > MAX_PUBLICATION_BATCH as usize {
            return Err(ContractError::InvalidInput(
                "publication completion batch is too large".into(),
            )
            .into());
        }
        let mut identities = std::collections::HashSet::new();
        for item in completions {
            item.validate()?;
            if !identities.insert((
                &item.dispatch.task_id,
                &item.publication_id,
                &item.lease_token,
            )) {
                return Err(
                    ContractError::InvalidInput("duplicate publication completion".into()).into(),
                );
            }
        }
        if completions.is_empty() {
            return Ok(());
        }
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let now = db::now(&mut tx).await?;
        let mut ordered: Vec<_> = completions.iter().collect();
        ordered.sort_by(|a, b| a.dispatch.task_id.cmp(&b.dispatch.task_id));
        for item in ordered {
            let delay = match item.outcome {
                PublicationOutcome::Confirmed => PUBLICATION_REPAIR_MS,
                PublicationOutcome::Retry => PUBLICATION_RETRY_MS,
            };
            let next = codec::ms(now.checked_add(delay).ok_or_else(|| {
                ContractError::Unavailable("publication deadline overflow".into())
            })?)?;
            let confirmed = if item.outcome == PublicationOutcome::Confirmed {
                Some(codec::ms(now)?)
            } else {
                None
            };
            // A stale completion is harmless: a new generation, repair or lease
            // can never be overwritten by an old publisher response.
            sqlx::query("UPDATE dispatch_intents i SET lease_token=NULL,lease_until_ms=NULL,next_publish_at_ms=$5,last_confirmed_at_ms=$6 FROM tasks t WHERE i.task_id=$1 AND i.generation=$2 AND i.publication_id=$3 AND i.lease_token=$4 AND t.task_id=i.task_id AND t.tenant_id=$7 AND t.namespace=$8 AND t.queue=$9")
                .bind(&item.dispatch.task_id).bind(i64::from(item.dispatch.generation)).bind(&item.publication_id).bind(&item.lease_token)
                .bind(next).bind(confirmed).bind(&item.dispatch.scope.tenant_id).bind(&item.dispatch.scope.namespace).bind(&item.dispatch.queue)
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

/// Bind routing once while retaining a shared route lock through acceptance.
pub(crate) async fn submission_destination(
    connection: &mut sqlx::PgConnection,
    command: &SubmitCommand,
) -> StoreResult<Option<String>> {
    let input = &command.input;
    if let Some(destination) = sqlx::query_scalar::<_, Option<String>>("SELECT destination FROM dispatch_routes WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 FOR SHARE")
        .bind(&input.tenant_id).bind(&input.namespace).bind(&input.queue).fetch_optional(&mut *connection).await? {
        return Ok(destination);
    }
    sqlx::query("INSERT INTO dispatch_routes(tenant_id,namespace,queue) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
        .bind(&input.tenant_id).bind(&input.namespace).bind(&input.queue).execute(&mut *connection).await?;
    Ok(sqlx::query_scalar("SELECT destination FROM dispatch_routes WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 FOR SHARE")
        .bind(&input.tenant_id).bind(&input.namespace).bind(&input.queue).fetch_one(connection).await?)
}

/// Called under the task transaction. Same-generation repeats preserve pending
/// publication identity; leaving queued invalidates the old obligation.
pub(crate) async fn sync_intent(
    connection: &mut sqlx::PgConnection,
    task_id: &str,
) -> StoreResult<()> {
    sqlx::query("WITH eligible AS (SELECT task_id,attempt_count+1 AS generation,dispatch_destination AS destination,available_at_ms FROM tasks WHERE task_id=$1 AND state='queued' AND cancel_requested_at_ms IS NULL AND dispatch_destination IS NOT NULL), removed AS (DELETE FROM dispatch_intents WHERE task_id=$1 AND NOT EXISTS(SELECT 1 FROM eligible)) INSERT INTO dispatch_intents(task_id,generation,destination,available_at_ms,next_publish_at_ms,publication_id) SELECT task_id,generation,destination,available_at_ms,available_at_ms,'pub_'||gen_random_uuid()::text FROM eligible ON CONFLICT(task_id) DO UPDATE SET generation=EXCLUDED.generation,destination=EXCLUDED.destination,available_at_ms=EXCLUDED.available_at_ms,next_publish_at_ms=EXCLUDED.next_publish_at_ms,publication_epoch=1,publication_id=EXCLUDED.publication_id,lease_token=NULL,lease_until_ms=NULL,last_confirmed_at_ms=NULL WHERE dispatch_intents.generation <> EXCLUDED.generation")
        .bind(task_id).execute(connection).await?;
    Ok(())
}
