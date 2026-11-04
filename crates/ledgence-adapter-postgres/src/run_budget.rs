//! One authority per budgeted execution tree. Retained membership is separate
//! from execution detail, and settlement cannot mint another authorization.
use crate::{codec, persistence as db, *};
use sqlx::{PgConnection, Row};

pub(crate) fn task(id: &str) -> RunBudgetSubject {
    RunBudgetSubject::Task {
        task_id: id.to_owned(),
    }
}
pub(crate) fn workflow(id: &str) -> RunBudgetSubject {
    RunBudgetSubject::Workflow {
        workflow_id: id.to_owned(),
    }
}
fn parts(s: &RunBudgetSubject) -> (&'static str, &str) {
    match s {
        RunBudgetSubject::Task { task_id } => ("task", task_id),
        RunBudgetSubject::Workflow { workflow_id } => ("workflow", workflow_id),
    }
}
fn subject(kind: &str, id: String) -> Result<RunBudgetSubject> {
    match kind {
        "task" => Ok(RunBudgetSubject::Task { task_id: id }),
        "workflow" => Ok(RunBudgetSubject::Workflow { workflow_id: id }),
        _ => Err(corrupt()),
    }
}
fn corrupt() -> ContractError {
    ContractError::Unavailable("inconsistent durable run budget".into())
}
fn proof(v: Option<bool>) -> StoreResult<()> {
    if v == Some(true) {
        Ok(())
    } else {
        Err(corrupt().into())
    }
}
pub(crate) async fn create(
    tx: &mut PgConnection,
    scope: &Scope,
    root: &RunBudgetSubject,
    policy: &RunBudgetPolicy,
) -> StoreResult<()> {
    policy.validate()?;
    root.validate()?;
    let (k, id) = parts(root);
    proof(sqlx::query_scalar("INSERT INTO run_budgets(tenant_id,namespace,root_kind,root_id,policy_bytes)
         VALUES($1,$2,$3,$4,$5)
         RETURNING tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND policy_bytes=$5 AND calls=0 AND delegations=0 AND last_sequence=0")

.bind(&scope.tenant_id)
.bind(&scope.namespace)
.bind(k)
.bind(id)
.bind(codec::encode(policy)?)
.fetch_optional(&mut *tx).await?)?;
    enroll(tx, scope, root, root).await
}
pub(crate) async fn root_for(
    tx: &mut PgConnection,
    scope: &Scope,
    member: &RunBudgetSubject,
) -> StoreResult<Option<RunBudgetSubject>> {
    let (k, id) = parts(member);
    let r:Option<(String,String)>=sqlx::query_as("SELECT root_kind,root_id FROM run_budget_members WHERE tenant_id=$1 AND namespace=$2 AND kind=$3 AND member_id=$4")
.bind(&scope.tenant_id)
.bind(&scope.namespace)
.bind(k)
.bind(id)
.fetch_optional(tx).await?;
    Ok(r.map(|(k, id)| subject(&k, id)).transpose()?)
}
pub(crate) async fn enroll(
    tx: &mut PgConnection,
    scope: &Scope,
    root: &RunBudgetSubject,
    member: &RunBudgetSubject,
) -> StoreResult<()> {
    let (k, id) = parts(member);
    let (r, root_id) = parts(root);
    proof(sqlx::query_scalar("INSERT INTO run_budget_members(tenant_id,namespace,kind,member_id,root_kind,root_id)
         VALUES($1,$2,$3,$4,$5,$6)
         RETURNING tenant_id=$1 AND namespace=$2 AND kind=$3 AND member_id=$4 AND root_kind=$5 AND root_id=$6 AND cardinality(ancestor_workflows)=0 AND stopped_at_ms IS NULL")

.bind(&scope.tenant_id)
.bind(&scope.namespace)
.bind(k)
.bind(id)
.bind(r)
.bind(root_id)
.fetch_optional(tx).await?)
}
pub(crate) async fn inherit(
    tx: &mut PgConnection,
    scope: &Scope,
    parent: &RunBudgetSubject,
    member: &RunBudgetSubject,
) -> StoreResult<()> {
    let (pk, parent_id) = parts(parent);
    let (k, id) = parts(member);
    // Membership is immutable. Read one indexed parent, copy its bounded
    // ancestry and prove the complete copy in one statement.
    proof(sqlx::query_scalar("WITH parent AS MATERIALIZED (SELECT root_kind,root_id,ancestor_workflows || member_id AS ancestors
         FROM run_budget_members
         WHERE tenant_id=$1 AND namespace=$2 AND kind=$3 AND member_id=$4), saved AS (INSERT INTO run_budget_members(tenant_id,namespace,kind,member_id,root_kind,root_id,ancestor_workflows) SELECT $1,$2,$5,$6,root_kind,root_id,ancestors
         FROM parent
         RETURNING *) SELECT s.tenant_id=$1 AND s.namespace=$2 AND s.kind=$5 AND s.member_id=$6 AND s.root_kind=p.root_kind AND s.root_id=p.root_id AND s.ancestor_workflows=p.ancestors AND s.stopped_at_ms IS NULL
         FROM saved s
         CROSS JOIN parent p")

.bind(&scope.tenant_id)
.bind(&scope.namespace)
.bind(pk)
.bind(parent_id)
.bind(k)
.bind(id)
.fetch_optional(tx).await?)
}

pub(crate) struct Account {
    pub scope: Scope,
    pub root: RunBudgetSubject,
    pub policy: RunBudgetPolicy,
    calls: i64,
    delegations: i64,
    seq: i64,
    deadline_reached: bool,
    stopped: bool,
}
pub(crate) async fn lock(
    tx: &mut PgConnection,
    scope: &Scope,
    member: &RunBudgetSubject,
) -> StoreResult<Option<Account>> {
    let Some(root) = root_for(tx, scope, member).await? else {
        return Ok(None);
    };
    let (k, id) = parts(&root);
    // Membership FKs take KEY SHARE. NO KEY UPDATE serializes siblings without
    // deadlocking two transactions which both just inserted new members.
    let (bytes, calls, delegations, seq, deadline_reached): (Vec<u8>, i64, i64, i64, bool) =
        sqlx::query_as(
            "SELECT policy_bytes,calls,delegations,last_sequence,deadline_reached
         FROM run_budgets
         WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4
         FOR NO KEY UPDATE",
        )
        .bind(&scope.tenant_id)
        .bind(&scope.namespace)
        .bind(k)
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    // The account lock serializes all grants and stop writes. Use a fresh
    // READ COMMITTED statement after any lock wait to see a preceding stop.
    let (member_kind, member_id) = parts(member);
    let stopped:bool=sqlx::query_scalar("SELECT m.stopped_at_ms IS NOT NULL OR EXISTS(SELECT
         FROM unnest(m.ancestor_workflows) a(id)
         JOIN run_budget_members p ON p.tenant_id=m.tenant_id AND p.namespace=m.namespace AND p.kind='workflow' AND p.member_id=a.id
         WHERE p.stopped_at_ms IS NOT NULL)
         FROM run_budget_members m
         WHERE m.tenant_id=$1 AND m.namespace=$2 AND m.kind=$3 AND m.member_id=$4")

.bind(&scope.tenant_id)
.bind(&scope.namespace)
.bind(member_kind)
.bind(member_id)
.fetch_one(&mut *tx).await?;
    let policy: RunBudgetPolicy = codec::decode(&bytes)?;
    policy.validate()?;
    Ok(Some(Account {
        scope: scope.clone(),
        root,
        policy,
        calls,
        delegations,
        seq,
        deadline_reached,
        stopped,
    }))
}

struct EntrySource<'a> {
    action: RunBudgetAction,
    id: &'a str,
    approval: Option<&'a EffectOccurrence>,
}
impl Account {
    pub(crate) fn denial(
        &self,
        action: RunBudgetAction,
        count: u32,
        now: u64,
    ) -> Option<RunBudgetDenial> {
        if self.stopped {
            return Some(RunBudgetDenial::Cancelled);
        }
        if self.deadline_reached || self.policy.deadline_at_ms.is_some_and(|d| now >= d) {
            return Some(RunBudgetDenial::DeadlineExceeded);
        }
        let (used, limit, denial) = match action {
            RunBudgetAction::Call => (
                self.calls,
                self.policy.max_calls,
                RunBudgetDenial::CallsExhausted,
            ),
            RunBudgetAction::Delegation => (
                self.delegations,
                self.policy.max_delegations,
                RunBudgetDenial::DelegationsExhausted,
            ),
        };
        limit
            .is_some_and(|maximum| used > i64::from(maximum) - i64::from(count))
            .then_some(denial)
    }
    async fn append(
        &mut self,
        tx: &mut PgConnection,
        owner: &RunBudgetSubject,
        source: EntrySource<'_>,
        denial: Option<RunBudgetDenial>,
        now: u64,
    ) -> StoreResult<()> {
        let EntrySource {
            action,
            id: source_id,
            approval,
        } = source;
        let (k, id) = parts(&self.root);
        let label = codec::label(&action)?;
        // Only interactive denied requests can reenter this method with the same
        // source. Reconciliation returns the original entry without a sequence gap.
        if denial.is_some() {
            let existing:Option<Vec<u8>>=sqlx::query_scalar("SELECT entry_bytes FROM run_budget_entries WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND action=$5 AND source_id=$6")
.bind(&self.scope.tenant_id)
.bind(&self.scope.namespace)
.bind(k)
.bind(id)
.bind(&label)
.bind(source_id)
.fetch_optional(&mut *tx).await?;
            if let Some(bytes) = existing {
                let e: RunBudgetEntry = codec::decode(&bytes)?;
                if e.subject != *owner || e.reason != denial {
                    return Err(ContractError::Conflict.into());
                }
                return Ok(());
            }
        }
        self.deadline_reached |= denial == Some(RunBudgetDenial::DeadlineExceeded);
        self.seq = self.seq.checked_add(1).ok_or_else(corrupt)?;
        if denial.is_none() {
            let used = match action {
                RunBudgetAction::Call => &mut self.calls,
                RunBudgetAction::Delegation => &mut self.delegations,
            };
            *used = used.checked_add(1).ok_or_else(corrupt)?;
        }
        let entry = RunBudgetEntry {
            scope: self.scope.clone(),
            root: self.root.clone(),
            subject: owner.clone(),
            action,
            outcome: if denial.is_some() {
                RunBudgetOutcome::Denied
            } else {
                RunBudgetOutcome::Authorized
            },
            reason: denial,
            source_id: source_id.to_owned(),
            approval: approval.cloned(),
            usage: RunBudgetUsage::Unknown {},
            recorded_at: now,
            sequence: self.seq as u64,
        };
        entry.validate()?;
        proof(sqlx::query_scalar("INSERT INTO run_budget_entries(tenant_id,namespace,root_kind,root_id,sequence,action,source_id,entry_bytes)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8)
         RETURNING tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND sequence=$5 AND action=$6 AND source_id=$7 AND entry_bytes=$8")

.bind(&self.scope.tenant_id)
.bind(&self.scope.namespace)
.bind(k)
.bind(id)
.bind(self.seq)
.bind(label)
.bind(source_id)
.bind(codec::encode(&entry)?)
.fetch_optional(&mut *tx).await?)?;
        proof(
            sqlx::query_scalar(
                "UPDATE run_budgets
         SET calls=$5,delegations=$6,last_sequence=$7,deadline_reached=$8
         WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND last_sequence=$7-1
         RETURNING calls=$5 AND delegations=$6 AND last_sequence=$7 AND deadline_reached=$8",
            )
            .bind(&self.scope.tenant_id)
            .bind(&self.scope.namespace)
            .bind(k)
            .bind(id)
            .bind(self.calls)
            .bind(self.delegations)
            .bind(self.seq)
            .bind(self.deadline_reached)
            .fetch_optional(tx)
            .await?,
        )
    }
}
pub(crate) async fn denial_id<T: serde::Serialize>(
    tx: &mut PgConnection,
    value: &T,
) -> StoreResult<String> {
    let bytes = canonical_json_bytes(&serde_json::to_value(value).map_err(|_| corrupt())?)
        .map_err(ContractError::from)?;
    Ok(
        sqlx::query_scalar("SELECT 'den_' || encode(sha256($1::bytea),'hex')")
            .bind(bytes)
            .fetch_one(tx)
            .await?,
    )
}
pub(crate) async fn usage(
    tx: &mut PgConnection,
    scope: &Scope,
    owner: &RunBudgetSubject,
    source: &str,
    usage: Option<&RunBudgetUsage>,
) -> StoreResult<()> {
    let Some(usage) = usage else { return Ok(()) };
    usage.validate()?;
    let Some(root) = root_for(tx, scope, owner).await? else {
        return Ok(());
    };
    let (k, id) = parts(&root);
    let bytes:Vec<u8>=sqlx::query_scalar("SELECT entry_bytes
         FROM run_budget_entries
         WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND action='call' AND source_id=$5
         FOR UPDATE")
.bind(&scope.tenant_id)
.bind(&scope.namespace)
.bind(k)
.bind(id)
.bind(source)
.fetch_one(&mut *tx).await?;
    let mut entry: RunBudgetEntry = codec::decode(&bytes)?;
    if entry.subject != *owner || entry.outcome != RunBudgetOutcome::Authorized {
        return Err(corrupt().into());
    }
    if entry.usage == *usage {
        return Ok(());
    }
    if !matches!(entry.usage, RunBudgetUsage::Unknown {}) {
        return Err(ContractError::Conflict.into());
    }
    entry.usage = usage.clone();
    entry.validate()?;
    proof(sqlx::query_scalar("UPDATE run_budget_entries
         SET entry_bytes=$6
         WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND action='call' AND source_id=$5 AND entry_bytes=$7
         RETURNING entry_bytes=$6")

.bind(&scope.tenant_id)
.bind(&scope.namespace)
.bind(k)
.bind(id)
.bind(source)
.bind(codec::encode(&entry)?)
.bind(bytes)
.fetch_optional(tx).await?)
}
impl RunBudgetOperations for PostgresStore {
    fn run_budget<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a RunBudgetQuery,
    ) -> ContractFuture<'a, RunBudgetSnapshot> {
        Box::pin(self.run(move || self.inspect_budget_once(principal, query)))
    }
    fn list_run_budget_entries<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a RunBudgetEntriesQuery,
    ) -> ContractFuture<'a, RunBudgetPage> {
        Box::pin(self.run(move || self.budget_entries_once(principal, query)))
    }
}
impl PostgresStore {
    async fn inspect_budget_once(
        &self,
        principal: &VerifiedPrincipal,
        query: &RunBudgetQuery,
    ) -> StoreResult<RunBudgetSnapshot> {
        query.validate()?;
        self.require_scope(&query.scope)?;
        self.admission_configured()?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_read().await?;
        let observed_at = db::now(&mut tx).await?;
        self.require_operator(principal, AccessCapability::MetadataRead, observed_at)?;
        let root = root_for(&mut tx, &query.scope, &query.subject)
            .await?
            .ok_or(ContractError::NotFound)?;
        let (kind, id) = parts(&root);
        let row = sqlx::query(
            "SELECT policy_bytes,calls,delegations FROM run_budgets
                             WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4",
        )
        .bind(&query.scope.tenant_id)
        .bind(&query.scope.namespace)
        .bind(kind)
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        let snapshot = RunBudgetSnapshot {
            scope: query.scope.clone(),
            subject: query.subject.clone(),
            root,
            observed_at,
            policy: codec::decode(&row.try_get::<Vec<u8>, _>("policy_bytes")?)?,
            calls: u64::try_from(row.try_get::<i64, _>("calls")?).map_err(|_| corrupt())?,
            delegations: u64::try_from(row.try_get::<i64, _>("delegations")?)
                .map_err(|_| corrupt())?,
        };
        snapshot.validate_for(query)?;
        tx.commit().await?;
        Ok(snapshot)
    }
    async fn budget_entries_once(
        &self,
        principal: &VerifiedPrincipal,
        query: &RunBudgetEntriesQuery,
    ) -> StoreResult<RunBudgetPage> {
        query.validate()?;
        self.require_scope(&query.scope)?;
        self.admission_configured()?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_read().await?;
        self.require_operator(
            principal,
            AccessCapability::MetadataRead,
            db::now(&mut tx).await?,
        )?;
        let root = root_for(&mut tx, &query.scope, &query.subject)
            .await?
            .ok_or(ContractError::NotFound)?;
        let (kind, id) = parts(&root);
        let upper = query
            .before
            .and_then(|value| i64::try_from(value - 1).ok())
            .unwrap_or(i64::MAX);
        let rows: Vec<(i64, Vec<u8>)> = sqlx::query_as(
            "SELECT sequence,entry_bytes FROM run_budget_entries
               WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND sequence<=$5
               ORDER BY sequence DESC LIMIT $6",
        )
        .bind(&query.scope.tenant_id)
        .bind(&query.scope.namespace)
        .bind(kind)
        .bind(id)
        .bind(upper)
        .bind(i64::from(query.limit) + 1)
        .fetch_all(&mut *tx)
        .await?;
        let more = rows.len() > query.limit as usize;
        let items = rows
            .into_iter()
            .take(query.limit as usize)
            .map(|(sequence, bytes)| {
                let entry: RunBudgetEntry = codec::decode(&bytes)?;
                if entry.sequence != sequence as u64 {
                    return Err(corrupt());
                }
                Ok(entry)
            })
            .collect::<Result<Vec<_>>>()?;
        let next = more
            .then(|| items.last().map(|entry| entry.sequence))
            .flatten();
        let page = RunBudgetPage {
            scope: query.scope.clone(),
            subject: query.subject.clone(),
            root,
            items,
            next,
        };
        page.validate_for(query)?;
        tx.commit().await?;
        Ok(page)
    }
}

pub(crate) async fn mark_workflow(tx: &mut PgConnection, id: &str) -> StoreResult<()> {
    proof(sqlx::query_scalar("UPDATE workflow_runs SET has_run_budget=true WHERE workflow_id=$1 RETURNING has_run_budget")
.bind(id)
.fetch_optional(tx).await?)
}
pub(crate) async fn delegate(
    tx: &mut PgConnection,
    scope: &Scope,
    parent: &str,
    children: &[RunBudgetSubject],
) -> StoreResult<std::result::Result<Option<u64>, RunBudgetDenial>> {
    let owner = workflow(parent);
    let mut account = lock(tx, scope, &owner).await?.ok_or_else(corrupt)?;
    let now = db::now(tx).await?;
    let count = u32::try_from(children.len()).map_err(|_| corrupt())?;
    if let Some(denial) = account.denial(RunBudgetAction::Delegation, count, now) {
        return Ok(Err(denial));
    }
    for child in children {
        account
            .append(
                tx,
                &owner,
                EntrySource {
                    action: RunBudgetAction::Delegation,
                    id: parts(child).1,
                    approval: None,
                },
                None,
                now,
            )
            .await?;
    }
    Ok(Ok(account.policy.deadline_at_ms))
}
pub(crate) async fn record_denial(
    tx: &mut PgConnection,
    scope: &Scope,
    member: &RunBudgetSubject,
    action: RunBudgetAction,
    key: &str,
    denial: RunBudgetDenial,
    approval: Option<&EffectOccurrence>,
) -> StoreResult<()> {
    let mut account = lock(tx, scope, member).await?.ok_or_else(corrupt)?;
    let now = db::now(tx).await?;
    account
        .append(
            tx,
            member,
            EntrySource {
                action,
                id: key,
                approval,
            },
            Some(denial),
            now,
        )
        .await
}
/// Must be called only on a fresh physical grant, under existing owner, resource
/// and provider uniqueness locks. The enclosing transaction commits all facts.
pub(crate) async fn call(
    tx: &mut PgConnection,
    scope: &Scope,
    member: &RunBudgetSubject,
    source_id: &str,
    approval: Option<&EffectOccurrence>,
) -> StoreResult<std::result::Result<(u64, Option<u64>), RunBudgetDenial>> {
    let mut account = lock(tx, scope, member).await?.ok_or_else(corrupt)?;
    let now = db::now(tx).await?;
    if let Some(denial) = account.denial(RunBudgetAction::Call, 1, now) {
        return Ok(Err(denial));
    }
    account
        .append(
            tx,
            member,
            EntrySource {
                action: RunBudgetAction::Call,
                id: source_id,
                approval,
            },
            None,
            now,
        )
        .await?;
    Ok(Ok((now, account.policy.deadline_at_ms)))
}

impl PostgresStore {
    /// A restored DB cannot prove an absent budget was absent before recovery.
    /// This read uses installation configuration, not a database lookup.
    pub(crate) fn check_dispatch_quarantine(&self) -> Result<()> {
        if self
            .admission_config()
            .is_some_and(|c| c.dispatch == AdmissionDispatchPolicy::Quarantined)
        {
            Err(ContractError::Forbidden)
        } else {
            Ok(())
        }
    }
}

/// Return the first committed reason even when another limit has since elapsed.
/// A changed command cannot adopt an earlier refusal.
pub(crate) async fn bound_call_refusal<T: serde::Serialize>(
    tx: &mut PgConnection,
    scope: &Scope,
    member: &RunBudgetSubject,
    key: &str,
    reason: RunBudgetDenial,
    command: &T,
    approval: &EffectOccurrence,
) -> StoreResult<RunBudgetDenial> {
    let mut account = lock(tx, scope, member).await?.ok_or_else(corrupt)?;
    let (k, id) = parts(&account.root);
    let bytes = canonical_json_bytes(&serde_json::to_value(command).map_err(|_| corrupt())?)
        .map_err(ContractError::from)?;
    let prior:Option<(Vec<u8>,bool)>=sqlx::query_as("SELECT entry_bytes,denial_request_digest IS NOT DISTINCT
         FROM sha256($6::bytea)
         FROM run_budget_entries
         WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND action='call' AND source_id=$5")

.bind(&scope.tenant_id)
.bind(&scope.namespace)
.bind(k)
.bind(id)
.bind(key)
.bind(&bytes)
.fetch_optional(&mut *tx).await?;
    if let Some((entry, same)) = prior {
        if !same {
            return Err(ContractError::Conflict.into());
        }
        let entry: RunBudgetEntry = codec::decode(&entry)?;
        if &entry.subject != member {
            return Err(corrupt().into());
        }
        return entry.reason.ok_or_else(|| corrupt().into());
    }
    let now = db::now(tx).await?;
    account
        .append(
            tx,
            member,
            EntrySource {
                action: RunBudgetAction::Call,
                id: key,
                approval: Some(approval),
            },
            Some(reason),
            now,
        )
        .await?;
    let (k, id) = parts(&account.root);
    proof(sqlx::query_scalar("UPDATE run_budget_entries
         SET denial_request_digest=sha256($6::bytea)
         WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND action='call' AND source_id=$5 AND denial_request_digest IS NULL
         RETURNING denial_request_digest=sha256($6::bytea)")

.bind(&scope.tenant_id)
.bind(&scope.namespace)
.bind(k)
.bind(id)
.bind(key)
.bind(bytes)
.fetch_optional(tx).await?)?;
    Ok(reason)
}

pub(crate) fn check_grant(grant: Option<RunBudgetGrant>, now: u64) -> Result<()> {
    if grant.is_some_and(|g| now < g.authorized_at || now >= g.expires_at) {
        Err(ContractError::Unavailable(
            "run budget grant expired before commit; reconcile the same command".into(),
        ))
    } else {
        Ok(())
    }
}

/// The same stable approval occurrence identifies generic permission in v1 and v2.
pub(crate) async fn effect_source(
    tx: &mut PgConnection,
    approval: &EffectOccurrence,
) -> StoreResult<String> {
    let bytes = canonical_json_bytes(&serde_json::to_value(approval).map_err(|_| corrupt())?)
        .map_err(ContractError::from)?;
    Ok(
        sqlx::query_scalar("SELECT 'effect_' || encode(sha256($1::bytea),'hex')")
            .bind(bytes)
            .fetch_one(tx)
            .await?,
    )
}

/// Preserve the stop after execution detail is retired. Only the stopped
/// member and its descendants are affected, independently of drain progress.
pub(crate) async fn stop(
    tx: &mut PgConnection,
    scope: &Scope,
    member: &RunBudgetSubject,
    now: u64,
) -> StoreResult<()> {
    lock(tx, scope, member).await?.ok_or_else(corrupt)?;
    let (k, id) = parts(member);
    proof(
        sqlx::query_scalar(
            "UPDATE run_budget_members
         SET stopped_at_ms=coalesce(stopped_at_ms,$5)
         WHERE tenant_id=$1 AND namespace=$2 AND kind=$3 AND member_id=$4
         RETURNING stopped_at_ms IS NOT NULL",
        )
        .bind(&scope.tenant_id)
        .bind(&scope.namespace)
        .bind(k)
        .bind(id)
        .bind(codec::ms(now)?)
        .fetch_optional(tx)
        .await?,
    )
}
