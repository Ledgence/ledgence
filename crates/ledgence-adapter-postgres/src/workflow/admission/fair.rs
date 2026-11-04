//! Weighted sharing measures committed offer opportunities. The group ordering
//! and cursor live on the same resource as the occupancy and rate authority.
use super::*;

#[derive(Debug)]
pub(crate) enum Owner {
    Task(TaskAdmissionOccurrence),
    Workflow(AdmissionOccurrence),
}

pub(crate) struct Head {
    pub owner: Owner,
    pub(super) sequence: i64,
    queue: Option<String>,
    index: usize,
    round: u32,
    eligible: u32,
}

pub(crate) fn group<'a>(r: &'a Resource, queue: &str) -> Option<&'a str> {
    r.record
        .policy
        .fair_sharing
        .as_ref()?
        .group_for(queue)
        .map(|g| g.name.as_str())
}

// Shares are relative: scaling every weight must not stretch the cycle. Use
// all configured groups; temporarily idle groups never change its geometry.
fn gcd(mut a: u32, mut b: u32) -> u32 {
    while b > 0 {
        (a, b) = (b, a % b);
    }
    a
}
pub(super) fn divisor(p: &ResourceFairSharing) -> u32 {
    p.groups
        .iter()
        .map(|g| g.weight)
        .reduce(gcd)
        .expect("validated nonempty fair policy")
}
pub(super) fn same_rotation(
    old: Option<&ResourceFairSharing>,
    new: Option<&ResourceFairSharing>,
) -> bool {
    let (Some(a), Some(b)) = (old, new) else {
        return old.is_none() && new.is_none();
    };
    if a.default_group != b.default_group || a.groups.len() != b.groups.len() {
        return false;
    }
    let (ad, bd) = (divisor(a), divisor(b));
    a.groups.iter().zip(&b.groups).all(|(left, right)| {
        left.name == right.name
            && left.weight / ad == right.weight / bd
            && (left.name == a.default_group
                || (left.queues.len() == right.queues.len()
                    && left.queues.iter().all(|q| right.queues.contains(q))))
    })
}

// Interleave visits to groups, taking at most one from each group per round.
// Returning (wrapped, round, index) lets us compare next visits in O(groups)
// without materializing or walking up to sum(weights) service opportunities.
fn next_visit(index: usize, weight: u32, last: (u32, i32)) -> (bool, u32, usize) {
    let (round, last_index) = last;
    if round == 0 {
        (false, 1, index)
    } else if weight >= round && index as i32 > last_index {
        (false, round, index)
    } else if weight > round {
        (false, round + 1, index)
    } else {
        (true, 1, index)
    }
}

pub(crate) async fn head(
    tx: &mut PgConnection,
    r: &Resource,
    at: Option<u64>,
) -> StoreResult<Option<Head>> {
    let Some(policy) = &r.record.policy.fair_sharing else {
        return Ok(None);
    };
    let names: Vec<&str> = policy.groups.iter().map(|g| g.name.as_str()).collect();
    let rows = sqlx::query(include_str!("../../../queries/admission_fair_heads.sql"))
        .bind(&r.record.scope.tenant_id)
        .bind(&r.record.scope.namespace)
        .bind(&r.record.name)
        .bind(&names)
        .bind(at.map(codec::ms).transpose()?)
        .fetch_all(&mut *tx)
        .await?;
    let eligible = rows.len() as u32;
    let unit = divisor(policy);
    let mut choice = None;
    for row in rows {
        // WITH ORDINALITY returns the one-based position in our own policy array.
        let index = (row.try_get::<i64, _>("position")? - 1) as usize;
        let visit = next_visit(
            index,
            policy.groups[index].weight / unit,
            (r.fair_round, r.fair_index),
        );
        if choice.as_ref().is_none_or(|(best, _)| visit < *best) {
            choice = Some((visit, row));
        }
    }
    let Some(((_, round, index), row)) = choice else {
        return Ok(None);
    };
    let scope = r.record.scope.clone();
    let owner = if let Some(task_id) = row.try_get("task_id")? {
        Owner::Task(TaskAdmissionOccurrence {
            scope,
            task_id,
            generation: u32::try_from(row.try_get::<i64, _>("generation")?)
                .map_err(|_| corrupt("task generation"))?,
        })
    } else {
        Owner::Workflow(AdmissionOccurrence {
            scope,
            workflow_id: row.try_get("workflow_id")?,
            origin_activation_id: row.try_get("origin_activation_id")?,
            key: row.try_get("logical_key")?,
        })
    };
    Ok(Some(Head {
        owner,
        sequence: row.try_get("request_seq")?,
        queue: row.try_get("origin_queue")?,
        index,
        round,
        eligible,
    }))
}

/// Recheck after owner -> resource -> occurrence locks. A lost race returns
/// None and changes no cursor, rate credit, occupancy or decision history.
pub(crate) async fn winner(
    tx: &mut PgConnection,
    r: &Resource,
    seq: i64,
    now: u64,
) -> StoreResult<Option<Winner>> {
    if r.record.policy.fair_sharing.is_some() {
        // The locked probe also checks for a due workflow deadline or task
        // promotion at this exact timestamp. The next tick settles it first.
        Ok(head(tx, r, Some(now))
            .await?
            .filter(|h| h.sequence == seq)
            .map(Winner::Weighted))
    } else {
        Ok((crate::task_admission::first_ready(tx, r).await? == Some(seq)).then_some(Winner::Fifo))
    }
}
pub(crate) enum Winner {
    Fifo,
    Weighted(Head),
}

impl Winner {
    pub(crate) async fn record(
        self,
        tx: &mut PgConnection,
        r: &mut Resource,
        provider: bool,
        generation: u64,
        now: u64,
    ) -> StoreResult<()> {
        let Self::Weighted(head) = self else {
            return Ok(());
        };
        let group = &r
            .record
            .policy
            .fair_sharing
            .as_ref()
            .ok_or_else(|| corrupt("fair policy"))?
            .groups[head.index];
        let queue = head
            .queue
            .ok_or_else(|| corrupt("weighted offer without an origin queue"))?;
        if self::group(r, &queue) != Some(group.name.as_str()) {
            return Err(corrupt("fair group disagrees with origin queue").into());
        }
        let subject = match head.owner {
            Owner::Task(o) => ResourceDecisionSubject::Task {
                task_id: o.task_id,
                generation: o.generation,
            },
            Owner::Workflow(o) if provider => ResourceDecisionSubject::Provider {
                workflow_id: o.workflow_id,
                origin_activation_id: o.origin_activation_id,
                key: o.key,
            },
            Owner::Workflow(o) => ResourceDecisionSubject::Workflow {
                workflow_id: o.workflow_id,
                origin_activation_id: o.origin_activation_id,
                key: o.key,
            },
        };
        r.fair_round = head.round;
        r.fair_index = head.index as i32;
        r.fair_sequence = r
            .fair_sequence
            .checked_add(1)
            .ok_or_else(|| corrupt("fair sequence overflow"))?;
        let d = ResourceOfferDecision {
            scope: r.record.scope.clone(),
            resource: r.record.name.clone(),
            sequence: r.fair_sequence,
            policy_revision: r.record.revision,
            group: group.name.clone(),
            weight: group.weight,
            eligible_groups: head.eligible,
            reason: if head.eligible == 1 {
                ResourceDecisionReason::SoleEligible
            } else {
                ResourceDecisionReason::Weighted
            },
            queue,
            subject,
            offer_generation: generation,
            offered_at: now,
            cycle_round: head.round,
        };
        d.validate()?;
        let valid: Option<bool> = sqlx::query_scalar("INSERT INTO admission_offer_decisions(tenant_id,namespace,resource_name,sequence,decision_bytes) VALUES($1,$2,$3,$4,$5) RETURNING tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND sequence=$4 AND decision_bytes=$5")
            .bind(&d.scope.tenant_id).bind(&d.scope.namespace).bind(&d.resource).bind(d.sequence as i64).bind(codec::encode(&d)?).fetch_optional(tx).await?;
        proof(valid, "weighted decision suppressed or changed")
    }
}

/// Policy edits are the only backlog-wide operation. Admissions hold the same
/// resource lock when registering/reparking. Terminal evidence is immutable.
pub(super) async fn reclassify(tx: &mut PgConnection, r: &Resource) -> StoreResult<()> {
    let Some(policy) = &r.record.policy.fair_sharing else {
        return Ok(());
    };
    let mut queues = Vec::new();
    let mut groups = Vec::new();
    for g in &policy.groups {
        for q in &g.queues {
            queues.push(q.as_str());
            groups.push(g.name.as_str());
        }
    }
    for statement in [
        include_str!("../../../queries/admission_reclassify_workflows.sql"),
        include_str!("../../../queries/admission_reclassify_tasks.sql"),
    ] {
        let valid: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(statement))
            .bind(&r.record.scope.tenant_id)
            .bind(&r.record.scope.namespace)
            .bind(&r.record.name)
            .bind(&queues)
            .bind(&groups)
            .bind(&policy.default_group)
            .fetch_one(&mut *tx)
            .await?;
        proof(Some(valid), "fair reclassification suppressed or changed")?;
    }
    Ok(())
}

impl PostgresStore {
    pub(super) async fn resource_decisions_once(
        &self,
        p: &VerifiedPrincipal,
        q: &ResourceDecisionQuery,
    ) -> StoreResult<ResourceDecisionPage> {
        q.validate()?;
        self.admission_configured()?;
        self.require_scope(&q.scope)?;
        let mut c = self.transaction_connection().await?;
        let mut tx = c.begin_read().await?;
        self.require_operator(p, AccessCapability::MetadataRead, db::now(&mut tx).await?)?;
        resource(&mut tx, &q.scope, &q.name, false).await?;
        let rows = sqlx::query("SELECT sequence,decision_bytes FROM admission_offer_decisions WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND sequence <= $4 ORDER BY sequence DESC LIMIT $5")
            .bind(&q.scope.tenant_id).bind(&q.scope.namespace).bind(&q.name).bind(q.before.and_then(|s| i64::try_from(s-1).ok()).unwrap_or(i64::MAX)).bind(i64::from(q.limit)+1).fetch_all(&mut *tx).await?;
        let mut page = ResourceDecisionPage {
            items: Vec::new(),
            next: None,
        };
        let mut bytes = 128;
        for row in rows.iter().take(q.limit as usize) {
            let wire: Vec<u8> = row.try_get("decision_bytes")?;
            bytes += wire.len() + 1;
            if bytes > ADMISSION_PAGE_MAX_BYTES {
                break;
            }
            let d: ResourceOfferDecision = codec::decode(&wire)?;
            if d.sequence as i64 != row.try_get::<i64, _>("sequence")? {
                return Err(corrupt("decision projection").into());
            }
            page.items.push(d);
        }
        if rows.len() > page.items.len() {
            page.next = page.items.last().map(|d| d.sequence);
        }
        page.validate_for(q)?;
        self.require_operator(p, AccessCapability::MetadataRead, db::now(&mut tx).await?)?;
        tx.commit().await?;
        Ok(page)
    }
}
