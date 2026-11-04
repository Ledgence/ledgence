use super::*;
use ledgence_orchestration_api::console::ConsoleAdmissionSubject;
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Episode {
    pub key: String,
    pub occurrence: AdmissionOccurrence,
    pub resource: String,
    #[serde(default)]
    pub subject: ConsoleAdmissionSubject,
    pub deadline_at: u64,
}
pub(super) async fn enqueue(
    tx: &mut PgConnection,
    workflow: &str,
    parking: &str,
    now: u64,
    due: u64,
) -> StoreResult<()> {
    // A late fact can notify an existing wait but cannot recreate a retired run.
    sqlx::query("INSERT INTO workflow_work(id,workflow_id,task_id,kind,available_at_ms,created_at_ms) SELECT 'admission-wait:'||$2,$1,$2,'admission_wait',$3,$4 FROM workflow_runs WHERE workflow_id=$1 AND state='waiting' AND admission_wait_activation_id=$2 AND retiring_at_ms IS NULL ON CONFLICT(id) DO UPDATE SET processed_at_ms=NULL,available_at_ms=LEAST(workflow_work.available_at_ms,EXCLUDED.available_at_ms),lease_token=NULL,lease_until_ms=NULL")
        .bind(workflow).bind(parking).bind(codec::ms(due)?).bind(codec::ms(now)?).execute(&mut *tx).await?;
    Ok(())
}
pub(in crate::workflow) async fn notify(
    tx: &mut PgConnection,
    a: &Request,
    now: u64,
) -> StoreResult<()> {
    if let Some(parking) = &a.parking {
        enqueue(tx, &a.snapshot.occurrence.workflow_id, parking, now, now).await?;
    }
    Ok(())
}
pub(super) async fn load(
    tx: &mut PgConnection,
    workflow: &str,
    parking: &str,
) -> StoreResult<Episode> {
    let bytes:Vec<u8>=sqlx::query_scalar("SELECT wait_bytes FROM workflow_admission_waits WHERE workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms IS NULL").bind(workflow).bind(parking).fetch_optional(&mut *tx).await?.ok_or_else(||corrupt("admission wait episode"))?;
    Ok(codec::decode(&bytes)?)
}
pub(in crate::workflow) async fn install_wait(
    tx: &mut PgConnection,
    run: &mut RunRecord,
    parking: &str,
    wait: &WorkflowWait,
    now: u64,
) -> StoreResult<Option<TaskSnapshot>> {
    let WorkflowWait::ResourceAdmission {
        key,
        request: requested,
        timeout_ms,
    } = wait
    else {
        return Err(corrupt("admission wait kind").into());
    };
    let (occurrence, name, new_binding) = match requested {
        AdmissionWaitRequest::New {
            resource,
            operation,
        } => (
            AdmissionOccurrence {
                scope: run.snapshot.scope.clone(),
                workflow_id: run.snapshot.workflow_id.clone(),
                origin_activation_id: parking.into(),
                key: key.clone(),
            },
            resource.clone(),
            Some(AdmissionBinding::Operation(OperationAdmissionBinding {
                resource: resource.clone(),
                program: run.controller.clone(),
                operation: operation.clone(),
            })),
        ),
        AdmissionWaitRequest::Provider { approval_key } => {
            let effect = EffectOccurrence {
                scope: run.snapshot.scope.clone(),
                workflow_id: run.snapshot.workflow_id.clone(),
                activation_id: parking.into(),
                approval_key: approval_key.clone(),
            };
            let evidence =
                approved_effects::Evidence::load_provider_occurrence(tx, &run.snapshot, &effect)
                    .await?;
            if evidence.prior.is_some() || evidence.claimed {
                return Err(ContractError::Conflict.into());
            }
            let binding = evidence
                .approval
                .provider_binding
                .as_ref()
                .ok_or(ContractError::Conflict)?;
            let resource = binding
                .admission
                .as_ref()
                .ok_or(ContractError::Conflict)?
                .resource
                .clone();
            if binding.program != run.controller {
                return Err(ContractError::Conflict.into());
            }
            let ticket = ProviderOperationTicket {
                occurrence: effect,
                binding_digest: binding.digest(&evidence.approval.action)?,
            };
            let subject = ProviderAdmissionSubject {
                ticket,
                binding: binding.request(),
                action: evidence.approval.action,
            };
            (
                AdmissionOccurrence {
                    scope: run.snapshot.scope.clone(),
                    workflow_id: run.snapshot.workflow_id.clone(),
                    origin_activation_id: parking.into(),
                    key: key.clone(),
                },
                resource.clone(),
                Some(AdmissionBinding::Provider(Box::new(
                    ProviderAdmissionBinding {
                        resource,
                        program: run.controller.clone(),
                        provider: subject,
                    },
                ))),
            )
        }
        AdmissionWaitRequest::Existing { occurrence } => {
            if occurrence.scope != run.snapshot.scope
                || occurrence.workflow_id != run.snapshot.workflow_id
            {
                return Err(ContractError::Conflict.into());
            }
            (
                occurrence.clone(),
                request_resource(tx, occurrence).await?,
                None,
            )
        }
    };
    let mut r = resource(tx, &occurrence.scope, &name, true).await?;
    let now = db::now(tx).await?.max(now);
    r.refill(now)?;
    let until = now
        .checked_add(*timeout_ms)
        .ok_or_else(|| invalid("admission timeout overflow"))?;
    let mut a = match new_binding {
        Some(binding) => {
            let snapshot = AdmissionSnapshot {
                occurrence: occurrence.clone(),
                binding,
                state: AdmissionState::Waiting,
                revision: 1,
                created_at: now,
                updated_at: now,
                deadline_at: until,
                offer: None,
                source: None,
                finish: None,
                provider: None,
            };
            snapshot.validate()?;
            let valid:Option<bool>=sqlx::query_scalar("INSERT INTO admission_requests(tenant_id,namespace,workflow_id,origin_activation_id,logical_key,resource_name,state,revision,created_at_ms,updated_at_ms,deadline_at_ms,parking_activation_id,snapshot_bytes,origin_queue,fair_group) VALUES($1,$2,$3,$4,$5,$6,'waiting',1,$7,$7,$8,$9,$10,$11,$12) RETURNING snapshot_bytes=$10 AND parking_activation_id=$9 AND state='waiting' AND origin_queue=$11 AND fair_group IS NOT DISTINCT FROM $12")
                .bind(&occurrence.scope.tenant_id).bind(&occurrence.scope.namespace).bind(&occurrence.workflow_id).bind(&occurrence.origin_activation_id).bind(&occurrence.key).bind(&name).bind(codec::ms(now)?).bind(codec::ms(until)?).bind(parking).bind(codec::encode(&snapshot)?).bind(&run.submission.input.queue).bind(fair::group(&r,&run.submission.input.queue)).fetch_optional(&mut *tx).await?;
            proof(valid, "admission registration")?;
            if let Some(subject) = snapshot.binding.provider() {
                let o = &subject.ticket.occurrence;
                let linked: Option<bool> = sqlx::query_scalar("INSERT INTO provider_admission_links(tenant_id,namespace,workflow_id,activation_id,approval_key,origin_activation_id,logical_key) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING RETURNING tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND origin_activation_id=$6 AND logical_key=$7")
                    .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key)
                    .bind(&occurrence.origin_activation_id).bind(&occurrence.key).fetch_optional(&mut *tx).await?;
                if linked != Some(true) {
                    return Err(ContractError::Conflict.into());
                }
            }
            Request {
                snapshot,
                parking: Some(parking.into()),
                short_episode_deadline: None,
                generation: 0,
                check_at: None,
                start_bytes: None,
                finish_bytes: None,
            }
        }
        None => {
            let mut a = request(tx, &occurrence).await?;
            if a.parking.is_some() {
                return Err(ContractError::Conflict.into());
            }
            invalidate_offer(&mut a, &mut r, now)?;
            // Reparking an unconsumed offer explicitly relinquishes that reservation.
            if let Some(offer) = a.snapshot.offer.take() {
                if offer.policy_revision == r.record.revision {
                    r.offered = r
                        .offered
                        .checked_sub(1)
                        .ok_or_else(|| corrupt("admission offer counter"))?;
                }
                a.snapshot.state = AdmissionState::AwaitingRewait;
            }
            if a.snapshot.state == AdmissionState::AwaitingRewait {
                a.snapshot.state = AdmissionState::Waiting;
            }
            a.parking = Some(parking.into());
            a.bump(now)?;
            a
        }
    };
    let deadline_at = if a.snapshot.source.is_some() {
        until
    } else {
        until.min(a.snapshot.deadline_at).max(now)
    };
    // FIFO retains its original workflow-work timer, including throughout
    // rolling upgrades. Enabling weights backfills the actual open episode.
    a.short_episode_deadline = (r.record.policy.fair_sharing.is_some()
        && a.snapshot.state == AdmissionState::Waiting
        && deadline_at < a.snapshot.deadline_at)
        .then_some(deadline_at);
    let episode = Episode {
        key: key.clone(),
        occurrence: occurrence.clone(),
        resource: name.clone(),
        subject: ConsoleAdmissionSubject::from_binding(&a.snapshot.binding),
        deadline_at,
    };
    let valid:Option<bool>=sqlx::query_scalar("INSERT INTO workflow_admission_waits(workflow_id,parking_activation_id,logical_key,wait_bytes,registered_at_ms,deadline_at_ms) VALUES($1,$2,$3,$4,$5,$6) RETURNING wait_bytes=$4 AND registered_at_ms=$5 AND deadline_at_ms=$6 AND closed_at_ms IS NULL")
        .bind(&occurrence.workflow_id).bind(parking).bind(key).bind(codec::encode(&episode)?).bind(codec::ms(now)?).bind(codec::ms(deadline_at)?).fetch_optional(&mut *tx).await?;
    proof(valid, "admission wait registration")?;
    run.snapshot.state = WorkflowState::Waiting;
    run.snapshot.activation_id = None;
    run.wait_activation = Some(parking.into());
    run.wait_keys.clear();
    run.external_wait_key = None;
    run.native_wait_activation = None;
    run.provider_wait_activation = None;
    run.admission_wait_activation = Some(parking.into());
    run.has_admissions = true;
    save_run(tx, run).await?;
    explorer::admission_wait(
        tx,
        parking,
        key,
        &occurrence,
        &a.snapshot.binding,
        deadline_at,
        now,
    )
    .await?;
    r.wake(now);
    save_request(tx, &a).await?;
    save_resource(tx, &r).await?;
    if let Some(selection) = resolved(&a, deadline_at, now) {
        return select(
            tx, run, parking, &episode, &mut a, &mut r, selection, now, None,
        )
        .await
        .map(Some);
    }
    enqueue(tx, &occurrence.workflow_id, parking, now, deadline_at).await?;
    Ok(None)
}
pub(super) fn resolved(a: &Request, deadline: u64, now: u64) -> Option<AdmissionSelection> {
    if let Some(record) = &a.snapshot.provider {
        return Some(AdmissionSelection::Provider {
            record: Box::new(record.clone()),
        });
    }
    match a.snapshot.state {
        AdmissionState::AdministrativelyReleased => Some(AdmissionSelection::Closed {
            reason: AdmissionClosedReason::AdministrativelyReleased,
        }),
        AdmissionState::Finished => {
            a.snapshot
                .finish
                .clone()
                .map(|receipt| AdmissionSelection::Finished {
                    receipt: Box::new(receipt),
                })
        }
        AdmissionState::ClosedBeforeStart => Some(AdmissionSelection::Closed {
            reason: AdmissionClosedReason::Deadline,
        }),
        AdmissionState::PossiblyDispatched | AdmissionState::UnknownOccupied if now >= deadline => {
            a.snapshot
                .source
                .as_ref()
                .map(|s| AdmissionSelection::NeedsReview {
                    source_id: s.source_id.clone(),
                })
        }
        AdmissionState::Waiting | AdmissionState::AwaitingRewait if now >= deadline => {
            Some(AdmissionSelection::Closed {
                reason: AdmissionClosedReason::Deadline,
            })
        }
        _ => None,
    }
}
pub(in crate::workflow) async fn apply_wait(
    tx: &mut PgConnection,
    run: &mut RunRecord,
    parking: &str,
    now: u64,
) -> StoreResult<Option<TaskSnapshot>> {
    if run.snapshot.state != WorkflowState::Waiting
        || run.admission_wait_activation.as_deref() != Some(parking)
    {
        return Ok(None);
    }
    let episode = load(tx, &run.snapshot.workflow_id, parking).await?;
    let name = request_resource(tx, &episode.occurrence).await?;
    let mut r = resource(tx, &episode.occurrence.scope, &name, true).await?;
    let now = db::now(tx).await?.max(now);
    let mut a = request(tx, &episode.occurrence).await?;
    if a.parking.as_deref() != Some(parking) {
        return Err(corrupt("admission parking identity").into());
    }
    if let Some(selection) = resolved(&a, episode.deadline_at, now) {
        select(
            tx, run, parking, &episode, &mut a, &mut r, selection, now, None,
        )
        .await
        .map(Some)
    } else {
        Ok(None)
    }
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn select(
    tx: &mut PgConnection,
    run: &mut RunRecord,
    parking: &str,
    episode: &Episode,
    a: &mut Request,
    r: &mut Resource,
    selection: AdmissionSelection,
    now: u64,
    task_id: Option<String>,
) -> StoreResult<TaskSnapshot> {
    let task_id = match task_id {
        Some(id) => id,
        None => db::id(tx, "task").await?,
    };
    let selected_at = now.max(a.snapshot.updated_at);
    let wake = AdmissionWake {
        key: episode.key.clone(),
        occurrence: a.snapshot.occurrence.clone(),
        waiting_activation_id: parking.into(),
        resumed_activation_id: task_id.clone(),
        resumed_revision: run.snapshot.revision,
        binding: a.snapshot.binding.clone(),
        selected_at,
        selection,
    };
    wake.validate()?;
    let valid:Option<bool>=sqlx::query_scalar("UPDATE workflow_admission_waits SET closed_at_ms=$3,wake_bytes=$4 WHERE workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms IS NULL RETURNING closed_at_ms=$3 AND wake_bytes=$4")
        .bind(&run.snapshot.workflow_id).bind(parking).bind(codec::ms(selected_at)?).bind(codec::encode(&wake)?).fetch_optional(&mut *tx).await?;
    proof(valid, "admission wake selection")?;
    // Selecting the wake also closes its obsolete timer obligation. Leaving a
    // year-long pending timer would unnecessarily pin terminal history.
    sqlx::query("UPDATE workflow_work SET processed_at_ms=$2,lease_token=NULL,lease_until_ms=NULL WHERE id='admission-wait:'||$1 AND processed_at_ms IS NULL")
        .bind(parking).bind(codec::ms(selected_at)?).execute(&mut *tx).await?;
    if matches!(wake.selection, AdmissionSelection::Closed { .. }) && a.snapshot.source.is_none() {
        a.snapshot.state = AdmissionState::ClosedBeforeStart;
        a.snapshot.offer = None;
    }
    a.parking = None;
    a.bump(selected_at)?;
    r.refill(selected_at)?;
    r.wake(selected_at);
    save_request(tx, a).await?;
    save_resource(tx, r).await?;
    explorer::admission_closed(tx, parking, Some(&wake), selected_at).await?;
    let task = schedule_activation_with_id(
        tx,
        run,
        BTreeMap::new(),
        Some(WorkflowWake::ResourceAdmission(Box::new(wake))),
        selected_at,
        task_id,
    )
    .await?;
    record_history(
        tx,
        &run.snapshot.workflow_id,
        Some(&task.task_id),
        selected_at,
        "resource_admission_resumed",
    )
    .await?;
    Ok(task)
}
pub(in crate::workflow) async fn close_wait(
    tx: &mut PgConnection,
    run: &mut RunRecord,
    now: u64,
) -> StoreResult<()> {
    if let Some(parking) = run.admission_wait_activation.take() {
        let episode = load(tx, &run.snapshot.workflow_id, &parking).await?;
        let name = request_resource(tx, &episode.occurrence).await?;
        let mut r = resource(tx, &episode.occurrence.scope, &name, true).await?;
        let mut a = request(tx, &episode.occurrence).await?;
        scheduler::close_request(&mut a, &mut r, now)?;
        a.parking = None;
        save_request(tx, &a).await?;
        save_resource(tx, &r).await?;
        let valid:Option<bool>=sqlx::query_scalar("UPDATE workflow_admission_waits SET closed_at_ms=$3 WHERE workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms IS NULL RETURNING closed_at_ms=$3 AND wake_bytes IS NULL").bind(&run.snapshot.workflow_id).bind(&parking).bind(codec::ms(now.max(a.snapshot.updated_at))?).fetch_optional(&mut *tx).await?;
        proof(valid, "admission wait closure")?;
        sqlx::query("UPDATE workflow_work SET processed_at_ms=$2,lease_token=NULL,lease_until_ms=NULL WHERE id='admission-wait:'||$1 AND processed_at_ms IS NULL").bind(&parking).bind(codec::ms(now)?).execute(&mut *tx).await?;
        explorer::admission_closed(tx, &parking, None, now).await?;
    }
    Ok(())
}
