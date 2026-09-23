use super::*;

/// Records loaded in one transaction. `previous` resolves the cursor's saved
/// assignment, including a completed/expired attempt; `candidate` is a locked
/// eligible queued task. An adapter must not substitute another task on replay.
pub struct Acquisition<'a> {
    pub session: Option<&'a WorkerSession>,
    pub cursor: Option<&'a ConsumerCursor>,
    pub previous: Option<(&'a TaskSnapshot, &'a AttemptSnapshot)>,
    pub candidate: Option<&'a TaskSnapshot>,
    pub ids: Option<&'a AttemptIds>,
}
#[derive(Debug, Clone)]
pub struct AcquireTransition {
    pub cursor: ConsumerCursor,
    pub changes: Option<Transition<()>>,
    pub reply: AcquireReply,
}

/// Close one acquisition sequence. During long-poll waiting an adapter does not
/// call this with `candidate=None` until it decides to return a confirmed Empty.
pub fn acquire(
    context: Acquisition<'_>,
    command: &AcquireCommand,
    now: Timestamp,
) -> Result<AcquireTransition> {
    let session = context.session.ok_or(ContractError::UnknownSession)?;
    check_session(session, now)?;
    if command.scope != session.scope
        || command.queue != session.queue
        || command.worker_session_id != session.id
        || command.consumer_id >= session.concurrency
    {
        return Err(invalid(
            "acquisition does not match the registered consumer",
        ));
    }
    if command.sequence == 0 {
        return Err(ContractError::OutOfOrder);
    }
    if let Some(cursor) = context.cursor {
        let old = &cursor.command;
        if old.scope != command.scope
            || old.queue != command.queue
            || old.worker_session_id != command.worker_session_id
            || old.consumer_id != command.consumer_id
        {
            return Err(ContractError::Conflict);
        }
        if command.sequence < old.sequence {
            return Err(ContractError::ObsoleteOperation);
        }
        if command.sequence > old.sequence.saturating_add(1) {
            return Err(ContractError::OutOfOrder);
        }
        let previous = match &cursor.assignment {
            None => None,
            Some(reference) => {
                let (task, attempt) = context
                    .previous
                    .ok_or_else(|| invalid("previous assignment snapshot is required"))?;
                validate_attempt(task, attempt)?;
                if reference.task_id != task.task_id
                    || reference.attempt_id != attempt.lease.owner.attempt_id
                    || attempt.lease.owner.worker_session_id != session.id
                    || attempt.lease.owner.consumer_id != command.consumer_id
                {
                    return Err(invalid("previous assignment does not match cursor"));
                }
                Some((task, attempt))
            }
        };
        if command.sequence == old.sequence {
            let reply = match previous {
                None => AcquireReply::Empty {
                    sequence: command.sequence,
                },
                Some((task, attempt)) if live(task, attempt, now) => {
                    assigned(task, attempt, command.sequence, now)
                }
                Some(_) => AcquireReply::OwnershipLost {
                    sequence: command.sequence,
                    assignment: cursor
                        .assignment
                        .clone()
                        .expect("previous assignment checked"),
                },
            };
            return Ok(AcquireTransition {
                cursor: cursor.clone(),
                changes: None,
                reply,
            });
        }
        if previous.is_some_and(|(task, attempt)| live(task, attempt, now)) {
            return Err(ContractError::Busy);
        }
    } else if command.sequence != 1 {
        return Err(ContractError::OutOfOrder);
    }

    let Some(task) = context.candidate else {
        return Ok(AcquireTransition {
            cursor: ConsumerCursor {
                command: command.clone(),
                assignment: None,
            },
            changes: None,
            reply: AcquireReply::Empty {
                sequence: command.sequence,
            },
        });
    };
    let ids = context
        .ids
        .ok_or_else(|| invalid("new assignment IDs are required"))?;
    if task.state != TaskState::Queued
        || task.cancel_requested_at.is_some()
        || task.available_at > now
    {
        return Err(ContractError::Busy);
    }
    task.input.validate()?;
    task.descriptor.validate()?;
    if task.scope() != command.scope
        || task.input.queue != command.queue
        || task.descriptor.program != task.input.program
        || task.current_attempt_id.is_some()
    {
        return Err(invalid("candidate task does not match acquisition"));
    }
    let number = task
        .attempt_count
        .checked_add(1)
        .ok_or_else(|| invalid("attempt number overflow"))?;
    if number > task.input.retry_policy.max_attempts {
        return Err(invalid("retry budget exhausted"));
    }
    for id in [&ids.attempt_id, &ids.lease_id, &ids.event_id] {
        validate_text(id, 128)?;
    }
    validate_trace(ids.trace.as_ref())?;
    let deadline = add_time(now, task.input.attempt_timeout_ms)?;
    let authority_deadline = add_time(deadline, CLEANUP_GRACE_MS)?;
    let expires_at = add_time(now, LEASE_DURATION_MS)?
        .min(authority_deadline)
        .min(session.expires_at);
    validate_workflow_lineage(
        task.workflow_id.as_deref(),
        task.parent_workflow_id.as_deref(),
        task.root_workflow_id.as_deref(),
    )?;
    let mut event = json!({"specversion":"1.0","id":ids.event_id,"source":"urn:ledgence:orchestrator",
        "type":"com.ledgence.task.invocation.requested.v1", "subject":format!("tasks/{}",task.task_id),
        "time":timestamp(now)?, "datacontenttype":"application/json",
        "ldgtenantid":task.input.tenant_id,"ldgnamespace":task.input.namespace,"ldgrunid":task.run_id,
        "ldgtaskid":task.task_id,"ldgattemptid":ids.attempt_id,"ldgattemptno":number,"data":task.input.data});
    if let Some(id) = &task.workflow_id {
        event["ldgworkflowid"] = Value::String(id.clone());
    }
    if let Some(id) = &task.parent_workflow_id {
        event["ldgparentworkflowid"] = Value::String(id.clone());
    }
    if let Some(id) = &task.root_workflow_id {
        event["ldgrootworkflowid"] = Value::String(id.clone());
    }
    if let Some(id) = &task.workflow_activation_id {
        event["ldgactivationid"] = Value::String(id.clone());
    }
    if let Some(trace) = &ids.trace {
        insert_trace(&mut event, trace);
    }
    let attempt = AttemptSnapshot {
        event: CloudEvent::new(event)?,
        descriptor: task.descriptor.clone(),
        lease: Lease {
            owner: LeaseOwner {
                scope: task.scope(),
                task_id: task.task_id.clone(),
                attempt_id: ids.attempt_id.clone(),
                lease_id: ids.lease_id.clone(),
                generation: number,
                worker_session_id: session.id.clone(),
                consumer_id: command.consumer_id,
            },
            expires_at,
        },
        deadline,
        authority_deadline,
        state: AttemptState::Active,
        execution_may_have_started: false,
        last_renewal: None,
        quiescence: Quiescence::Unconfirmed,
        settlement: None,
        finished_at: None,
    };
    let mut next = task.clone();
    next.state = TaskState::Active;
    next.attempt_count = number;
    next.current_attempt_id = Some(ids.attempt_id.clone());
    let reply = assigned(&next, &attempt, command.sequence, now);
    Ok(AcquireTransition {
        cursor: ConsumerCursor {
            command: command.clone(),
            assignment: Some(AttemptRef {
                task_id: task.task_id.clone(),
                attempt_id: ids.attempt_id.clone(),
            }),
        },
        changes: Some(Transition {
            history: vec![history(
                &next,
                Some(&attempt),
                now,
                TransitionReason::Claimed,
            )],
            task: next,
            attempt: Some(attempt),
            reply: (),
        }),
        reply,
    })
}

fn assigned(
    task: &TaskSnapshot,
    attempt: &AttemptSnapshot,
    sequence: u64,
    now: Timestamp,
) -> AcquireReply {
    AcquireReply::Assigned {
        sequence,
        assignment: Box::new(Assignment {
            workflow_activation_id: task.workflow_activation_id.clone(),
            descriptor: attempt.descriptor.clone(),
            event: attempt.event.clone(),
            lease: attempt.lease.clone(),
            authority: authority(task, attempt, now),
            attempt_deadline: attempt.deadline,
        }),
    }
}
