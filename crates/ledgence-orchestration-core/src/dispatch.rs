use crate::*;

/// A readiness reference can grant a new claim only for the next eligible
/// attempt. Other successful decisions prove durable handoff without authority.
#[derive(Debug)]
pub enum DispatchDecision {
    Ready,
    Handled(ClaimDisposition),
}

pub fn classify_dispatch(
    task: &TaskSnapshot,
    reference: &DispatchRef,
    now: Timestamp,
    recoverable_future: bool,
) -> Result<DispatchDecision> {
    reference.validate()?;
    if reference.scope != task.scope()
        || reference.queue != task.input.queue
        || reference.task_id != task.task_id
    {
        return Err(ContractError::Conflict);
    }
    let next = task
        .attempt_count
        .checked_add(1)
        .ok_or_else(|| invalid("attempt generation overflow"))?;
    if reference.generation > next {
        return Err(invalid("dispatch generation has never been issued"));
    }
    if task.state.is_terminal() || task.cancel_requested_at.is_some() {
        return Ok(DispatchDecision::Handled(
            ClaimDisposition::TerminalOrSuperseded,
        ));
    }
    if task.state == TaskState::Active {
        if reference.generation > task.attempt_count {
            return Err(invalid("dispatch generation has never been issued"));
        }
        if reference.generation == task.attempt_count {
            let attempt_id = task
                .current_attempt_id
                .clone()
                .ok_or_else(|| invalid("active task has no attempt"))?;
            return Ok(DispatchDecision::Handled(
                ClaimDisposition::AlreadyHandedOff {
                    attempt: AttemptRef {
                        task_id: task.task_id.clone(),
                        attempt_id,
                    },
                },
            ));
        }
        return Ok(DispatchDecision::Handled(
            ClaimDisposition::TerminalOrSuperseded,
        ));
    }
    if reference.generation < next {
        return Ok(DispatchDecision::Handled(
            ClaimDisposition::TerminalOrSuperseded,
        ));
    }
    if task.available_at > now {
        if !recoverable_future {
            return Err(ContractError::Unavailable(
                "future dispatch has no durable delivery obligation".into(),
            ));
        }
        return Ok(DispatchDecision::Handled(ClaimDisposition::Deferred {
            available_at: task.available_at,
        }));
    }
    Ok(DispatchDecision::Ready)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_worker_api::{Digest, ProgramDescriptor};

    fn task() -> TaskSnapshot {
        let command = SubmitCommand::decode(br#"{"idempotency_key":"key","input":{"tenant_id":"acme","namespace":"billing","queue":"python","program":{"id":"invoice","version":"1"},"data":{}}}"#).unwrap();
        let descriptor = ProgramDescriptor {
            program: command.input.program.clone(),
            digest: Digest(format!("sha256:{}", "a".repeat(64))),
            size: 1,
        };
        crate::submit(&command, &descriptor, "task_1", "run_1", 1_000)
            .unwrap()
            .task
    }
    fn reference(task: &TaskSnapshot, generation: u32) -> DispatchRef {
        DispatchRef {
            scope: task.scope(),
            queue: task.input.queue.clone(),
            task_id: task.task_id.clone(),
            generation,
        }
    }

    #[test]
    fn only_matching_issued_generation_can_be_ready() {
        let task = task();
        assert!(matches!(
            classify_dispatch(&task, &reference(&task, 1), 1_000, false).unwrap(),
            DispatchDecision::Ready
        ));
        for mutate in [
            |r: &mut DispatchRef| r.task_id = "other".into(),
            |r: &mut DispatchRef| r.queue = "other".into(),
            |r: &mut DispatchRef| r.scope.namespace = "other".into(),
            |r: &mut DispatchRef| r.generation = 2,
            |r: &mut DispatchRef| r.generation = 0,
        ] {
            let mut r = reference(&task, 1);
            mutate(&mut r);
            assert!(classify_dispatch(&task, &r, 1_000, true).is_err());
        }
    }

    #[test]
    fn deferred_handoff_needs_a_matching_durable_obligation() {
        let mut task = task();
        task.available_at = 2_000;
        assert!(matches!(
            classify_dispatch(&task, &reference(&task, 1), 1_999, false),
            Err(ContractError::Unavailable(_))
        ));
        assert!(matches!(
            classify_dispatch(&task, &reference(&task, 1), 1_999, true).unwrap(),
            DispatchDecision::Handled(ClaimDisposition::Deferred {
                available_at: 2_000
            })
        ));
        assert!(matches!(
            classify_dispatch(&task, &reference(&task, 1), 2_000, false).unwrap(),
            DispatchDecision::Ready
        ));
    }

    #[test]
    fn active_generation_proves_handoff_without_new_authority() {
        let mut task = task();
        task.state = TaskState::Active;
        task.attempt_count = 2;
        task.current_attempt_id = Some("attempt_2".into());
        assert!(
            matches!(classify_dispatch(&task, &reference(&task, 2), 1_000, false).unwrap(), DispatchDecision::Handled(ClaimDisposition::AlreadyHandedOff { attempt }) if attempt.task_id == task.task_id && attempt.attempt_id == "attempt_2")
        );
        assert!(matches!(
            classify_dispatch(&task, &reference(&task, 1), 1_000, false).unwrap(),
            DispatchDecision::Handled(ClaimDisposition::TerminalOrSuperseded)
        ));
        assert!(classify_dispatch(&task, &reference(&task, 3), 1_000, true).is_err());
    }

    #[test]
    fn queued_retry_supersedes_old_record_and_preserves_next_generation() {
        let mut task = task();
        task.attempt_count = 1;
        assert!(matches!(
            classify_dispatch(&task, &reference(&task, 1), 1_000, false).unwrap(),
            DispatchDecision::Handled(ClaimDisposition::TerminalOrSuperseded)
        ));
        assert!(matches!(
            classify_dispatch(&task, &reference(&task, 2), 1_000, false).unwrap(),
            DispatchDecision::Ready
        ));
    }

    #[test]
    fn terminal_or_cancel_requested_tasks_never_grant_new_execution() {
        for state in [
            TaskState::Succeeded,
            TaskState::Failed,
            TaskState::Cancelled,
        ] {
            let mut task = task();
            task.state = state;
            task.terminal_at = Some(1_001);
            assert!(matches!(
                classify_dispatch(&task, &reference(&task, 1), 2_000, false).unwrap(),
                DispatchDecision::Handled(ClaimDisposition::TerminalOrSuperseded)
            ));
        }
        let mut task = task();
        task.cancel_requested_at = Some(1_001);
        assert!(matches!(
            classify_dispatch(&task, &reference(&task, 1), 2_000, false).unwrap(),
            DispatchDecision::Handled(ClaimDisposition::TerminalOrSuperseded)
        ));
    }

    #[test]
    fn contradictory_active_state_does_not_fabricate_handoff_evidence() {
        let mut task = task();
        task.state = TaskState::Active;
        task.attempt_count = 1;
        assert!(classify_dispatch(&task, &reference(&task, 1), 1_000, true).is_err());
        task.attempt_count = u32::MAX;
        assert!(classify_dispatch(&task, &reference(&task, 1), 1_000, true).is_err());
    }
}
