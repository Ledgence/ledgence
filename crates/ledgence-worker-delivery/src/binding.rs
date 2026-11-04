//! Reconcile a single route-bound acquisition through completion or drain.

use super::*;

/// Count the fresh logical handoff, not retries or repeated plan revisions.
pub(super) fn trace_poll(
    command: &AcquireCommand,
    revision: u64,
    event: &str,
    outcome: Option<&str>,
) {
    tracing::debug!(target: "ledgence_worker_delivery::polls",
        event, outcome, queue = %command.queue, plan_revision = revision,
        worker_session_id = %command.worker_session_id, consumer_id = command.consumer_id,
        sequence = command.sequence, "planned worker poll");
}

/// The first poll of this handoff chooses the route and enters its source
/// without another cancellation point. If stop wins before that poll, neither
/// the planner nor the source observes an issue. Subsequent cancellation or
/// transport failure reconciles this binding without drawing again.
pub(super) async fn acquire(
    routes: &[RegisteredRoute],
    commands: &[AcquireCommand],
    planner: Option<&poll_plan::PollPlanner>,
    config: &DeliveryConfig,
    shared: &Shared,
) -> Option<(usize, SourceReply)> {
    let mut bound = None;
    let deadline = Instant::now() + config.request_timeout;
    let first = tokio::select! {
        biased;
        _ = shared.stopped() => None,
        outcome = exchange_until(deadline, || {
            let selection = match planner {
                Some(planner) => planner.issue_with_revision().map(|s| (s.route, Some(s.revision))),
                None => Some((0,None)),
            };
            let request = selection.map(|(route,revision)| {
                let command = &commands[route];
                let options = AcquireOptions::new(config.acquire_wait, deadline)
                    .expect("validated acquisition timing");
                let span = tracing::debug_span!("acquire_bound", queue = %command.queue,
                    worker_session_id = %command.worker_session_id, consumer_id = command.consumer_id,
                    sequence = command.sequence, plan_revision = revision);
                bound = Some((route, revision, span.clone()));
                let future = span.in_scope(|| {
                    if let Some(revision) = revision {
                        trace_poll(command, revision, "worker.poll.issued", None);
                    }
                    routes[route].source.acquire(command, options)
                });
                future.instrument(span)
            });
            // The deadline guard checks again after synchronous construction,
            // before polling a future that might start remote work.
            async move {
                match request {
                    Some(request) => request.await.map(Some),
                    None => Ok(None), // paused during consumer reservation
                }
            }
        }) => outcome.transpose(),
    };
    let Some((route, revision, span)) = bound else {
        // Deadline expiry before the source handoff consumed no plan turn.
        if let Some(Err(error)) = first {
            shared.error(error);
            pause(config.retry_delay, shared).await;
        }
        return None;
    };
    let command = &commands[route];
    let reply = reconcile(
        routes[route].source.as_ref(),
        config,
        shared,
        command,
        first,
    )
    .instrument(span)
    .await?;
    if let Some(revision) = revision {
        let outcome = match &reply {
            SourceReply::Completed(AcquireReply::Empty { .. }) => "empty",
            SourceReply::Completed(AcquireReply::Assigned { .. }) => "assigned",
            SourceReply::Completed(AcquireReply::OwnershipLost { .. }) => "ownership_lost",
            SourceReply::Idle => "idle",
            SourceReply::Discarded { .. } => "discarded",
            SourceReply::Stopped { .. } => "stopped",
        };
        trace_poll(command, revision, "worker.poll.resolved", Some(outcome));
    }
    Some((route, reply))
}

async fn reconcile(
    source: &dyn AcquisitionSource,
    config: &DeliveryConfig,
    shared: &Shared,
    command: &AcquireCommand,
    mut first: Option<Result<SourceReply>>,
) -> Option<SourceReply> {
    loop {
        let result = if let Some(result) = first.take() {
            result
        } else {
            let started = Instant::now();
            let draining = shared.stopping();
            let wait = if draining {
                Duration::ZERO
            } else {
                config.acquire_wait
            };
            let options = AcquireOptions::new(wait, started + config.request_timeout)
                .expect("validated acquisition timing");
            let request = exchange(config.request_timeout, || {
                if draining {
                    source.drain(command, options)
                } else {
                    source.acquire(command, options)
                }
            });
            tokio::select! {
                biased;
                _ = shared.stopped(), if !draining => continue,
                result = request => result,
            }
        };
        match result {
            Ok(reply @ (SourceReply::Idle | SourceReply::Stopped { .. })) => {
                return Some(reply);
            }
            Ok(SourceReply::Discarded { sequence }) => {
                if sequence == command.sequence {
                    return Some(SourceReply::Discarded { sequence });
                }
                shared.fatal(protocol("discarded handoff changed sequence"));
            }
            Ok(SourceReply::Completed(reply)) => {
                let sequence = match &reply {
                    AcquireReply::Empty { sequence }
                    | AcquireReply::Assigned { sequence, .. }
                    | AcquireReply::OwnershipLost { sequence, .. } => *sequence,
                };
                let valid = if sequence != command.sequence {
                    Err(protocol("acquisition reply changed sequence"))
                } else if let AcquireReply::Assigned { assignment, .. } = &reply {
                    attempt::validate_assignment(command, assignment)
                } else {
                    Ok(())
                };
                match valid {
                    Ok(()) => return Some(SourceReply::Completed(reply)),
                    Err(error) => shared.fatal(error),
                }
            }
            Err(error @ (ContractError::UnknownSession | ContractError::SessionExpired)) => {
                shared.fatal(error);
                return None;
            }
            Err(error) if retryable(&error) => shared.error(error),
            Err(error) => shared.fatal(error),
        }
        tokio::time::sleep(config.retry_delay).await;
    }
}

#[cfg(test)]
mod tests;
