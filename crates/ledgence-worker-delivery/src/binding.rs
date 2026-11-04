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

/// Once issued, retain this exact queue/session/consumer/sequence until its
/// authority is reconciled. Replays, settlement and shutdown don't draw
/// from the planner or advance service on another route.
pub(super) async fn acquire(
    source: &dyn AcquisitionSource,
    config: &DeliveryConfig,
    shared: &Shared,
    command: &AcquireCommand,
    planned: bool,
) -> Option<SourceReply> {
    let mut first_planned = planned;
    loop {
        let started = Instant::now();
        // A planned selection was charged at issuance; give that exchange
        // its first poll before reconciling it. Legacy sources instead
        // drain immediately when stop has already won, without starting
        // a fresh broker receive.
        let draining = !first_planned && shared.stopping();
        first_planned = false;
        let max_wait = if draining {
            Duration::ZERO
        } else {
            config.acquire_wait
        };
        let options = AcquireOptions::new(max_wait, started + config.request_timeout)
            .expect("validated acquisition timing");
        let result = {
            let request = exchange(config.request_timeout, || {
                if draining {
                    source.drain(command, options)
                } else {
                    source.acquire(command, options)
                }
            });
            tokio::pin!(request);
            tokio::select! {
                biased;
                result = &mut request => Some(result),
                _ = shared.stopped(), if !draining => {
                    // Reconcile immediately with the same sequence. Dropping
                    // this future cannot prove that no claim committed.
                    None
                }
            }
        };
        let Some(result) = result else { continue };
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
mod tests {
    use super::*;

    #[derive(Default)]
    struct PendingSource(Mutex<Vec<&'static str>>);

    impl AcquisitionSource for PendingSource {
        fn acquire<'a>(
            &'a self,
            _: &'a AcquireCommand,
            _: AcquireOptions,
        ) -> ContractFuture<'a, SourceReply> {
            Box::pin(async {
                self.0.lock().unwrap().push("acquire");
                std::future::pending().await
            })
        }
        fn drain<'a>(
            &'a self,
            _: &'a AcquireCommand,
            _: AcquireOptions,
        ) -> ContractFuture<'a, SourceReply> {
            Box::pin(async {
                self.0.lock().unwrap().push("drain");
                Ok(SourceReply::Idle)
            })
        }
    }

    #[tokio::test]
    async fn stop_at_first_exchange_drains_legacy_source_without_a_new_receive() {
        let config = DeliveryConfig::new(
            Scope {
                tenant_id: "t".into(),
                namespace: "n".into(),
            },
            "q",
        );
        let command = AcquireCommand {
            scope: config.scope.clone(),
            queue: "q".into(),
            worker_session_id: "worker".into(),
            consumer_id: 0,
            sequence: 1,
        };
        let shared = Shared {
            stop: AtomicBool::new(true),
            stop_changed: tokio::sync::Notify::new(),
            status: Mutex::default(),
        };
        for planned in [false, true] {
            let source = PendingSource::default();
            let result = acquire(&source, &config, &shared, &command, planned).await;
            assert!(matches!(result, Some(SourceReply::Idle)));
            let expected = if planned {
                &["acquire", "drain"][..]
            } else {
                &["drain"][..]
            };
            assert_eq!(*source.0.lock().unwrap(), expected);
        }
    }
}
