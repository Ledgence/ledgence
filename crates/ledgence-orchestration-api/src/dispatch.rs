//! Durable handoff contracts independent of broker receipts or SDK types.
//!
//! A dispatch record is a reference to existing task authority, not permission
//! to execute. Adapters may settle transport delivery only after validating a
//! successful, identity-bound claim response. Errors provide no handoff proof.

use crate::*;
use std::time::{Duration, Instant};

/// Complete broker-record/claim-command limit, including JSON whitespace. These
/// envelopes contain identifiers only; application payloads remain in task state.
pub const DISPATCH_MAX_BYTES: usize = 16 * 1024;
/// Claim responses can contain a complete assignment and its application data.
pub const CLAIM_REPLY_MAX_BYTES: usize = 16 * 1024 * 1024;
/// Maximum records leased or completed in one maintenance operation.
pub const MAX_PUBLICATION_BATCH: u32 = 100;

/// One readiness generation. Retransmission preserves this identity; a retry
/// made eligible by the lifecycle advances the generation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchRef {
    pub scope: Scope,
    pub queue: String,
    pub task_id: String,
    pub generation: u32,
}
impl DispatchRef {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.queue, 128)?;
        validate_text(&self.task_id, 128)?;
        if !(1..=1_000).contains(&self.generation) {
            return Err(invalid("dispatch generation must be between 1 and 1000"));
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode_unique_json(bytes, DISPATCH_MAX_BYTES)?;
        value.validate()?;
        Ok(value)
    }
}

/// Publication identity remains unchanged across uncertain send retries. A
/// deliberate repair publication gets a new identity for the same generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedDispatch {
    pub dispatch: DispatchRef,
    pub publication_id: String,
}
impl PublishedDispatch {
    pub fn validate(&self) -> Result<()> {
        self.dispatch.validate()?;
        validate_text(&self.publication_id, 128)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode_unique_json(bytes, DISPATCH_MAX_BYTES)?;
        value.validate()?;
        Ok(value)
    }
}

/// The session, consumer, and sequence identify one claim operation. Its exact
/// dispatch binding is immutable even if the exchange outcome is unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimCommand {
    pub acquisition: AcquireCommand,
    pub dispatch: DispatchRef,
}
impl ClaimCommand {
    pub fn validate(&self) -> Result<()> {
        self.dispatch.validate()?;
        let acquisition = &self.acquisition;
        acquisition.scope.validate()?;
        validate_text(&acquisition.queue, 128)?;
        validate_text(&acquisition.worker_session_id, 128)?;
        if acquisition.sequence == 0 {
            return Err(invalid("claim sequence must be nonzero"));
        }
        if acquisition.scope != self.dispatch.scope || acquisition.queue != self.dispatch.queue {
            return Err(invalid("claim consumer and dispatch scope/queue differ"));
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode_unique_json(bytes, DISPATCH_MAX_BYTES)?;
        value.validate()?;
        Ok(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClaimDisposition {
    /// New assignment or the same claimant's replay. OwnershipLost is a durable
    /// replay of its former handoff; Empty is never a valid claimed disposition.
    Claimed { reply: AcquireReply },
    /// Another claim has established durable recovery. This grants no authority.
    AlreadyHandedOff { attempt: AttemptRef },
    /// The referenced generation is durably terminal or superseded. A missing or
    /// unknown task is not evidence of this disposition.
    TerminalOrSuperseded,
    /// Matching future delivery is durably guaranteed by an unfulfilled intent.
    /// The timestamp alone is not permission for a worker to create an attempt.
    Deferred { available_at: Timestamp },
}

/// Every successful disposition consumes the claim sequence and is persisted
/// with its exact command. Receipts survive subsequent consumer-cursor updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimReply {
    pub command: ClaimCommand,
    pub disposition: ClaimDisposition,
}
impl ClaimReply {
    /// Check handoff evidence before settling any broker record. Validation
    /// failure is an unavailable/unknown result, never permission to acknowledge.
    pub fn validate_reply_against(&self, expected: &ClaimCommand) -> Result<()> {
        expected.validate()?;
        self.validate_identity(expected)
            .map_err(|_| inconsistent("claim response does not match the requested dispatch"))
    }

    pub fn decode(bytes: &[u8], expected: &ClaimCommand) -> Result<Self> {
        let value: Self = decode_unique_json(bytes, CLAIM_REPLY_MAX_BYTES)
            .map_err(|_| inconsistent("invalid claim response JSON"))?;
        value.validate_reply_against(expected)?;
        Ok(value)
    }

    fn validate_identity(&self, expected: &ClaimCommand) -> Result<()> {
        if self.command != *expected {
            return Err(invalid("claim command changed"));
        }
        match &self.disposition {
            ClaimDisposition::Claimed { reply } => match reply {
                AcquireReply::Assigned {
                    sequence,
                    assignment,
                } => {
                    if *sequence != expected.acquisition.sequence {
                        return Err(invalid("claim sequence changed"));
                    }
                    let owner = &assignment.lease.owner;
                    let event = &assignment.event;
                    if owner.scope != expected.dispatch.scope
                        || owner.task_id != expected.dispatch.task_id
                        || owner.generation != expected.dispatch.generation
                        || owner.worker_session_id != expected.acquisition.worker_session_id
                        || owner.consumer_id != expected.acquisition.consumer_id
                        || assignment.authority.owner != *owner
                        || assignment.authority.expires_at != assignment.lease.expires_at
                        || event.tenant_id() != owner.scope.tenant_id
                        || event.namespace() != owner.scope.namespace
                        || event.task_id() != owner.task_id
                        || event.attempt_id() != owner.attempt_id
                        || event.value()["ldgattemptno"].as_u64()
                            != Some(u64::from(owner.generation))
                    {
                        return Err(invalid("claim assignment identity changed"));
                    }
                    validate_text(&owner.attempt_id, 128)?;
                    validate_text(&owner.lease_id, 128)?;
                    assignment.descriptor.validate()?;
                    assignment.validate_workflow_identity()?;
                    for key in ["id", "ldgrunid", "ldgtaskid", "ldgattemptid"] {
                        validate_text(event.value()[key].as_str().unwrap_or_default(), 128)?;
                    }
                    validate_text(event.value()["source"].as_str().unwrap_or_default(), 2048)
                }
                AcquireReply::OwnershipLost {
                    sequence,
                    assignment,
                } => {
                    if *sequence != expected.acquisition.sequence {
                        return Err(invalid("claim sequence changed"));
                    }
                    validate_attempt_ref(assignment, &expected.dispatch)
                }
                AcquireReply::Empty { .. } => Err(invalid("claimed response cannot be empty")),
            },
            ClaimDisposition::AlreadyHandedOff { attempt } => {
                validate_attempt_ref(attempt, &expected.dispatch)
            }
            ClaimDisposition::TerminalOrSuperseded => Ok(()),
            ClaimDisposition::Deferred { available_at } => {
                if *available_at > i64::MAX as u64 {
                    return Err(invalid("deferred timestamp exceeds supported range"));
                }
                Ok(())
            }
        }
    }
}

fn validate_attempt_ref(attempt: &AttemptRef, dispatch: &DispatchRef) -> Result<()> {
    if attempt.task_id != dispatch.task_id {
        return Err(invalid("claim attempt references another task"));
    }
    validate_text(&attempt.attempt_id, 128)
}

/// External routing binds a logical queue to a stable opaque destination alias.
/// Provider URLs, credentials, and SDK options belong in adapter configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchRoute {
    pub scope: Scope,
    pub queue: String,
    pub destination: String,
}
impl DispatchRoute {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.queue, 128)?;
        validate_text(&self.destination, 128)
    }
}

/// A bounded publication reservation. It contains no task execution authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationLease {
    pub record: PublishedDispatch,
    pub destination: String,
    pub lease_token: String,
}
impl PublicationLease {
    pub fn validate(&self) -> Result<()> {
        self.record.validate()?;
        validate_text(&self.destination, 128)?;
        validate_text(&self.lease_token, 128)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationOutcome {
    /// Positive broker evidence for this exact publication. The still-unclaimed
    /// generation retains a durable intent with a bounded repair deadline.
    Confirmed,
    /// Retry failed or uncertain delivery with the same publication identity.
    Retry,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationCompletion {
    pub dispatch: DispatchRef,
    pub publication_id: String,
    pub lease_token: String,
    pub outcome: PublicationOutcome,
}
impl PublicationCompletion {
    pub fn validate(&self) -> Result<()> {
        self.dispatch.validate()?;
        validate_text(&self.publication_id, 128)?;
        validate_text(&self.lease_token, 128)
    }
}

/// Maintenance of durable delivery obligations. Implementations commit intent
/// creation/invalidation atomically with the corresponding task transition.
/// Publishing is external I/O and must never run while a state transaction is
/// held. Finite leases and retry/repair delays are backend policy, not execution
/// concurrency settings. Unknown operation outcomes are safe to retry.
pub trait DispatchIntentStore: Send + Sync {
    /// Idempotent for an identical existing route; reject changing its destination.
    /// Initial external activation rejects a queue containing queued or active tasks. This
    /// prevents already accepted integrated tasks being silently stranded.
    fn configure_route<'a>(&'a self, route: &'a DispatchRoute) -> ContractFuture<'a, ()>;

    /// Reserve at most `limit` due intents, where 1 <= limit <= 100. The deadline
    /// bounds admission, database work, and commit acknowledgement. Expired leases
    /// become recoverable; an uncertain send keeps its publication identity.
    fn lease_publications<'a>(
        &'a self,
        destination: &'a str,
        limit: u32,
        deadline: Instant,
    ) -> ContractFuture<'a, Vec<PublicationLease>>;

    /// Complete at most 100 leases, conditional on dispatch generation,
    /// publication identity, and lease token. Late/stale or repeated completions
    /// are harmless. A later error may follow earlier per-item commits.
    fn complete_publications<'a>(
        &'a self,
        completions: &'a [PublicationCompletion],
        deadline: Instant,
    ) -> ContractFuture<'a, ()>;
}

/// Upper bound for an opaque receipt copied into the shared handoff coordinator.
/// Receipts remain transport handles; they never identify execution authority.
pub const QUEUE_RECEIPT_MAX_BYTES: usize = 16 * 1024;

/// Configured transport bounds. These advertise capacities, not ordering,
/// scheduling, deduplication, durability, or exactly-once execution promises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueLimits {
    pub max_publish_batch: u32,
    pub max_receive_batch: u32,
    pub max_ack_batch: u32,
    pub max_message_bytes: usize,
}
impl QueueLimits {
    pub fn validate(&self) -> Result<()> {
        if [
            self.max_publish_batch,
            self.max_receive_batch,
            self.max_ack_batch,
        ]
        .into_iter()
        .any(|limit| !(1..=MAX_PUBLICATION_BATCH).contains(&limit))
            || self.max_message_bytes == 0
        {
            return Err(invalid("queue limits exceed the portable contract"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishResult {
    pub publication_id: String,
    pub outcome: PublicationOutcome,
}

/// Individual-ack delivery model. A stream checkpoint requires a separate port;
/// implementations must not disguise prefix commits as arbitrary receipt deletes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDelivery {
    pub body: Vec<u8>,
    pub receipt: String,
}
impl QueueDelivery {
    /// Bound copied transport data before parsing it. Empty/malformed JSON is
    /// deliberately a decode error handled by the coordinator, not an empty poll.
    pub fn validate(&self, limits: QueueLimits) -> Result<()> {
        limits.validate()?;
        if self.body.len() > limits.max_message_bytes.min(DISPATCH_MAX_BYTES) {
            return Err(invalid("queue record exceeds dispatch byte limit"));
        }
        validate_receipt(&self.receipt)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckResult {
    pub receipt: String,
    /// True requires positive evidence for this exact receipt. False preserves
    /// an uncertain/retry outcome; it does not undo the durable task handoff.
    pub confirmed: bool,
}

/// Publish compact dispatch references. Implementations obey both their declared
/// limits and the enclosing deadline. They must bound response bytes before
/// allocation where the transport permits it. Partial responses are per item;
/// absent, malformed, duplicate, or unexpected identities are never confirmation.
pub trait DispatchPublisher: Send + Sync {
    fn limits(&self) -> QueueLimits;
    fn publish<'a>(
        &'a self,
        records: &'a [PublishedDispatch],
        deadline: Instant,
    ) -> ContractFuture<'a, Vec<PublishResult>>;
}

/// Receive and individually acknowledge transport records. The coordinator
/// bounds records, bytes, and receipt sizes; these do not create additional
/// execution concurrency. SDK prefetch/buffers must also have documented bounds.
pub trait AckQueue: Send + Sync {
    fn limits(&self) -> QueueLimits;
    fn receive(
        &self,
        max: u32,
        wait: Duration,
        deadline: Instant,
    ) -> ContractFuture<'_, Vec<QueueDelivery>>;
    /// Called only after verified durable handoff. Partial/missing results and
    /// errors leave the corresponding transport acknowledgment unconfirmed.
    fn acknowledge<'a>(
        &'a self,
        receipts: &'a [String],
        deadline: Instant,
    ) -> ContractFuture<'a, Vec<AckResult>>;
}

fn validate_receipt(receipt: &str) -> Result<()> {
    if receipt.is_empty() || receipt.len() > QUEUE_RECEIPT_MAX_BYTES {
        return Err(invalid("invalid queue receipt byte length"));
    }
    Ok(())
}

fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn inconsistent(message: &str) -> ContractError {
    ContractError::Unavailable(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_worker_api::{CloudEvent, Digest, ProgramDescriptor, ProgramRef};
    use serde_json::json;

    fn dispatch() -> DispatchRef {
        DispatchRef {
            scope: Scope {
                tenant_id: "acme".into(),
                namespace: "billing".into(),
            },
            queue: "invoices".into(),
            task_id: "task_1042".into(),
            generation: 1,
        }
    }
    fn command() -> ClaimCommand {
        let dispatch = dispatch();
        ClaimCommand {
            acquisition: AcquireCommand {
                scope: dispatch.scope.clone(),
                queue: dispatch.queue.clone(),
                worker_session_id: "worker_1".into(),
                consumer_id: 0,
                sequence: 1,
            },
            dispatch,
        }
    }
    fn assigned() -> ClaimReply {
        let command = command();
        let owner = LeaseOwner {
            scope: command.dispatch.scope.clone(),
            task_id: command.dispatch.task_id.clone(),
            attempt_id: "attempt_1".into(),
            lease_id: "lease_1".into(),
            generation: 1,
            worker_session_id: command.acquisition.worker_session_id.clone(),
            consumer_id: 0,
        };
        let assignment = Assignment {
            workflow_activation_id: None,            descriptor: ProgramDescriptor {
                program: ProgramRef { id: "invoice".into(), version: "1".into() },
                digest: Digest(format!("sha256:{}", "a".repeat(64))), size: 100,
            },
            event: CloudEvent::new(json!({
                "specversion":"1.0","id":"event_1","source":"urn:ledgence:orchestrator",
                "type":"com.ledgence.task.invocation.requested.v1","datacontenttype":"application/json",
                "ldgtenantid":"acme","ldgnamespace":"billing","ldgrunid":"run_1042",
                "ldgtaskid":"task_1042","ldgattemptid":"attempt_1","ldgattemptno":1,
                "data":{"value":9007199254740993_u64}
            })).unwrap(),
            lease: Lease { owner: owner.clone(), expires_at: 61_000 },
            authority: Authority {
                owner, expires_at: 61_000, remaining_ms: 60_000, execution_remaining_ms: 60_000,
                renew_sequence: 0, cancel_requested: false, dispatch_allowed: false,
            },
            attempt_deadline: 301_000,
        };
        ClaimReply {
            command,
            disposition: ClaimDisposition::Claimed {
                reply: AcquireReply::Assigned {
                    sequence: 1,
                    assignment: Box::new(assignment),
                },
            },
        }
    }
    fn assignment(reply: &mut ClaimReply) -> &mut Assignment {
        let ClaimDisposition::Claimed {
            reply: AcquireReply::Assigned { assignment, .. },
        } = &mut reply.disposition
        else {
            panic!("assignment fixture")
        };
        assignment
    }

    #[test]
    fn queue_limits_and_copied_transport_bytes_are_bounded() {
        let limits = QueueLimits {
            max_publish_batch: 10,
            max_receive_batch: 10,
            max_ack_batch: 10,
            max_message_bytes: 1024 * 1024,
        };
        assert!(limits.validate().is_ok());
        for bad in [
            QueueLimits {
                max_publish_batch: 0,
                ..limits
            },
            QueueLimits {
                max_receive_batch: 101,
                ..limits
            },
            QueueLimits {
                max_ack_batch: 101,
                ..limits
            },
            QueueLimits {
                max_message_bytes: 0,
                ..limits
            },
        ] {
            assert!(bad.validate().is_err());
        }
        let mut delivery = QueueDelivery {
            body: vec![b' '; DISPATCH_MAX_BYTES],
            receipt: "receipt".into(),
        };
        assert!(delivery.validate(limits).is_ok());
        assert!(
            delivery
                .validate(QueueLimits {
                    max_message_bytes: DISPATCH_MAX_BYTES - 1,
                    ..limits
                })
                .is_err()
        );
        delivery.body.push(b' ');
        assert!(delivery.validate(limits).is_err());
        delivery.body.clear();
        delivery.receipt = "r".repeat(QUEUE_RECEIPT_MAX_BYTES);
        assert!(delivery.validate(limits).is_ok());
        delivery.receipt.push('r');
        assert!(delivery.validate(limits).is_err());
        delivery.receipt.clear();
        assert!(delivery.validate(limits).is_err());
    }

    #[test]
    fn publication_and_command_round_trip_preserve_identity() {
        let record = PublishedDispatch {
            dispatch: dispatch(),
            publication_id: "publication_1".into(),
        };
        assert_eq!(
            PublishedDispatch::decode(&serde_json::to_vec(&record).unwrap()).unwrap(),
            record
        );
        let mut command = command();
        command.acquisition.sequence = u64::MAX;
        assert_eq!(
            ClaimCommand::decode(&serde_json::to_vec(&command).unwrap()).unwrap(),
            command
        );
    }

    #[test]
    fn decoding_rejects_duplicate_unknown_fractional_and_oversized_records() {
        let record = PublishedDispatch {
            dispatch: dispatch(),
            publication_id: "publication_1".into(),
        };
        let bytes = serde_json::to_vec(&record).unwrap();
        let text = String::from_utf8(bytes.clone()).unwrap();
        for invalid in [
            text.replace("\"generation\":1", "\"generation\":1,\"generation\":1"),
            text.replace(
                "\"generation\":1",
                "\"generation\":1,\"generatio\\u006e\":1",
            ),
            text.replace("\"generation\":1", "\"generation\":1.5"),
            text.replace("\"generation\":1", "\"generation\":1,\"extra\":true"),
            text.replacen('{', "{\"extra\":true,", 1),
        ] {
            assert!(
                PublishedDispatch::decode(invalid.as_bytes()).is_err(),
                "{invalid}"
            );
        }
        let mut bounded = bytes;
        bounded.resize(DISPATCH_MAX_BYTES, b' ');
        assert!(PublishedDispatch::decode(&bounded).is_ok());
        bounded.push(b' ');
        assert!(PublishedDispatch::decode(&bounded).is_err());
    }

    #[test]
    fn claim_validation_binds_queue_scope_sequence_and_generation() {
        let original = command();
        for mutate in [
            |c: &mut ClaimCommand| c.acquisition.scope.tenant_id = "other".into(),
            |c: &mut ClaimCommand| c.acquisition.queue = "other".into(),
            |c: &mut ClaimCommand| c.acquisition.sequence = 0,
            |c: &mut ClaimCommand| c.dispatch.generation = 0,
            |c: &mut ClaimCommand| c.dispatch.generation = 1001,
            |c: &mut ClaimCommand| c.dispatch.task_id = "x".repeat(129),
            |c: &mut ClaimCommand| c.acquisition.worker_session_id = "bad\nvalue".into(),
        ] {
            let mut bad = original.clone();
            mutate(&mut bad);
            assert!(bad.validate().is_err());
        }
    }

    #[test]
    fn valid_claim_round_trip_preserves_large_user_integer() {
        let reply = assigned();
        let decoded = ClaimReply::decode(&serde_json::to_vec(&reply).unwrap(), &command()).unwrap();
        let ClaimDisposition::Claimed {
            reply: AcquireReply::Assigned { assignment, .. },
        } = decoded.disposition
        else {
            panic!("assignment expected")
        };
        assert_eq!(
            assignment.event.value()["data"]["value"].as_u64(),
            Some(9007199254740993)
        );
    }

    #[test]
    fn changed_echoed_command_never_provides_handoff_evidence() {
        for mutate in [
            |c: &mut ClaimCommand| c.acquisition.sequence += 1,
            |c: &mut ClaimCommand| c.acquisition.worker_session_id = "worker_2".into(),
            |c: &mut ClaimCommand| c.acquisition.consumer_id = 1,
            |c: &mut ClaimCommand| c.dispatch.task_id = "task_2".into(),
            |c: &mut ClaimCommand| c.dispatch.generation = 2,
            |c: &mut ClaimCommand| c.dispatch.queue = "other".into(),
            |c: &mut ClaimCommand| c.dispatch.scope.namespace = "other".into(),
        ] {
            let mut reply = assigned();
            mutate(&mut reply.command);
            assert!(matches!(
                reply.validate_reply_against(&command()),
                Err(ContractError::Unavailable(_))
            ));
        }
    }

    #[test]
    fn changed_assignment_authority_never_provides_handoff_evidence() {
        for mutate in [
            |a: &mut Assignment| a.lease.owner.task_id = "task_2".into(),
            |a: &mut Assignment| a.lease.owner.attempt_id = "attempt_2".into(),
            |a: &mut Assignment| a.lease.owner.generation = 2,
            |a: &mut Assignment| a.lease.owner.scope.tenant_id = "other".into(),
            |a: &mut Assignment| a.lease.owner.worker_session_id = "worker_2".into(),
            |a: &mut Assignment| a.lease.owner.consumer_id = 1,
            |a: &mut Assignment| a.authority.owner.lease_id = "lease_2".into(),
            |a: &mut Assignment| a.authority.expires_at += 1,
            |a: &mut Assignment| a.descriptor.size = 0,
        ] {
            let mut reply = assigned();
            mutate(assignment(&mut reply));
            assert!(matches!(
                reply.validate_reply_against(&command()),
                Err(ContractError::Unavailable(_))
            ));
        }
    }

    #[test]
    fn event_identity_must_match_both_claim_and_lease() {
        for (key, value) in [
            ("ldgtenantid", json!("other")),
            ("ldgnamespace", json!("other")),
            ("ldgtaskid", json!("task_2")),
            ("ldgattemptid", json!("attempt_2")),
            ("ldgattemptno", json!(2)),
            ("id", json!("x".repeat(129))),
        ] {
            let mut reply = assigned();
            let a = assignment(&mut reply);
            let mut event = a.event.value().clone();
            event[key] = value;
            a.event = CloudEvent::new(event).unwrap();
            assert!(reply.validate_reply_against(&command()).is_err(), "{key}");
        }
    }

    #[test]
    fn only_identity_bound_durable_nonauthority_replies_are_accepted() {
        let reference = AttemptRef {
            task_id: "task_1042".into(),
            attempt_id: "attempt_1".into(),
        };
        for disposition in [
            ClaimDisposition::AlreadyHandedOff {
                attempt: reference.clone(),
            },
            ClaimDisposition::TerminalOrSuperseded,
            ClaimDisposition::Deferred {
                available_at: 90_000,
            },
            ClaimDisposition::Claimed {
                reply: AcquireReply::OwnershipLost {
                    sequence: 1,
                    assignment: reference,
                },
            },
        ] {
            assert!(
                ClaimReply {
                    command: command(),
                    disposition
                }
                .validate_reply_against(&command())
                .is_ok()
            );
        }
        for disposition in [
            ClaimDisposition::Claimed {
                reply: AcquireReply::Empty { sequence: 1 },
            },
            ClaimDisposition::AlreadyHandedOff {
                attempt: AttemptRef {
                    task_id: "other".into(),
                    attempt_id: "attempt_1".into(),
                },
            },
            ClaimDisposition::Claimed {
                reply: AcquireReply::OwnershipLost {
                    sequence: 2,
                    assignment: AttemptRef {
                        task_id: "task_1042".into(),
                        attempt_id: "attempt_1".into(),
                    },
                },
            },
            ClaimDisposition::Deferred {
                available_at: u64::MAX,
            },
        ] {
            assert!(
                ClaimReply {
                    command: command(),
                    disposition
                }
                .validate_reply_against(&command())
                .is_err()
            );
        }
    }

    #[test]
    fn publication_leases_and_completion_require_bounded_opaque_identity() {
        let route = DispatchRoute {
            scope: dispatch().scope,
            queue: "invoices".into(),
            destination: "billing-primary".into(),
        };
        assert!(route.validate().is_ok());
        let mut lease = PublicationLease {
            record: PublishedDispatch {
                dispatch: dispatch(),
                publication_id: "publication_1".into(),
            },
            destination: route.destination,
            lease_token: "token_1".into(),
        };
        assert!(lease.validate().is_ok());
        lease.lease_token.clear();
        assert!(lease.validate().is_err());
        let mut completion = PublicationCompletion {
            dispatch: dispatch(),
            publication_id: "publication_1".into(),
            lease_token: "token_1".into(),
            outcome: PublicationOutcome::Retry,
        };
        assert!(completion.validate().is_ok());
        completion.publication_id = "x".repeat(129);
        assert!(completion.validate().is_err());
    }
}
