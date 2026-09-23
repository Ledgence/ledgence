//! Worker-local authority accounting. Database timestamps never become local
//! deadlines; each response is charged for elapsed time since its request began.

use ledgence_orchestration_api::{Authority, LEASE_SAFETY_MARGIN_MS, LeaseOwner};
use std::{
    fmt,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    Unconfirmed,
    Active,
    Cancelled,
    OwnershipLost,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseUpdate {
    Applied,
    Replay,
    IgnoredOlder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseTrackerError {
    Unconfirmed,
    WrongOwner,
    InvalidTiming,
    InvalidAuthority,
    DispatchNotAuthorized,
    Cancelled,
    OwnershipLost,
    Expired,
}
impl fmt::Display for LeaseTrackerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for LeaseTrackerError {}

/// One local attempt's permission to invoke/continue user work, bound to every
/// field of its lease owner.
///
/// Callers supply the monotonic request-start and observation times. Once local
/// authority expires, is cancelled, or is lost, this tracker never reactivates.
/// A new tracker must not be used to revive the same locally expired attempt.
/// Expiry/cancellation stops user work, not cleanup or control communication:
/// renewal and settlement may continue using the full server lease authority
/// until reporting/cleanup is resolved. The execution bound cannot be extended
/// by later samples, even if a later request has less network latency.
///
/// This does not deduplicate execution: the delivery driver must separately
/// ensure a confirmed dispatch permission starts an invocation at most once.
/// Monotonic clock progress is an environmental assumption; after suspension or
/// other clock discontinuity the caller must stop rather than assume authority.
#[derive(Debug)]
pub struct LeaseTracker {
    owner: LeaseOwner,
    state: LeaseState,
    deadline: Option<Instant>,
    execution_deadline: Option<Instant>,
    sequence: Option<u64>,
    dispatch_allowed: bool,
}

impl LeaseTracker {
    pub fn new(owner: LeaseOwner) -> Self {
        Self {
            owner,
            state: LeaseState::Unconfirmed,
            deadline: None,
            execution_deadline: None,
            sequence: None,
            dispatch_allowed: false,
        }
    }

    pub fn owner(&self) -> &LeaseOwner {
        &self.owner
    }

    /// Last confirmed deadline, retained for diagnostics even after revocation.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Fixed execution bound inferred conservatively from confirmed responses.
    pub fn execution_deadline(&self) -> Option<Instant> {
        self.execution_deadline
    }

    pub fn renew_sequence(&self) -> Option<u64> {
        self.sequence
    }

    /// Observing time also records expiry, so later replies cannot revive it.
    pub fn state(&mut self, now: Instant) -> LeaseState {
        if self.state == LeaseState::Active && self.deadline.is_some_and(|at| now >= at) {
            self.stop(LeaseState::Expired);
        }
        self.state
    }

    /// Apply a response using its own request's start time, never receive time.
    /// The response's absolute database `expires_at` is diagnostic only here.
    pub fn apply(
        &mut self,
        authority: &Authority,
        request_started: Instant,
        now: Instant,
    ) -> Result<LeaseUpdate, LeaseTrackerError> {
        if authority.owner != self.owner {
            return Err(LeaseTrackerError::WrongOwner);
        }
        self.state(now);
        self.check_stopped()?;
        if request_started > now {
            return Err(LeaseTrackerError::InvalidTiming);
        }

        // Revocation is monotone even if it arrives on an older exchange.
        if authority.cancel_requested {
            self.stop(LeaseState::Cancelled);
            return Err(LeaseTrackerError::Cancelled);
        }
        if authority.remaining_ms == 0 {
            self.stop(LeaseState::OwnershipLost);
            return Err(LeaseTrackerError::OwnershipLost);
        }
        if self
            .sequence
            .is_some_and(|sequence| authority.renew_sequence < sequence)
        {
            return Ok(LeaseUpdate::IgnoredOlder);
        }
        if authority.renew_sequence == 0 && authority.dispatch_allowed {
            return Err(LeaseTrackerError::InvalidAuthority);
        }
        let lease_remaining = authority
            .remaining_ms
            .saturating_sub(LEASE_SAFETY_MARGIN_MS);
        let execution_remaining = authority
            .execution_remaining_ms
            .saturating_sub(LEASE_SAFETY_MARGIN_MS);
        let lease_candidate = request_started
            .checked_add(Duration::from_millis(lease_remaining))
            .ok_or(LeaseTrackerError::InvalidTiming)?;
        let execution_candidate = request_started
            .checked_add(Duration::from_millis(execution_remaining))
            .ok_or(LeaseTrackerError::InvalidTiming)?;
        let execution_deadline = self
            .execution_deadline
            .map_or(execution_candidate, |old| old.min(execution_candidate));
        let candidate = lease_candidate.min(execution_deadline);
        if now >= candidate {
            self.stop(LeaseState::Expired);
            return Err(LeaseTrackerError::Expired);
        }
        self.execution_deadline = Some(execution_deadline);

        if self.sequence == Some(authority.renew_sequence) {
            // Recovery never buys more local time or restores permission that
            // another response at this revision has already withdrawn.
            self.deadline = Some(self.deadline.map_or(candidate, |old| old.min(candidate)));
            self.dispatch_allowed &= authority.dispatch_allowed;
            return Ok(LeaseUpdate::Replay);
        }
        self.deadline = Some(candidate);
        self.sequence = Some(authority.renew_sequence);
        self.dispatch_allowed = authority.dispatch_allowed;
        self.state = LeaseState::Active;
        Ok(LeaseUpdate::Applied)
    }

    pub fn check_dispatch(&mut self, now: Instant) -> Result<(), LeaseTrackerError> {
        self.state(now);
        self.check_stopped()?;
        match self.state {
            LeaseState::Unconfirmed => Err(LeaseTrackerError::Unconfirmed),
            _ if !self.dispatch_allowed => Err(LeaseTrackerError::DispatchNotAuthorized),
            _ => Ok(()),
        }
    }

    pub fn cancel(&mut self) {
        if matches!(self.state, LeaseState::Unconfirmed | LeaseState::Active) {
            self.stop(LeaseState::Cancelled);
        }
    }

    /// Apply an authoritative ownership rejection for this exact request owner.
    /// An unavailable transport is not an authoritative ownership rejection.
    pub fn mark_ownership_lost(&mut self, owner: &LeaseOwner) -> Result<(), LeaseTrackerError> {
        if owner != &self.owner {
            return Err(LeaseTrackerError::WrongOwner);
        }
        if matches!(self.state, LeaseState::Unconfirmed | LeaseState::Active) {
            self.stop(LeaseState::OwnershipLost);
        }
        Ok(())
    }

    fn stop(&mut self, state: LeaseState) {
        self.state = state;
        self.dispatch_allowed = false;
    }

    fn check_stopped(&self) -> Result<(), LeaseTrackerError> {
        match self.state {
            LeaseState::Cancelled => Err(LeaseTrackerError::Cancelled),
            LeaseState::OwnershipLost => Err(LeaseTrackerError::OwnershipLost),
            LeaseState::Expired => Err(LeaseTrackerError::Expired),
            LeaseState::Unconfirmed | LeaseState::Active => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_orchestration_api::Scope;

    fn owner() -> LeaseOwner {
        LeaseOwner {
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "namespace".into(),
            },
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_id: "lease".into(),
            generation: 1,
            worker_session_id: "session".into(),
            consumer_id: 0,
        }
    }
    fn response(sequence: u64, remaining_ms: u64, dispatch_allowed: bool) -> Authority {
        Authority {
            owner: owner(),
            expires_at: 1,
            remaining_ms,
            execution_remaining_ms: 300_000,
            renew_sequence: sequence,
            cancel_requested: false,
            dispatch_allowed,
        }
    }
    fn at(start: Instant, milliseconds: u64) -> Instant {
        start + Duration::from_millis(milliseconds)
    }
    fn claimed(start: Instant) -> LeaseTracker {
        let mut tracker = LeaseTracker::new(owner());
        tracker
            .apply(&response(0, 60_000, false), start, start)
            .unwrap();
        tracker
    }

    #[test]
    fn dispatch_requires_confirmed_permission_and_charges_request_latency() {
        let start = Instant::now();
        let mut tracker = LeaseTracker::new(owner());
        assert_eq!(
            tracker.check_dispatch(start),
            Err(LeaseTrackerError::Unconfirmed)
        );
        tracker
            .apply(&response(0, 60_000, false), start, at(start, 20_000))
            .unwrap();
        assert_eq!(tracker.deadline(), Some(at(start, 55_000)));
        assert_eq!(
            tracker.check_dispatch(at(start, 20_000)),
            Err(LeaseTrackerError::DispatchNotAuthorized)
        );
        tracker
            .apply(
                &response(1, 60_000, true),
                at(start, 21_000),
                at(start, 22_000),
            )
            .unwrap();
        assert_eq!(tracker.deadline(), Some(at(start, 76_000)));
        assert_eq!(tracker.check_dispatch(at(start, 22_000)), Ok(()));
        assert_eq!(
            tracker.check_dispatch(at(start, 76_000)),
            Err(LeaseTrackerError::Expired)
        );
    }

    #[test]
    fn reversed_replies_do_not_replace_newer_deadline_or_reopen_permission() {
        let start = Instant::now();
        let mut tracker = claimed(start);
        tracker
            .apply(
                &response(2, 60_000, false),
                at(start, 20_000),
                at(start, 21_000),
            )
            .unwrap();
        let deadline = tracker.deadline();
        assert_eq!(
            tracker.apply(
                &response(1, 60_000, true),
                at(start, 10_000),
                at(start, 66_000)
            ),
            Ok(LeaseUpdate::IgnoredOlder)
        );
        assert_eq!(tracker.deadline(), deadline);
        assert_eq!(tracker.renew_sequence(), Some(2));
        assert_eq!(
            tracker.check_dispatch(at(start, 66_000)),
            Err(LeaseTrackerError::DispatchNotAuthorized)
        );
    }

    #[test]
    fn same_sequence_replay_cannot_extend_or_reopen_permission() {
        let start = Instant::now();
        let mut tracker = claimed(start);
        tracker
            .apply(
                &response(1, 60_000, true),
                at(start, 10_000),
                at(start, 11_000),
            )
            .unwrap();
        let deadline = tracker.deadline();
        assert_eq!(
            tracker.apply(
                &response(1, 60_000, false),
                at(start, 20_000),
                at(start, 21_000)
            ),
            Ok(LeaseUpdate::Replay)
        );
        tracker
            .apply(
                &response(1, 60_000, true),
                at(start, 22_000),
                at(start, 23_000),
            )
            .unwrap();
        assert_eq!(tracker.deadline(), deadline);
        assert_eq!(
            tracker.check_dispatch(at(start, 23_000)),
            Err(LeaseTrackerError::DispatchNotAuthorized)
        );
    }

    #[test]
    fn matching_cancellation_is_sticky_even_from_an_older_reply() {
        let start = Instant::now();
        let mut tracker = claimed(start);
        tracker
            .apply(
                &response(2, 60_000, true),
                at(start, 20_000),
                at(start, 21_000),
            )
            .unwrap();
        let mut cancelled = response(1, 60_000, false);
        cancelled.cancel_requested = true;
        assert_eq!(
            tracker.apply(&cancelled, at(start, 10_000), at(start, 22_000)),
            Err(LeaseTrackerError::Cancelled)
        );
        assert_eq!(
            tracker.apply(
                &response(3, 60_000, true),
                at(start, 23_000),
                at(start, 24_000)
            ),
            Err(LeaseTrackerError::Cancelled)
        );
        assert_eq!(
            tracker.check_dispatch(at(start, 24_000)),
            Err(LeaseTrackerError::Cancelled)
        );
    }

    #[test]
    fn expired_local_authority_cannot_be_revived_by_a_newer_response() {
        let start = Instant::now();
        let mut tracker = claimed(start);
        assert_eq!(
            tracker.apply(
                &response(1, 60_000, true),
                at(start, 50_000),
                at(start, 55_000)
            ),
            Err(LeaseTrackerError::Expired)
        );
        assert_eq!(tracker.state(at(start, 56_000)), LeaseState::Expired);
        assert_eq!(tracker.renew_sequence(), Some(0));
    }

    #[test]
    fn delayed_initial_or_newer_response_with_no_safe_time_expires() {
        let start = Instant::now();
        let mut initial = LeaseTracker::new(owner());
        assert_eq!(
            initial.apply(&response(0, 60_000, false), start, at(start, 55_000)),
            Err(LeaseTrackerError::Expired)
        );
        let mut tracker = claimed(start);
        assert_eq!(
            tracker.apply(
                &response(1, 10_000, true),
                at(start, 10_000),
                at(start, 15_000)
            ),
            Err(LeaseTrackerError::Expired)
        );
        assert_eq!(tracker.state(at(start, 15_000)), LeaseState::Expired);
    }

    #[test]
    fn cleanup_lease_time_does_not_extend_work_past_execution_deadline() {
        let start = Instant::now();
        let mut tracker = LeaseTracker::new(owner());
        let mut initial = response(0, 60_000, false);
        initial.execution_remaining_ms = 30_000;
        tracker.apply(&initial, start, start).unwrap();
        assert_eq!(tracker.deadline(), Some(at(start, 25_000)));
        let mut dispatch = response(1, 38_000, true);
        dispatch.execution_remaining_ms = 8_000;
        // Sampled after two seconds outbound latency; another two seconds pass
        // before receipt. The cleanup lease is live but no safe work time remains.
        assert_eq!(
            tracker.apply(&dispatch, at(start, 20_000), at(start, 24_000)),
            Err(LeaseTrackerError::Expired)
        );
        assert_eq!(
            tracker.check_dispatch(at(start, 24_000)),
            Err(LeaseTrackerError::Expired)
        );
    }

    #[test]
    fn improved_network_latency_cannot_extend_the_fixed_execution_bound() {
        let start = Instant::now();
        let mut tracker = LeaseTracker::new(owner());
        let mut initial = response(0, 60_000, false);
        // Absolute execution deadline is start+100s, sampled at start+10s.
        initial.execution_remaining_ms = 90_000;
        tracker.apply(&initial, start, at(start, 11_000)).unwrap();
        assert_eq!(tracker.execution_deadline(), Some(at(start, 85_000)));
        let mut first = response(1, 60_000, true);
        first.execution_remaining_ms = 80_000;
        tracker
            .apply(&first, at(start, 20_000), at(start, 20_000))
            .unwrap();
        let mut second = response(2, 60_000, false);
        second.execution_remaining_ms = 60_000;
        tracker
            .apply(&second, at(start, 40_000), at(start, 40_000))
            .unwrap();
        assert_eq!(tracker.execution_deadline(), Some(at(start, 85_000)));
        assert_eq!(tracker.deadline(), Some(at(start, 85_000)));
        assert_eq!(tracker.state(at(start, 85_000)), LeaseState::Expired);
    }

    #[test]
    fn every_owner_field_is_bound_and_wrong_owner_does_not_mutate_authority() {
        let start = Instant::now();
        let mut variants = Vec::new();
        for index in 0..8 {
            let mut different = owner();
            match index {
                0 => different.scope.tenant_id = "other".into(),
                1 => different.scope.namespace = "other".into(),
                2 => different.task_id = "other".into(),
                3 => different.attempt_id = "other".into(),
                4 => different.lease_id = "other".into(),
                5 => different.generation += 1,
                6 => different.worker_session_id = "other".into(),
                _ => different.consumer_id += 1,
            }
            variants.push(different);
        }
        let mut tracker = claimed(start);
        let deadline = tracker.deadline();
        for different in variants {
            let mut reply = response(1, 60_000, true);
            reply.owner = different.clone();
            reply.cancel_requested = true;
            assert_eq!(
                tracker.apply(&reply, start, start),
                Err(LeaseTrackerError::WrongOwner)
            );
            assert_eq!(
                tracker.mark_ownership_lost(&different),
                Err(LeaseTrackerError::WrongOwner)
            );
        }
        assert_eq!(tracker.state(start), LeaseState::Active);
        assert_eq!(tracker.deadline(), deadline);
        assert_eq!(tracker.renew_sequence(), Some(0));
    }

    #[test]
    fn ownership_loss_is_sticky_for_rejection_and_zero_authority() {
        let start = Instant::now();
        for explicit in [false, true] {
            let mut tracker = claimed(start);
            if explicit {
                tracker.mark_ownership_lost(&owner()).unwrap();
            } else {
                assert_eq!(
                    tracker.apply(&response(0, 0, false), start, start),
                    Err(LeaseTrackerError::OwnershipLost)
                );
            }
            assert_eq!(
                tracker.apply(&response(1, 60_000, true), start, start),
                Err(LeaseTrackerError::OwnershipLost)
            );
            assert_eq!(
                tracker.check_dispatch(start),
                Err(LeaseTrackerError::OwnershipLost)
            );
        }
    }

    #[test]
    fn zero_sequence_dispatch_and_future_request_start_are_rejected() {
        let start = Instant::now();
        let mut tracker = LeaseTracker::new(owner());
        assert_eq!(
            tracker.apply(&response(0, 60_000, true), start, start),
            Err(LeaseTrackerError::InvalidAuthority)
        );
        assert_eq!(
            tracker.apply(&response(0, 60_000, false), at(start, 1), start),
            Err(LeaseTrackerError::InvalidTiming)
        );
        assert_eq!(tracker.state(start), LeaseState::Unconfirmed);
        assert_eq!(tracker.deadline(), None);
        tracker.cancel();
        assert_eq!(
            tracker.apply(&response(0, 60_000, false), start, start),
            Err(LeaseTrackerError::Cancelled)
        );
    }
}
