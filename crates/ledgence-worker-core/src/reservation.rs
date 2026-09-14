//! Admission ownership spanning future acquisition and external settlement.

use super::*;

/// One consumer slot from the worker's existing concurrency limit.
///
/// Reserve before asking an orchestration service for work. The reservation is
/// deliberately not cloneable and authorizes at most one local execution. Keep
/// it until acquisition/settlement is resolved, then call [`Self::release`] or
/// drop it. Retained execution, preparation, or cleanup continues owning the
/// same slot if the caller disappears before that local work finishes.
///
/// This type does not itself claim remote work or certify durable settlement.
#[must_use = "keep the consumer reservation until acquisition and settlement are resolved"]
pub struct ConsumerReservation {
    worker: Worker,
    ownership: Arc<ConsumerOwnership>,
    used: bool,
}

impl ConsumerReservation {
    /// Execute at most one assignment using this already reserved consumer.
    ///
    /// The execution control governs this invocation; it is independent from
    /// the control used to wait for admission. Dropping this future requests
    /// cancellation, and does not make the reservation reusable. A preparation
    /// timeout may return before its retained operation actually finishes.
    pub async fn execute(
        &mut self,
        request: ExecutionRequest,
        control: RunControl,
    ) -> ExecutionResult {
        self.execute_inner(request, control, None).await
    }

    /// Execute one interactive assignment using this same reserved consumer.
    pub async fn execute_interactive(
        &mut self,
        request: ExecutionRequest,
        control: RunControl,
        extension: RuntimeExtension,
        handler: Arc<dyn RuntimeRequestHandler>,
    ) -> ExecutionResult {
        self.execute_inner(
            request,
            control,
            Some(InteractiveExecution { extension, handler }),
        )
        .await
    }

    async fn execute_inner(
        &mut self,
        request: ExecutionRequest,
        control: RunControl,
        interactive: Option<InteractiveExecution>,
    ) -> ExecutionResult {
        if self.used {
            return Err(logged_failure(
                Error::new(ErrorKind::InvalidInput, "consumer reservation already used"),
                Phase::Admission,
                false,
                &ExecutionContext::from(&request),
            ));
        }
        self.used = true;
        self.worker
            .execute_with_reservation(request, control, Some(self.ownership.clone()), interactive)
            .await
    }

    /// Shutdown or fail-closed supervision has asked this delivery owner to stop.
    /// A driver must resolve its remote acquisition/settlement and release the
    /// reservation; shutdown cannot make that remote decision on its behalf.
    pub fn is_cancellation_requested(&self) -> bool {
        self.ownership.cancelled.load(Ordering::Acquire)
    }

    /// Whether this reservation has no unfinished local invocation or cleanup.
    ///
    /// This is also true before execution starts. Healthy warm processes may
    /// remain reusable; retained preparation, quarantined cleanup, and unknown
    /// operation lifetimes keep this false even after a result was delivered.
    /// Concurrent internal observations can conservatively return false.
    ///
    /// This observes only local work. It does not acknowledge remote settlement,
    /// release consumer capacity, or certify that the whole worker has stopped.
    pub fn is_quiescent(&self) -> bool {
        // The unique public handle owns one strong reference. Every operation
        // must own another before starting, including Registration through its
        // Drop and every quarantined/unresolved cleanup handle. Once only this
        // reference remains, no operation can start without borrowing this
        // non-Clone handle mutably. Registry Weak upgrades only observe/cancel
        // existing owners, so they can cause a false negative, never new work.
        Arc::strong_count(&self.ownership) == 1
    }

    /// Relinquish caller ownership after the external delivery is resolved.
    /// Ongoing local operations and quarantined sessions retain their ownership
    /// until actual cleanup. Releasing this value is not a cleanup certificate.
    pub fn release(self) {}
}

impl Drop for ConsumerReservation {
    fn drop(&mut self) {
        self.ownership.cancel();
        self.ownership.external.store(false, Ordering::Release);
    }
}

pub(super) struct ConsumerOwnership {
    _permit: OwnedSemaphorePermit,
    pub external: AtomicBool,
    pub cancelled: AtomicBool,
    execution: StdMutex<ReservationExecution>,
}

#[derive(Default)]
struct ReservationExecution {
    control: Option<RunControl>,
    supervisor_finished: bool,
    // A single-use invocation can leave at most one quarantined session: each
    // retirement path stops on its first failed close. Neither an early report
    // nor a transient Arc observer is evidence that this work has finished.
    cleanup_pending: bool,
    operation_unresolved: bool,
}
impl ReservationExecution {
    fn detach_finished_control(&mut self) {
        if self.supervisor_finished && !self.cleanup_pending && !self.operation_unresolved {
            self.control = None;
        }
    }
}

impl ConsumerOwnership {
    pub(super) fn begin_execution(&self, control: RunControl) {
        self.execution
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .control = Some(control);
    }

    pub(super) fn finish_supervisor(&self) {
        let mut execution = self.execution.lock().unwrap_or_else(|p| p.into_inner());
        execution.supervisor_finished = true;
        execution.detach_finished_control();
    }

    pub(super) fn retain_cleanup(&self) {
        self.execution
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cleanup_pending = true;
    }

    pub(super) fn confirm_cleanup(&self) {
        let mut execution = self.execution.lock().unwrap_or_else(|p| p.into_inner());
        execution.cleanup_pending = false;
        execution.detach_finished_control();
    }

    pub(super) fn retain_unresolved_operation(&self) {
        self.execution
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .operation_unresolved = true;
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(control) = self
            .execution
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .control
            .as_ref()
        {
            control.cancel();
        }
    }
}

pub(super) enum ConsumerPermit {
    Direct { _permit: OwnedSemaphorePermit },
    Reserved(Arc<ConsumerOwnership>),
}

impl ConsumerPermit {
    pub fn reservation(&self) -> Option<Arc<ConsumerOwnership>> {
        match self {
            Self::Direct { .. } => None,
            Self::Reserved(owner) => Some(owner.clone()),
        }
    }
}

impl Worker {
    /// Wait for a consumer slot before acquiring an external assignment.
    ///
    /// `control` governs this admission wait only. Once admitted, ownership lasts
    /// until the reservation and all retained local work release the slot. The
    /// reservation reports shutdown through its cancellation flag. Dropping an
    /// admission waiter has no side effects and cannot leak a permit.
    pub async fn reserve_consumer(&self, control: RunControl) -> Result<ConsumerReservation> {
        let acquire = self.inner.consumers.clone().acquire_owned();
        tokio::pin!(acquire);
        let permit = loop {
            control.check()?;
            if !self
                .inner
                .registry
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .accepting
            {
                return Err(Error::new(ErrorKind::Unavailable, "worker is draining"));
            }
            tokio::select! {
                permit = &mut acquire => break permit.map_err(|error| {
                    Error::new(ErrorKind::Unavailable, error.to_string())
                })?,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        };
        // Shutdown and successful reservation registration serialize here. There
        // is no await between the last checks and publishing this local owner.
        let mut registry = self
            .inner
            .registry
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        control.check()?;
        if !registry.accepting {
            return Err(Error::new(ErrorKind::Unavailable, "worker is draining"));
        }
        let ownership = Arc::new(ConsumerOwnership {
            _permit: permit,
            external: AtomicBool::new(true),
            cancelled: AtomicBool::new(false),
            execution: StdMutex::new(ReservationExecution::default()),
        });
        registry
            .reservations
            .retain(|owner| owner.strong_count() != 0);
        registry.reservations.push(Arc::downgrade(&ownership));
        Ok(ConsumerReservation {
            worker: self.clone(),
            ownership,
            used: false,
        })
    }
}

impl Registry {
    pub(super) fn cancel_all(&self) {
        for control in self.active.values() {
            control.cancel();
        }
        for owner in self.reservations.iter().filter_map(Weak::upgrade) {
            owner.cancel();
        }
    }

    pub(super) fn retain_unresolved_consumer(&mut self, key: &AttemptKey) {
        if let Some(owner) = self.pending_settlement.get(key).and_then(Weak::upgrade)
            && !self
                .unresolved_consumers
                .iter()
                .any(|held| Arc::ptr_eq(held, &owner))
        {
            // A panicking adapter never supplied a recoverable operation handle.
            // Match its permanent unresolved marker with retained admission.
            owner.retain_unresolved_operation();
            self.unresolved_consumers.push(owner);
        }
    }
}
