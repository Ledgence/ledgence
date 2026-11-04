//! Worker-local distribution of fresh logical polls across a fixed set of queues.
//!
//! A poll counts once when issued, including when it returns empty. A retry uses
//! its original route and sequence. Installing a plan or completing a task does
//! not advance the distribution. Selection state is shared within one worker
//! process and starts fresh on worker restart.

mod strategy;
#[cfg(test)]
mod tests;

use ledgence_orchestration_api::{ContractError, Result, validate_text};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::sync::watch;

use strategy::Selector;

/// Maximum number of queue registrations per planned worker.
pub const MAX_POLL_QUEUES: usize = 64;

/// A revision of relative poll weights. A zero weight disables a queue; all
/// queues may be disabled at once. Queue order on the first plan is the stable
/// route order used for registration and to break Stride ties.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PollPlan {
    revision: u64,
    routes: Vec<(String, u32)>,
}

impl PollPlan {
    /// Validates the queue names and normalizes equivalent integer weights.
    pub fn new(revision: u64, mut routes: Vec<(String, u32)>) -> Result<Self> {
        if !(1..=MAX_POLL_QUEUES).contains(&routes.len()) {
            return Err(invalid("a poll plan must contain 1 to 64 queues"));
        }
        let mut names = HashSet::with_capacity(routes.len());
        let mut divisor = 0;
        for (queue, weight) in &routes {
            validate_text(queue, 128)?;
            if !names.insert(queue) {
                return Err(invalid("duplicate queue in poll plan"));
            }
            divisor = gcd(divisor, *weight);
        }
        if divisor > 1 {
            for (_, weight) in &mut routes {
                *weight /= divisor;
            }
        }
        Ok(Self { revision, routes })
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn routes(&self) -> &[(String, u32)] {
        &self.routes
    }
}

/// How the worker applies poll weights. Lottery uses a different randomized
/// seed for each controller unless a reproducible stream is explicitly selected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PollStrategy {
    Stride,
    Lottery,
    SeededLottery(u64),
}

/// The stable index in [`PollPlanner::queues`] and plan revision of an issued poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PollSelection {
    pub route: usize,
    pub revision: u64,
}

/// A cloneable worker-local controller. All clones observe the same plan and
/// serialize decisions against one shared history. An embedder can compute a
/// new plan by any algorithm and publish it using [`Self::install`].
#[derive(Clone)]
pub struct PollPlanner {
    inner: Arc<Inner>,
}

struct Inner {
    queues: Vec<String>,
    indices: HashMap<String, usize>,
    state: Mutex<State>,
    revisions: watch::Sender<u64>,
}

struct State {
    revision: u64,
    weights: Vec<u32>,
    total_weight: u64,
    selector: Selector,
}

fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}

fn gcd(mut a: u32, mut b: u32) -> u32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

impl PollPlanner {
    pub fn new(plan: PollPlan, strategy: PollStrategy) -> Self {
        let revision = plan.revision;
        let (queues, weights): (Vec<_>, Vec<_>) = plan.routes.into_iter().unzip();
        let indices = queues
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, q)| (q, i))
            .collect();
        let selector = Selector::new(strategy, &weights);
        let total_weight = weights.iter().copied().map(u64::from).sum();
        Self {
            inner: Arc::new(Inner {
                queues,
                indices,
                state: Mutex::new(State {
                    revision,
                    weights,
                    total_weight,
                    selector,
                }),
                revisions: watch::channel(revision).0,
            }),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// Immutable registration order, independent of subsequent input ordering.
    pub fn queues(&self) -> &[String] {
        &self.inner.queues
    }

    pub fn revision(&self) -> u64 {
        self.state().revision
    }

    pub fn is_idle(&self) -> bool {
        self.state().total_weight == 0
    }

    /// Installs a complete replacement for the same registered queues. Replays
    /// of equivalent weights are idempotent. Stale and conflicting revisions
    /// return an error without changing the live plan or consuming service.
    pub fn install(&self, plan: PollPlan) -> Result<()> {
        let indices = &self.inner.indices;
        if plan.routes.len() != indices.len() {
            return Err(invalid(
                "a poll plan cannot change the worker's registered queues",
            ));
        }
        let mut weights = vec![0; indices.len()];
        for (queue, weight) in plan.routes {
            let &index = indices
                .get(&queue)
                .ok_or_else(|| invalid("unknown queue in poll plan"))?;
            weights[index] = weight;
        }
        let mut state = self.state();
        if plan.revision < state.revision {
            return Err(invalid("stale poll plan revision"));
        }
        if plan.revision == state.revision {
            return if weights == state.weights {
                Ok(())
            } else {
                Err(invalid("conflicting poll plan revision"))
            };
        }
        if weights != state.weights {
            state.selector.configure(&weights);
            state.total_weight = weights.iter().copied().map(u64::from).sum();
            state.weights = weights;
        }
        state.revision = plan.revision;
        drop(state);
        // Never hold the selection lock while waking receivers. Concurrent
        // publishers can finish notifying in a different order than their
        // commits; the watch only advances to a newer revision.
        self.inner.revisions.send_if_modified(|notified| {
            if *notified >= plan.revision {
                return false;
            }
            *notified = plan.revision;
            true
        });
        Ok(())
    }

    /// Returns a revision watch. Subscribe before checking state to avoid
    /// missing an install between the idle decision and the wait.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.revisions.subscribe()
    }

    /// Wait without issuing a poll or consuming worker capacity. A subsequent
    /// [`Self::issue`] can still observe a newer idle revision.
    pub async fn wait_active(&self) {
        if !self.is_idle() {
            return;
        }
        let mut revisions = self.subscribe();
        while self.is_idle() {
            revisions
                .changed()
                .await
                .expect("the planner retains its sender");
        }
    }

    /// Select exactly once at issuance; retain the selection for any retry.
    pub fn issue(&self) -> Option<usize> {
        self.issue_with_revision().map(|s| s.route)
    }

    /// Like [`Self::issue`], retaining the matching revision without a second read.
    pub fn issue_with_revision(&self) -> Option<PollSelection> {
        let mut state = self.state();
        let State {
            revision,
            weights,
            total_weight,
            selector,
        } = &mut *state;
        if *total_weight == 0 {
            return None;
        }
        Some(PollSelection {
            route: selector.issue(weights, *total_weight),
            revision: *revision,
        })
    }
}
