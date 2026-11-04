//! Weighted admission is local to a single resource and governs offers, not compute time.
use super::*;
use crate::queue_policies::{decimal, optional_decimal};
use serde::Deserializer;
use std::collections::BTreeSet;

pub const RESOURCE_FAIR_GROUP_LIMIT: usize = 16;
pub const RESOURCE_FAIR_QUEUE_LIMIT: usize = 64;
pub const RESOURCE_FAIR_WEIGHT_MAX: u32 = 1000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceFairGroup {
    pub name: String,
    pub weight: u32,
    pub queues: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceFairSharing {
    pub groups: Vec<ResourceFairGroup>,
    pub default_group: String,
}
pub(super) fn present_policy<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<ResourceFairSharing>, D::Error> {
    ResourceFairSharing::deserialize(deserializer).map(Some)
}
impl ResourceFairSharing {
    pub fn validate(&self) -> Result<()> {
        if !(1..=RESOURCE_FAIR_GROUP_LIMIT).contains(&self.groups.len()) {
            return Err(invalid("resource fair sharing requires 1..16 groups"));
        }
        let mut names = BTreeSet::new();
        let mut queues = BTreeSet::new();
        for group in &self.groups {
            name(&group.name)?;
            if !names.insert(group.name.as_str())
                || !(1..=RESOURCE_FAIR_WEIGHT_MAX).contains(&group.weight)
            {
                return Err(invalid(
                    "resource fair group must be unique with weight 1..1000",
                ));
            }
            for queue in &group.queues {
                validate_text(queue, 128)?;
                if !queues.insert(queue) || queues.len() > RESOURCE_FAIR_QUEUE_LIMIT {
                    return Err(invalid(
                        "resource fair queues must be unique with at most 64 assignments",
                    ));
                }
            }
        }
        if !names.contains(self.default_group.as_str()) {
            return Err(invalid(
                "resource fair sharing default_group must name a configured group",
            ));
        }
        // Leave space in the 16 KiB resource record for its scope, policy and revision.
        bound(self, 12 * 1024)
    }
    pub fn group_for(&self, queue: &str) -> Option<&ResourceFairGroup> {
        self.groups
            .iter()
            .find(|g| g.queues.iter().any(|q| q == queue))
            .or_else(|| self.groups.iter().find(|g| g.name == self.default_group))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceDecisionReason {
    Weighted,
    SoleEligible,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceDecisionSubject {
    Task {
        task_id: String,
        generation: u32,
    },
    Workflow {
        workflow_id: String,
        origin_activation_id: String,
        key: String,
    },
    Provider {
        workflow_id: String,
        origin_activation_id: String,
        key: String,
    },
}
impl ResourceDecisionSubject {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Task {
                task_id,
                generation,
            } => {
                validate_text(task_id, 128)?;
                if !(1..=MAX_TASK_ATTEMPT_GENERATION).contains(generation) {
                    return Err(invalid("invalid task generation in resource decision"));
                }
            }
            Self::Workflow {
                workflow_id,
                origin_activation_id,
                key,
            }
            | Self::Provider {
                workflow_id,
                origin_activation_id,
                key,
            } => {
                for id in [workflow_id, origin_activation_id, key] {
                    validate_text(id, 128)?;
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceOfferDecision {
    pub scope: Scope,
    pub resource: String,
    #[serde(with = "decimal")]
    pub sequence: u64,
    #[serde(with = "decimal")]
    pub policy_revision: u64,
    pub group: String,
    pub weight: u32,
    pub eligible_groups: u32,
    pub cycle_round: u32,
    pub reason: ResourceDecisionReason,
    pub queue: String,
    pub subject: ResourceDecisionSubject,
    #[serde(with = "decimal")]
    pub offer_generation: u64,
    pub offered_at: Timestamp,
}
impl ResourceOfferDecision {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        name(&self.resource)?;
        name(&self.group)?;
        validate_text(&self.queue, 128)?;
        self.subject.validate()?;
        time(self.offered_at)?;
        if self.sequence == 0
            || self.policy_revision == 0
            || self.offer_generation == 0
            || !(1..=RESOURCE_FAIR_WEIGHT_MAX).contains(&self.weight)
            || !(1..=RESOURCE_FAIR_GROUP_LIMIT as u32).contains(&self.eligible_groups)
            || !(1..=self.weight).contains(&self.cycle_round)
            || (self.reason == ResourceDecisionReason::SoleEligible) != (self.eligible_groups == 1)
        {
            return Err(invalid("inconsistent weighted resource offer decision"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDecisionQuery {
    pub scope: Scope,
    pub name: String,
    #[serde(with = "optional_decimal")]
    pub before: Option<u64>,
    pub limit: u32,
}
impl ResourceDecisionQuery {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        name(&self.name)?;
        if self.before == Some(0) || !(1..=100).contains(&self.limit) {
            return Err(invalid("invalid resource decision cursor or page size"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDecisionPage {
    pub items: Vec<ResourceOfferDecision>,
    #[serde(with = "optional_decimal")]
    pub next: Option<u64>,
}
impl ResourceDecisionPage {
    pub fn validate(&self) -> Result<()> {
        if self.items.len() > 100 {
            return Err(invalid("resource decision page exceeds bound"));
        }
        let mut previous = None;
        for item in &self.items {
            item.validate()?;
            if previous.is_some_and(|seq| seq <= item.sequence) {
                return Err(invalid(
                    "resource decisions must descend by unique sequence",
                ));
            }
            previous = Some(item.sequence);
        }
        if self.next.is_some() && self.next != previous {
            return Err(invalid(
                "resource decision cursor disagrees with last sequence",
            ));
        }
        bound(self, ADMISSION_PAGE_MAX_BYTES)
    }
    pub fn validate_for(&self, query: &ResourceDecisionQuery) -> Result<()> {
        self.validate()?;
        query.validate()?;
        if self.items.len() > query.limit as usize
            || self.items.iter().any(|i| {
                i.scope != query.scope
                    || i.resource != query.name
                    || query.before.is_some_and(|seq| i.sequence >= seq)
            })
        {
            return Err(invalid("resource decision page disagrees with query"));
        }
        Ok(())
    }
}
