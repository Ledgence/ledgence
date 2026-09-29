//! Relationships derived exclusively from one retained explorer record. References
//! are scoped to the selected workflow, including endpoints outside this page.
use super::*;

pub const CONSOLE_EXPLORER_MAX_RELATIONS: usize = WORKFLOW_MAX_COMMANDS + 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConsoleExplorerReference {
    Entrypoint { activation_id: String },
    Child { key: String },
    Fork { key: String },
    Local { activation_id: String, key: String },
    ChildWait { activation_id: String },
    ExternalWait { key: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleExplorerRelationKind {
    Invokes,
    Registers,
    Branch,
    /// All named members must be terminal; failure/cancellation also satisfy it.
    AwaitsTerminal,
    Resumes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleExplorerRelation {
    /// Canonical JSON tuple [workflow_id, kind, source, target]. Treat as opaque.
    pub id: String,
    pub kind: ConsoleExplorerRelationKind,
    pub source: ConsoleExplorerReference,
    pub target: ConsoleExplorerReference,
}

impl ConsoleExplorerNode {
    /// Project only this record's facts; no endpoint lookup or page membership is
    /// required. Duplicate evidence across carriers has the same relation ID.
    pub fn derive_relations(&self, workflow_id: &str) -> Result<Vec<ConsoleExplorerRelation>> {
        use ConsoleExplorerReference as Reference;
        use ConsoleExplorerRelationKind as Kind;

        validate_text(workflow_id, 128)?;
        validate_text(&self.activation_id, 128)?;
        self.data.validate()?;
        let owner = Reference::Entrypoint {
            activation_id: self.activation_id.clone(),
        };
        let mut relations = Vec::new();
        let mut add = |kind: Kind, source: Reference, target: Reference| -> Result<()> {
            let id = serde_json::to_string(&(workflow_id, kind, &source, &target))
                .map_err(|_| invalid("invalid explorer relation identity"))?;
            relations.push(ConsoleExplorerRelation {
                id,
                kind,
                source,
                target,
            });
            Ok(())
        };
        match &self.data {
            ConsoleExplorerData::Entrypoint {
                applied_at: Some(_),
                decision_kind: Some(ConsoleDecisionKind::Continue),
                error: None,
                resumed_activation_id: Some(activation_id),
                ..
            } => add(
                Kind::Resumes,
                owner,
                Reference::Entrypoint {
                    activation_id: activation_id.clone(),
                },
            )?,
            ConsoleExplorerData::Child { key, fork_key, .. } => {
                let child = Reference::Child { key: key.clone() };
                add(Kind::Invokes, owner, child.clone())?;
                if let Some(key) = fork_key {
                    add(Kind::Branch, Reference::Fork { key: key.clone() }, child)?;
                }
            }
            ConsoleExplorerData::Fork {
                key, branch_keys, ..
            } => {
                let fork = Reference::Fork { key: key.clone() };
                add(Kind::Registers, owner, fork.clone())?;
                for key in branch_keys {
                    add(
                        Kind::Branch,
                        fork.clone(),
                        Reference::Child { key: key.clone() },
                    )?;
                }
            }
            ConsoleExplorerData::Local { key, .. } => add(
                Kind::Invokes,
                owner,
                Reference::Local {
                    activation_id: self.activation_id.clone(),
                    key: key.clone(),
                },
            )?,
            ConsoleExplorerData::ChildWait {
                member_keys,
                resumed_activation_id,
                ..
            } => {
                let wait = Reference::ChildWait {
                    activation_id: self.activation_id.clone(),
                };
                add(Kind::Registers, owner, wait.clone())?;
                for key in member_keys {
                    add(
                        Kind::AwaitsTerminal,
                        Reference::Child { key: key.clone() },
                        wait.clone(),
                    )?;
                }
                if let Some(activation_id) = resumed_activation_id {
                    add(
                        Kind::Resumes,
                        wait,
                        Reference::Entrypoint {
                            activation_id: activation_id.clone(),
                        },
                    )?;
                }
            }
            ConsoleExplorerData::ExternalWait {
                key,
                wake_reason,
                resumed_activation_id,
                ..
            } => {
                let wait = Reference::ExternalWait { key: key.clone() };
                add(Kind::Registers, owner, wait.clone())?;
                if wake_reason.is_some()
                    && let Some(activation_id) = resumed_activation_id
                {
                    add(
                        Kind::Resumes,
                        wait,
                        Reference::Entrypoint {
                            activation_id: activation_id.clone(),
                        },
                    )?;
                }
            }
            ConsoleExplorerData::Entrypoint { .. } => {}
        }
        Ok(relations)
    }
}
