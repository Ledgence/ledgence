//! Console projections omit the installation's private scope and storage binding.
use crate::{
    ArtifactManifest, ArtifactPage, ArtifactProducer, ArtifactRef, ExecutionTarget, Timestamp,
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleArtifactManifest {
    pub reference: ArtifactRef,
    pub root: ExecutionTarget,
    pub producer: ArtifactProducer,
    pub key: String,
    pub filename: Option<String>,
    pub created_at: Timestamp,
    pub accepted_at: Timestamp,
    pub first_attempt_id: Option<String>,
}
impl From<ArtifactManifest> for ConsoleArtifactManifest {
    fn from(value: ArtifactManifest) -> Self {
        Self {
            reference: value.reference,
            root: value.root,
            producer: value.producer,
            key: value.key,
            filename: value.filename,
            created_at: value.created_at,
            accepted_at: value.accepted_at,
            first_attempt_id: value.first_attempt_id,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleArtifactPage {
    pub items: Vec<ConsoleArtifactManifest>,
    pub next_cursor: Option<String>,
}
impl From<ArtifactPage> for ConsoleArtifactPage {
    fn from(value: ArtifactPage) -> Self {
        Self {
            items: value.items.into_iter().map(Into::into).collect(),
            next_cursor: value.next_cursor,
        }
    }
}
