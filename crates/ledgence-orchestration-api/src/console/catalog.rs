use super::*;
use ledgence_worker_api::ProgramManifest;

/// Maximum compact JSON bytes for all accepted display metadata. The longest
/// kind and object framing occupy 57 bytes; each accepted byte in the 128-byte
/// name and 4096-byte description needs at most two JSON bytes when escaped.
pub const PROGRAM_DISPLAY_METADATA_MAX_BYTES: usize = 57 + 2 * (128 + 4096);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleProgramKind {
    Task,
    Workflow,
    #[default]
    Unspecified,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramDisplayMetadata {
    pub display_name: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub kind: ConsoleProgramKind,
}
impl ProgramDisplayMetadata {
    pub fn validate(&self) -> Result<()> {
        if let Some(name) = &self.display_name {
            validate_text(name, 128)?;
        }
        if let Some(description) = &self.description
            && (description.len() > 4096
                || description
                    .chars()
                    .any(|c| c.is_control() && c != '\n' && c != '\t'))
        {
            return Err(invalid("invalid program description"));
        }

        crate::submission::check_encoded_size(
            self,
            PROGRAM_DISPLAY_METADATA_MAX_BYTES,
            "program display metadata",
        )
        .map_err(Into::into)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterProgram {
    pub program: ProgramRef,
    #[serde(default)]
    pub metadata: ProgramDisplayMetadata,
    /// Explicitly update descriptive metadata only after immutable bytes match.
    #[serde(default)]
    pub update_metadata: bool,
}
impl RegisterProgram {
    pub fn validate(&self) -> Result<()> {
        self.program.validate()?;
        self.metadata.validate()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleProgramSummary {
    pub program_id: String,
    pub metadata: ProgramDisplayMetadata,
    pub registered_versions: ConsoleU64,
    pub last_registered_at: Timestamp,
}
/// New catalog contract: membership is aggregated from registered versions,
/// independently of the last-edited program display metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleProgramCatalogEntry {
    pub program: ConsoleProgramSummary,
    pub kinds: Vec<ConsoleProgramKind>,
}
impl ConsoleRecord for ConsoleProgramCatalogEntry {
    fn position(&self) -> ConsolePosition {
        self.program.position()
    }
    fn validate(&self) -> Result<()> {
        self.program.validate()?;
        if self.kinds.is_empty()
            || self.kinds.len() > 3
            || self.kinds.windows(2).any(|v| v[0] >= v[1])
        {
            return Err(invalid("invalid registered program kind membership"));
        }
        Ok(())
    }
}
impl ConsoleRecord for ConsoleProgramSummary {
    fn position(&self) -> ConsolePosition {
        vec![ConsoleKey::Text(self.program_id.clone())]
    }
    fn validate(&self) -> Result<()> {
        ProgramRef {
            id: self.program_id.clone(),
            version: "validation".into(),
        }
        .validate()?;
        self.metadata.validate()?;
        timestamp(self.last_registered_at)?;
        if self.registered_versions.0 == 0 {
            return Err(invalid("empty registered program"));
        }
        Ok(())
    }
}
/// Provenance describes the verification route, never a private store path/URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgramRegistrationProvenance {
    ConfiguredProgramStore,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleProgramVersion {
    pub descriptor: ConsoleProgramDescriptor,
    pub manifest: ProgramManifest,
    pub metadata: ProgramDisplayMetadata,
    pub registered_at: Timestamp,
    pub provenance: ProgramRegistrationProvenance,
}
impl ConsoleRecord for ConsoleProgramVersion {
    fn position(&self) -> ConsolePosition {
        vec![
            ConsoleKey::Number(ConsoleU64(self.registered_at)),
            ConsoleKey::Text(self.descriptor.program.version.clone()),
        ]
    }
    fn validate(&self) -> Result<()> {
        self.descriptor.validate()?;
        self.manifest.validate()?;
        self.metadata.validate()?;
        timestamp(self.registered_at)?;
        if self.descriptor.program != self.manifest.program {
            return Err(invalid("catalog descriptor/manifest mismatch"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleProgramDetail {
    pub version: ConsoleProgramVersion,
    pub observed_at: Timestamp,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterProgramReply {
    pub version: ConsoleProgramVersion,
    pub already_registered: bool,
    pub metadata_updated: bool,
}
#[derive(Debug, Clone)]
pub enum ProgramCatalogQuery {
    Catalog {
        kind: Option<ConsoleProgramKind>,
        page: ConsolePagination,
    },
    Programs(ConsolePagination),
    Versions {
        program_id: String,
        page: ConsolePagination,
    },
    Inspect(ProgramRef),
}
impl ProgramCatalogQuery {
    pub fn binding(&self, scope: &Scope) -> Result<ConsoleCursorBinding> {
        scope.validate()?;
        let (endpoint, parent, descending, numeric_keys) = match self {
            Self::Catalog { .. } => ("programs/catalog", vec![], false, vec![false]),
            Self::Programs(_) => ("programs", vec![], false, vec![false]),
            Self::Versions { program_id, .. } => {
                ProgramRef {
                    id: program_id.clone(),
                    version: "validation".into(),
                }
                .validate()?;
                (
                    "programs/versions",
                    vec![program_id.clone()],
                    true,
                    vec![true, false],
                )
            }
            Self::Inspect(program) => {
                program.validate()?;
                (
                    "programs/inspect",
                    vec![program.id.clone(), program.version.clone()],
                    false,
                    vec![],
                )
            }
        };
        Ok(ConsoleCursorBinding {
            endpoint,
            scope: scope.clone(),
            parent,
            filters: match self {
                Self::Catalog { kind, .. } => {
                    serde_json::to_value(kind).map_err(|_| invalid("invalid catalog kind"))?
                }
                _ => serde_json::Value::Null,
            },
            descending,
            numeric_keys,
        })
    }
    pub fn validate(&self, scope: &Scope) -> Result<Option<ConsolePosition>> {
        let binding = self.binding(scope)?;
        match self {
            Self::Catalog { page, .. } | Self::Programs(page) | Self::Versions { page, .. } => {
                page.validate(&binding)
            }
            Self::Inspect(_) => Ok(None),
        }
    }
}
#[derive(Debug, Clone)]
pub enum ProgramCatalogReply {
    Catalog(ConsolePage<ConsoleProgramCatalogEntry>),
    Programs(ConsolePage<ConsoleProgramSummary>),
    Versions(ConsolePage<ConsoleProgramVersion>),
    Inspect(Box<ConsoleProgramDetail>),
}
impl ProgramCatalogReply {
    pub fn validate(&self, scope: &Scope, query: &ProgramCatalogQuery) -> Result<()> {
        query.validate(scope)?;
        let binding = query.binding(scope)?;
        let mismatch = || inconsistent("inconsistent catalog observation");
        match (self, query) {
            (Self::Catalog(reply), ProgramCatalogQuery::Catalog { kind, page }) => {
                reply.validate(page, &binding)?;
                if kind
                    .is_some_and(|kind| reply.items.iter().any(|item| !item.kinds.contains(&kind)))
                {
                    return Err(mismatch());
                }
                Ok(())
            }
            (Self::Programs(reply), ProgramCatalogQuery::Programs(page)) => {
                reply.validate(page, &binding)
            }
            (Self::Versions(reply), ProgramCatalogQuery::Versions { program_id, page }) => {
                reply.validate(page, &binding)?;
                if reply
                    .items
                    .iter()
                    .any(|v| &v.descriptor.program.id != program_id)
                {
                    return Err(mismatch());
                }
                Ok(())
            }
            (Self::Inspect(reply), ProgramCatalogQuery::Inspect(program)) => {
                reply.version.validate().map_err(|_| mismatch())?;
                timestamp(reply.observed_at).map_err(|_| mismatch())?;
                metadata_size(reply).map_err(|_| mismatch())?;
                if &reply.version.descriptor.program != program {
                    return Err(mismatch());
                }
                Ok(())
            }
            _ => Err(mismatch()),
        }
    }
}
/// Immutable reference registration is atomic and idempotent. Descriptive changes
/// require update_metadata; neither this operation nor reads execute any program.
pub trait ProgramCatalogStore: Send + Sync {
    fn register_program<'a>(
        &'a self,
        scope: &'a Scope,
        command: &'a RegisterProgram,
        descriptor: &'a ProgramDescriptor,
        manifest: &'a ProgramManifest,
    ) -> ContractFuture<'a, RegisterProgramReply>;
    fn query_programs<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a ProgramCatalogQuery,
    ) -> ContractFuture<'a, ProgramCatalogReply>;
}
pub trait ProgramCatalogService: Send + Sync {
    fn register_program<'a>(
        &'a self,
        command: &'a RegisterProgram,
    ) -> ContractFuture<'a, RegisterProgramReply>;
    fn query_programs<'a>(
        &'a self,
        query: &'a ProgramCatalogQuery,
    ) -> ContractFuture<'a, ProgramCatalogReply>;
}
/// Concrete archive adapters verify all compressed members and return only a
/// validated manifest. Host compatibility is metadata, not a registration gate.
pub trait ProgramPackageVerifier: Send + Sync {
    fn verify_package<'a>(
        &'a self,
        descriptor: &'a ProgramDescriptor,
        bytes: Vec<u8>,
    ) -> ContractFuture<'a, ProgramManifest>;
}
