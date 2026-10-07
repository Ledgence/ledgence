use super::{files, invalid};
use ledgence_orchestration_api::{
    Result,
    console::{ConsoleProgramKind, ProgramDisplayMetadata},
};
use ledgence_worker_api::{Platform, ProgramManifest, ProgramRef, PythonRuntime};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug)]
pub(super) struct Options {
    pub config: PathBuf,
    pub output: Option<PathBuf>,
    pub context: Option<String>,
    pub timeout: Duration,
}
impl Options {
    pub fn parse(arguments: Vec<String>) -> Result<Self> {
        let mut values = BTreeMap::new();
        let mut arguments = arguments.into_iter();
        while let Some(key) = arguments.next() {
            if !["--config", "--output", "--context", "--timeout-seconds"].contains(&key.as_str()) {
                return Err(invalid(
                    "unknown program build option; use program build --help",
                ));
            }
            let value = arguments
                .next()
                .filter(|v| !v.is_empty() && !v.starts_with("--"))
                .ok_or_else(|| invalid(format!("missing value for {key}")))?;
            if values.insert(key, value).is_some() {
                return Err(invalid("duplicate program build option"));
            }
        }
        let seconds = values
            .remove("--timeout-seconds")
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| invalid("timeout-seconds must be an integer in 1..=3600"))?
            .unwrap_or(600);
        if !(1..=3600).contains(&seconds) {
            return Err(invalid("timeout-seconds must be in 1..=3600"));
        }
        Ok(Self {
            config: values
                .remove("--config")
                .unwrap_or_else(|| "ledgence.toml".into())
                .into(),
            output: values.remove("--output").map(Into::into),
            context: values.remove("--context"),
            timeout: Duration::from_secs(seconds),
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    schema_version: u32,
    pub program: Program,
    pub source: Source,
    #[serde(default)]
    pub files: Vec<AdditionalFile>,
    pub dependencies: Option<Dependencies>,
    pub target: Target,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Program {
    pub id: String,
    pub version: String,
    pub handler: String,
    pub kind: Option<ConsoleProgramKind>,
    pub display_name: Option<String>,
    pub description: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Source {
    pub root: PathBuf,
    pub include: Vec<PathBuf>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdditionalFile {
    pub source: PathBuf,
    pub destination: PathBuf,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Dependencies {
    pub requirements: PathBuf,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Target {
    pub platform: String,
    pub python: String,
    pub protocol: u32,
    pub image: String,
}
impl Config {
    pub fn read(path: &Path) -> Result<(Self, Vec<u8>)> {
        let bytes = files::read_regular(path, 65536)?;
        let value: Self = toml::from_str(
            std::str::from_utf8(&bytes).map_err(|_| invalid("build config must be UTF-8"))?,
        )
        .map_err(|_| {
            invalid("invalid ledgence.toml; check its schema, field names and value types")
        })?;
        value.validate()?;
        Ok((value, bytes))
    }
    pub fn manifest(&self) -> Result<ProgramManifest> {
        let arch = match self.target.platform.as_str() {
            "linux/amd64" => "x86_64",
            "linux/arm64" => "aarch64",
            _ => {
                return Err(invalid(
                    "build target platform must be linux/amd64 or linux/arm64",
                ));
            }
        };
        let manifest = ProgramManifest {
            schema_version: 1,
            program: ProgramRef {
                id: self.program.id.clone(),
                version: self.program.version.clone(),
            },
            runtime: PythonRuntime {
                kind: "python".into(),
                python: self.target.python.clone(),
                protocol: self.target.protocol,
            },
            handler: self.program.handler.clone(),
            platform: Platform {
                os: "linux".into(),
                arch: arch.into(),
            },
        };
        manifest.validate().map_err(|e| invalid(e.to_string()))?;
        Ok(manifest)
    }
    pub fn metadata(&self) -> ProgramDisplayMetadata {
        ProgramDisplayMetadata {
            kind: self.program.kind.unwrap_or(ConsoleProgramKind::Unspecified),
            display_name: self.program.display_name.clone(),
            description: self.program.description.clone(),
        }
    }
    fn validate(&self) -> Result<()> {
        if self.schema_version != 1 {
            return Err(invalid(
                "unsupported build config schema_version; expected 1",
            ));
        }
        self.manifest()?;
        self.metadata().validate()?;
        let Some((name, digest)) = self.target.image.rsplit_once("@sha256:") else {
            return Err(invalid(
                "target.image must be the worker runtime image pinned with @sha256:<64 lowercase hex digits>",
            ));
        };
        if name.is_empty()
            || name.starts_with('-')
            || name.contains('@')
            || name.contains("://")
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-/:".contains(&b))
            || digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid("invalid digest-pinned target.image"));
        }
        if self.source.include.is_empty() {
            return Err(invalid(
                "source.include must explicitly list at least one application path",
            ));
        }
        files::relative(&self.source.root, true)?;
        for include in &self.source.include {
            files::relative(include, false)?;
        }
        for file in &self.files {
            files::relative(&file.source, false)?;
            files::relative(&file.destination, false)?;
        }
        if let Some(dependencies) = &self.dependencies {
            files::relative(&dependencies.requirements, false)?;
        }
        Ok(())
    }
}
