//! Local project scaffolding. Creating source never executes user code or
//! selects a worker target implicitly.
mod templates;

use crate::args::invalid;
use ledgence_orchestration_api::{ContractError, Result};
use ledgence_worker_api::ProgramRef;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Task,
    Workflow,
}
impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Workflow => "workflow",
        }
    }
}

#[derive(Debug)]
struct Target {
    platform: String,
    python: String,
    image: String,
}

#[derive(Debug)]
struct Options {
    directory: PathBuf,
    program: ProgramRef,
    kind: Kind,
    target: Option<Target>,
}
impl Options {
    fn parse(arguments: Vec<String>) -> Result<Self> {
        let mut arguments = arguments.into_iter();
        let directory = arguments
            .next()
            .filter(|value| !value.is_empty() && !value.starts_with('-'))
            .ok_or_else(|| {
                invalid("program new requires PATH before its options; use program new --help")
            })?;
        // Reject explicit dot destinations instead of silently normalizing them
        // into a different name. Parent traversal remains valid in the parent.
        if matches!(
            directory.trim_end_matches('/').rsplit('/').next(),
            Some("." | "..")
        ) {
            return Err(invalid("program new PATH must name a new directory"));
        }
        let directory = PathBuf::from(directory);
        let basename = directory
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| invalid("program new PATH must name a new directory"))?;
        let mut values = BTreeMap::new();
        while let Some(key) = arguments.next() {
            if !["--name", "--kind", "--platform", "--python", "--image"].contains(&key.as_str()) {
                return Err(invalid(
                    "unknown program new option; use program new --help",
                ));
            }
            let value = arguments
                .next()
                .filter(|value| !value.is_empty() && !value.starts_with('-'))
                .ok_or_else(|| invalid(format!("missing value for {key}")))?;
            if values.insert(key, value).is_some() {
                return Err(invalid("duplicate program new option"));
            }
        }
        let program = ProgramRef {
            id: values.remove("--name").unwrap_or_else(|| basename.into()),
            version: "0.1.0".into(),
        };
        program
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        if !program.id.as_bytes()[0].is_ascii_alphanumeric()
            || !program.id.as_bytes()[program.id.len() - 1].is_ascii_alphanumeric()
        {
            return Err(invalid(
                "program name must begin and end with a lowercase letter or digit to be a Python project name",
            ));
        }
        let kind = match values.remove("--kind").as_deref().unwrap_or("task") {
            "task" => Kind::Task,
            "workflow" => Kind::Workflow,
            _ => return Err(invalid("program new kind must be task or workflow")),
        };
        let target = match (
            values.remove("--platform"),
            values.remove("--python"),
            values.remove("--image"),
        ) {
            (None, None, None) => None,
            (Some(platform), Some(python), Some(image)) => Some(Target {
                platform,
                python,
                image,
            }),
            _ => {
                return Err(invalid(
                    "provide --platform, --python and --image together, or omit all three and configure the target later",
                ));
            }
        };
        Ok(Self {
            directory,
            program,
            kind,
            target,
        })
    }
}

#[derive(Serialize)]
struct Created {
    directory: PathBuf,
    program: ProgramRef,
    kind: Kind,
    files: Vec<&'static str>,
    target_configured: bool,
}

pub fn run(arguments: Vec<String>) -> ExitCode {
    match Options::parse(arguments).and_then(create) {
        Ok(created) => match serde_json::to_string(&created) {
            Ok(json) => crate::write_stdout(&format!("{json}\n")),
            Err(_) => crate::diagnose(
                operational("program created but cannot encode its result"),
                None,
            ),
        },
        Err(error) => crate::diagnose(error, None),
    }
}

fn create(options: Options) -> Result<Created> {
    let files = templates::render(&options)?;
    if options.target.is_some() {
        // Share the actual builder's parser and contracts so scaffolding cannot
        // advertise a target that the next command would reject.
        crate::program_build::validate_configuration(&files["ledgence.toml"])?;
    }
    let parent = options
        .directory
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = parent
        .canonicalize()
        .map_err(|_| invalid("program new parent directory must already exist"))?;
    if !parent.is_dir() {
        return Err(invalid("program new parent must be a directory"));
    }
    let directory = parent.join(
        options
            .directory
            .file_name()
            .ok_or_else(|| invalid("program new PATH must name a new directory"))?,
    );
    if directory.to_str().is_none() {
        return Err(invalid(
            "program new destination must resolve to a UTF-8 path",
        ));
    }
    match fs::symlink_metadata(&directory) {
        Ok(_) => {
            return Err(invalid(
                "program new destination already exists; choose a new directory",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(operational("cannot inspect program new destination")),
    }
    let staging = tempfile::Builder::new()
        .prefix(".ledgence-new-")
        .tempdir_in(&parent)
        .map_err(|_| operational("cannot create program staging directory"))?;
    // Move only a child of the owned temporary directory, leaving its cleanup
    // guard attached to the same directory throughout publication.
    let project = staging.path().join("project");
    fs::create_dir(&project).map_err(|_| operational("cannot create staged program"))?;
    fs::create_dir(project.join("tests")).map_err(|_| operational("cannot create staged tests"))?;
    for (name, contents) in &files {
        fs::write(project.join(name), contents)
            .map_err(|_| operational("cannot write staged program files"))?;
    }
    publish_new(&project, &directory)?;
    Ok(Created {
        directory,
        program: options.program,
        kind: options.kind,
        files: files.into_keys().collect(),
        target_configured: options.target.is_some(),
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn publish_new(source: &Path, target: &Path) -> Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        source,
        rustix::fs::CWD,
        target,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(|_| operational("cannot create program directory without replacing existing data"))
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn publish_new(_source: &Path, _target: &Path) -> Result<()> {
    Err(invalid("program new is supported on Linux and macOS"))
}
fn operational(message: impl Into<String>) -> ContractError {
    ContractError::Unavailable(message.into())
}

#[cfg(test)]
mod tests;
