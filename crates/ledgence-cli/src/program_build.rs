//! Explicit preparation of operator-trusted Python code for a pinned runtime.
//! The application is never imported. Docker is solely the local build adapter.
mod config;
mod docker;
mod files;

use crate::args::invalid;
use config::{Config, Options};
use docker::{Docker, DockerProcess, Process};
use ledgence_adapter_artifact::{ArtifactLimits, pack_directory};
use ledgence_orchestration_api::{ContractError, Result, console::ProgramDisplayMetadata};
use ledgence_worker_api::{ProgramDescriptor, ProgramManifest};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::ExitCode,
};

fn operational(message: impl Into<String>) -> ContractError {
    ContractError::Unavailable(message.into())
}

// Two hash maps can each contain nearly 4096 portable paths of up to 1024
// bytes, plus SHA-256 values and JSON framing. This bound covers that supported
// package shape while still rejecting unexpectedly large recovery metadata.
const MAX_BUILD_RECEIPT_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BuildReceipt {
    pub schema_version: u32,
    pub prepared_directory: PathBuf,
    pub manifest: ProgramManifest,
    pub metadata: ProgramDisplayMetadata,
    pub descriptor: ProgramDescriptor,
    pub target_image: String,
    pub target_platform: String,
    pub inputs: BTreeMap<String, String>,
    pub prepared_files: BTreeMap<String, String>,
}
#[derive(Serialize)]
struct BuildResult {
    prepared_directory: PathBuf,
    receipt: PathBuf,
    manifest: ProgramManifest,
    metadata: ProgramDisplayMetadata,
    descriptor: ProgramDescriptor,
}
pub(crate) fn receipt_path(source: &Path) -> PathBuf {
    let mut name = source.file_name().unwrap_or_default().to_owned();
    name.push(".build.json");
    source.with_file_name(name)
}
pub(crate) fn load_receipt(source: &Path, descriptor: &ProgramDescriptor) -> Result<BuildReceipt> {
    let receipt: BuildReceipt = serde_json::from_slice(&files::read_regular(
        &receipt_path(source),
        MAX_BUILD_RECEIPT_BYTES,
    )?)
    .map_err(|_| invalid("invalid program build receipt"))?;
    receipt
        .manifest
        .validate()
        .map_err(|e| invalid(e.to_string()))?;
    receipt.metadata.validate()?;
    if receipt.schema_version != 1
        || &receipt.descriptor != descriptor
        || receipt.manifest.program != descriptor.program
        || receipt.prepared_directory
            != source
                .canonicalize()
                .map_err(|_| invalid("prepared directory is unavailable"))?
        || receipt.prepared_files != files::prepared_hashes(source)?
    {
        return Err(invalid(
            "build receipt does not match prepared output; rebuild into a new output",
        ));
    }
    Ok(receipt)
}
pub fn run(arguments: Vec<String>) -> ExitCode {
    let options = match Options::parse(arguments) {
        Ok(value) => value,
        Err(error) => return crate::diagnose(error, None),
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => return crate::diagnose(operational("cannot start program builder"), None),
    };
    match runtime.block_on(execute(options, &DockerProcess)) {
        Ok(result) => match serde_json::to_string(&result) {
            Ok(json) => crate::write_stdout(&format!("{json}\n")),
            Err(_) => crate::diagnose(operational("cannot encode build result"), None),
        },
        Err(error) => crate::diagnose(error, None),
    }
}
async fn execute(options: Options, process: &impl Process) -> Result<BuildResult> {
    let deadline = tokio::time::Instant::now() + options.timeout;
    let mut cancellation = docker::Cancellation::new()?;
    let (config, config_bytes) = Config::read(&options.config)?;
    let config_path = options
        .config
        .canonicalize()
        .map_err(|_| invalid("cannot resolve build configuration"))?;
    let project = config_path
        .parent()
        .ok_or_else(|| invalid("config must have a parent directory"))?;
    // All paths, including an explicit relative --output, are relative to the
    // configuration directory. Host cwd never changes the selected source tree.
    let output = project.join(
        options
            .output
            .unwrap_or_else(|| ".ledgence/prepared".into()),
    );
    let parent = output
        .parent()
        .ok_or_else(|| invalid("build output must have a parent directory"))?;
    let name = output
        .file_name()
        .ok_or_else(|| invalid("build output must have a directory name"))?;
    let plan = files::Plan::create(project, &config, &config_bytes, &output)?;
    fs::create_dir_all(parent).map_err(|_| operational("cannot create build output parent"))?;
    let parent = parent
        .canonicalize()
        .map_err(|_| operational("cannot resolve build output parent"))?;
    let output = parent.join(name);
    let receipt_file = receipt_path(&output);
    let mut lock_name = std::ffi::OsString::from(".");
    lock_name.push(name);
    lock_name.push(".build.lock");
    let mut lock_options = OpenOptions::new();
    lock_options.write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use nix::fcntl::OFlag;
        use std::os::unix::fs::OpenOptionsExt;
        lock_options.custom_flags((OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK).bits());
    }
    let lock = lock_options
        .open(parent.join(lock_name))
        .map_err(|_| operational("cannot acquire build output lock"))?;
    if !lock
        .metadata()
        .map_err(|_| operational("cannot inspect build lock"))?
        .is_file()
    {
        return Err(invalid("build lock must be a regular file"));
    }
    lock.try_lock().map_err(|_| {
        operational("another build owns this output; choose a different --output or wait")
    })?;
    if fs::symlink_metadata(&output).is_ok() || fs::symlink_metadata(&receipt_file).is_ok() {
        return Err(invalid(
            "build output or receipt already exists; choose a new --output (existing builds are never overwritten)",
        ));
    }
    let stage = tempfile::Builder::new()
        .prefix(".ledgence-build-")
        .tempdir_in(&parent)
        .map_err(|_| operational("cannot create build staging"))?;
    let input = stage.path().join("input");
    let control = stage.path().join("control");
    let results = stage.path().join("results");
    for directory in [&input, &control, &results] {
        fs::create_dir(directory).map_err(|_| operational("cannot create build staging"))?;
    }
    plan.stage(&input)?;
    let manifest = config.manifest()?;
    fs::write(
        control.join("prepare.py"),
        include_bytes!("program_build/prepare.py"),
    )
    .map_err(|_| operational("cannot stage preparation adapter"))?;
    fs::write(
        control.join("build.json"),
        serde_json::to_vec(
            &serde_json::json!({"manifest": manifest, "requirements": plan.requirements.is_some()}),
        )
        .map_err(|_| operational("cannot encode preparation plan"))?,
    )
    .map_err(|_| operational("cannot stage preparation plan"))?;
    let selected_context = options.context.or_else(|| {
        std::env::var("DOCKER_CONTEXT")
            .ok()
            .filter(|v| !v.is_empty())
    });
    let connection = tokio::time::timeout_at(
        deadline,
        Docker::connect(
            process,
            selected_context.as_deref(),
            std::env::var_os("DOCKER_HOST").is_some(),
        ),
    );
    let docker = tokio::select! {
        biased;
        _ = cancellation.wait() => return Err(operational("program build cancelled before the builder started")),
        result = connection => result.map_err(|_| operational("program build timed out while connecting to Docker"))??,
    };
    let container = format!(
        "ledgence-build-{}",
        stage
            .path()
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("invalid")
            .trim_start_matches('.')
    );
    docker
        .prepare(
            docker::Preparation {
                input: &input,
                control: &control,
                output: &results,
                image: &config.target.image,
                platform: &config.target.platform,
                timeout: deadline.saturating_duration_since(tokio::time::Instant::now()),
                name: &container,
            },
            &mut cancellation,
        )
        .await?;
    let prepared = results.join("prepared");
    let final_output = output.clone();
    let finalization =
        tokio::task::spawn_blocking(move || finalize(config, plan, results, final_output));
    let (receipt, receipt_stage) =
        await_finalization(finalization, deadline, cancellation.wait()).await?;
    // No visibility before the owned finalizer has completed and both the
    // deadline and retained cancellation subscription have been checked.
    receipt_stage
        .persist_noclobber(&receipt_file)
        .map_err(|_| operational("cannot publish build receipt without replacing existing data"))?;
    if let Err(error) = rename_new(&prepared, &output) {
        // This invocation created the receipt under its held output lock.
        let _ = fs::remove_file(&receipt_file);
        return Err(error);
    }
    files::sync_directory(&parent)?;
    Ok(BuildResult {
        prepared_directory: output,
        receipt: receipt_file,
        manifest: receipt.manifest,
        metadata: receipt.metadata,
        descriptor: receipt.descriptor,
    })
}

async fn await_finalization<T>(
    mut task: tokio::task::JoinHandle<Result<T>>,
    deadline: tokio::time::Instant,
    cancelled: impl std::future::Future<Output = ()>,
) -> Result<T> {
    tokio::pin!(cancelled);
    let result = tokio::select! {
        biased;
        _ = &mut cancelled => {
            // Blocking filesystem work cannot be aborted safely. It only writes
            // staging; retain ownership until it finishes, then discard it.
            let _ = task.await;
            return Err(operational("program build cancelled; prepared output was not published"));
        }
        _ = tokio::time::sleep_until(deadline) => {
            let _ = task.await;
            return Err(operational("program build timed out; prepared output was not published"));
        }
        result = &mut task => result.map_err(|_| operational("program build verification failed"))??,
    };
    // Give the signal driver a turn after a fast blocking result, then check
    // again before committing. A new signal after this decision can race normal
    // completion, but cannot turn an already reported failure into a publication.
    tokio::task::yield_now().await;
    tokio::select! {
        biased;
        _ = &mut cancelled => return Err(operational("program build cancelled; prepared output was not published")),
        _ = std::future::ready(()) => {},
    }
    if tokio::time::Instant::now() >= deadline {
        return Err(operational(
            "program build timed out; prepared output was not published",
        ));
    }
    Ok(result)
}

fn finalize(
    config: Config,
    plan: files::Plan,
    results: PathBuf,
    output: PathBuf,
) -> Result<(BuildReceipt, tempfile::NamedTempFile)> {
    let manifest = config.manifest()?;
    let parent = output
        .parent()
        .ok_or_else(|| invalid("build output has no parent"))?;
    let observed: serde_json::Value =
        serde_json::from_slice(&files::read_regular(&results.join("runtime.json"), 4096)?)
            .map_err(|_| operational("builder did not report its runtime"))?;
    if observed
        != serde_json::json!({"os": manifest.platform.os, "arch": manifest.platform.arch,
        "python": manifest.runtime.python, "implementation": "cpython"})
    {
        return Err(invalid(
            "builder runtime report differs from explicit target",
        ));
    }
    let prepared = results.join("prepared");
    let packed = pack_directory(&prepared, &ArtifactLimits::default())
        .map_err(|e| invalid(e.to_string()))?;
    let actual_manifest: ProgramManifest = serde_json::from_slice(&files::read_regular(
        &prepared.join("ledgence-program.json"),
        65536,
    )?)
    .map_err(|_| invalid("builder emitted an invalid manifest"))?;
    if actual_manifest != manifest {
        return Err(invalid("builder changed the requested program manifest"));
    }
    let prepared_files = files::prepared_hashes(&prepared)?;
    for (name, input) in &plan.files {
        if prepared_files.get(name) != Some(&files::digest(&input.bytes)) {
            return Err(invalid(
                "builder omitted or changed a selected application file",
            ));
        }
    }
    let receipt = BuildReceipt {
        schema_version: 1,
        prepared_directory: output.clone(),
        manifest: manifest.clone(),
        metadata: config.metadata(),
        descriptor: packed.descriptor.clone(),
        target_image: config.target.image.clone(),
        target_platform: config.target.platform.clone(),
        inputs: plan.hashes,
        prepared_files,
    };
    // The persistent receipt intentionally lives outside the hashed package.
    let mut receipt_stage = tempfile::NamedTempFile::new_in(parent)
        .map_err(|_| operational("cannot create build receipt"))?;
    let receipt_bytes = encode_receipt(&receipt)?;
    receipt_stage
        .write_all(&receipt_bytes)
        .and_then(|_| receipt_stage.as_file().sync_all())
        .map_err(|_| operational("cannot synchronize build receipt"))?;
    sync_tree(&prepared)?;
    Ok((receipt, receipt_stage))
}

fn encode_receipt(receipt: &BuildReceipt) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(receipt)
        .map_err(|_| operational("cannot encode build receipt"))?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_BUILD_RECEIPT_BYTES {
        return Err(invalid(
            "build receipt exceeds its supported size limit; output was not published",
        ));
    }
    Ok(bytes)
}
fn sync_tree(directory: &Path) -> Result<()> {
    for entry in
        fs::read_dir(directory).map_err(|_| operational("cannot synchronize prepared tree"))?
    {
        let entry = entry.map_err(|_| operational("cannot synchronize prepared tree"))?;
        if entry
            .file_type()
            .map_err(|_| operational("cannot inspect prepared entry"))?
            .is_dir()
        {
            sync_tree(&entry.path())?;
        } else {
            std::fs::File::open(entry.path())
                .and_then(|f| f.sync_all())
                .map_err(|_| operational("cannot synchronize prepared file"))?;
        }
    }
    files::sync_directory(directory)
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn rename_new(source: &Path, target: &Path) -> Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        source,
        rustix::fs::CWD,
        target,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(|_| operational("cannot publish prepared directory without replacing existing data"))
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn rename_new(_source: &Path, _target: &Path) -> Result<()> {
    Err(invalid("program build is supported on Linux and macOS"))
}

#[cfg(test)]
mod tests;
