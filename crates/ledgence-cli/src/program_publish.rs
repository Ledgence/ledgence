//! Local packaging and HTTP publication are distinct from building and catalog registration.
mod receipt;
use crate::args::invalid;
use ledgence_adapter_artifact::{ArtifactLimits, PackedProgram, pack_directory, publish_directory};
use ledgence_adapter_http::{HttpProgramPublisher, HttpTaskService};
use ledgence_orchestration_api::{
    ContractError, Result,
    console::{ConsoleProgramKind, ProgramDisplayMetadata, RegisterProgram, RegisterProgramReply},
};
use ledgence_worker_api::{
    ProgramDescriptor, PublicationError, PublicationErrorKind, PublishArtifactResult,
};
use receipt::{Phase, Session};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
enum Command {
    Local {
        source: PathBuf,
        store: PathBuf,
    },
    Remote {
        source: PathBuf,
        server: String,
        register: bool,
        kind: Option<ConsoleProgramKind>,
        display_name: Option<String>,
        description: Option<String>,
        update_metadata: bool,
    },
    Resume(PathBuf),
}
pub fn run(arguments: Vec<String>) -> ExitCode {
    let command = match parse(arguments) {
        Ok(c) => c,
        Err(e) => return crate::diagnose(e, None),
    };
    if let Command::Local { source, store } = command {
        return match publish_directory(source, store, &ArtifactLimits::default())
            .map_err(worker_error)
        {
            Ok(descriptor) => output(&descriptor),
            Err(error) => {
                // Preserve the original local publish contract: once parsing
                // succeeds, packaging/storage failures are operation failures.
                let _ = crate::diagnose(error, None);
                ExitCode::FAILURE
            }
        };
    }
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => return crate::diagnose(io_error(e), None),
    };
    let mut logs = match crate::logging::Logs::stderr() {
        Ok(logs) => logs,
        Err(error) => return crate::diagnose(io_error(error), None),
    };
    let telemetry = match crate::telemetry::Telemetry::start("ledgence-cli", logs.sink.clone()) {
        Ok(telemetry) => telemetry,
        Err(error) => {
            let result = crate::diagnose_into(invalid(error), None, Some(&logs.sink));
            runtime.block_on(async {
                let _ = logs.finish().await;
            });
            return result;
        }
    };
    let trace = telemetry.bridge();
    let request_id = Arc::new(Mutex::new(None::<String>));
    let result = runtime.block_on(async {
        let mut session = match command {
            Command::Resume(path) => Session::open(&path)?,
            Command::Remote { source, server, register, kind, display_name, description, update_metadata } => {
                let publisher = publisher(&server, trace.clone(), &request_id)?;
                let capabilities = publisher.capabilities().await.map_err(publication_error)?;
                if !capabilities.enabled { return Err(invalid("server publication is disabled; enable --allow-program-publication with an instance configuration or use --store")); }
                if register && !capabilities.registration_enabled { return Err(invalid("server catalog registration is unavailable")); }
                let source = std::fs::canonicalize(source).map_err(io_error)?;
                let packed = pack_directory(&source, &ArtifactLimits::default()).map_err(worker_error)?;
                let mut metadata = if crate::program_build::receipt_path(&source).exists() {
                    crate::program_build::load_receipt(&source, &packed.descriptor)?.metadata
                } else { ProgramDisplayMetadata::default() };
                if let Some(value) = kind { metadata.kind = value; }
                if display_name.is_some() { metadata.display_name = display_name; }
                if description.is_some() { metadata.description = description; }
                metadata.validate()?;
                let registration = register.then(|| RegisterProgram {
                    program: packed.descriptor.program.clone(), expected_descriptor: Some(packed.descriptor.clone()), metadata, update_metadata,
                });
                Session::create(&source, server, packed, registration)?
            }
            Command::Local { .. } => unreachable!(),
        };
        let _ = logs.sink.json(&json!({"level":"INFO", "message":"Publication receipt saved", "receipt":session.path}));
        let outcome = proceed(&mut session, trace.clone(), &request_id).await;
        let registration = session.receipt.registration.as_ref();
        let recovery = registration.map(|r| registration_arguments(&session.receipt.server, r));
        let report = json!({
            "descriptor": session.receipt.descriptor,
            "request_id": latest_request_id(&request_id),
            "phase": session.receipt.phase,
            "receipt": session.path,
            "publication": session.receipt.publication,
            "registration": session.receipt.registered,
            "resume": ["ledgence", "program", "publish", "--resume", session.path],
            "register_command": recovery,
            "publication_outcome": if session.receipt.phase != Phase::Prepared { "confirmed" }
                else if outcome.as_ref().err().is_some_and(ProceedFailure::uncertain_publication) { "unknown" }
                else { "unconfirmed" },
            "error": outcome.as_ref().err().map(ProceedFailure::structured),
        });
        let printed = output(&report);
        match outcome { Ok(()) => Ok(printed), Err(error) => { let _ = crate::diagnose_into(error.error, latest_request_id(&request_id), Some(&logs.sink)); Ok(ExitCode::FAILURE) } }
    });
    let result = result.unwrap_or_else(|error| {
        crate::diagnose_into(error, latest_request_id(&request_id), Some(&logs.sink))
    });
    runtime.block_on(async {
        telemetry.finish().await;
        let _ = logs.finish().await;
    });
    result
}
struct ProceedFailure {
    error: ContractError,
    publication: Option<PublicationError>,
}
impl From<ContractError> for ProceedFailure {
    fn from(error: ContractError) -> Self {
        Self {
            error,
            publication: None,
        }
    }
}
impl ProceedFailure {
    fn publication(error: PublicationError) -> Self {
        Self {
            error: publication_error(error.clone()),
            publication: Some(error),
        }
    }
    fn structured(&self) -> serde_json::Value {
        match &self.publication {
            Some(error) => serde_json::to_value(error).expect("publication errors serialize"),
            None => serde_json::to_value(&self.error).expect("contract errors serialize"),
        }
    }
    fn uncertain_publication(&self) -> bool {
        self.publication.as_ref().is_some_and(|error| {
            matches!(
                error.kind,
                PublicationErrorKind::OutcomeUnknown | PublicationErrorKind::Storage
            )
        })
    }
}
async fn proceed(
    session: &mut Session,
    trace: Arc<dyn ledgence_worker_api::TraceBridge>,
    request_id: &Arc<Mutex<Option<String>>>,
) -> std::result::Result<(), ProceedFailure> {
    // Verify the retained bytes even for a completed receipt: a recovery bundle
    // with missing or changed payload must never be reported as reproducible.
    let archive = session.archive()?;
    if session.receipt.phase == Phase::Prepared {
        let publisher = publisher(&session.receipt.server, trace.clone(), request_id)?;
        let published = publisher
            .publish(&session.receipt.descriptor, archive)
            .await
            .map_err(ProceedFailure::publication)?;
        session.receipt.publication = Some(published);
        session.receipt.phase = Phase::Published;
        session.save().map_err(|_| ContractError::Unavailable("publication confirmed but receipt update failed; resume the saved receipt to reconcile the exact archive".into()))?;
    }
    if session.receipt.phase == Phase::Published
        && let Some(command) = &session.receipt.registration
    {
        let observed = request_id.clone();
        let client = HttpTaskService::new(&session.receipt.server)?
            .with_trace_bridge(trace)
            .with_observer(move |metadata| {
                *observed
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = metadata.request_id.clone();
            });
        let reply = client.register_program(command).await?;
        if reply.version.descriptor != session.receipt.descriptor.clone().into() {
            return Err(ContractError::Unavailable(
                "registration response did not match the published artifact".into(),
            )
            .into());
        }
        session.receipt.registered = Some(reply);
        session.receipt.phase = Phase::Registered;
        session.save().map_err(|_| ContractError::Unavailable("registration confirmed but receipt update failed; resume the saved receipt to reconcile registration".into()))?;
    }
    Ok(())
}
fn parse(arguments: Vec<String>) -> Result<Command> {
    let mut values = HashMap::new();
    let mut args = arguments.into_iter();
    let mut register = false;
    while let Some(key) = args.next() {
        if key == "--register" {
            if register {
                return Err(invalid("duplicate --register"));
            }
            register = true;
            continue;
        }
        if !matches!(
            key.as_str(),
            "--source"
                | "--store"
                | "--server"
                | "--resume"
                | "--kind"
                | "--display-name"
                | "--description"
                | "--update-metadata"
        ) {
            return Err(invalid("unknown program publish option"));
        }
        let value = args
            .next()
            .filter(|v| !v.starts_with("--"))
            .ok_or_else(|| invalid(format!("missing value for {key}")))?;
        if values.insert(key.clone(), value).is_some() {
            return Err(invalid(format!("duplicate option {key}")));
        }
    }
    if let Some(path) = values.remove("--resume") {
        if register || !values.is_empty() {
            return Err(invalid(
                "--resume uses the recorded destination and registration; do not combine it with other options",
            ));
        }
        return Ok(Command::Resume(path.into()));
    }
    let source = values
        .remove("--source")
        .unwrap_or_else(|| ".ledgence/prepared".into())
        .into();
    let store = values.remove("--store");
    let server = values.remove("--server");
    let (None, Some(server)) = (&store, &server) else {
        if let (Some(store), None) = (store, server) {
            if register || !values.is_empty() {
                return Err(invalid("catalog options require --server and --register"));
            }
            return Ok(Command::Local {
                source,
                store: store.into(),
            });
        }
        return Err(invalid(
            "specify exactly one destination: --store or --server",
        ));
    };
    if !register && !values.is_empty() {
        return Err(invalid("catalog metadata options require --register"));
    }
    let kind = match values.remove("--kind").as_deref() {
        None => None,
        Some("task") => Some(ConsoleProgramKind::Task),
        Some("workflow") => Some(ConsoleProgramKind::Workflow),
        Some("unspecified") => Some(ConsoleProgramKind::Unspecified),
        _ => return Err(invalid("invalid program kind")),
    };
    let update_metadata = match values.remove("--update-metadata").as_deref() {
        None | Some("false") => false,
        Some("true") => true,
        _ => return Err(invalid("update-metadata must be true or false")),
    };
    let display_name = values.remove("--display-name");
    let description = values.remove("--description");
    ProgramDisplayMetadata {
        kind: kind.unwrap_or_default(),
        display_name: display_name.clone(),
        description: description.clone(),
    }
    .validate()?;
    Ok(Command::Remote {
        source,
        server: server.clone(),
        register,
        kind,
        display_name,
        description,
        update_metadata,
    })
}
fn registration_arguments(server: &str, registration: &RegisterProgram) -> Vec<String> {
    let mut args = vec![
        "ledgence".into(),
        "program".into(),
        "register".into(),
        "--server".into(),
        server.into(),
        "--program".into(),
        registration.program.id.clone(),
        "--version".into(),
        registration.program.version.clone(),
    ];
    if let Some(expected) = &registration.expected_descriptor {
        args.extend([
            "--expected-digest".into(),
            expected.digest.0.clone(),
            "--expected-size".into(),
            expected.size.to_string(),
        ]);
    }
    let kind = match registration.metadata.kind {
        ConsoleProgramKind::Task => "task",
        ConsoleProgramKind::Workflow => "workflow",
        ConsoleProgramKind::Unspecified => "unspecified",
    };
    args.extend([
        "--kind".into(),
        kind.into(),
        "--update-metadata".into(),
        registration.update_metadata.to_string(),
    ]);
    if let Some(value) = &registration.metadata.display_name {
        args.extend(["--display-name".into(), value.clone()]);
    }
    if let Some(value) = &registration.metadata.description {
        args.extend(["--description".into(), value.clone()]);
    }
    args
}
fn output(value: &impl Serialize) -> ExitCode {
    match serde_json::to_string(value) {
        Ok(text) => crate::write_stdout(&(text + "\n")),
        Err(_) => crate::diagnose(
            ContractError::Unavailable("cannot encode publication output".into()),
            None,
        ),
    }
}
fn worker_error(error: ledgence_worker_api::Error) -> ContractError {
    use ledgence_worker_api::ErrorKind;
    match error.kind {
        ErrorKind::InvalidInput | ErrorKind::Integrity | ErrorKind::Incompatible => {
            invalid(error.message)
        }
        ErrorKind::NotFound => ContractError::NotFound,
        ErrorKind::Capacity => ContractError::Busy,
        ErrorKind::Unavailable
        | ErrorKind::Cancelled
        | ErrorKind::TimedOut
        | ErrorKind::Runtime
        | ErrorKind::Protocol
        | ErrorKind::Io => ContractError::Unavailable(error.message),
    }
}
fn publication_error(error: PublicationError) -> ContractError {
    match error.kind {
        PublicationErrorKind::InvalidArtifact | PublicationErrorKind::TooLarge => {
            invalid(error.message)
        }
        PublicationErrorKind::ImmutableConflict => ContractError::Conflict,
        PublicationErrorKind::Saturated => ContractError::Busy,
        PublicationErrorKind::Disabled
        | PublicationErrorKind::Storage
        | PublicationErrorKind::OutcomeUnknown => ContractError::Unavailable(error.message),
    }
}
fn io_error(error: std::io::Error) -> ContractError {
    ContractError::Unavailable(format!("publication file operation failed: {error}"))
}

fn publisher(
    server: &str,
    trace: Arc<dyn ledgence_worker_api::TraceBridge>,
    request_id: &Arc<Mutex<Option<String>>>,
) -> Result<HttpProgramPublisher> {
    let observed = request_id.clone();
    Ok(HttpProgramPublisher::new(server)
        .map_err(publication_error)?
        .with_trace_bridge(trace)
        .with_observer(move |metadata| {
            *observed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = metadata.request_id.clone();
        }))
}
fn latest_request_id(request_id: &Arc<Mutex<Option<String>>>) -> Option<String> {
    request_id
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}
