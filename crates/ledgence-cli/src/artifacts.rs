//! Scoped execution-artifact administration with explicit uncertainty recovery.
mod files;
#[cfg(test)]
mod tests;
use crate::{args::invalid, credentials};
use ledgence_orchestration_api::*;
use serde_json::json;
use std::{
    collections::HashMap,
    io::Write,
    path::PathBuf,
    process::ExitCode,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug)]
struct Options {
    server: String,
    scope: Scope,
    operation: Operation,
    timeout: Duration,
}
#[derive(Debug)]
enum Operation {
    Put {
        target: ExecutionTarget,
        key: String,
        file: PathBuf,
        media_type: String,
    },
    Inspect {
        id: String,
    },
    List {
        target: ExecutionTarget,
        cursor: Option<String>,
        limit: u32,
    },
    Download {
        id: String,
        file: PathBuf,
    },
    Status {
        upload: String,
    },
    Reconcile {
        selection: ArtifactSelection,
    },
    Replace {
        command: ArtifactReplaceCommand,
    },
}
impl Operation {
    fn mutating(&self) -> bool {
        matches!(
            self,
            Self::Put { .. } | Self::Reconcile { .. } | Self::Replace { .. }
        )
    }
}
fn parse(arguments: Vec<String>) -> Result<Options> {
    let mut args = arguments.into_iter();
    let operation = args
        .next()
        .ok_or_else(|| invalid("missing artifact command"))?;
    let mut values = HashMap::new();
    while let Some(key) = args.next() {
        if !matches!(
            key.as_str(),
            "--server"
                | "--tenant"
                | "--namespace"
                | "--task"
                | "--workflow"
                | "--key"
                | "--file"
                | "--media-type"
                | "--artifact"
                | "--upload"
                | "--revision"
                | "--expected-revision"
                | "--cursor"
                | "--limit"
                | "--timeout-ms"
        ) {
            return Err(invalid(format!("unknown artifact option {key}")));
        }
        let value = args
            .next()
            .filter(|v| !v.is_empty() && !v.starts_with("--"))
            .ok_or_else(|| invalid(format!("missing {key} value")))?;
        if values.insert(key.clone(), value).is_some() {
            return Err(invalid(format!("duplicate {key}")));
        }
    }
    fn required(values: &mut HashMap<String, String>, key: &str) -> Result<String> {
        values
            .remove(key)
            .ok_or_else(|| invalid(format!("missing {key}")))
    }
    fn target(values: &mut HashMap<String, String>) -> Result<ExecutionTarget> {
        let target = match (values.remove("--task"), values.remove("--workflow")) {
            (Some(id), None) => ExecutionTarget::Task { id },
            (None, Some(id)) => ExecutionTarget::Workflow { id },
            _ => return Err(invalid("choose exactly one of --task or --workflow")),
        };
        target.validate()?;
        Ok(target)
    }
    let server = required(&mut values, "--server")?;
    let scope = Scope {
        tenant_id: required(&mut values, "--tenant")?,
        namespace: required(&mut values, "--namespace")?,
    };
    scope.validate()?;
    let timeout = values
        .remove("--timeout-ms")
        .map(|v| number(&v, "--timeout-ms"))
        .transpose()?
        .unwrap_or(ARTIFACT_TRANSFER_TIMEOUT_MS);
    if timeout == 0 || timeout > ARTIFACT_TRANSFER_TIMEOUT_MS {
        return Err(invalid("artifact timeout must be between 1 and 120000 ms"));
    }
    let operation = match operation.as_str() {
        "put" => {
            let target = target(&mut values)?;
            let key = required(&mut values, "--key")?;
            validate_text(&key, 128)?;
            Operation::Put {
                target,
                key,
                file: required(&mut values, "--file")?.into(),
                media_type: values
                    .remove("--media-type")
                    .unwrap_or_else(|| "application/octet-stream".into()),
            }
        }
        "inspect" => Operation::Inspect {
            id: required(&mut values, "--artifact")?,
        },
        "list" => Operation::List {
            target: target(&mut values)?,
            cursor: values.remove("--cursor"),
            limit: u32::try_from(
                values
                    .remove("--limit")
                    .map(|v| number(&v, "--limit"))
                    .transpose()?
                    .unwrap_or(100),
            )
            .map_err(|_| invalid("invalid artifact page limit"))?,
        },
        "download" => Operation::Download {
            id: required(&mut values, "--artifact")?,
            file: required(&mut values, "--file")?.into(),
        },
        "status" => Operation::Status {
            upload: required(&mut values, "--upload")?,
        },
        "reconcile" => Operation::Reconcile {
            selection: ArtifactSelection {
                artifact_id: required(&mut values, "--artifact")?,
                upload_id: required(&mut values, "--upload")?,
                revision: number(&required(&mut values, "--revision")?, "--revision")?,
            },
        },
        "replace" => Operation::Replace {
            command: ArtifactReplaceCommand {
                upload_id: required(&mut values, "--upload")?,
                expected_selection_revision: number(
                    &required(&mut values, "--expected-revision")?,
                    "--expected-revision",
                )?,
            },
        },
        _ => {
            return Err(invalid(
                "unknown artifacts command; use ledgence artifacts --help",
            ));
        }
    };
    if !values.is_empty() {
        return Err(invalid("artifact option does not apply to this command"));
    }
    match &operation {
        Operation::Inspect { id } | Operation::Download { id, .. } => validate_text(id, 128)?,
        Operation::Status { upload } => validate_text(upload, 128)?,
        Operation::Reconcile { selection } => selection.validate()?,
        Operation::Replace { command } => command.validate()?,
        Operation::List {
            target,
            cursor,
            limit,
        } => ArtifactListQuery {
            scope: scope.clone(),
            target: target.clone(),
            cursor: cursor.clone(),
            limit: *limit,
        }
        .validate()?,
        Operation::Put { media_type, .. } => ArtifactMetadata {
            digest: ledgence_worker_api::Digest(format!("sha256:{}", "0".repeat(64))),
            size: 0,
            media_type: media_type.clone(),
            filename: None,
        }
        .validate()?,
    }
    Ok(Options {
        server,
        scope,
        operation,
        timeout: Duration::from_millis(timeout),
    })
}
fn number(value: &str, key: &str) -> Result<u64> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid(format!("{key} must be an unsigned integer")));
    }
    value
        .parse()
        .map_err(|_| invalid(format!("{key} exceeds supported range")))
}
fn unavailable(message: impl Into<String>) -> ContractError {
    ContractError::Unavailable(message.into())
}

fn recovery_required(status: &ArtifactUploadStatus) -> ArtifactClientError {
    let message = "artifact upload requires explicit status/reconcile/replace using artifact_upload; no file was resent".to_string();
    if matches!(
        status.stage,
        ArtifactUploadStage::Writing | ArtifactUploadStage::Uncertain
    ) {
        ArtifactClientError::OutcomeUnknown { message }
    } else {
        ArtifactClientError::Rejected(unavailable(message))
    }
}

pub fn run(mut arguments: Vec<String>) -> ExitCode {
    let prepared = (|| {
        let credentials = credentials::take(&mut arguments)?;
        let options = parse(arguments)?;
        let client = credentials::task_client(&options.server, credentials.as_ref())?.artifacts();
        Ok::<_, ContractError>((options, client))
    })();
    let (options, client) = match prepared {
        Ok(value) => value,
        Err(error) => return diagnostic(error, None, None, false),
    };
    let mutating = options.operation.mutating();
    let request_id = Arc::new(Mutex::new(None));
    let observed = request_id.clone();
    let client = client.with_observer(move |metadata| {
        *observed.lock().unwrap_or_else(|e| e.into_inner()) = metadata.request_id.clone();
    });
    let selection = Arc::new(Mutex::new(None));
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(v) => v,
        Err(e) => return diagnostic(unavailable(e.to_string()), None, None, false),
    };
    let result = runtime.block_on(execute(&client, options, selection.clone()));
    // Runtime drop waits for non-abortable local file IO before this command exits.
    drop(runtime);
    let request_id = request_id.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let selection = selection.lock().unwrap_or_else(|e| e.into_inner()).clone();
    match result {
        Ok(value) => {
            let output = serde_json::to_vec(&value).expect("JSON value");
            let mut out = std::io::stdout().lock();
            if out
                .write_all(&output)
                .and_then(|()| out.write_all(b"\n"))
                .is_err()
            {
                return diagnostic(
                    unavailable("artifact operation completed but output could not be written"),
                    request_id,
                    selection,
                    mutating,
                );
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            let unknown = mutating && matches!(error, ArtifactClientError::OutcomeUnknown { .. });
            diagnostic(error.into(), request_id, selection, unknown)
        }
    }
}
fn diagnostic(
    error: ContractError,
    request_id: Option<String>,
    selection: Option<ArtifactSelection>,
    unknown: bool,
) -> ExitCode {
    let status = if matches!(error, ContractError::InvalidInput(_)) {
        2
    } else {
        1
    };
    let _ = crate::write_diagnostic(
        &json!({"error":error,"request_id":request_id,"outcome_may_be_unknown":unknown,"artifact_upload":selection}),
    );
    ExitCode::from(status)
}
async fn execute<C: ExecutionArtifactClient + ArtifactMetadataReader>(
    client: &C,
    options: Options,
    saved: Arc<Mutex<Option<ArtifactSelection>>>,
) -> ArtifactClientResult<serde_json::Value> {
    let deadline = Instant::now() + options.timeout;
    let requester = ArtifactRequester::External {
        scope: options.scope.clone(),
    };
    let value = match options.operation {
        Operation::Put {
            target,
            key,
            file,
            media_type,
        } => {
            let (reader, metadata) = files::prepare(file, media_type, deadline).await?;
            let command = ArtifactPrepareCommand {
                scope: options.scope,
                target,
                key,
                metadata: metadata.clone(),
            };
            command.validate()?;
            let reply = client.prepare(&requester, &command, deadline).await?;
            reply
                .upload
                .validate()
                .map_err(|_| ArtifactClientError::InvalidResponse {
                    message: "invalid artifact preparation status".into(),
                })?;
            *saved.lock().unwrap_or_else(|e| e.into_inner()) = Some(reply.upload.selection.clone());
            if let Some(reference) = files::accepted(&reply.upload, &metadata)? {
                json!(reference)
            } else {
                if !reply.upload.selected
                    || reply.upload.stage != ArtifactUploadStage::Prepared
                    || reply.upload.retry != ArtifactRetryGuidance::Upload
                {
                    return Err(recovery_required(&reply.upload));
                }
                let selection = reply.upload.selection;
                let reply = client
                    .put(requester, selection.clone(), reader, deadline)
                    .await?;
                if reply.upload.selection != selection {
                    return Err(ArtifactClientError::InvalidResponse {
                        message: "upload receipt changed selection".into(),
                    });
                }
                json!(
                    files::accepted(&reply.upload, &metadata)?
                        .ok_or_else(|| recovery_required(&reply.upload))?
                )
            }
        }
        Operation::Inspect { id } => json!(
            client
                .inspect(
                    &ArtifactInspectQuery {
                        scope: options.scope,
                        artifact_id: id
                    },
                    deadline
                )
                .await?
        ),
        Operation::List {
            target,
            cursor,
            limit,
        } => json!(
            client
                .list(
                    &ArtifactListQuery {
                        scope: options.scope,
                        target,
                        cursor,
                        limit
                    },
                    deadline
                )
                .await?
        ),
        Operation::Download { id, file } => {
            let manifest = client
                .inspect(
                    &ArtifactInspectQuery {
                        scope: options.scope,
                        artifact_id: id,
                    },
                    deadline,
                )
                .await?;
            let body = client
                .download(requester, manifest.reference.clone(), deadline)
                .await?;
            files::download(body, &manifest.reference, file.clone(), deadline).await?;
            json!({"reference":manifest.reference,"file":file})
        }
        Operation::Status { upload } => {
            json!(client.upload_status(&requester, &upload, deadline).await?)
        }
        Operation::Reconcile { selection } => {
            *saved.lock().unwrap_or_else(|e| e.into_inner()) = Some(selection.clone());
            json!(client.reconcile(&requester, &selection, deadline).await?)
        }
        Operation::Replace { command } => {
            json!(client.replace(&requester, &command, deadline).await?)
        }
    };
    Ok(value)
}
