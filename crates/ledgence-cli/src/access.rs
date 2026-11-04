//! Offline credential provisioning. Each command publishes a new private bundle.
use crate::args::invalid;
use ledgence_adapter_access::{AccessConfig, CredentialSpec, wall_clock_ms};
use ledgence_adapter_http::HttpCredentials;
use ledgence_orchestration_api::{
    AccessCapability, ContractError, PrincipalClass, Result, SelfHostedInstanceConfig,
    decode_unique_json,
};
use serde_json::json;
use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::ExitCode,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub fn run(arguments: Vec<String>) -> ExitCode {
    match create(arguments) {
        Ok(report) => match serde_json::to_vec(&report) {
            Ok(mut bytes) => {
                bytes.push(b'\n');
                if std::io::stdout().write_all(&bytes).is_ok() {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                }
            }
            Err(_) => crate::diagnose(
                unavailable("cannot encode credential provisioning report"),
                None,
            ),
        },
        Err(error) => crate::diagnose(error, None),
    }
}
fn create(arguments: Vec<String>) -> Result<serde_json::Value> {
    let mut arguments = arguments.into_iter();
    let operation = arguments
        .next()
        .ok_or_else(|| invalid("missing access command"))?;
    let mut options = HashMap::new();
    while let Some(key) = arguments.next() {
        let value = arguments
            .next()
            .filter(|v| !v.is_empty() && !v.starts_with("--"))
            .ok_or_else(|| invalid("access options require a value"))?;
        if !key.starts_with("--") || options.insert(key, value).is_some() {
            return Err(invalid("invalid or duplicate access option"));
        }
    }
    let server = required(&mut options, "--server")?;
    HttpCredentials::new(&server, "validate-destination")?;
    let directory = PathBuf::from(required(&mut options, "--directory")?);
    let subject = required(&mut options, "--subject")?;
    let expires_at_ms = expiry(&required(&mut options, "--expires-at")?)?;
    let now_ms = wall_clock_ms()?;
    let mut files = Vec::new();
    let mut config;
    let spec;
    match operation.as_str() {
        "init" => {
            let path = required(&mut options, "--instance-config")?;
            let instance = load_instance(Path::new(&path))?;
            config = AccessConfig::new(&instance.context())?;
            spec = CredentialSpec {
                subject,
                class: PrincipalClass::Operator,
                expires_at_ms,
                capabilities: vec![
                    AccessCapability::MetadataRead,
                    AccessCapability::SensitiveRead,
                    AccessCapability::ExecutionSubmit,
                    AccessCapability::ExecutionControl,
                    AccessCapability::ProgramPublish,
                    AccessCapability::ApprovalDecide,
                    AccessCapability::EffectResolve,
                    AccessCapability::ResourceControl,
                ],
            };
            let worker = config.issue(
                CredentialSpec {
                    subject: "worker".into(),
                    class: PrincipalClass::Worker,
                    expires_at_ms,
                    capabilities: vec![AccessCapability::WorkerExecute],
                },
                now_ms,
            )?;
            files.push((
                "worker.json",
                json!({"server":server,"token":worker.expose_secret()}),
            ));
        }
        "issue" => {
            config = AccessConfig::load(Path::new(&required(&mut options, "--config")?))?;
            let class: PrincipalClass =
                serde_json::from_value(json!(required(&mut options, "--class")?))
                    .map_err(|_| invalid("class must be operator, service, or worker"))?;
            let capabilities = required(&mut options, "--capabilities")?
                .split(',')
                .map(|value| {
                    serde_json::from_value(json!(value))
                        .map_err(|_| invalid("invalid capability name"))
                })
                .collect::<Result<Vec<AccessCapability>>>()?;
            spec = CredentialSpec {
                subject,
                class,
                expires_at_ms,
                capabilities,
            };
        }
        _ => return Err(invalid("expected access init or issue")),
    }
    if !options.is_empty() {
        return Err(invalid("unknown access option"));
    }
    let credential = config.issue(spec, now_ms)?;
    let filename = if operation == "init" {
        "operator.json"
    } else {
        "credential.json"
    };
    files.push((
        filename,
        json!({"server":server,"token":credential.expose_secret()}),
    ));
    files.push((
        "access.json",
        serde_json::to_value(&config)
            .map_err(|_| unavailable("cannot encode access configuration"))?,
    ));
    let names: Vec<_> = files.iter().map(|(name, _)| *name).collect();
    publish_bundle(&directory, &files)?;
    Ok(
        json!({"directory":directory,"files":names,"authority_id":config.authority_id,
        "credential_id":credential.credential_id(),"expires_at_ms":expires_at_ms,
        "activation":"Start every orchestrator with the generated access.json after draining the previous API processes."}),
    )
}
fn required(options: &mut HashMap<String, String>, key: &str) -> Result<String> {
    options
        .remove(key)
        .ok_or_else(|| invalid(format!("missing {key}")))
}
fn expiry(value: &str) -> Result<u64> {
    let timestamp = OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| invalid("expires-at must be an RFC3339 timestamp"))?;
    u64::try_from(timestamp.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| invalid("expires-at must be after the Unix epoch"))
}
fn load_instance(path: &Path) -> Result<SelfHostedInstanceConfig> {
    let bytes = crate::submission::read_bytes(path, 64 * 1024, "instance configuration")?;
    let instance: SelfHostedInstanceConfig = decode_unique_json(&bytes, 64 * 1024)
        .map_err(|_| invalid("invalid instance configuration JSON"))?;
    instance.validate()?;
    Ok(instance)
}
fn publish_bundle(directory: &Path, files: &[(&str, serde_json::Value)]) -> Result<()> {
    let parent = directory
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent =
        fs::canonicalize(parent).map_err(|_| invalid("credential bundle parent must exist"))?;
    let name = directory
        .file_name()
        .ok_or_else(|| invalid("directory must name a new credential bundle"))?;
    let destination = parent.join(name);
    let staging = tempfile::Builder::new()
        .prefix(".ledgence-access-")
        .tempdir_in(&parent)
        .map_err(|_| unavailable("cannot create private credential staging directory"))?;
    let bundle = staging.path().join("bundle");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&bundle)
            .map_err(|_| unavailable("cannot create credential bundle"))?;
    }
    #[cfg(not(unix))]
    return Err(invalid(
        "access provisioning is supported on Linux and macOS",
    ));
    for (name, value) in files {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(bundle.join(name))
            .map_err(|_| unavailable("cannot create credential bundle file"))?;
        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|_| unavailable("cannot encode credential bundle"))?;
        file.write_all(&bytes)
            .and_then(|_| file.write_all(b"\n"))
            .and_then(|_| file.sync_all())
            .map_err(|_| unavailable("cannot persist credential bundle file"))?;
    }
    fs::File::open(&bundle)
        .and_then(|f| f.sync_all())
        .map_err(|_| unavailable("cannot sync credential bundle"))?;
    crate::program_new::publish_new(&bundle, &destination).map_err(|_| {
        unavailable("cannot publish credential bundle without replacing existing data")
    })?;
    fs::File::open(&parent)
        .and_then(|f| f.sync_all())
        .map_err(|_| {
            unavailable("credential bundle created but parent sync failed; inspect the destination")
        })?;
    Ok(())
}
fn unavailable(message: &str) -> ContractError {
    ContractError::Unavailable(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_adapter_access::OpaqueCredentialVerifier;
    use ledgence_orchestration_api::CredentialVerifier;
    #[cfg(unix)]
    #[test]
    fn instance_configuration_rejects_fifo_and_symlink_without_blocking() {
        use nix::{sys::stat::Mode, unistd::mkfifo};
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let fifo = root.path().join("instance.fifo");
        mkfifo(&fifo, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let started = std::time::Instant::now();
        assert!(load_instance(&fifo).is_err());
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        let file = root.path().join("instance.json");
        fs::write(&file, br#"{"instance_id":"test","name":"Test"}"#).unwrap();
        let link = root.path().join("instance-link.json");
        symlink(&file, &link).unwrap();
        assert!(load_instance(&link).is_err());
        assert!(load_instance(&file).is_ok());
    }
    #[test]
    fn init_and_issue_produce_private_verifiable_bundles_without_secret_reports() {
        let root = tempfile::tempdir().unwrap();
        let instance = root.path().join("instance.json");
        fs::write(&instance, br#"{"instance_id":"test","name":"Test"}"#).unwrap();
        let initial = root.path().join("initial");
        let args = vec![
            "init",
            "--instance-config",
            instance.to_str().unwrap(),
            "--server",
            "http://127.0.0.1:8080",
            "--directory",
            initial.to_str().unwrap(),
            "--subject",
            "alice",
            "--expires-at",
            "2099-01-01T00:00:00Z",
        ];
        let report = create(args.iter().map(|x| (*x).to_owned()).collect()).unwrap();
        let config = AccessConfig::load(&initial.join("access.json")).unwrap();
        let verifier = OpaqueCredentialVerifier::new(config.clone()).unwrap();
        for filename in ["worker.json", "operator.json"] {
            let value: serde_json::Value =
                serde_json::from_slice(&fs::read(initial.join(filename)).unwrap()).unwrap();
            let token = value["token"].as_str().unwrap();
            assert!(!report.to_string().contains(token));
            assert!(
                !fs::read_to_string(initial.join("access.json"))
                    .unwrap()
                    .contains(token)
            );
            verifier.verify(token, wall_clock_ms().unwrap()).unwrap();
            HttpCredentials::from_file(&initial.join(filename)).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(initial.join(filename))
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600
                );
            }
        }
        assert!(create(args.iter().map(|x| (*x).to_owned()).collect()).is_err());
        let issued = root.path().join("issued");
        let path = initial.join("access.json");
        create(
            [
                "issue",
                "--config",
                path.to_str().unwrap(),
                "--server",
                "http://127.0.0.1:8080",
                "--directory",
                issued.to_str().unwrap(),
                "--subject",
                "alice",
                "--class",
                "operator",
                "--capabilities",
                "metadata_read,sensitive_read,approval_decide",
                "--expires-at",
                "2099-01-01T00:00:00Z",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        )
        .unwrap();
        assert_eq!(AccessConfig::load(&path).unwrap().credentials.len(), 2);
        let replacement = AccessConfig::load(&issued.join("access.json")).unwrap();
        assert_eq!(replacement.credentials.len(), 3);
        assert_eq!(replacement.authority_id, config.authority_id);
        assert!(expiry("not-a-time").is_err());
    }
}
