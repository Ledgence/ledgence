//! Local lifecycle is a thin, stateful adapter over a pinned Compose kit. An
//! executable update never replaces an installation's image, schema, or config.

mod docker;
mod kit;

use crate::args::invalid;
use docker::Docker;
use kit::Distribution;
use ledgence_orchestration_api::{ContractError, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    net::{Ipv4Addr, TcpListener},
    path::{Path, PathBuf},
    process::ExitCode,
};

const STATE_FILE: &str = ".ledgence-state.json";
// The connected worker accepts at most 1024 consumers/processes.
const MAX_CONCURRENCY: u32 = 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Action {
    Up,
    Status,
    Logs,
    Down,
}

struct Options {
    action: Action,
    directory: PathBuf,
    distribution: Option<PathBuf>,
    port: Option<u16>,
    concurrency: Option<u32>,
    context: Option<String>,
    follow: bool,
    tail: u32,
    service: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct State {
    format: u32,
    project: String,
    directory: PathBuf,
    distribution: Distribution,
    context: String,
    endpoint: String,
    port: u16,
    concurrency: u32,
}

pub fn run(arguments: Vec<String>) -> ExitCode {
    match Options::parse(arguments).and_then(execute) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => crate::diagnose(error, None),
    }
}

fn operational(message: impl Into<String>) -> ContractError {
    ContractError::Unavailable(message.into())
}

impl Options {
    fn parse(arguments: Vec<String>) -> Result<Self> {
        let mut arguments = arguments.into_iter();
        let action = match arguments.next().as_deref() {
            Some("up") => Action::Up,
            Some("status") => Action::Status,
            Some("logs") => Action::Logs,
            Some("down") => Action::Down,
            _ => return Err(invalid("expected local up, status, logs, or down")),
        };
        let mut values = BTreeMap::new();
        let mut follow = false;
        while let Some(key) = arguments.next() {
            if key == "--follow" && action == Action::Logs {
                if follow {
                    return Err(invalid("duplicate --follow"));
                }
                follow = true;
                continue;
            }
            let allowed = key == "--directory"
                || (action == Action::Up
                    && matches!(
                        key.as_str(),
                        "--distribution" | "--port" | "--concurrency" | "--context"
                    ))
                || (action == Action::Logs && matches!(key.as_str(), "--tail" | "--service"));
            if !allowed {
                return Err(invalid(format!("unknown local option {key}")));
            }
            let value = arguments
                .next()
                .filter(|value| !value.is_empty() && !value.starts_with("--"))
                .ok_or_else(|| invalid(format!("missing value for {key}")))?;
            if values.insert(key.clone(), value).is_some() {
                return Err(invalid(format!("duplicate {key}")));
            }
        }
        let directory = values
            .remove("--directory")
            .map(PathBuf::from)
            .map_or_else(default_directory, Ok)?;
        let port = number(&mut values, "--port", 65535)?.map(|value| value as u16);
        let concurrency = number(&mut values, "--concurrency", MAX_CONCURRENCY)?;
        let tail = number(&mut values, "--tail", 10000)?.unwrap_or(100);
        let service = values.remove("--service");
        if service.as_deref().is_some_and(|value| {
            !matches!(value, "postgres" | "migrate" | "orchestrator" | "worker")
        }) {
            return Err(invalid(
                "--service must be postgres, migrate, orchestrator, or worker",
            ));
        }
        Ok(Self {
            action,
            directory,
            distribution: values.remove("--distribution").map(PathBuf::from),
            port,
            concurrency,
            context: values.remove("--context"),
            follow,
            tail,
            service,
        })
    }
}

fn number(values: &mut BTreeMap<String, String>, key: &str, maximum: u32) -> Result<Option<u32>> {
    values
        .remove(key)
        .map(|value| {
            value
                .parse::<u32>()
                .ok()
                .filter(|value| *value > 0 && *value <= maximum)
                .ok_or_else(|| invalid(format!("{key} must be between 1 and {maximum}")))
        })
        .transpose()
}

fn default_directory() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("XDG_DATA_HOME").filter(|path| !path.is_empty()) {
        let path = PathBuf::from(path);
        if !path.is_absolute() {
            return Err(invalid(
                "XDG_DATA_HOME must be absolute; alternatively use --directory",
            ));
        }
        return Ok(path.join("ledgence/local"));
    }
    let home = std::env::var_os("HOME")
        .filter(|path| !path.is_empty())
        .ok_or_else(|| invalid("cannot find a user data directory; use --directory"))?;
    let home = PathBuf::from(home);
    if !home.is_absolute() {
        return Err(invalid(
            "HOME must be absolute; alternatively use --directory",
        ));
    }
    Ok(home.join(".local/share/ledgence/local"))
}

fn execute(options: Options) -> Result<()> {
    let directory = directory(&options.directory, options.action == Action::Up)?;
    // Keep the lock outside the directory so first initialization is an atomic
    // directory rename. OS locks are released even after interruption/crash.
    let parent = directory
        .parent()
        .ok_or_else(|| invalid("local directory cannot be a filesystem root"))?;
    let lock_name = format!(
        ".ledgence-local-{}.lock",
        &kit::hash(directory.to_string_lossy().as_bytes())[..24]
    );
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(parent.join(lock_name))
        .map_err(|error| operational(format!("cannot open local installation lock: {error}")))?;
    lock.try_lock().map_err(|error| {
        operational(format!(
            "another local command may be using this directory: {error}"
        ))
    })?;
    let (state, docker) = if directory.join(STATE_FILE).exists() {
        let state = read_state(&directory)?;
        match_options(&options, &state)?;
        let docker = Docker::connect(Some(&state.context))?;
        if docker.endpoint != state.endpoint {
            return Err(invalid(
                "the saved Docker context now points to a different endpoint; the local installation was not changed",
            ));
        }
        docker.validate(&state.distribution)?;
        (state, docker)
    } else {
        if options.action != Action::Up {
            return Err(invalid(
                "no local installation found; run ledgence local up first",
            ));
        }
        if directory.exists()
            && fs::read_dir(&directory)
                .map_err(|error| operational(error.to_string()))?
                .next()
                .is_some()
        {
            return Err(invalid(
                "local directory is not empty and has no Ledgence state; choose a new --directory without removing its contents",
            ));
        }
        let source = options.distribution.clone().or_else(|| crate::bundle::root().map(|root| root.join("local")).filter(|path| path.is_dir()))
            .ok_or_else(|| invalid("no bundled local distribution found; install a complete release or pass --distribution DIR"))?;
        let distribution = kit::read(&source)?;
        if distribution.version != env!("CARGO_PKG_VERSION") {
            return Err(invalid(format!(
                "new installation requires a {} distribution; found {}",
                env!("CARGO_PKG_VERSION"),
                distribution.version
            )));
        }
        let docker = Docker::connect(options.context.as_deref())?;
        docker.validate(&distribution)?;
        // A port conflict before any containers exist must not persist an
        // unusable choice: the user can retry this directory with another port.
        let port = options.port.unwrap_or(8080);
        let _available_port = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).map_err(|error| {
            operational(format!("local port {port} is unavailable ({error}); retry local up with another --port. No installation was created"))
        })?;
        let state = initialize(&directory, &source, distribution, &docker, &options)?;
        (state, docker)
    };
    match options.action {
        Action::Up => {
            if let Err(error) = TcpListener::bind((Ipv4Addr::LOCALHOST, state.port))
                && !docker.owns_port(&directory, &state)?
            {
                return Err(operational(format!(
                    "local port {} is unavailable ({error}); stop its owner or use a new --directory with --port. Saved installation and data were preserved",
                    state.port
                )));
            }
            compose_status(
                docker.compose(&directory, &state).args([
                    "up",
                    "--detach",
                    "--no-build",
                    "--wait",
                    "--wait-timeout",
                    "180",
                ]),
                "start",
                &directory,
            )?;
            describe(&state, &directory, &docker.platform, "Local stack started")?;
        }
        Action::Status => {
            compose_status(
                docker.compose(&directory, &state).args(["ps", "--all"]),
                "inspect",
                &directory,
            )?;
            describe(
                &state,
                &directory,
                &docker.platform,
                "Saved local installation (container status above)",
            )?;
        }
        Action::Down => {
            compose_status(
                docker
                    .compose(&directory, &state)
                    .args(["down", "--timeout", "65"]),
                "stop",
                &directory,
            )?;
            writeln!(
                std::io::stdout(),
                "Local stack stopped. Volumes and configuration preserved in {}.",
                directory.display()
            )
            .map_err(|error| operational(format!("stack stopped, but output failed: {error}")))?;
        }
        Action::Logs => {
            // Following logs must not prevent another shell from stopping it.
            drop(lock);
            let mut command = docker.compose(&directory, &state);
            command.args(["logs", "--tail", &options.tail.to_string()]);
            if options.follow {
                command.arg("--follow");
            }
            if let Some(service) = options.service {
                command.arg(service);
            }
            compose_status(&mut command, "read logs for", &directory)?;
        }
    }
    Ok(())
}

fn directory(path: &Path, create: bool) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| operational(error.to_string()))?
            .join(path)
    };
    if absolute.exists() {
        if !absolute.is_dir() {
            return Err(invalid("--directory must be a directory"));
        }
        return absolute
            .canonicalize()
            .map_err(|error| operational(error.to_string()));
    }
    let parent = absolute
        .parent()
        .ok_or_else(|| invalid("invalid local directory"))?;
    if create {
        fs::create_dir_all(parent).map_err(|error| {
            operational(format!("cannot create local parent directory: {error}"))
        })?;
    }
    let parent = parent
        .canonicalize()
        .map_err(|error| invalid(format!("local directory does not exist: {error}")))?;
    let name = absolute
        .file_name()
        .ok_or_else(|| invalid("invalid local directory"))?;
    Ok(parent.join(name))
}

fn initialize(
    directory: &Path,
    source: &Path,
    distribution: Distribution,
    docker: &Docker,
    options: &Options,
) -> Result<State> {
    let parent = directory.parent().expect("validated directory has parent");
    let staged = tempfile::Builder::new()
        .prefix(".ledgence-local-staging-")
        .tempdir_in(parent)
        .map_err(|error| operational(format!("cannot stage local installation: {error}")))?;
    kit::copy(source, staged.path())?;
    if kit::read(staged.path())? != distribution {
        return Err(invalid(
            "distribution changed during initialization; no installation was activated",
        ));
    }
    let identity =
        kit::hash(format!("{}:{}", directory.display(), staged.path().display()).as_bytes());
    let state = State {
        format: 1,
        project: format!("ledgence-{}", &identity[..24]),
        directory: directory.to_owned(),
        distribution,
        context: docker.context.clone(),
        endpoint: docker.endpoint.clone(),
        port: options.port.unwrap_or(8080),
        concurrency: options.concurrency.unwrap_or(1),
    };
    let state_bytes =
        serde_json::to_vec_pretty(&state).map_err(|error| operational(error.to_string()))?;
    for (name, bytes) in [
        (STATE_FILE, state_bytes.as_slice()),
        (
            ".ledgence.env",
            b"# Installation options are persisted in .ledgence-state.json.\n".as_slice(),
        ),
    ] {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(staged.path().join(name))
            .map_err(|error| {
                operational(format!("cannot create local installation state: {error}"))
            })?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| {
                operational(format!("cannot persist local installation state: {error}"))
            })?;
    }
    File::open(staged.path())
        .and_then(|file| file.sync_all())
        .map_err(|error| {
            operational(format!("cannot persist staged local installation: {error}"))
        })?;
    fs::rename(staged.path(), directory)
        .map_err(|error| operational(format!("cannot activate local installation: {error}")))?;
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|error| {
            operational(format!(
                "installation saved, but directory sync failed: {error}; retry the same command"
            ))
        })?;
    Ok(state)
}

fn read_state(directory: &Path) -> Result<State> {
    let bytes = fs::read(directory.join(STATE_FILE))
        .map_err(|error| invalid(format!("cannot read local state: {error}")))?;
    let state: State = serde_json::from_slice(&bytes).map_err(|error| {
        invalid(format!(
            "invalid local state: {error}; existing files were preserved"
        ))
    })?;
    if state.format != 1
        || state.port == 0
        || state.concurrency == 0
        || state.concurrency > MAX_CONCURRENCY
        || state.directory != directory
        || !state.project.starts_with("ledgence-")
        || state.project.len() != 33
        || !state.project[9..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid(
            "invalid or relocated local installation state; configuration was preserved",
        ));
    }
    if kit::read(directory)? != state.distribution {
        return Err(invalid(
            "local kit no longer matches the saved distribution; no upgrade was performed",
        ));
    }
    Ok(state)
}

fn match_options(options: &Options, state: &State) -> Result<()> {
    if options.port.is_some_and(|port| port != state.port)
        || options
            .concurrency
            .is_some_and(|value| value != state.concurrency)
        || options
            .context
            .as_ref()
            .is_some_and(|context| context != &state.context)
    {
        return Err(invalid(
            "installation options differ from saved state; use a new --directory for a separate stack. Existing ports, context and concurrency were preserved",
        ));
    }
    if let Some(source) = &options.distribution
        && kit::read(source)? != state.distribution
    {
        return Err(invalid(
            "--distribution differs from the installed version/image; automatic upgrades are not supported",
        ));
    }
    Ok(())
}

fn compose_status(
    command: &mut std::process::Command,
    action: &str,
    directory: &Path,
) -> Result<()> {
    let status = command
        .status()
        .map_err(|error| operational(format!("cannot {action} local stack: {error}")))?;
    if !status.success() {
        return Err(operational(format!(
            "Docker Compose could not {action} local stack (exit {status}); configuration and volumes were preserved in {}. Inspect ledgence local status/logs with the same --directory before retrying",
            directory.display()
        )));
    }
    Ok(())
}

fn describe(state: &State, directory: &Path, platform: &str, heading: &str) -> Result<()> {
    writeln!(std::io::stdout(), "{heading}\nAPI: http://127.0.0.1:{}\nConsole: http://127.0.0.1:{}/console/\nScope: tenant=acme namespace=demo queue=demo\nVersion: {}\nImage: {}\nDocker context: {}\nProject: {}\nDirectory: {}\nWorker programs: {} / CPython {} (match the engine architecture)\nDistribution guide: {}",
        state.port, state.port, state.distribution.version, state.distribution.image, state.context, state.project,
        directory.display(), platform, state.distribution.python, directory.join("README.md").display())
        .map_err(|error| operational(format!("local command completed, but output failed: {error}")))
}
