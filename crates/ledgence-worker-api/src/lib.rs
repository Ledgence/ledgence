//! Portable, vendor-independent worker contracts.
//!
//! This first milestone models preparation and reusable execution. It does not
//! implement distributed attempt leases or durable orchestration settlement.

mod execution;
pub use execution::{
    ExecutionContext, ExecutionFailure, ExecutionReport, ExecutionRequest, ExecutionResult, Phase,
};
mod invocation;
mod json;
pub use json::decode_json;
mod wire;
pub use invocation::InvocationIdentity;
pub use wire::{MAX_WIRE_VALUE_DEPTH, validate_wire_value};

use iri_string::types::{UriAbsoluteStr, UriReferenceStr};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    fmt,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// An adapter operation. Shared clients may be cloned/borrowed by their owner.
pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    InvalidInput,
    NotFound,
    Integrity,
    Incompatible,
    Unavailable,
    Cancelled,
    TimedOut,
    Runtime,
    Protocol,
    Io,
    Capacity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}
impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::new(ErrorKind::Io, value.to_string())
    }
}

/// Names are filesystem-safe, lowercase, immutable release identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramRef {
    pub id: String,
    pub version: String,
}
impl ProgramRef {
    pub fn validate(&self) -> Result<()> {
        for (field, value) in [("program id", &self.id), ("program version", &self.version)] {
            if value.is_empty()
                || value.len() > 128
                || value == "."
                || value == ".."
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
            {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("invalid {field}"),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Digest(pub String);
impl Digest {
    pub fn validate(&self) -> Result<()> {
        if self.0.len() != 71
            || !self.0.starts_with("sha256:")
            || !self.0[7..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "expected sha256: followed by 64 lowercase hexadecimal digits",
            ));
        }
        Ok(())
    }
    pub fn hex(&self) -> &str {
        self.0.strip_prefix("sha256:").unwrap_or(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramDescriptor {
    pub program: ProgramRef,
    pub digest: Digest,
    pub size: u64,
}
impl ProgramDescriptor {
    pub fn validate(&self) -> Result<()> {
        self.program.validate()?;
        self.digest.validate()?;
        if self.size == 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "empty program archive"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PythonRuntime {
    pub kind: String,
    pub python: String,
    pub protocol: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Platform {
    pub os: String,
    pub arch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramManifest {
    pub schema_version: u32,
    pub program: ProgramRef,
    pub runtime: PythonRuntime,
    pub handler: String,
    pub platform: Platform,
}
impl ProgramManifest {
    pub fn validate(&self) -> Result<()> {
        self.program.validate()?;
        if self.schema_version != 1 || self.runtime.kind != "python" || self.runtime.protocol != 1 {
            return Err(Error::new(
                ErrorKind::Incompatible,
                "unsupported manifest, runtime, or protocol version",
            ));
        }
        let Some(minor) = self
            .runtime
            .python
            .strip_prefix("3.")
            .and_then(|s| s.parse::<u32>().ok())
        else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "python must be an exact 3.minor version",
            ));
        };
        if minor < 11 || self.runtime.python != format!("3.{minor}") {
            return Err(Error::new(
                ErrorKind::Incompatible,
                "CPython 3.11 or newer is required",
            ));
        }
        let valid_identifier = |part: &str| {
            !part.is_empty()
                && part.bytes().enumerate().all(|(i, b)| {
                    b.is_ascii_alphabetic() || b == b'_' || (i > 0 && b.is_ascii_digit())
                })
        };
        let Some((module, function)) = self.handler.split_once(':') else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "handler must be module:function",
            ));
        };
        if !module.split('.').all(valid_identifier) || !valid_identifier(function) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "invalid Python handler",
            ));
        }
        if !["linux", "macos"].contains(&self.platform.os.as_str())
            || !["x86_64", "aarch64"].contains(&self.platform.arch.as_str())
        {
            return Err(Error::new(
                ErrorKind::Incompatible,
                "initial platforms are Linux/macOS on x86_64/aarch64",
            ));
        }
        Ok(())
    }
    pub fn validate_host(&self) -> Result<()> {
        self.validate()?;
        if self.platform.os != std::env::consts::OS || self.platform.arch != std::env::consts::ARCH
        {
            return Err(Error::new(
                ErrorKind::Incompatible,
                "package platform differs from this worker",
            ));
        }
        Ok(())
    }
}

/// Owns the original logical JSON event without rewriting user-owned `data`.
///
/// This invocation profile uses CloudEvents 1.0.2 context names/types, requires
/// JSON data and Ledgence execution identifiers, and accepts W3C traceparent
/// version 00. Optional tracestate requires traceparent and is limited to 512
/// ASCII bytes and 32 members. Invalid metadata is rejected, never repaired.
/// Optional context attributes must be omitted rather than encoded as null.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct CloudEvent(Value);
impl CloudEvent {
    pub fn new(value: Value) -> Result<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "CloudEvent must be an object"))?;
        for (name, field) in object {
            if name == "data" {
                continue;
            }
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
            {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("invalid CloudEvent context name {name}"),
                ));
            }
            match field {
                Value::String(text) if valid_context_string(text) => {}
                Value::Bool(_) => {}
                Value::Number(number)
                    if number
                        .as_i64()
                        .and_then(|n| i32::try_from(n).ok())
                        .is_some() => {}
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        format!("invalid CloudEvent context value for {name}"),
                    ));
                }
            }
        }
        if object.get("specversion").and_then(Value::as_str) != Some("1.0") {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "CloudEvent specversion must be 1.0",
            ));
        }
        for key in [
            "id",
            "source",
            "type",
            "ldgtenantid",
            "ldgnamespace",
            "ldgrunid",
            "ldgtaskid",
            "ldgattemptid",
        ] {
            if object
                .get(key)
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("missing nonempty CloudEvent {key}"),
                ));
            }
        }
        if object
            .get("ldgattemptno")
            .and_then(Value::as_i64)
            .is_none_or(|n| !(1..=i64::from(i32::MAX)).contains(&n))
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "ldgattemptno must be a positive signed 32-bit integer",
            ));
        }
        let source = context_string(object, "source")?.expect("required source checked");
        UriReferenceStr::new(source)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "source must be a URI-reference"))?;
        if let Some(schema) = context_string(object, "dataschema")? {
            UriAbsoluteStr::new(schema).map_err(|_| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "dataschema must be an absolute URI without a fragment",
                )
            })?;
        }
        if context_string(object, "subject")?.is_some_and(str::is_empty) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "subject must be nonempty when present",
            ));
        }
        if let Some(timestamp) = context_string(object, "time")? {
            OffsetDateTime::parse(timestamp, &Rfc3339).map_err(|_| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "time must be an RFC 3339 timestamp",
                )
            })?;
        }
        if !object.contains_key("data") || object.contains_key("data_base64") {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "this profile requires user-owned JSON data",
            ));
        }
        if object.get("datacontenttype").and_then(Value::as_str) != Some("application/json") {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "datacontenttype must be application/json",
            ));
        }
        if let Some(trace) = object.get("traceparent") {
            let Some(trace) = trace.as_str() else {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "traceparent must be a string",
                ));
            };
            validate_traceparent(trace)?;
        }
        if let Some(state) = context_string(object, "tracestate")? {
            if !object.contains_key("traceparent") {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "tracestate requires traceparent in this invocation profile",
                ));
            }
            validate_tracestate(state)?;
        }
        Ok(Self(value))
    }
    pub fn value(&self) -> &Value {
        &self.0
    }
    pub fn into_value(self) -> Value {
        self.0
    }
    pub fn id(&self) -> &str {
        self.string("id")
    }
    pub fn attempt_id(&self) -> &str {
        self.string("ldgattemptid")
    }
    pub fn task_id(&self) -> &str {
        self.string("ldgtaskid")
    }
    pub fn tenant_id(&self) -> &str {
        self.string("ldgtenantid")
    }
    pub fn namespace(&self) -> &str {
        self.string("ldgnamespace")
    }
    pub fn traceparent(&self) -> Option<&str> {
        self.0.get("traceparent").and_then(Value::as_str)
    }
    fn string(&self, key: &str) -> &str {
        self.0[key].as_str().expect("validated CloudEvent string")
    }
}
impl<'de> Deserialize<'de> for CloudEvent {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct EventVisitor;
        impl<'de> serde::de::Visitor<'de> for EventVisitor {
            type Value = CloudEvent;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a CloudEvent object with unique context attributes")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut access: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut object = Map::new();
                while let Some((key, value)) = access.next_entry::<String, Value>()? {
                    if object.insert(key.clone(), value).is_some() {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate CloudEvent field {key}"
                        )));
                    }
                }
                CloudEvent::new(Value::Object(object)).map_err(serde::de::Error::custom)
            }
        }
        deserializer.deserialize_map(EventVisitor)
    }
}

fn valid_context_string(value: &str) -> bool {
    value.chars().all(|character| {
        let code = u32::from(character);
        !character.is_control() && !(0xfdd0..=0xfdef).contains(&code) && (code & 0xfffe) != 0xfffe
    })
}

fn context_string<'a>(object: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>> {
    object
        .get(key)
        .map(|value| {
            value.as_str().ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, format!("{key} must be a string"))
            })
        })
        .transpose()
}

fn validate_tracestate(state: &str) -> Result<()> {
    let invalid = || Error::new(ErrorKind::InvalidInput, "invalid or oversized tracestate");
    if state.len() > 512 || !state.is_ascii() {
        return Err(invalid());
    }
    let mut keys = std::collections::HashSet::new();
    for (index, member) in state.split(',').enumerate() {
        if index >= 32 {
            return Err(invalid());
        }
        // CloudEvents String already excludes tabs/control characters. Spaces
        // surrounding W3C list members are allowed without changing the event.
        let member = member.trim_matches(' ');
        if member.is_empty() {
            continue;
        }
        let Some((key, value)) = member.split_once('=') else {
            return Err(invalid());
        };
        if !valid_tracestate_key(key)
            || !keys.insert(key)
            || value.is_empty()
            || value.len() > 256
            || !value
                .bytes()
                .all(|b| (0x20..=0x7e).contains(&b) && b != b',' && b != b'=')
        {
            return Err(invalid());
        }
    }
    Ok(())
}

fn valid_tracestate_key(key: &str) -> bool {
    let allowed = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_-*/".contains(&b);
    let starts_lower = |part: &str| part.bytes().next().is_some_and(|b| b.is_ascii_lowercase());
    if let Some((tenant, system)) = key.split_once('@') {
        !tenant.is_empty()
            && tenant.len() <= 241
            && tenant
                .bytes()
                .next()
                .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
            && tenant.bytes().all(allowed)
            && !system.is_empty()
            && system.len() <= 14
            && starts_lower(system)
            && system.bytes().all(allowed)
    } else {
        !key.is_empty() && key.len() <= 256 && starts_lower(key) && key.bytes().all(allowed)
    }
}

fn validate_traceparent(trace: &str) -> Result<()> {
    // The first protocol version intentionally supports the W3C version-00 shape.
    let segments: Vec<_> = trace.split('-').collect();
    if segments.len() != 4
        || segments[0] != "00"
        || segments[1].len() != 32
        || segments[2].len() != 16
        || segments[3].len() != 2
        || !segments.iter().all(|s| {
            s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        || segments[1].bytes().all(|b| b == b'0')
        || segments[2].bytes().all(|b| b == b'0')
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "invalid or unsupported traceparent",
        ));
    }
    Ok(())
}

/// Cancellation and a monotonic deadline; contains no runtime-specific types.
#[derive(Debug, Clone)]
pub struct RunControl {
    cancelled: Arc<AtomicBool>,
    deadline: Instant,
}
impl RunControl {
    /// Constructs a deadline. An unrepresentable duration fails closed as an
    /// immediately expired control; use `try_new` to report invalid input.
    pub fn new(timeout: Duration) -> Self {
        let now = Instant::now();
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            deadline: now.checked_add(timeout).unwrap_or(now),
        }
    }
    /// Constructs a deadline, rejecting durations that overflow the host clock.
    pub fn try_new(timeout: Duration) -> Result<Self> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "timeout exceeds the host clock range",
            )
        })?;
        Ok(Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            deadline,
        })
    }
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Error::new(ErrorKind::Cancelled, "invocation cancelled"))
        } else if Instant::now() >= self.deadline {
            Err(Error::new(
                ErrorKind::TimedOut,
                "invocation deadline expired",
            ))
        } else {
            Ok(())
        }
    }
}

pub trait ArtifactLease: Send + Sync {}
impl<T: Send + Sync> ArtifactLease for T {}

/// Clones retain an opaque cache lease until the last runtime/consumer releases it.
#[derive(Clone)]
pub struct PreparedArtifact {
    root: PathBuf,
    manifest: ProgramManifest,
    digest: Digest,
    lease: Arc<dyn ArtifactLease>,
}
impl PreparedArtifact {
    pub fn new(
        root: PathBuf,
        manifest: ProgramManifest,
        digest: Digest,
        lease: Arc<dyn ArtifactLease>,
    ) -> Self {
        Self {
            root,
            manifest,
            digest,
            lease,
        }
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn manifest(&self) -> &ProgramManifest {
        &self.manifest
    }
    pub fn digest(&self) -> &Digest {
        &self.digest
    }
}
impl fmt::Debug for PreparedArtifact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedArtifact")
            .field("root", &self.root)
            .field("manifest", &self.manifest)
            .field("digest", &self.digest)
            .field("pin_count", &Arc::strong_count(&self.lease))
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProgramOutcome {
    Success { output: Value },
    Failure { kind: String, message: String },
}

/// Store futures must retain their underlying I/O until completion. Dropping a
/// future is not proof that blocking or remote work stopped. The worker retains
/// an admitted fetch after its response deadline until that future completes.
/// Adapters must report completion only after their owned work has finished.
pub trait ProgramStore: Send + Sync {
    fn resolve<'a>(&'a self, program: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor>;
    fn fetch<'a>(&'a self, descriptor: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>>;
}
pub trait ArtifactCache: Send + Sync {
    fn lookup<'a>(
        &'a self,
        descriptor: &'a ProgramDescriptor,
    ) -> PortFuture<'a, Option<PreparedArtifact>>;
    fn publish<'a>(
        &'a self,
        descriptor: &'a ProgramDescriptor,
        archive: Vec<u8>,
    ) -> PortFuture<'a, PreparedArtifact>;
}
/// Startup retains ownership whenever cleanup cannot be confirmed.
pub enum StartOutcome {
    Ready(Box<dyn ExecutionSession>),
    CleanupRequired {
        error: Error,
        session: Box<dyn ExecutionSession>,
    },
}

pub trait ExecutionRuntime: Send + Sync {
    /// An outer error guarantees no process remains owned. Otherwise return a
    /// ready session or a cleanup handle that must continue occupying pool capacity.
    fn start<'a>(
        &'a self,
        artifact: PreparedArtifact,
        control: RunControl,
    ) -> PortFuture<'a, StartOutcome>;
}
pub trait ExecutionSession: Send {
    fn pid(&self) -> u32;
    /// Runtime/protocol errors require retiring the session; business Failure may be reused.
    /// The session must retain process ownership if this future is dropped or panics,
    /// so `close` can still confirm cleanup. Adapter panics close worker admission.
    fn execute<'a>(
        &'a mut self,
        event: CloudEvent,
        control: RunControl,
    ) -> PortFuture<'a, ProgramOutcome>;
    /// Resolves successfully only once the process group is stopped and child reaped.
    /// Calls must be retryable after cancellation or failure, retaining confirmed
    /// cleanup progress and the artifact pin until cleanup is complete.
    fn close(&mut self) -> PortFuture<'_, ()>;
}

/// Helpers for constructing complete events in test/demo adapters; callers own IDs.
pub fn event_extensions(value: &CloudEvent) -> Map<String, Value> {
    value
        .value()
        .as_object()
        .expect("validated object")
        .iter()
        .filter(|(k, _)| k.starts_with("ldg") || *k == "traceparent" || *k == "tracestate")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
