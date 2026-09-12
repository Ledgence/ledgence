use crate::{
    ArtifactLimits,
    archive::read_bounded,
    error::{AdapterError, Result},
};
use ledgence_worker_api::{
    Error, ErrorKind, PortFuture, ProgramDescriptor, ProgramRef, ProgramStore,
};
use std::{
    fs::{self, File},
    net::IpAddr,
    path::{Path, PathBuf},
};

/// A trusted local program store using the same immutable layout as HTTP stores.
#[derive(Clone)]
pub struct FileProgramStore {
    root: PathBuf,
    limits: ArtifactLimits,
}
impl FileProgramStore {
    pub fn new(
        root: impl Into<PathBuf>,
        limits: ArtifactLimits,
    ) -> ledgence_worker_api::Result<Self> {
        limits.validate()?;
        let root = fs::canonicalize(root.into())?;
        if !root.is_dir() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "program store root must be a directory",
            ));
        }
        Ok(Self { root, limits })
    }
}
impl ProgramStore for FileProgramStore {
    fn resolve<'a>(&'a self, program: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        Box::pin(async move {
            program.validate()?;
            let store = self.clone();
            let program = program.clone();
            blocking(move || {
                let bytes = local_bytes(
                    &store.root,
                    &descriptor_path(&program),
                    store.limits.max_descriptor_bytes,
                )?;
                decode_descriptor(&bytes, &program, &store.limits)
            })
            .await
        })
    }
    fn fetch<'a>(&'a self, descriptor: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        Box::pin(async move {
            self.limits.descriptor(descriptor).map_err(Error::from)?;
            let store = self.clone();
            let descriptor = descriptor.clone();
            blocking(move || {
                let bytes = local_bytes(&store.root, &blob_path(&descriptor), descriptor.size)?;
                check_length(bytes, descriptor.size)
            })
            .await
        })
    }
}

/// Bounded HTTP program storage. HTTPS is required except for literal loopback
/// addresses. Redirects are rejected, preserving the configured trust boundary.
#[derive(Clone)]
pub struct HttpProgramStore {
    base: reqwest::Url,
    client: reqwest::Client,
    limits: ArtifactLimits,
}
impl HttpProgramStore {
    pub fn new(base: &str, limits: ArtifactLimits) -> ledgence_worker_api::Result<Self> {
        limits.validate()?;
        let mut base = reqwest::Url::parse(base)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "invalid program store URL"))?;
        let loopback = base
            .host_str()
            .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
        if !(base.scheme() == "https" || (base.scheme() == "http" && loopback))
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "program store URL requires HTTPS or literal loopback HTTP, without credentials, query or fragment",
            ));
        }
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(limits.request_timeout)
            .connect_timeout(
                limits
                    .request_timeout
                    .min(std::time::Duration::from_secs(10)),
            )
            .build()
            .map_err(AdapterError::from)
            .map_err(Error::from)?;
        Ok(Self {
            base,
            client,
            limits,
        })
    }
    async fn get(&self, path: &str, limit: u64) -> Result<Vec<u8>> {
        let url = self
            .base
            .join(path)
            .map_err(|_| AdapterError::Invalid("invalid store-relative URL".into()))?;
        let mut response = self
            .client
            .get(url)
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()
            .await?
            .error_for_status()?;
        if !response.status().is_success() {
            return Err(AdapterError::Public(Error::new(
                ErrorKind::Unavailable,
                "program store redirects are unsupported",
            )));
        }
        if response.content_length().is_some_and(|size| size > limit) {
            return Err(AdapterError::Limit("HTTP content length".into()));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if (bytes.len() as u64)
                .checked_add(chunk.len() as u64)
                .is_none_or(|size| size > limit)
            {
                return Err(AdapterError::Limit("HTTP response bytes".into()));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}
impl ProgramStore for HttpProgramStore {
    fn resolve<'a>(&'a self, program: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        Box::pin(async move {
            program.validate()?;
            let bytes = self
                .get(&descriptor_path(program), self.limits.max_descriptor_bytes)
                .await
                .map_err(Error::from)?;
            decode_descriptor(&bytes, program, &self.limits).map_err(Error::from)
        })
    }
    fn fetch<'a>(&'a self, descriptor: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        Box::pin(async move {
            self.limits.descriptor(descriptor).map_err(Error::from)?;
            let bytes = self
                .get(&blob_path(descriptor), descriptor.size)
                .await
                .map_err(Error::from)?;
            check_length(bytes, descriptor.size).map_err(Error::from)
        })
    }
}
fn descriptor_path(program: &ProgramRef) -> String {
    format!(
        "programs/{}/{}/descriptor.json",
        program.id, program.version
    )
}
fn blob_path(descriptor: &ProgramDescriptor) -> String {
    format!("blobs/{}.zip", descriptor.digest.hex())
}
fn decode_descriptor(
    bytes: &[u8],
    program: &ProgramRef,
    limits: &ArtifactLimits,
) -> Result<ProgramDescriptor> {
    let descriptor: ProgramDescriptor = serde_json::from_slice(bytes)?;
    limits.descriptor(&descriptor)?;
    if descriptor.program != *program {
        return Err(AdapterError::Invalid(
            "resolved program differs from request".into(),
        ));
    }
    Ok(descriptor)
}
fn check_length(bytes: Vec<u8>, expected: u64) -> Result<Vec<u8>> {
    if bytes.len() as u64 != expected {
        return Err(AdapterError::Invalid(
            "download size differs from descriptor".into(),
        ));
    }
    Ok(bytes)
}
fn local_bytes(root: &Path, relative: &str, limit: u64) -> Result<Vec<u8>> {
    let path = fs::canonicalize(root.join(relative))?;
    if !path.starts_with(root) {
        return Err(AdapterError::Invalid(
            "program store path escapes root".into(),
        ));
    }
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(AdapterError::Invalid(
            "program store entry must be a regular file".into(),
        ));
    }
    if metadata.len() > limit {
        return Err(AdapterError::Limit("local store bytes".into()));
    }
    read_bounded(&mut file, limit)
}
pub(crate) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> ledgence_worker_api::Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| Error::new(ErrorKind::Io, format!("artifact operation failed: {error}")))?
        .map_err(Error::from)
}
