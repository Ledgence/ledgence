use crate::{CHUNK_BYTES, MAX_BYTES, check_deadline, chunk_limit, invalid, unavailable};
use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig},
    primitives::ByteStream,
    types::{ChecksumAlgorithm, ChecksumMode},
};
use aws_smithy_types::body::SdkBody;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use ledgence_orchestration_api::{
    ArtifactObjectDescriptor, ArtifactReader, ArtifactStoreIdentity, BlobDeleteOutcome,
    BlobPutOutcome, BlobRead, ContractFuture, ExecutionArtifactBlobStore, Result,
};
use ledgence_worker_api::Digest;
use sha2::{Digest as _, Sha256};
use std::{
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

mod body;
mod transport;

/// Server configuration only. Credentials use the standard AWS chain, except
/// fixed nonsecret credentials explicitly selected for a literal loopback test.
#[derive(Clone, Debug)]
pub struct S3ArtifactOptions {
    pub store_id: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub local_credentials: bool,
}
impl S3ArtifactOptions {
    pub fn new(
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            store_id: store_id.into(),
            bucket: bucket.into(),
            prefix: String::new(),
            region: region.into(),
            endpoint_url: None,
            force_path_style: false,
            local_credentials: false,
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.identity_and_transport().map(|_| ())
    }
    fn identity_and_transport(&self) -> Result<(ArtifactStoreIdentity, transport::Transport)> {
        if self.region.is_empty()
            || self.region.len() > 128
            || !self
                .region
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(invalid("invalid artifact S3 region"));
        }
        if !(3..=63).contains(&self.bucket.len())
            || !self
                .bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-'))
        {
            return Err(invalid("invalid artifact S3 bucket"));
        }
        if self.prefix.len() > 512
            || (!self.prefix.is_empty()
                && self.prefix.split('/').any(|part| {
                    part.is_empty()
                        || part == "."
                        || part == ".."
                        || !part
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
                }))
        {
            return Err(invalid("invalid artifact S3 prefix"));
        }
        let endpoint = self
            .endpoint_url
            .as_ref()
            .map(|value| {
                url::Url::parse(value).map_err(|_| invalid("invalid artifact S3 endpoint"))
            })
            .transpose()?;
        if let Some(endpoint) = &endpoint {
            if endpoint.username() != ""
                || endpoint.password().is_some()
                || endpoint.query().is_some()
                || endpoint.fragment().is_some()
                || endpoint.path() != "/"
                || endpoint.host_str().is_none()
            {
                return Err(invalid(
                    "artifact S3 endpoint must be an origin without credentials",
                ));
            }
            let loopback = endpoint
                .host_str()
                .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
                .is_some_and(|ip| ip.is_loopback());
            if endpoint.scheme() != "https"
                && !(endpoint.scheme() == "http" && self.local_credentials && loopback)
            {
                return Err(invalid(
                    "artifact S3 requires HTTPS except explicit literal-loopback tests",
                ));
            }
            if self.local_credentials && !loopback {
                return Err(invalid(
                    "local artifact S3 credentials require a literal-loopback endpoint",
                ));
            }
        } else if self.local_credentials {
            return Err(invalid(
                "local artifact S3 credentials require an explicit endpoint",
            ));
        }
        let endpoint_identity = endpoint
            .as_ref()
            .map(|url| url.as_str().trim_end_matches('/').to_owned())
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", self.region));
        let fingerprint = Digest(format!(
            "sha256:{:x}",
            Sha256::digest(
                format!(
                    "ledgence-s3-v1\0{}\0{}\0{}\0{}",
                    endpoint_identity, self.region, self.bucket, self.prefix
                )
                .as_bytes()
            )
        ));
        let identity = ArtifactStoreIdentity {
            store_id: self.store_id.clone(),
            fingerprint,
        };
        identity.validate()?;
        let transport = if self.local_credentials {
            if endpoint.is_some_and(|endpoint| endpoint.scheme() == "http") {
                transport::Transport::LocalHttp
            } else {
                transport::Transport::LocalTls
            }
        } else {
            transport::Transport::Standard
        };
        Ok((identity, transport))
    }
}
#[derive(Clone)]
pub struct S3ExecutionArtifactStore {
    client: Client,
    options: S3ArtifactOptions,
    identity: ArtifactStoreIdentity,
}
impl S3ExecutionArtifactStore {
    /// Resolve the server credential chain and verify that the bucket is
    /// reachable. Conditional-write/checksum conformance is a deployment gate,
    /// not something a harmless bucket HEAD can establish.
    pub async fn connect(options: S3ArtifactOptions) -> Result<Self> {
        let (identity, transport) = options.identity_and_transport()?;
        let deadline = Instant::now() + Duration::from_secs(15);
        let client = tokio::time::timeout_at(deadline.into(), async {
            let http = transport::client(transport);
            let mut loader = aws_config::defaults(BehaviorVersion::v2026_01_12())
                .region(Region::new(options.region.clone()))
                .http_client(http.clone())
                .retry_config(RetryConfig::disabled())
                .timeout_config(timeouts());
            if options.local_credentials {
                loader = loader.credentials_provider(Credentials::new(
                    "ledgence-local",
                    "ledgence-local",
                    None,
                    None,
                    "explicit-loopback",
                ));
            }
            let shared = loader.load().await;
            let mut config = aws_sdk_s3::config::Builder::from(&shared)
                .http_client(http)
                .retry_config(RetryConfig::disabled())
                .timeout_config(timeouts())
                .force_path_style(options.force_path_style);
            if let Some(endpoint) = &options.endpoint_url {
                config = config.endpoint_url(endpoint);
            }
            let client = Client::from_conf(config.build());
            client
                .head_bucket()
                .bucket(&options.bucket)
                .send()
                .await
                .map_err(|_| unavailable("artifact S3 bucket check failed"))?;
            Ok::<_, ledgence_orchestration_api::ContractError>(client)
        })
        .await
        .map_err(|_| unavailable("artifact S3 configuration deadline exceeded"))??;
        Ok(Self {
            client,
            options,
            identity,
        })
    }
    fn validate(&self, object: &ArtifactObjectDescriptor) -> Result<()> {
        object.validate()?;
        if object.store != self.identity {
            return Err(invalid("artifact belongs to a different store"));
        }
        Ok(())
    }
    fn key(&self, object: &ArtifactObjectDescriptor) -> String {
        if self.options.prefix.is_empty() {
            object.key.clone()
        } else {
            format!("{}/{}", self.options.prefix, object.key)
        }
    }
}
fn timeouts() -> TimeoutConfig {
    TimeoutConfig::builder()
        .operation_timeout(Duration::from_secs(120))
        .operation_attempt_timeout(Duration::from_secs(120))
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(120))
        .build()
}
fn checksum(digest: &Digest) -> Result<String> {
    digest.validate()?;
    let raw = (0..32)
        .map(|i| {
            u8::from_str_radix(&digest.hex()[i * 2..i * 2 + 2], 16)
                .map_err(|_| invalid("invalid artifact checksum"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(STANDARD.encode(raw))
}
impl ExecutionArtifactBlobStore for S3ExecutionArtifactStore {
    fn identity(&self) -> &ArtifactStoreIdentity {
        &self.identity
    }
    fn put_once<'a>(
        &'a self,
        object: &'a ArtifactObjectDescriptor,
        reader: Box<dyn ArtifactReader>,
        deadline: Instant,
    ) -> ContractFuture<'a, BlobPutOutcome> {
        Box::pin(async move {
            self.validate(object)?;
            let deadline = crate::transfer_deadline(deadline);
            check_deadline(deadline)?;
            let sha256 = checksum(&object.metadata.digest)?;
            let (body, verified) =
                body::StreamingBody::new(reader, object.metadata.clone(), deadline);
            let request = self
                .client
                .put_object()
                .bucket(&self.options.bucket)
                .key(self.key(object))
                .if_none_match("*")
                .content_length(object.metadata.size as i64)
                .content_type(&object.metadata.media_type)
                .checksum_algorithm(ChecksumAlgorithm::Sha256)
                .checksum_sha256(&sha256)
                .body(ByteStream::new(SdkBody::from_body_1_x(body)));
            match tokio::time::timeout_at(deadline.into(), request.send()).await {
                Ok(Ok(reply))
                    if reply.checksum_sha256() == Some(sha256.as_str())
                        && verified.load(std::sync::atomic::Ordering::Acquire) =>
                {
                    Ok(BlobPutOutcome::Created)
                }
                Ok(Err(error))
                    if error.raw_response().is_some_and(|response| {
                        matches!(response.status().as_u16(), 400 | 403 | 404 | 409 | 412)
                    }) =>
                {
                    Ok(BlobPutOutcome::ConfirmedNoWrite)
                }
                _ => Ok(BlobPutOutcome::Uncertain),
            }
        })
    }
    fn open<'a>(
        &'a self,
        object: &'a ArtifactObjectDescriptor,
        deadline: Instant,
    ) -> ContractFuture<'a, Option<BlobRead>> {
        Box::pin(async move {
            self.validate(object)?;
            let deadline = crate::transfer_deadline(deadline);
            check_deadline(deadline)?;
            let request = self
                .client
                .get_object()
                .bucket(&self.options.bucket)
                .key(self.key(object))
                .checksum_mode(ChecksumMode::Enabled);
            let output = match tokio::time::timeout_at(deadline.into(), request.send()).await {
                Ok(Ok(output)) => output,
                Ok(Err(error))
                    if error
                        .as_service_error()
                        .is_some_and(|error| error.is_no_such_key()) =>
                {
                    return Ok(None);
                }
                _ => return Err(unavailable("artifact S3 read failed")),
            };
            if output.content_length() != Some(object.metadata.size as i64)
                || output.checksum_sha256() != Some(checksum(&object.metadata.digest)?.as_str())
            {
                return Err(unavailable(
                    "artifact S3 object metadata differs from manifest",
                ));
            }
            Ok(Some(BlobRead {
                reader: Box::new(S3Reader::new(
                    output.body,
                    object.metadata.clone(),
                    deadline,
                )),
            }))
        })
    }
    fn delete<'a>(
        &'a self,
        object: &'a ArtifactObjectDescriptor,
        deadline: Instant,
    ) -> ContractFuture<'a, BlobDeleteOutcome> {
        Box::pin(async move {
            self.validate(object)?;
            let deadline = crate::transfer_deadline(deadline);
            check_deadline(deadline)?;
            let request = self
                .client
                .delete_object()
                .bucket(&self.options.bucket)
                .key(self.key(object));
            match tokio::time::timeout_at(deadline.into(), request.send()).await {
                Ok(Ok(_)) => Ok(BlobDeleteOutcome::Deleted),
                _ => Err(unavailable("artifact S3 delete unconfirmed")),
            }
        })
    }
}
struct S3Reader {
    body: Arc<Mutex<Option<ByteStream>>>,
    pending: Bytes,
    deadline: Instant,
    remaining: u64,
    digest: Digest,
    hash: Sha256,
    eof: bool,
}
impl S3Reader {
    fn new(
        body: ByteStream,
        metadata: ledgence_orchestration_api::ArtifactMetadata,
        deadline: Instant,
    ) -> Self {
        let body = Arc::new(Mutex::new(Some(body)));
        let weak = Arc::downgrade(&body);
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline.into()).await;
            if let Some(body) = weak.upgrade() {
                body.lock().await.take();
            }
        });
        Self {
            body,
            pending: Bytes::new(),
            deadline,
            remaining: metadata.size,
            digest: metadata.digest,
            hash: Sha256::new(),
            eof: false,
        }
    }
}
impl ArtifactReader for S3Reader {
    fn read_chunk(&mut self, max_bytes: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            chunk_limit(max_bytes)?;
            if self.eof {
                return Ok(Vec::new());
            }
            check_deadline(self.deadline)?;
            while self.pending.is_empty() {
                let next = tokio::time::timeout_at(self.deadline.into(), async {
                    let mut held = self.body.lock().await;
                    held.as_mut()
                        .ok_or_else(|| unavailable("artifact S3 stream expired"))?
                        .next()
                        .await
                        .transpose()
                        .map_err(|_| unavailable("artifact S3 stream failed"))
                })
                .await
                .map_err(|_| unavailable("artifact S3 stream deadline exceeded"))??;
                match next {
                    Some(bytes) if bytes.len() as u64 <= self.remaining => self.pending = bytes,
                    Some(_) => return Err(unavailable("artifact S3 body exceeds declared length")),
                    None => {
                        self.body.lock().await.take();
                        if self.remaining != 0
                            || format!("sha256:{:x}", self.hash.clone().finalize()) != self.digest.0
                        {
                            return Err(unavailable("artifact S3 content integrity mismatch"));
                        }
                        self.eof = true;
                        return Ok(Vec::new());
                    }
                }
            }
            let chunk = self.pending.split_to(self.pending.len().min(max_bytes));
            self.remaining -= chunk.len() as u64;
            self.hash.update(&chunk);
            Ok(chunk.to_vec())
        })
    }
}
#[cfg(test)]
mod tests;
