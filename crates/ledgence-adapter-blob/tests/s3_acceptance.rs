#![cfg(feature = "s3")]
//! Explicitly invoked by tools/check-artifact-s3.py against an owned local
//! external fixture. Ignored in ordinary unit runs; the release harness must
//! invoke both phases, including replacement of the fixture server.
use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig},
};
use ledgence_adapter_blob::{S3ArtifactOptions, S3ExecutionArtifactStore};
use ledgence_orchestration_api::{
    ArtifactMetadata, ArtifactObjectDescriptor, ArtifactReader, BlobPutOutcome, ContractFuture,
    ExecutionArtifactBlobStore,
};
use ledgence_worker_api::Digest;
use sha2::{Digest as _, Sha256};
use std::time::{Duration, Instant};
const MAX: u64 = 64 * 1024 * 1024;
const CHUNK: usize = 256 * 1024;
struct Repeat {
    byte: u8,
    remaining: u64,
}
impl ArtifactReader for Repeat {
    fn read_chunk(&mut self, max: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            let count = self.remaining.min(max as u64) as usize;
            self.remaining -= count as u64;
            Ok(vec![self.byte; count])
        })
    }
}
fn digest(byte: u8, size: u64) -> Digest {
    let chunk = vec![byte; CHUNK];
    let mut hash = Sha256::new();
    let mut remaining = size;
    while remaining > 0 {
        let count = remaining.min(CHUNK as u64) as usize;
        hash.update(&chunk[..count]);
        remaining -= count as u64;
    }
    Digest(format!("sha256:{:x}", hash.finalize()))
}
fn object(
    store: &S3ExecutionArtifactStore,
    name: &str,
    size: u64,
    byte: u8,
) -> ArtifactObjectDescriptor {
    ArtifactObjectDescriptor {
        store: store.identity().clone(),
        artifact_id: format!("art_{name}"),
        upload_id: "upload_acceptance".into(),
        key: format!("objects/art_{name}/upload_acceptance"),
        metadata: ArtifactMetadata {
            digest: digest(byte, size),
            size,
            media_type: "application/octet-stream".into(),
            filename: Some(format!("{name}.bin")),
        },
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(120)
}
async fn put(
    store: &S3ExecutionArtifactStore,
    object: &ArtifactObjectDescriptor,
    byte: u8,
) -> BlobPutOutcome {
    store
        .put_once(
            object,
            Box::new(Repeat {
                byte,
                remaining: object.metadata.size,
            }),
            deadline(),
        )
        .await
        .unwrap()
}
async fn verify(store: &S3ExecutionArtifactStore, object: &ArtifactObjectDescriptor) {
    let mut reader = store
        .open(object, deadline())
        .await
        .unwrap()
        .unwrap()
        .reader;
    let mut hash = Sha256::new();
    let mut size = 0;
    loop {
        let bytes = reader.read_chunk(CHUNK).await.unwrap();
        if bytes.is_empty() {
            break;
        }
        size += bytes.len() as u64;
        hash.update(bytes);
    }
    assert_eq!(size, object.metadata.size);
    assert_eq!(
        format!("sha256:{:x}", hash.finalize()),
        object.metadata.digest.0
    );
}
fn options() -> S3ArtifactOptions {
    let endpoint = std::env::var("LEDGENCE_S3_TEST_ENDPOINT")
        .expect("explicit local fixture endpoint required");
    let bucket =
        std::env::var("LEDGENCE_S3_TEST_BUCKET").expect("disposable fixture bucket required");
    let mut options = S3ArtifactOptions::new("local-acceptance", bucket, "us-east-1");
    options.endpoint_url = Some(endpoint);
    options.local_credentials = true;
    options.force_path_style = true;
    options.validate().unwrap();
    options
}
async fn create_bucket(options: &S3ArtifactOptions) {
    let http =
        aws_smithy_http_client::Builder::new().build_with_connector_fn(|settings, components| {
            let mut connector = aws_smithy_http_client::ConnectorBuilder::default();
            connector.set_connector_settings(settings.cloned());
            if let Some(runtime) = components {
                connector.set_sleep_impl(runtime.sleep_impl());
            }
            connector
                .set_proxy_config(Some(aws_smithy_http_client::proxy::ProxyConfig::disabled()));
            connector.build_http()
        });
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::v2026_01_12())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            "ledgence-local",
            "ledgence-local",
            None,
            None,
            "owned-fixture",
        ))
        .endpoint_url(options.endpoint_url.as_ref().unwrap())
        .force_path_style(true)
        .http_client(http)
        .retry_config(RetryConfig::disabled())
        .build();
    let client = Client::from_conf(config);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match client.create_bucket().bucket(&options.bucket).send().await {
            Ok(_) => return,
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_bucket_already_owned_by_you()) =>
            {
                return;
            }
            Err(error) if Instant::now() >= deadline => {
                panic!("fixture bucket initialization failed: {error}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicitly owned local RustFS fixture; mandatory release harness invokes this"]
async fn local_s3_acceptance() {
    let phase = std::env::var("LEDGENCE_S3_TEST_PHASE").expect("fixture phase required");
    let options = options();
    if phase == "write" {
        create_bucket(&options).await;
    }
    let store = S3ExecutionArtifactStore::connect(options).await.unwrap();
    let empty = object(&store, "empty", 0, 0);
    let large = object(&store, "maximum", MAX, 41);
    if phase == "write" {
        assert_eq!(put(&store, &empty, 0).await, BlobPutOutcome::Created);
        assert_eq!(put(&store, &large, 41).await, BlobPutOutcome::Created);
        verify(&store, &empty).await;
        verify(&store, &large).await;
        assert_eq!(
            put(&store, &large, 41).await,
            BlobPutOutcome::ConfirmedNoWrite
        );
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(4));
        let mut tasks = Vec::new();
        for byte in 1..=4 {
            let store = store.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                let object = object(&store, "race", 2 * 1024 * 1024, byte);
                barrier.wait().await;
                let outcome = put(&store, &object, byte).await;
                (object, outcome)
            }));
        }
        let mut winners = Vec::new();
        let mut conflicts = 0;
        for task in tasks {
            let (object, outcome) = task.await.unwrap();
            match outcome {
                BlobPutOutcome::Created => winners.push(object),
                BlobPutOutcome::ConfirmedNoWrite => conflicts += 1,
                BlobPutOutcome::Uncertain => panic!("unexpected fixture race uncertainty"),
            }
        }
        assert_eq!(winners.len(), 1);
        assert_eq!(conflicts, 3);
        verify(&store, &winners[0]).await;
        let invalid = object(&store, "bad_digest", 32, 1);
        assert_ne!(put(&store, &invalid, 2).await, BlobPutOutcome::Created);
        assert!(store.open(&invalid, deadline()).await.unwrap().is_none());
        let mut oversized = object(&store, "oversize", 0, 0);
        oversized.metadata.size = MAX + 1;
        assert!(
            store
                .put_once(
                    &oversized,
                    Box::new(Repeat {
                        byte: 0,
                        remaining: 0
                    }),
                    deadline()
                )
                .await
                .is_err()
        );
        store.delete(&winners[0], deadline()).await.unwrap();
        assert!(store.open(&winners[0], deadline()).await.unwrap().is_none());
        store.delete(&winners[0], deadline()).await.unwrap();
    } else {
        assert_eq!(phase, "read");
        verify(&store, &empty).await;
        verify(&store, &large).await;
        store.delete(&empty, deadline()).await.unwrap();
        store.delete(&large, deadline()).await.unwrap();
        assert!(store.open(&large, deadline()).await.unwrap().is_none());
    }
    println!("LEDGENCE_S3_ACCEPTANCE phase={phase} passed=true max_bytes={MAX}");
}
