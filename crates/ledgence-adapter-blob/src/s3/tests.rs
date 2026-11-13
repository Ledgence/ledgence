use super::*;
#[test]
fn endpoints_and_prefixes_are_explicit_and_never_accept_credentials() {
    let mut options = S3ArtifactOptions::new("test", "test-bucket", "us-east-1");
    options.validate().unwrap();
    options.endpoint_url = Some("http://localhost:9000".into());
    assert!(options.validate().is_err());
    options.local_credentials = true;
    assert!(options.validate().is_err());
    options.endpoint_url = Some("http://127.0.0.1:9000".into());
    options.validate().unwrap();
    let (first, _) = options.identity_and_transport().unwrap();
    options.prefix = "some/prefix".into();
    let (second, _) = options.identity_and_transport().unwrap();
    assert_ne!(first, second);
    options.prefix = "../escape".into();
    assert!(options.validate().is_err());
    options.prefix.clear();
    for endpoint in [
        "https://user:secret@127.0.0.1:9000",
        "https://127.0.0.1:9000/?token=secret",
        "http://192.0.2.1:9000",
    ] {
        options.endpoint_url = Some(endpoint.into());
        assert!(options.validate().is_err());
    }
}

mod faults;

#[tokio::test]
async fn unpolled_storage_stream_expires_without_a_reader_poll() {
    let metadata = ledgence_orchestration_api::ArtifactMetadata {
        digest: Digest(format!("sha256:{:x}", Sha256::digest(b"data"))),
        size: 4,
        media_type: "application/octet-stream".into(),
        filename: None,
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut reader = S3Reader::new(ByteStream::from_static(b"data"), metadata, deadline);
    assert!(reader.body.lock().await.is_some());
    tokio::time::pause();
    tokio::time::advance(
        tokio::time::Instant::from_std(deadline) - tokio::time::Instant::now()
            + Duration::from_millis(1),
    )
    .await;
    tokio::time::resume();
    // Advancing makes the watcher eligible; wait for its effect independently
    // of read_chunk, which would otherwise be able to mask a missing watcher.
    tokio::time::timeout(Duration::from_secs(2), async {
        while reader.body.lock().await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(reader.read_chunk(4).await.is_err());
}
