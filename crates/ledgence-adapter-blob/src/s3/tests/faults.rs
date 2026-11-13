use super::super::*;
use ledgence_orchestration_api::ArtifactMetadata;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Clone, Copy)]
enum Fault {
    LostPutReply,
    LatePut,
    DeniedPut,
    ConflictPut,
    RetryablePut,
    CorruptGet,
    TruncatedGet,
    DeleteDenied,
}
struct Server {
    endpoint: String,
    puts: Arc<AtomicUsize>,
    accepted: Arc<AtomicBool>,
    put_received: Arc<tokio::sync::Notify>,
    accept_put: Arc<tokio::sync::Notify>,
    put_accepted: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(fault: Fault) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let puts = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicBool::new(false));
        let put_received = Arc::new(tokio::sync::Notify::new());
        let accept_put = Arc::new(tokio::sync::Notify::new());
        let put_accepted = Arc::new(tokio::sync::Notify::new());
        let count = puts.clone();
        let stored = accepted.clone();
        let received = put_received.clone();
        let accept = accept_put.clone();
        let confirmed = put_accepted.clone();
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let count = count.clone();
                let stored = stored.clone();
                let received = received.clone();
                let accept = accept.clone();
                let confirmed = confirmed.clone();
                tokio::spawn(async move {
                    let Some(method) = request(&mut stream).await else {
                        return;
                    };
                    match method.as_str() {
                        "HEAD" => respond(&mut stream, 200, &[], &[]).await,
                        "PUT" => {
                            count.fetch_add(1, Ordering::SeqCst);
                            match fault {
                                Fault::LostPutReply => {
                                    stored.store(true, Ordering::SeqCst);
                                }
                                Fault::LatePut => {
                                    // request() has consumed the complete PUT body.
                                    // Keep acceptance under test control even after
                                    // the client has timed out and disconnected.
                                    received.notify_one();
                                    accept.notified().await;
                                    stored.store(true, Ordering::SeqCst);
                                    confirmed.notify_one();
                                    respond(
                                        &mut stream,
                                        200,
                                        &[("x-amz-checksum-sha256", checksum(&digest()).unwrap())],
                                        &[],
                                    )
                                    .await;
                                }
                                Fault::DeniedPut => xml(&mut stream, 403, "AccessDenied").await,
                                Fault::ConflictPut => {
                                    xml(&mut stream, 412, "PreconditionFailed").await
                                }
                                Fault::RetryablePut => xml(&mut stream, 503, "SlowDown").await,
                                _ => {
                                    respond(
                                        &mut stream,
                                        200,
                                        &[("x-amz-checksum-sha256", checksum(&digest()).unwrap())],
                                        &[],
                                    )
                                    .await
                                }
                            }
                        }
                        "GET" => match fault {
                            Fault::CorruptGet => {
                                respond(
                                    &mut stream,
                                    200,
                                    &[("x-amz-checksum-sha256", checksum(&digest()).unwrap())],
                                    b"nope",
                                )
                                .await
                            }
                            Fault::TruncatedGet => {
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nx-amz-checksum-sha256: {}\r\nConnection: close\r\n\r\nda",
                                    checksum(&digest()).unwrap()
                                );
                                let _ = stream.write_all(response.as_bytes()).await;
                            }
                            _ if !stored.load(Ordering::SeqCst) => {
                                xml(&mut stream, 404, "NoSuchKey").await
                            }
                            _ => {
                                respond(
                                    &mut stream,
                                    200,
                                    &[("x-amz-checksum-sha256", checksum(&digest()).unwrap())],
                                    b"data",
                                )
                                .await
                            }
                        },
                        "DELETE" => xml(&mut stream, 403, "AccessDenied").await,
                        _ => xml(&mut stream, 400, "InvalidRequest").await,
                    }
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self {
            endpoint,
            puts,
            accepted,
            put_received,
            accept_put,
            put_accepted,
            task,
        }
    }
    async fn store(&self) -> S3ExecutionArtifactStore {
        let mut options = S3ArtifactOptions::new("fault-test", "test-bucket", "us-east-1");
        options.endpoint_url = Some(self.endpoint.clone());
        options.local_credentials = true;
        options.force_path_style = true;
        S3ExecutionArtifactStore::connect(options).await.unwrap()
    }
}
async fn request(stream: &mut TcpStream) -> Option<String> {
    let mut bytes = Vec::new();
    let mut piece = [0_u8; 4096];
    let end;
    loop {
        let count = stream.read(&mut piece).await.ok()?;
        if count == 0 {
            return None;
        }
        bytes.extend_from_slice(&piece[..count]);
        if let Some(index) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
            end = index + 4;
            break;
        }
        if bytes.len() > 65536 {
            return None;
        }
    }
    let headers = String::from_utf8_lossy(&bytes[..end]).to_string();
    let method = headers.split_whitespace().next()?.to_owned();
    let length = headers
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    if headers
        .to_ascii_lowercase()
        .contains("expect: 100-continue")
    {
        stream
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .await
            .ok()?;
    }
    let mut remaining = length.saturating_sub(bytes.len() - end);
    while remaining > 0 {
        let count = stream.read(&mut piece).await.ok()?;
        if count == 0 {
            return None;
        }
        remaining = remaining.saturating_sub(count);
    }
    Some(method)
}
async fn respond(stream: &mut TcpStream, status: u16, headers: &[(&str, String)], body: &[u8]) {
    let mut response = format!(
        "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str("\r\n");
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.write_all(body).await;
}
async fn xml(stream: &mut TcpStream, status: u16, code: &str) {
    respond(
        stream,
        status,
        &[("Content-Type", "application/xml".into())],
        format!("<Error><Code>{code}</Code><Message>fixture</Message></Error>").as_bytes(),
    )
    .await;
}
fn digest() -> Digest {
    Digest(format!("sha256:{:x}", Sha256::digest(b"data")))
}
fn object(store: &S3ExecutionArtifactStore) -> ArtifactObjectDescriptor {
    ArtifactObjectDescriptor {
        store: store.identity().clone(),
        artifact_id: "art_fault".into(),
        upload_id: "upload_fault".into(),
        key: "objects/art_fault/upload_fault".into(),
        metadata: ArtifactMetadata {
            digest: digest(),
            size: 4,
            media_type: "application/octet-stream".into(),
            filename: None,
        },
    }
}
struct Reader {
    data: Vec<u8>,
}
impl ArtifactReader for Reader {
    fn read_chunk(&mut self, max: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            let count = self.data.len().min(max);
            Ok(self.data.drain(..count).collect())
        })
    }
}
fn reader() -> Box<dyn ArtifactReader> {
    Box::new(Reader {
        data: b"data".to_vec(),
    })
}
fn deadline() -> Instant {
    (tokio::time::Instant::now() + Duration::from_secs(5)).into_std()
}
#[tokio::test]
async fn lost_reply_and_retryable_failure_do_not_retry_put() {
    for fault in [Fault::LostPutReply, Fault::RetryablePut] {
        let server = Server::start(fault).await;
        let store = server.store().await;
        let object = object(&store);
        assert_eq!(
            store.put_once(&object, reader(), deadline()).await.unwrap(),
            BlobPutOutcome::Uncertain
        );
        assert_eq!(server.puts.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn denied_and_conditional_conflict_are_positive_no_write() {
    for fault in [Fault::DeniedPut, Fault::ConflictPut] {
        let server = Server::start(fault).await;
        let store = server.store().await;
        let object = object(&store);
        assert_eq!(
            store.put_once(&object, reader(), deadline()).await.unwrap(),
            BlobPutOutcome::ConfirmedNoWrite
        );
        assert_eq!(server.puts.load(Ordering::SeqCst), 1);
        assert!(!server.accepted.load(Ordering::SeqCst));
    }
}
#[tokio::test]
async fn missing_probe_is_not_quiescence_after_timed_out_put() {
    let server = Server::start(Fault::LatePut).await;
    let store = server.store().await;
    let object = object(&store);
    let put_deadline = deadline();
    let operation = {
        let store = store.clone();
        let object = object.clone();
        tokio::spawn(async move { store.put_once(&object, reader(), put_deadline).await })
    };
    tokio::time::timeout(Duration::from_secs(2), server.put_received.notified())
        .await
        .unwrap();
    assert_eq!(server.puts.load(Ordering::SeqCst), 1);
    assert!(!server.accepted.load(Ordering::SeqCst));
    assert!(!operation.is_finished());
    tokio::time::pause();
    tokio::time::advance(
        tokio::time::Instant::from_std(put_deadline) - tokio::time::Instant::now()
            + Duration::from_millis(1),
    )
    .await;
    tokio::time::resume();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), operation)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        BlobPutOutcome::Uncertain
    );
    assert!(store.open(&object, deadline()).await.unwrap().is_none());
    assert!(!server.accepted.load(Ordering::SeqCst));
    assert_eq!(server.puts.load(Ordering::SeqCst), 1);
    server.accept_put.notify_one();
    tokio::time::timeout(Duration::from_secs(2), server.put_accepted.notified())
        .await
        .unwrap();
    assert!(server.accepted.load(Ordering::SeqCst));
    assert!(store.open(&object, deadline()).await.unwrap().is_some());
    assert_eq!(server.puts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn actual_stream_corruption_and_truncation_are_rejected() {
    for fault in [Fault::CorruptGet, Fault::TruncatedGet] {
        let server = Server::start(fault).await;
        let store = server.store().await;
        let object = object(&store);
        let mut read = store
            .open(&object, deadline())
            .await
            .unwrap()
            .unwrap()
            .reader;
        let mut failed = false;
        for _ in 0..8 {
            match read.read_chunk(2).await {
                Err(_) => {
                    failed = true;
                    break;
                }
                Ok(bytes) if bytes.is_empty() => break,
                _ => {}
            }
        }
        assert!(failed);
    }
}
#[tokio::test]
async fn delete_denial_is_not_acknowledged() {
    let server = Server::start(Fault::DeleteDenied).await;
    let store = server.store().await;
    assert!(store.delete(&object(&store), deadline()).await.is_err());
}
