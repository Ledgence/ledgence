//! Exercise the actual CLI process against an explicitly controlled HTTP peer.
use ledgence_adapter_artifact::{ArtifactLimits, pack_directory};
use ledgence_worker_api::PublicationLimits;
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::Output,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn invoke(args: &[&str]) -> Output {
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence"))
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("CLI must finish")
    .unwrap()
}
fn source() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir(directory.path().join("prepared")).unwrap();
    fs::write(
        directory.path().join("prepared/app.py"),
        "def handle(event): return event['data']\n",
    )
    .unwrap();
    fs::write(
        directory.path().join("prepared/ledgence-program.json"),
        serde_json::to_vec(&manifest()).unwrap(),
    )
    .unwrap();
    directory
}
fn manifest() -> Value {
    json!({"schema_version":1,"program":{"id":"publication-example","version":"1.0.0"},"handler":"app:handle",
        "runtime":{"kind":"python","python":"3.14","protocol":3},"platform":{"os":"linux","arch":"aarch64"}})
}
fn report(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "invalid CLI JSON {error}; stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}
#[derive(Clone, Copy)]
enum Mode {
    Success,
    DisconnectFirst,
    WrongDigest,
    OldServer,
}
struct Peer {
    url: String,
    puts: Arc<Mutex<Vec<Vec<u8>>>>,
    registrations: Arc<Mutex<Vec<Value>>>,
    requests: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Peer {
    async fn start(mode: Mode, fail_first_registration: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let puts = Arc::new(Mutex::new(Vec::new()));
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(AtomicUsize::new(0));
        let (put_records, registration_records, request_count) =
            (puts.clone(), registrations.clone(), requests.clone());
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (header, body) = read(&mut socket).await;
                request_count.fetch_add(1, Ordering::SeqCst);
                let route = header.lines().next().unwrap();
                if route.starts_with("GET /v1/programs/publication-capabilities ") {
                    if matches!(mode, Mode::OldServer) {
                        respond(&mut socket, 404, json!({"code":"route_not_found"})).await;
                        continue;
                    }
                    respond(&mut socket, 200, json!({"enabled":true,"registration_enabled":true,"mode":"immutable", "limits":PublicationLimits::default(),"max_concurrent_uploads":2,"transfer_timeout_ms":120000})).await;
                } else if route.starts_with("PUT /v1/programs/publication-example/1.0.0/artifact ")
                {
                    assert!(
                        header
                            .to_ascii_lowercase()
                            .contains("content-type: application/zip")
                    );
                    let already_published = {
                        let mut records = put_records.lock().unwrap();
                        let existing = !records.is_empty();
                        records.push(body.clone());
                        existing
                    };
                    if !already_published && matches!(mode, Mode::DisconnectFirst) {
                        drop(socket);
                        continue;
                    }
                    let sha = header
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("x-ledgence-archive-sha256")
                                .then(|| value.trim().to_owned())
                        })
                        .unwrap();
                    let digest = if matches!(mode, Mode::WrongDigest) {
                        format!("sha256:{}", "f".repeat(64))
                    } else {
                        format!("sha256:{sha}")
                    };
                    respond(&mut socket, if already_published {200} else {201}, json!({"descriptor":{"program":manifest()["program"],"digest":digest,"size":body.len()},"already_published":already_published})).await;
                } else if route.starts_with("POST /v1/console/programs/register ") {
                    let command: Value = serde_json::from_slice(&body).unwrap();
                    let already_registered = {
                        let mut records = registration_records.lock().unwrap();
                        let existing = !records.is_empty();
                        records.push(command.clone());
                        existing
                    };
                    if fail_first_registration && !already_registered {
                        respond(
                            &mut socket,
                            503,
                            json!({"code":"unavailable","message":"temporary catalog outage"}),
                        )
                        .await;
                        continue;
                    }
                    let mut descriptor = command["expected_descriptor"].clone();
                    descriptor["size"] =
                        Value::String(descriptor["size"].as_u64().unwrap().to_string());
                    respond(&mut socket, 200, json!({"version":{"descriptor":descriptor,"manifest":manifest(),"metadata":command["metadata"],"registered_at":1,"provenance":"configured_program_store"},"already_registered":already_registered,"metadata_updated":false})).await;
                } else {
                    panic!("unexpected CLI request {route}");
                }
            }
        });
        Self {
            url,
            puts,
            registrations,
            requests,
            task,
        }
    }
}
async fn read(socket: &mut TcpStream) -> (String, Vec<u8>) {
    let mut bytes = Vec::new();
    let end = loop {
        if let Some(index) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
            break index + 4;
        }
        let mut buffer = [0; 4096];
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
        assert!(bytes.len() < 65536);
    };
    let header = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let size = header
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    assert!(size < 1024 * 1024);
    while bytes.len() < end + size {
        let mut buffer = [0; 4096];
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
    (header, bytes[end..end + size].to_vec())
}
async fn respond(socket: &mut TcpStream, status: u16, value: Value) {
    let body = serde_json::to_vec(&value).unwrap();
    let header = format!(
        "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nRequest-Id: req-publication-test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await.unwrap();
    socket.write_all(&body).await.unwrap();
    socket.shutdown().await.unwrap();
}
fn path(path: &Path) -> &str {
    path.to_str().unwrap()
}

#[tokio::test]
async fn filesystem_publish_preserves_the_existing_plain_descriptor_output() {
    let fixture = source();
    let source = fixture.path().join("prepared");
    let store = fixture.path().join("store");
    let output = invoke(&[
        "program",
        "publish",
        "--source",
        path(&source),
        "--store",
        path(&store),
    ])
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value = report(&output);
    assert_eq!(value.as_object().unwrap().len(), 3);
    assert_eq!(
        value,
        serde_json::to_value(
            pack_directory(&source, &ArtifactLimits::default())
                .unwrap()
                .descriptor
        )
        .unwrap()
    );
    assert!(
        store
            .join("programs/publication-example/1.0.0/descriptor.json")
            .is_file()
    );
}
#[tokio::test]
async fn invalid_flags_never_contact_the_peer() {
    let peer = Peer::start(Mode::Success, false).await;
    for extra in [
        vec!["--store", "store"],
        vec!["--kind", "task"],
        vec!["--register", "--register"],
        vec!["--register", "--kind", "wrong"],
        vec!["--resume", "receipt.json"],
    ] {
        let mut args = vec!["program", "publish", "--server", &peer.url];
        args.extend(extra);
        assert_eq!(invoke(&args).await.status.code(), Some(2));
    }
    assert_eq!(peer.requests.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn old_server_reports_actionable_incompatibility_without_creating_a_receipt() {
    let fixture = source();
    let peer = Peer::start(Mode::OldServer, false).await;
    let output = invoke(&[
        "program",
        "publish",
        "--source",
        path(&fixture.path().join("prepared")),
        "--server",
        &peer.url,
    ])
    .await;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--store"));
    assert!(!fixture.path().join("publications").exists());
    assert!(peer.puts.lock().unwrap().is_empty());
}
#[tokio::test]
async fn a_mismatched_upload_reply_is_unknown_and_does_not_register() {
    let fixture = source();
    let peer = Peer::start(Mode::WrongDigest, false).await;
    let output = invoke(&[
        "program",
        "publish",
        "--source",
        path(&fixture.path().join("prepared")),
        "--server",
        &peer.url,
        "--register",
        "--kind",
        "task",
    ])
    .await;
    assert!(!output.status.success());
    let value = report(&output);
    assert_eq!(value["phase"], "prepared");
    assert_eq!(value["publication_outcome"], "unknown");
    assert_eq!(value["request_id"], "req-publication-test");
    assert_eq!(value["error"]["code"], "publication_outcome_unknown");
    assert!(peer.registrations.lock().unwrap().is_empty());
    assert!(Path::new(value["receipt"].as_str().unwrap()).is_file());
}
#[tokio::test]
async fn disconnect_and_resume_reuses_the_exact_zip_after_source_changes() {
    let fixture = source();
    let source = fixture.path().join("prepared");
    let peer = Peer::start(Mode::DisconnectFirst, false).await;
    let first = invoke(&[
        "program",
        "publish",
        "--source",
        path(&source),
        "--server",
        &peer.url,
    ])
    .await;
    assert!(!first.status.success());
    let value = report(&first);
    assert_eq!(value["publication_outcome"], "unknown");
    fs::write(
        source.join("app.py"),
        "changed source must never be rebuilt during recovery",
    )
    .unwrap();
    let resumed = invoke(&[
        "program",
        "publish",
        "--resume",
        value["receipt"].as_str().unwrap(),
    ])
    .await;
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let report = report(&resumed);
    assert_eq!(report["phase"], "published");
    assert_eq!(report["publication_outcome"], "confirmed");
    assert_eq!(report["publication"]["already_published"], true);
    let bodies = peer.puts.lock().unwrap();
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[0], bodies[1]);
}
#[tokio::test]
async fn catalog_failure_preserves_confirmed_publication_and_resume_only_registers() {
    let fixture = source();
    let peer = Peer::start(Mode::Success, true).await;
    let first = invoke(&[
        "program",
        "publish",
        "--source",
        path(&fixture.path().join("prepared")),
        "--server",
        &peer.url,
        "--register",
        "--kind",
        "workflow",
        "--display-name",
        "Example",
    ])
    .await;
    assert!(!first.status.success());
    let value = report(&first);
    assert_eq!(value["phase"], "published");
    assert_eq!(value["publication_outcome"], "confirmed");
    let command = &peer.registrations.lock().unwrap()[0].clone();
    assert_eq!(command["expected_descriptor"], value["descriptor"]);
    assert!(
        value["register_command"]
            .as_array()
            .unwrap()
            .contains(&json!("--expected-digest"))
    );
    let resumed = invoke(&[
        "program",
        "publish",
        "--resume",
        value["receipt"].as_str().unwrap(),
    ])
    .await;
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let result = report(&resumed);
    assert_eq!(result["phase"], "registered");
    assert_eq!(peer.puts.lock().unwrap().len(), 1);
    assert_eq!(peer.registrations.lock().unwrap().len(), 2);
    let completed = invoke(&[
        "program",
        "publish",
        "--resume",
        value["receipt"].as_str().unwrap(),
    ])
    .await;
    assert!(completed.status.success());
    assert_eq!(peer.registrations.lock().unwrap().len(), 2);
    let receipt = Path::new(value["receipt"].as_str().unwrap());
    let mut edited: Value = serde_json::from_slice(&fs::read(receipt).unwrap()).unwrap();
    edited["registered"]["version"]["metadata"]["display_name"] = json!("incorrect receipt");
    fs::write(receipt, serde_json::to_vec(&edited).unwrap()).unwrap();
    assert!(
        !invoke(&["program", "publish", "--resume", path(receipt)])
            .await
            .status
            .success()
    );
    assert_eq!(peer.registrations.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn corrupt_archives_receipts_and_concurrent_receipt_locks_are_rejected_before_network() {
    let fixture = source();
    let peer = Peer::start(Mode::DisconnectFirst, false).await;
    let first = invoke(&[
        "program",
        "publish",
        "--source",
        path(&fixture.path().join("prepared")),
        "--server",
        &peer.url,
    ])
    .await;
    let value = report(&first);
    let receipt = Path::new(value["receipt"].as_str().unwrap());
    let requests = peer.requests.load(Ordering::SeqCst);
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(receipt.with_file_name(".lock"))
        .unwrap();
    lock.try_lock().unwrap();
    let busy = invoke(&["program", "publish", "--resume", path(receipt)]).await;
    assert!(!busy.status.success());
    assert!(String::from_utf8_lossy(&busy.stderr).contains("already in use"));
    drop(lock);
    let original = fs::read(receipt).unwrap();
    fs::write(receipt, b"{bad JSON}").unwrap();
    assert!(
        !invoke(&["program", "publish", "--resume", path(receipt)])
            .await
            .status
            .success()
    );
    fs::write(receipt, original).unwrap();
    fs::write(receipt.with_file_name("artifact.zip"), b"changed artifact").unwrap();
    let broken = invoke(&["program", "publish", "--resume", path(receipt)]).await;
    assert!(!broken.status.success());
    assert_eq!(report(&broken)["publication_outcome"], "unconfirmed");
    assert_eq!(peer.requests.load(Ordering::SeqCst), requests);
}

#[tokio::test]
async fn local_publication_operation_failures_keep_exit_one_and_usage_errors_exit_two() {
    let fixture = source();
    let source = fixture.path().join("prepared");
    let store = fixture.path().join("store");
    let missing = fixture.path().join("missing");
    let output = invoke(&[
        "program",
        "publish",
        "--source",
        path(&missing),
        "--store",
        path(&store),
    ])
    .await;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());

    let invalid_store = fixture.path().join("store-is-a-file");
    fs::write(&invalid_store, b"existing data").unwrap();
    let output = invoke(&[
        "program",
        "publish",
        "--source",
        path(&source),
        "--store",
        path(&invalid_store),
    ])
    .await;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(fs::read(&invalid_store).unwrap(), b"existing data");

    let first = invoke(&[
        "program",
        "publish",
        "--source",
        path(&source),
        "--store",
        path(&store),
    ])
    .await;
    assert!(first.status.success());
    let descriptor_path = store.join("programs/publication-example/1.0.0/descriptor.json");
    let descriptor = fs::read(&descriptor_path).unwrap();
    fs::write(
        source.join("app.py"),
        "def handle(event): return 'different immutable content'\n",
    )
    .unwrap();
    let conflict = invoke(&[
        "program",
        "publish",
        "--source",
        path(&source),
        "--store",
        path(&store),
    ])
    .await;
    assert_eq!(conflict.status.code(), Some(1));
    assert!(conflict.stdout.is_empty());
    assert_eq!(fs::read(descriptor_path).unwrap(), descriptor);

    let usage = invoke(&["program", "publish", "--source"]).await;
    assert_eq!(usage.status.code(), Some(2));
}

#[tokio::test]
async fn remote_preparation_distinguishes_missing_files_from_invalid_manifest() {
    let fixture = source();
    let peer = Peer::start(Mode::Success, false).await;
    let missing = fixture.path().join("missing");
    let output = invoke(&[
        "program",
        "publish",
        "--source",
        path(&missing),
        "--server",
        &peer.url,
    ])
    .await;
    assert_eq!(output.status.code(), Some(1));
    let source = fixture.path().join("prepared");
    fs::write(source.join("ledgence-program.json"), b"invalid manifest").unwrap();
    let output = invoke(&[
        "program",
        "publish",
        "--source",
        path(&source),
        "--server",
        &peer.url,
    ])
    .await;
    assert_eq!(output.status.code(), Some(2));
    assert!(peer.puts.lock().unwrap().is_empty());
}
