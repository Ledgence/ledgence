//! Exercise the actual CLI process against an explicitly controlled HTTP peer.
use ledgence_adapter_artifact::{ArtifactLimits, ZipProgramPackageInspector, pack_directory};
use ledgence_worker_api::{ProgramPackageInspector, ProgramPackageProfile, PublicationLimits};
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
    headers: Arc<Mutex<Vec<String>>>,
    expanded_entries: Arc<Mutex<Option<u64>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Peer {
    async fn start(mode: Mode, fail_first_registration: bool) -> Self {
        Self::with_profile(mode, fail_first_registration, None).await
    }
    async fn with_profile(
        mode: Mode,
        fail_first_registration: bool,
        expanded_entries: Option<u64>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let puts = Arc::new(Mutex::new(Vec::new()));
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(AtomicUsize::new(0));
        let headers = Arc::new(Mutex::new(Vec::new()));
        let expanded_entries = Arc::new(Mutex::new(expanded_entries));
        let (put_records, registration_records, request_count) =
            (puts.clone(), registrations.clone(), requests.clone());
        let (header_records, selected_entries) = (headers.clone(), expanded_entries.clone());
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (header, body) = read(&mut socket).await;
                header_records.lock().unwrap().push(header.clone());
                request_count.fetch_add(1, Ordering::SeqCst);
                let route = header.lines().next().unwrap();
                if route.starts_with("GET /v1/programs/publication-capabilities ") {
                    if matches!(mode, Mode::OldServer) {
                        respond(&mut socket, 404, json!({"code":"route_not_found"})).await;
                        continue;
                    }
                    let entries = *selected_entries.lock().unwrap();
                    let mut limits = entries.map_or_else(PublicationLimits::default, |_| {
                        ProgramPackageProfile::Entries8192V1.limits()
                    });
                    if let Some(entries) = entries {
                        limits.max_entries = entries;
                    }
                    respond_with_profile(&mut socket, 200, json!({"enabled":true,"registration_enabled":true,"mode":"immutable", "limits":limits,"max_concurrent_uploads":2,"transfer_timeout_ms":120000}), entries.is_some()).await;
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
            headers,
            expanded_entries,
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
    respond_with_profile(socket, status, value, false).await;
}
async fn respond_with_profile(socket: &mut TcpStream, status: u16, value: Value, expanded: bool) {
    let body = serde_json::to_vec(&value).unwrap();
    let profile = if expanded {
        "x-ledgence-program-package-profile: entries-8192-v1\r\n"
    } else {
        ""
    };
    let header = format!(
        "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\n{profile}Request-Id: req-publication-test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await.unwrap();
    socket.write_all(&body).await.unwrap();
    socket.shutdown().await.unwrap();
}
fn large_source() -> tempfile::TempDir {
    let directory = source();
    // Actual empty directories are ZIP entries too. Include app and manifest
    // to make a real package one entry above the released legacy profile.
    for i in 0..PublicationLimits::default().max_entries - 1 {
        fs::create_dir(directory.path().join(format!("prepared/entry-{i:05}"))).unwrap();
    }
    directory
}
fn profile(header: &str) -> Option<&str> {
    header.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("x-ledgence-program-package-profile")
            .then(|| value.trim())
    })
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
async fn filesystem_publish_explicitly_uses_the_expanded_profile() {
    let fixture = large_source();
    let store = fixture.path().join("store");
    let output = invoke(&[
        "program",
        "publish",
        "--source",
        path(&fixture.path().join("prepared")),
        "--store",
        path(&store),
    ])
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let descriptor: ledgence_worker_api::ProgramDescriptor =
        serde_json::from_slice(&output.stdout).unwrap();
    let archive = fs::read(store.join(format!("blobs/{}.zip", &descriptor.digest.0[7..]))).unwrap();
    let inspected = ZipProgramPackageInspector
        .inspect(
            &descriptor,
            &archive,
            &ProgramPackageProfile::Entries8192V1.limits(),
        )
        .unwrap();
    assert_eq!(
        inspected.entry_count,
        PublicationLimits::default().max_entries + 1
    );
    assert!(
        ZipProgramPackageInspector
            .inspect(&descriptor, &archive, &PublicationLimits::default())
            .is_err()
    );
}

#[tokio::test]
async fn small_publications_use_the_selected_response_profile_even_with_lower_entry_bounds() {
    // An expanded peer can report a stricter entry ceiling than legacy. Its
    // response header, not the numeric maximum, selects the PUT profile.
    for entries in [None, Some(2)] {
        let fixture = source();
        let peer = Peer::with_profile(Mode::Success, false, entries).await;
        let output = invoke(&[
            "program",
            "publish",
            "--source",
            path(&fixture.path().join("prepared")),
            "--server",
            &peer.url,
        ])
        .await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(peer.puts.lock().unwrap().len(), 1);
        for header in peer.headers.lock().unwrap().iter() {
            let expected = if header.starts_with("GET ") || entries.is_some() {
                Some("entries-8192-v1")
            } else {
                None
            };
            assert_eq!(profile(header), expected);
        }
    }
}

#[tokio::test]
async fn legacy_and_stricter_expanded_peers_reject_large_packages_before_put() {
    for entries in [None, Some(PublicationLimits::default().max_entries)] {
        let fixture = large_source();
        let peer = Peer::with_profile(Mode::Success, false, entries).await;
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
        let result = report(&output);
        assert_eq!(result["phase"], "prepared");
        assert_eq!(result["publication_outcome"], "unconfirmed");
        assert!(peer.puts.lock().unwrap().is_empty());
        assert!(
            peer.headers
                .lock()
                .unwrap()
                .iter()
                .all(|h| h.starts_with("GET "))
        );
        let receipt = Path::new(result["receipt"].as_str().unwrap());
        assert!(receipt.is_file());
        assert!(receipt.with_file_name("artifact.zip").is_file());
    }
}

#[tokio::test]
async fn expanded_receipt_resume_renegotiates_and_keeps_the_exact_archive_on_downgrade() {
    let fixture = large_source();
    let peer = Peer::with_profile(
        Mode::DisconnectFirst,
        false,
        Some(ProgramPackageProfile::Entries8192V1.limits().max_entries),
    )
    .await;
    let first = invoke(&[
        "program",
        "publish",
        "--source",
        path(&fixture.path().join("prepared")),
        "--server",
        &peer.url,
    ])
    .await;
    assert!(!first.status.success());
    let initial = report(&first);
    assert_eq!(initial["publication_outcome"], "unknown");
    let receipt = Path::new(initial["receipt"].as_str().unwrap());
    let archive = fs::read(receipt.with_file_name("artifact.zip")).unwrap();
    assert_eq!(
        peer.puts.lock().unwrap().as_slice(),
        std::slice::from_ref(&archive)
    );
    fs::write(
        fixture.path().join("prepared/app.py"),
        "source changed after first upload",
    )
    .unwrap();

    *peer.expanded_entries.lock().unwrap() = None;
    let rejected = invoke(&["program", "publish", "--resume", path(receipt)]).await;
    assert!(!rejected.status.success());
    assert_eq!(report(&rejected)["publication_outcome"], "unconfirmed");
    assert_eq!(peer.puts.lock().unwrap().len(), 1);
    assert_eq!(
        fs::read(receipt.with_file_name("artifact.zip")).unwrap(),
        archive
    );

    *peer.expanded_entries.lock().unwrap() =
        Some(ProgramPackageProfile::Entries8192V1.limits().max_entries);
    let resumed = invoke(&["program", "publish", "--resume", path(receipt)]).await;
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert_eq!(report(&resumed)["publication_outcome"], "confirmed");
    let bodies = peer.puts.lock().unwrap();
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[0], bodies[1]);
    assert_eq!(bodies[1], archive);
    assert!(
        peer.headers
            .lock()
            .unwrap()
            .iter()
            .filter(|h| h.starts_with("PUT "))
            .all(|header| profile(header) == Some("entries-8192-v1"))
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
