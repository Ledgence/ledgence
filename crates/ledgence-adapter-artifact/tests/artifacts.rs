use ledgence_adapter_artifact::{
    ArtifactLimits, FileArtifactCache, FileProgramStore, HttpProgramStore, publish_directory,
};
use ledgence_worker_api::{
    ArtifactCache, Digest, ErrorKind, Platform, ProgramDescriptor, ProgramManifest, ProgramRef,
    ProgramStore, PythonRuntime,
};
use sha2::{Digest as _, Sha256};
use std::{
    fs,
    io::{Cursor, Write},
    path::Path,
};
use zip::{ZipWriter, write::SimpleFileOptions};

fn manifest(version: &str) -> ProgramManifest {
    ProgramManifest {
        schema_version: 1,
        program: ProgramRef {
            id: "example".into(),
            version: version.into(),
        },
        runtime: PythonRuntime {
            kind: "python".into(),
            python: "3.12".into(),
            protocol: 1,
        },
        handler: "app:handle".into(),
        platform: Platform {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
        },
    }
}
fn archive(version: &str, extras: &[(&str, &[u8])]) -> (ProgramDescriptor, Vec<u8>) {
    let value = manifest(version);
    let json = serde_json::to_vec(&value).unwrap();
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o644);
    for (name, bytes) in
        std::iter::once(("ledgence-program.json", json.as_slice())).chain(extras.iter().copied())
    {
        writer.start_file(name, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    let bytes = writer.finish().unwrap().into_inner();
    (describe(value.program, &bytes), bytes)
}
fn describe(program: ProgramRef, bytes: &[u8]) -> ProgramDescriptor {
    ProgramDescriptor {
        program,
        digest: Digest(format!("sha256:{:x}", Sha256::digest(bytes))),
        size: bytes.len() as u64,
    }
}
fn cache(root: &Path) -> FileArtifactCache {
    FileArtifactCache::new(root, ArtifactLimits::default()).unwrap()
}
fn footprint(descriptor: &ProgramDescriptor, expanded: u64) -> u64 {
    descriptor.size + expanded + serde_json::to_vec(descriptor).unwrap().len() as u64
}

#[tokio::test]
async fn publishes_hits_and_reopens_verified_cache() {
    let root = tempfile::tempdir().unwrap();
    let cache = cache(root.path());
    let (descriptor, bytes) = archive("v1", &[("app.py", b"def handle(event): return event")]);
    assert!(cache.lookup(&descriptor).await.unwrap().is_none());
    let prepared = cache.publish(&descriptor, bytes).await.unwrap();
    assert_eq!(
        fs::read(prepared.root().join("app.py")).unwrap(),
        b"def handle(event): return event"
    );
    let hit = cache.lookup(&descriptor).await.unwrap().unwrap();
    assert_eq!(hit.root(), prepared.root());
    assert_eq!(hit.manifest().program, descriptor.program);
    assert!(
        fs::metadata(hit.root().join("app.py"))
            .unwrap()
            .permissions()
            .readonly()
    );
    drop(hit);
    drop(prepared);
    drop(cache);
    let cache = FileArtifactCache::new(root.path(), ArtifactLimits::default()).unwrap();
    assert!(cache.lookup(&descriptor).await.unwrap().is_some());
}
#[tokio::test]
async fn pins_survive_cache_drop_and_block_other_owners() {
    let root = tempfile::tempdir().unwrap();
    let cache = cache(root.path());
    let (descriptor, bytes) = archive("v1", &[("app.py", b"pass")]);
    let prepared = cache.publish(&descriptor, bytes).await.unwrap();
    assert!(FileArtifactCache::new(root.path(), ArtifactLimits::default()).is_err());
    drop(cache);
    assert!(FileArtifactCache::new(root.path(), ArtifactLimits::default()).is_err());
    assert!(prepared.root().exists());
    drop(prepared);
    assert!(FileArtifactCache::new(root.path(), ArtifactLimits::default()).is_ok());
}
#[tokio::test]
async fn pinned_entries_reject_pressure_then_unpinned_entry_is_evicted() {
    let root = tempfile::tempdir().unwrap();
    let (first, first_bytes) = archive("v1", &[("app.py", b"pass")]);
    let (second, second_bytes) = archive("v2", &[("app.py", b"pass")]);
    let expanded = serde_json::to_vec(&manifest("v1")).unwrap().len() as u64 + 4;
    let limits = ArtifactLimits {
        max_cache_bytes: footprint(&first, expanded),
        ..Default::default()
    };
    let cache = FileArtifactCache::new(root.path(), limits).unwrap();
    let prepared = cache.publish(&first, first_bytes).await.unwrap();
    let clone = prepared.clone();
    drop(prepared);
    assert_eq!(
        cache
            .publish(&second, second_bytes.clone())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Capacity
    );
    drop(clone);
    let second_handle = cache.publish(&second, second_bytes).await.unwrap();
    assert!(cache.lookup(&first).await.unwrap().is_none());
    assert!(second_handle.root().exists());
}
#[tokio::test]
async fn digest_or_size_mismatch_never_publishes() {
    let root = tempfile::tempdir().unwrap();
    let cache = cache(root.path());
    let (descriptor, mut bytes) = archive("v1", &[("app.py", b"pass")]);
    bytes[0] ^= 1;
    assert_eq!(
        cache.publish(&descriptor, bytes).await.unwrap_err().kind,
        ErrorKind::Integrity
    );
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
}
#[tokio::test]
async fn unsafe_and_colliding_paths_are_rejected_without_escape() {
    let root = tempfile::tempdir().unwrap();
    let cache = cache(root.path());
    for path in [
        "../escape",
        "/absolute",
        "a/../../escape",
        "a\\b",
        "C:/escape",
        "a//b",
        "a/./b",
        "CON",
        "a.",
        "é.py",
    ] {
        let (descriptor, bytes) = archive("v1", &[(path, b"x")]);
        assert!(
            cache.publish(&descriptor, bytes).await.is_err(),
            "accepted {path}"
        );
    }
    for entries in [
        vec![("A.py", b"x".as_slice()), ("a.py", b"y".as_slice())],
        vec![("pkg/a", b"x".as_slice()), ("PKG/b", b"y".as_slice())],
        vec![("pkg", b"x".as_slice()), ("pkg/a", b"y".as_slice())],
    ] {
        let (descriptor, bytes) = archive("v1", &entries);
        assert!(cache.publish(&descriptor, bytes).await.is_err());
    }
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
}
#[tokio::test]
async fn duplicate_raw_records_and_symlinks_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let cache = cache(root.path());
    let (original, mut bytes) = archive("v1", &[("a.py", b"x"), ("b.py", b"y")]);
    for index in 0..bytes.len() - 4 {
        if &bytes[index..index + 4] == b"b.py" {
            bytes[index] = b'a';
        }
    }
    let duplicate = describe(original.program, &bytes);
    assert_eq!(
        cache.publish(&duplicate, bytes).await.unwrap_err().kind,
        ErrorKind::Integrity
    );
    let value = manifest("v1");
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file("ledgence-program.json", SimpleFileOptions::default())
        .unwrap();
    writer
        .write_all(&serde_json::to_vec(&value).unwrap())
        .unwrap();
    writer
        .add_symlink("app.py", "../outside", SimpleFileOptions::default())
        .unwrap();
    let bytes = writer.finish().unwrap().into_inner();
    let descriptor = describe(value.program, &bytes);
    assert_eq!(
        cache.publish(&descriptor, bytes).await.unwrap_err().kind,
        ErrorKind::Integrity
    );
}
#[tokio::test]
async fn crc_failure_rolls_back_staging() {
    let root = tempfile::tempdir().unwrap();
    let cache = cache(root.path());
    let (original, mut bytes) = archive("v1", &[("app.py", b"unique-python-code")]);
    let position = bytes
        .windows(18)
        .position(|v| v == b"unique-python-code")
        .unwrap();
    bytes[position] ^= 1;
    let descriptor = describe(original.program, &bytes);
    assert!(cache.publish(&descriptor, bytes).await.is_err());
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
}
#[tokio::test]
async fn manifest_identity_and_runtime_are_checked() {
    let root = tempfile::tempdir().unwrap();
    let cache = cache(root.path());
    let (mut descriptor, bytes) = archive("v1", &[("app.py", b"pass")]);
    descriptor.program.version = "v2".into();
    assert_eq!(
        cache.publish(&descriptor, bytes).await.unwrap_err().kind,
        ErrorKind::Integrity
    );
    let mut value = manifest("v1");
    value.runtime.kind = "node".into();
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file("ledgence-program.json", SimpleFileOptions::default())
        .unwrap();
    writer
        .write_all(&serde_json::to_vec(&value).unwrap())
        .unwrap();
    let bytes = writer.finish().unwrap().into_inner();
    let descriptor = describe(value.program, &bytes);
    assert_eq!(
        cache.publish(&descriptor, bytes).await.unwrap_err().kind,
        ErrorKind::Incompatible
    );
}
#[tokio::test]
async fn entry_and_expansion_limits_prevent_publication() {
    let (descriptor, bytes) = archive("v1", &[("app.py", &[0; 1024])]);
    for limits in [
        ArtifactLimits {
            max_entries: 1,
            ..Default::default()
        },
        ArtifactLimits {
            max_expanded_bytes: 100,
            ..Default::default()
        },
        ArtifactLimits {
            max_file_bytes: 512,
            ..Default::default()
        },
        ArtifactLimits {
            max_archive_bytes: 100,
            ..Default::default()
        },
        ArtifactLimits {
            max_manifest_bytes: 10,
            ..Default::default()
        },
    ] {
        let root = tempfile::tempdir().unwrap();
        let cache = FileArtifactCache::new(root.path(), limits).unwrap();
        assert!(cache.publish(&descriptor, bytes.clone()).await.is_err());
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }
}
#[tokio::test]
async fn tampered_materialization_and_extra_files_are_never_hits() {
    let root = tempfile::tempdir().unwrap();
    let cache = cache(root.path());
    let (descriptor, bytes) = archive("v1", &[("app.py", b"pass")]);
    let prepared = cache.publish(&descriptor, bytes).await.unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            prepared.root().join("app.py"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
    }
    fs::write(prepared.root().join("app.py"), b"evil").unwrap();
    assert_eq!(
        cache.lookup(&descriptor).await.unwrap_err().kind,
        ErrorKind::Integrity
    );
}
#[tokio::test]
async fn publication_is_deterministic_immutable_and_loads_through_store() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(
        source.join("ledgence-program.json"),
        serde_json::to_vec(&manifest("v1")).unwrap(),
    )
    .unwrap();
    fs::write(source.join("app.py"), b"pass").unwrap();
    let target = root.path().join("store");
    let limits = ArtifactLimits::default();
    let first = publish_directory(&source, &target, &limits).unwrap();
    assert_eq!(first, publish_directory(&source, &target, &limits).unwrap());
    let store = FileProgramStore::new(&target, limits).unwrap();
    assert_eq!(first, store.resolve(&first.program).await.unwrap());
    let cache = cache(&root.path().join("cache"));
    let prepared = cache
        .publish(&first, store.fetch(&first).await.unwrap())
        .await
        .unwrap();
    assert_eq!(fs::read(prepared.root().join("app.py")).unwrap(), b"pass");
    fs::write(source.join("app.py"), b"changed").unwrap();
    assert_eq!(
        publish_directory(&source, &target, &ArtifactLimits::default())
            .unwrap_err()
            .kind,
        ErrorKind::Integrity
    );
    assert_eq!(first, store.resolve(&first.program).await.unwrap());
}
#[tokio::test]
async fn file_store_rejects_wrong_descriptor_identity_and_symlink_escape() {
    let root = tempfile::tempdir().unwrap();
    let (descriptor, _) = archive("v1", &[("app.py", b"pass")]);
    let requested = ProgramRef {
        id: "example".into(),
        version: "v2".into(),
    };
    let release = root.path().join("programs/example/v2");
    fs::create_dir_all(&release).unwrap();
    fs::write(
        release.join("descriptor.json"),
        serde_json::to_vec(&descriptor).unwrap(),
    )
    .unwrap();
    let store = FileProgramStore::new(root.path(), ArtifactLimits::default()).unwrap();
    assert_eq!(
        store.resolve(&requested).await.unwrap_err().kind,
        ErrorKind::Integrity
    );
    #[cfg(unix)]
    {
        let outside = tempfile::NamedTempFile::new().unwrap();
        fs::remove_file(release.join("descriptor.json")).unwrap();
        std::os::unix::fs::symlink(outside.path(), release.join("descriptor.json")).unwrap();
        assert_eq!(
            store.resolve(&requested).await.unwrap_err().kind,
            ErrorKind::Integrity
        );
    }
}
#[test]
fn http_requires_secure_or_literal_loopback_urls() {
    for url in [
        "http://example.com",
        "http://localhost",
        "ftp://127.0.0.1",
        "https://user:secret@example.com",
        "https://example.com/?secret=x",
    ] {
        assert!(HttpProgramStore::new(url, ArtifactLimits::default()).is_err());
    }
    assert!(HttpProgramStore::new("https://example.com/prefix", ArtifactLimits::default()).is_ok());
    assert!(HttpProgramStore::new("http://[::1]:1234", ArtifactLimits::default()).is_ok());
}

async fn serve(responses: Vec<Vec<u8>>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let length = socket.read(&mut request).await.unwrap();
            requests.push(String::from_utf8_lossy(&request[..length]).into_owned());
            socket.write_all(&response).await.unwrap();
        }
        requests
    });
    (format!("http://{address}/prefix/"), task)
}
fn response(bytes: &[u8]) -> Vec<u8> {
    let mut result = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        bytes.len()
    )
    .into_bytes();
    result.extend_from_slice(bytes);
    result
}
#[tokio::test]
async fn http_store_resolves_and_downloads_over_loopback() {
    let (descriptor, bytes) = archive("v1", &[("app.py", b"pass")]);
    let (url, server) = serve(vec![
        response(&serde_json::to_vec(&descriptor).unwrap()),
        response(&bytes),
    ])
    .await;
    let store = HttpProgramStore::new(&url, ArtifactLimits::default()).unwrap();
    assert_eq!(
        store.resolve(&descriptor.program).await.unwrap(),
        descriptor
    );
    assert_eq!(store.fetch(&descriptor).await.unwrap(), bytes);
    let requests = server.await.unwrap();
    assert!(requests[0].starts_with("GET /prefix/programs/example/v1/descriptor.json "));
    assert!(requests[1].contains(&format!("/prefix/blobs/{}.zip", descriptor.digest.hex())));
}
#[tokio::test]
async fn http_streaming_limits_and_redirects_are_enforced() {
    let (descriptor, _) = archive("v1", &[("app.py", b"pass")]);
    let mut chunked =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
    chunked.extend_from_slice(format!("{:x}\r\n", descriptor.size + 1).as_bytes());
    chunked.extend(vec![0; descriptor.size as usize + 1]);
    chunked.extend_from_slice(b"\r\n0\r\n\r\n");
    let (url, server) = serve(vec![chunked]).await;
    let store = HttpProgramStore::new(&url, ArtifactLimits::default()).unwrap();
    assert_eq!(
        store.fetch(&descriptor).await.unwrap_err().kind,
        ErrorKind::InvalidInput
    );
    server.await.unwrap();
    let (url, server) = serve(vec![b"HTTP/1.1 302 Found\r\nLocation: http://example.com/evil\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()]).await;
    let store = HttpProgramStore::new(&url, ArtifactLimits::default()).unwrap();
    assert_eq!(
        store.fetch(&descriptor).await.unwrap_err().kind,
        ErrorKind::Unavailable
    );
    server.await.unwrap();
}

#[tokio::test]
async fn publication_accepts_cross_target_package_but_worker_cache_rejects_it() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    let mut target_manifest = manifest("v1");
    target_manifest.platform.os = if std::env::consts::OS == "linux" {
        "macos"
    } else {
        "linux"
    }
    .into();
    fs::write(
        source.join("ledgence-program.json"),
        serde_json::to_vec(&target_manifest).unwrap(),
    )
    .unwrap();
    fs::write(source.join("app.py"), b"pass").unwrap();
    let store_path = root.path().join("store");
    let descriptor = publish_directory(&source, &store_path, &ArtifactLimits::default()).unwrap();
    let store = FileProgramStore::new(&store_path, ArtifactLimits::default()).unwrap();
    assert_eq!(
        store.resolve(&descriptor.program).await.unwrap(),
        descriptor
    );
    let cache = cache(&root.path().join("cache"));
    assert_eq!(
        cache
            .publish(&descriptor, store.fetch(&descriptor).await.unwrap())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Incompatible
    );
}

#[tokio::test]
async fn failed_atomic_rename_removes_readonly_staging_tree() {
    let root = tempfile::tempdir().unwrap();
    let cache = cache(root.path());
    let (descriptor, bytes) = archive("v1", &[("pkg/app.py", b"pass")]);
    // Simulate an unexpected filesystem collision after the owner's initial scan.
    // The failed publication must clean even fully readonly extracted trees.
    let collision = root.path().join(descriptor.digest.hex());
    fs::create_dir(&collision).unwrap();
    fs::write(collision.join("sentinel"), b"preserve").unwrap();
    assert!(cache.publish(&descriptor, bytes).await.is_err());
    let names: Vec<_> = fs::read_dir(root.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(
        !names
            .iter()
            .any(|name| name.to_string_lossy().starts_with(".staging-"))
    );
    assert_eq!(fs::read(collision.join("sentinel")).unwrap(), b"preserve");
}

#[tokio::test]
async fn artifact_larger_than_entire_cache_is_a_hard_limit() {
    let root = tempfile::tempdir().unwrap();
    let cache = FileArtifactCache::new(
        root.path(),
        ArtifactLimits {
            max_cache_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let (descriptor, bytes) = archive("v1", &[("app.py", b"pass")]);
    assert_eq!(
        cache.publish(&descriptor, bytes).await.unwrap_err().kind,
        ErrorKind::InvalidInput
    );
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn publication_preserves_executable_dependencies_and_detects_mode_changes() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(
        source.join("ledgence-program.json"),
        serde_json::to_vec(&manifest("v1")).unwrap(),
    )
    .unwrap();
    fs::write(
        source.join("helper.sh"),
        b"#!/bin/sh\nprintf 'dependency works'\n",
    )
    .unwrap();
    // Neither setuid/setgid nor source write permissions belong in the cache.
    fs::set_permissions(source.join("helper.sh"), fs::Permissions::from_mode(0o6751)).unwrap();
    let store_root = root.path().join("store");
    let descriptor = publish_directory(&source, &store_root, &ArtifactLimits::default()).unwrap();
    assert_eq!(
        descriptor,
        publish_directory(&source, &store_root, &ArtifactLimits::default()).unwrap()
    );
    let store = FileProgramStore::new(&store_root, ArtifactLimits::default()).unwrap();
    let bytes = store.fetch(&descriptor).await.unwrap();
    let mut zip = zip::ZipArchive::new(Cursor::new(&bytes)).unwrap();
    assert_eq!(
        zip.by_name("helper.sh").unwrap().unix_mode().unwrap() & 0o7777,
        0o755
    );
    let cache = cache(&root.path().join("cache"));
    let prepared = cache.publish(&descriptor, bytes).await.unwrap();
    let helper = prepared.root().join("helper.sh");
    assert_eq!(
        fs::metadata(&helper).unwrap().permissions().mode() & 0o7777,
        0o555
    );
    let output = std::process::Command::new(&helper).output().unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"dependency works");
    assert!(cache.lookup(&descriptor).await.unwrap().is_some());
    fs::set_permissions(helper, fs::Permissions::from_mode(0o444)).unwrap();
    assert_eq!(
        cache.lookup(&descriptor).await.unwrap_err().kind,
        ErrorKind::Integrity
    );
}

#[tokio::test]
async fn publication_preserves_empty_directories_and_counts_them_towards_limits() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir_all(source.join("namespace/nested")).unwrap();
    fs::create_dir(source.join("other_empty")).unwrap();
    fs::write(
        source.join("ledgence-program.json"),
        serde_json::to_vec(&manifest("v1")).unwrap(),
    )
    .unwrap();
    let store_root = root.path().join("store");
    let descriptor = publish_directory(&source, &store_root, &ArtifactLimits::default()).unwrap();
    assert_eq!(
        descriptor,
        publish_directory(&source, &store_root, &ArtifactLimits::default()).unwrap()
    );
    let store = FileProgramStore::new(&store_root, ArtifactLimits::default()).unwrap();
    let bytes = store.fetch(&descriptor).await.unwrap();
    let mut zip = zip::ZipArchive::new(Cursor::new(&bytes)).unwrap();
    assert_eq!(zip.len(), 4);
    for name in ["namespace/", "namespace/nested/", "other_empty/"] {
        assert!(zip.by_name(name).unwrap().is_dir());
    }
    let cache_root = root.path().join("cache");
    let cache = cache(&cache_root);
    let prepared = cache.publish(&descriptor, bytes).await.unwrap();
    for name in ["namespace/nested", "other_empty"] {
        assert!(prepared.root().join(name).is_dir());
        assert_eq!(fs::read_dir(prepared.root().join(name)).unwrap().count(), 0);
    }
    drop(prepared);
    drop(cache);
    let reopened = FileArtifactCache::new(cache_root, ArtifactLimits::default()).unwrap();
    assert!(reopened.lookup(&descriptor).await.unwrap().is_some());
    let limited = ArtifactLimits {
        max_entries: 3,
        ..Default::default()
    };
    assert_eq!(
        publish_directory(&source, root.path().join("limited"), &limited)
            .unwrap_err()
            .kind,
        ErrorKind::InvalidInput
    );
}

#[cfg(unix)]
#[test]
fn fifo_descriptors_blobs_and_source_manifest_are_rejected_before_blocking_open() {
    use nix::{fcntl::OFlag, sys::stat::Mode, unistd::mkfifo};
    use std::{os::unix::fs::OpenOptionsExt, sync::mpsc, time::Duration};
    let root = tempfile::tempdir().unwrap();
    let (descriptor, _) = archive("v1", &[("app.py", b"pass")]);
    let store_root = root.path().join("store");
    let release = store_root.join("programs/example/v1");
    fs::create_dir_all(&release).unwrap();
    fs::create_dir(store_root.join("blobs")).unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    let paths = [
        release.join("descriptor.json"),
        store_root.join(format!("blobs/{}.zip", descriptor.digest.hex())),
        source.join("ledgence-program.json"),
    ];
    for path in &paths {
        mkfifo(path, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
    }
    let (sender, receiver) = mpsc::channel();
    let source_for_worker = source.clone();
    let output_store = root.path().join("output-store");
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let store = FileProgramStore::new(&store_root, ArtifactLimits::default()).unwrap();
        let outcomes = runtime.block_on(async {
            [
                store.resolve(&descriptor.program).await.unwrap_err().kind,
                store.fetch(&descriptor).await.unwrap_err().kind,
                publish_directory(&source_for_worker, output_store, &ArtifactLimits::default())
                    .unwrap_err()
                    .kind,
            ]
        });
        sender.send(outcomes).unwrap();
    });
    let result = receiver.recv_timeout(Duration::from_secs(1));
    if result.is_err() {
        // Unblock a regressed implementation before failing the test, avoiding
        // stranded blocking tasks. Correct code never needs a FIFO writer.
        for path in paths {
            let mut keeper = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(OFlag::O_NONBLOCK.bits())
                .open(path)
                .unwrap();
            keeper.write_all(b"{}\n").unwrap();
            std::thread::sleep(Duration::from_millis(30));
        }
    }
    worker.join().unwrap();
    assert_eq!(
        result.expect("special files blocked a local read"),
        [ErrorKind::Integrity; 3]
    );
}

#[cfg(unix)]
#[test]
fn publication_rejects_special_files_and_links_in_prepared_dependencies() {
    use nix::{sys::stat::Mode, unistd::mkfifo};
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(
        source.join("ledgence-program.json"),
        serde_json::to_vec(&manifest("v1")).unwrap(),
    )
    .unwrap();
    let dependency = source.join("dependency");
    mkfifo(&dependency, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
    assert_eq!(
        publish_directory(
            &source,
            root.path().join("store"),
            &ArtifactLimits::default()
        )
        .unwrap_err()
        .kind,
        ErrorKind::Integrity
    );
    fs::remove_file(&dependency).unwrap();
    std::os::unix::fs::symlink(source.join("ledgence-program.json"), &dependency).unwrap();
    assert_eq!(
        publish_directory(
            &source,
            root.path().join("store"),
            &ArtifactLimits::default()
        )
        .unwrap_err()
        .kind,
        ErrorKind::Integrity
    );
}

#[cfg(unix)]
#[tokio::test]
async fn unwritable_cache_parent_cannot_partially_delete_a_live_entry() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let (first, first_bytes) = archive("v1", &[("app.py", b"pass")]);
    let (second, second_bytes) = archive("v2", &[("app.py", b"pass")]);
    let expanded = serde_json::to_vec(&manifest("v1")).unwrap().len() as u64 + 4;
    let limits = ArtifactLimits {
        max_cache_bytes: footprint(&first, expanded),
        ..Default::default()
    };
    let cache = FileArtifactCache::new(root.path(), limits.clone()).unwrap();
    drop(cache.publish(&first, first_bytes).await.unwrap());
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o500)).unwrap();
    let result = cache.publish(&second, second_bytes).await;
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    // A privileged test user may bypass directory permissions entirely. The
    // injected partial-removal unit tests still exercise recovery in that case.
    if result.is_ok() {
        return;
    }
    assert_eq!(result.unwrap_err().kind, ErrorKind::Io);
    let hit = cache.lookup(&first).await.unwrap().unwrap();
    assert_eq!(fs::read(hit.root().join("app.py")).unwrap(), b"pass");
    drop(hit);
    drop(cache);
    let reopened = FileArtifactCache::new(root.path(), limits).unwrap();
    assert!(reopened.lookup(&first).await.unwrap().is_some());
}
