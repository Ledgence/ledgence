use super::*;
use ledgence_worker_api::{Platform, PythonRuntime};
use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn prepare(source: &Path, contents: &[u8]) -> PackedProgram {
    fs::create_dir_all(source).unwrap();
    let manifest = ProgramManifest {
        schema_version: 1,
        program: ProgramRef {
            id: "publication-test".into(),
            version: "1.0.0".into(),
        },
        runtime: PythonRuntime {
            kind: "python".into(),
            python: "3.14".into(),
            protocol: 3,
        },
        handler: "app:handle".into(),
        platform: Platform {
            os: "linux".into(),
            arch: "x86_64".into(),
        },
    };
    fs::write(
        source.join("ledgence-program.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    fs::write(source.join("app.py"), contents).unwrap();
    pack_directory(source, &ArtifactLimits::default()).unwrap()
}

fn persist(packed: &PackedProgram, store: &Path) -> PublicationResult<PublishArtifactResult> {
    persist_archive(
        packed.descriptor.program.clone(),
        packed.descriptor.digest.clone(),
        packed.archive.clone(),
        store,
        &ArtifactLimits::default(),
    )
}

fn release(store: &Path) -> PathBuf {
    store.join("programs/publication-test/1.0.0/descriptor.json")
}
fn blob(store: &Path, packed: &PackedProgram) -> PathBuf {
    store
        .join("blobs")
        .join(format!("{}.zip", packed.descriptor.digest.hex()))
}

#[test]
fn immutable_results_follow_descriptor_not_preexisting_blob() {
    let root = tempfile::tempdir().unwrap();
    let packed = prepare(&root.path().join("source"), b"def handle(e): return e");
    let store = root.path().join("store");
    fs::create_dir_all(store.join("blobs")).unwrap();
    fs::write(blob(&store, &packed), &packed.archive).unwrap();
    assert!(!persist(&packed, &store).unwrap().already_published);
    assert!(persist(&packed, &store).unwrap().already_published);
    let changed = prepare(&root.path().join("changed"), b"def handle(e): return 42");
    assert_eq!(
        persist(&changed, &store).unwrap_err().kind,
        PublicationErrorKind::ImmutableConflict
    );
    assert_eq!(fs::read(blob(&store, &packed)).unwrap(), packed.archive);
    assert!(
        blob(&store, &changed).exists(),
        "a conflict must not delete its unreferenced immutable blob"
    );
    assert_eq!(
        serde_json::from_slice::<ProgramDescriptor>(&fs::read(release(&store)).unwrap()).unwrap(),
        packed.descriptor
    );
}

#[test]
fn corrupted_blob_is_not_reconciled_as_an_identical_publication() {
    let root = tempfile::tempdir().unwrap();
    let packed = prepare(&root.path().join("source"), b"pass");
    let store = root.path().join("store");
    fs::create_dir_all(store.join("blobs")).unwrap();
    fs::write(blob(&store, &packed), b"corrupt").unwrap();
    let error = persist(&packed, &store).unwrap_err();
    assert_eq!(error.kind, PublicationErrorKind::Storage);
    assert!(!error.message.contains(root.path().to_str().unwrap()));
    assert!(!release(&store).exists());
}

#[test]
fn process_publication_child() {
    let Some(root) = std::env::var_os("LEDGENCE_TEST_PUBLICATION_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let index = std::env::var("LEDGENCE_TEST_PUBLICATION_INDEX").unwrap();
    let variant = std::env::var("LEDGENCE_TEST_PUBLICATION_VARIANT").unwrap();
    let archive = fs::read(root.join(format!("archive-{variant}.zip"))).unwrap();
    let descriptor: ProgramDescriptor =
        serde_json::from_slice(&fs::read(root.join(format!("descriptor-{variant}.json"))).unwrap())
            .unwrap();
    fs::write(root.join(format!("ready-{index}")), b"ready").unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !root.join("go").exists() {
        assert!(
            Instant::now() < deadline,
            "parent did not release process barrier"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let stop = std::env::var("LEDGENCE_TEST_PUBLICATION_STOP").unwrap_or_default();
    let result = persist_with_observer(
        descriptor.program,
        descriptor.digest,
        archive,
        &root.join("store"),
        &ArtifactLimits::default(),
        |stage| {
            if format!("{stage:?}") == stop {
                std::process::exit(77);
            }
            Ok(())
        },
    );
    fs::write(
        root.join(format!("result-{index}.json")),
        serde_json::to_vec(&result).unwrap(),
    )
    .unwrap();
}

fn child(root: &Path, index: usize, variant: usize, stop: &str) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "publish::tests::process_publication_child",
            "--nocapture",
        ])
        .env("LEDGENCE_TEST_PUBLICATION_ROOT", root)
        .env("LEDGENCE_TEST_PUBLICATION_INDEX", index.to_string())
        .env("LEDGENCE_TEST_PUBLICATION_VARIANT", variant.to_string())
        .env("LEDGENCE_TEST_PUBLICATION_STOP", stop)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
}
fn write_input(root: &Path, index: usize, packed: &PackedProgram) {
    fs::write(root.join(format!("archive-{index}.zip")), &packed.archive).unwrap();
    fs::write(
        root.join(format!("descriptor-{index}.json")),
        serde_json::to_vec(&packed.descriptor).unwrap(),
    )
    .unwrap();
}
fn barrier(root: &Path, count: usize) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while (0..count).any(|i| !root.join(format!("ready-{i}")).exists()) {
        assert!(
            Instant::now() < deadline,
            "child did not reach process barrier"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    fs::write(root.join("go"), b"go").unwrap();
}

#[test]
fn concurrent_processes_have_one_descriptor_winner() {
    for conflicting in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let first = prepare(&root.path().join("source0"), b"pass");
        let second = prepare(&root.path().join("source1"), b"# another artifact");
        write_input(root.path(), 0, &first);
        write_input(root.path(), 1, &second);
        let mut children: Vec<_> = (0..6)
            .map(|i| child(root.path(), i, if conflicting { i % 2 } else { 0 }, ""))
            .collect();
        barrier(root.path(), children.len());
        for child in &mut children {
            assert!(child.wait().unwrap().success());
        }
        let results: Vec<PublicationResult<PublishArtifactResult>> = (0..6)
            .map(|i| {
                serde_json::from_slice(
                    &fs::read(root.path().join(format!("result-{i}.json"))).unwrap(),
                )
                .unwrap()
            })
            .collect();
        assert_eq!(
            results
                .iter()
                .filter(|r| r.as_ref().is_ok_and(|p| !p.already_published))
                .count(),
            1
        );
        assert_eq!(
            results.iter().filter(|r| r.is_err()).count(),
            if conflicting { 3 } else { 0 }
        );
        for error in results.iter().filter_map(|r| r.as_ref().err()) {
            assert_eq!(error.kind, PublicationErrorKind::ImmutableConflict);
        }
        let descriptor: ProgramDescriptor =
            serde_json::from_slice(&fs::read(release(&root.path().join("store"))).unwrap())
                .unwrap();
        let stored = fs::read(
            root.path()
                .join("store/blobs")
                .join(format!("{}.zip", descriptor.digest.hex())),
        )
        .unwrap();
        verify_package(stored, &descriptor, &ArtifactLimits::default()).unwrap();
    }
}

#[test]
fn abrupt_process_exit_never_exposes_an_incomplete_release_and_retries_reconcile() {
    for (stage, exists) in [
        ("BeforeBlob", false),
        ("BlobStaged", false),
        ("BlobDurable", false),
        ("DescriptorStaged", false),
        ("DescriptorDurable", true),
    ] {
        let root = tempfile::tempdir().unwrap();
        let packed = prepare(&root.path().join("source"), b"pass");
        write_input(root.path(), 0, &packed);
        let mut process = child(root.path(), 0, 0, stage);
        barrier(root.path(), 1);
        assert_eq!(process.wait().unwrap().code(), Some(77));
        let store = root.path().join("store");
        assert_eq!(release(&store).exists(), exists, "stage {stage}");
        let result = persist(&packed, &store).unwrap();
        assert_eq!(result.already_published, exists, "stage {stage}");
        assert_eq!(fs::read(blob(&store, &packed)).unwrap(), packed.archive);
        assert!(persist(&packed, &store).unwrap().already_published);
    }
}

#[test]
fn failure_after_descriptor_reports_unknown_and_same_bytes_recover() {
    let root = tempfile::tempdir().unwrap();
    let packed = prepare(&root.path().join("source"), b"pass");
    let store = root.path().join("store");
    let error = persist_with_observer(
        packed.descriptor.program.clone(),
        packed.descriptor.digest.clone(),
        packed.archive.clone(),
        &store,
        &ArtifactLimits::default(),
        |stage| {
            if stage == PublicationStage::DescriptorDurable {
                Err(std::io::Error::other("injected response loss"))
            } else {
                Ok(())
            }
        },
    )
    .unwrap_err();
    assert_eq!(error.kind, PublicationErrorKind::OutcomeUnknown);
    assert!(persist(&packed, &store).unwrap().already_published);
}
