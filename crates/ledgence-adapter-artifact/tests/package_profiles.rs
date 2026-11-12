use ledgence_adapter_artifact::{
    ArtifactLimits, FileArtifactCache, FileProgramStore, ZipProgramPackageInspector,
    persist_archive, verify_program_package,
};
use ledgence_worker_api::{
    ArtifactCache, Digest, ErrorKind, Platform, ProgramDescriptor, ProgramManifest,
    ProgramPackageInspector, ProgramPackageProfile, ProgramRef, ProgramStore, PublicationErrorKind,
    PublicationLimits, PythonRuntime,
};
use sha2::{Digest as _, Sha256};
use std::{
    fs,
    io::{Cursor, Write},
    time::Duration,
};
use zip::{ZipWriter, write::SimpleFileOptions};

const APP: &[u8] = b"def handle(event): return event";

fn describe(program: ProgramRef, bytes: &[u8]) -> ProgramDescriptor {
    ProgramDescriptor {
        program,
        digest: Digest(format!("sha256:{:x}", Sha256::digest(bytes))),
        size: bytes.len() as u64,
    }
}

fn package(entries: usize) -> (ProgramDescriptor, Vec<u8>, u64) {
    assert!(entries >= 2);
    let manifest = ProgramManifest {
        schema_version: 1,
        program: ProgramRef {
            id: "profile-boundary".into(),
            version: format!("v{entries}"),
        },
        runtime: PythonRuntime {
            kind: "python".into(),
            python: "3.14".into(),
            protocol: 3,
        },
        handler: "app:handle".into(),
        platform: Platform {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
        },
    };
    let json = serde_json::to_vec(&manifest).unwrap();
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer.start_file("ledgence-program.json", options).unwrap();
    writer.write_all(&json).unwrap();
    writer.start_file("app.py", options).unwrap();
    writer.write_all(APP).unwrap();
    let mut expanded = (json.len() + APP.len()) as u64;
    for index in 2..entries {
        if index % 64 == 0 {
            writer
                .add_directory(format!("empty-{index}/"), options)
                .unwrap();
        } else {
            // The implicit deps/ parent is not a raw ZIP member.
            writer
                .start_file(format!("deps/file-{index}"), options)
                .unwrap();
            writer.write_all(b"x").unwrap();
            expanded += 1;
        }
    }
    let bytes = writer.finish().unwrap().into_inner();
    (describe(manifest.program, &bytes), bytes, expanded)
}

#[test]
fn artifact_profiles_preserve_cache_and_every_byte_bound() {
    let legacy = ArtifactLimits::default();
    assert_eq!(legacy.max_entries, 4096);
    for profile in [
        ProgramPackageProfile::Legacy4096,
        ProgramPackageProfile::Entries8192V1,
    ] {
        let limits = ArtifactLimits::for_profile(profile);
        let expected = profile.limits();
        assert_eq!(limits.max_entries as u64, expected.max_entries);
        assert_eq!(limits.max_archive_bytes, legacy.max_archive_bytes);
        assert_eq!(limits.max_expanded_bytes, legacy.max_expanded_bytes);
        assert_eq!(limits.max_file_bytes, legacy.max_file_bytes);
        assert_eq!(limits.max_manifest_bytes, legacy.max_manifest_bytes);
        assert_eq!(limits.max_descriptor_bytes, legacy.max_descriptor_bytes);
        assert_eq!(limits.max_cache_bytes, 1024 * 1024 * 1024);
        assert_eq!(limits.request_timeout, Duration::from_secs(60));
    }
}

#[tokio::test]
async fn exact_raw_entry_boundaries_control_inspection_publication_and_cache() {
    for (profile, count, accepted) in [
        (ProgramPackageProfile::Legacy4096, 4096, true),
        (ProgramPackageProfile::Legacy4096, 4097, false),
        (ProgramPackageProfile::Entries8192V1, 8192, true),
        (ProgramPackageProfile::Entries8192V1, 8193, false),
    ] {
        let (descriptor, bytes, _) = package(count);
        let limits = ArtifactLimits::for_profile(profile);
        let inspection = ZipProgramPackageInspector.inspect(&descriptor, &bytes, &profile.limits());
        let store_root = tempfile::tempdir().unwrap();
        let publication = persist_archive(
            descriptor.program.clone(),
            descriptor.digest.clone(),
            bytes.clone(),
            store_root.path(),
            &limits,
        );
        let cache_root = tempfile::tempdir().unwrap();
        let cache = FileArtifactCache::new(cache_root.path(), limits.clone()).unwrap();
        let materialization = cache.publish(&descriptor, bytes.clone()).await;
        if accepted {
            let inspection = inspection.unwrap();
            assert_eq!(inspection.entry_count, count as u64);
            assert_eq!(inspection.manifest.program, descriptor.program);
            assert_eq!(publication.unwrap().descriptor, descriptor);
            let reader = FileProgramStore::new(store_root.path(), limits.clone()).unwrap();
            assert_eq!(
                reader.resolve(&descriptor.program).await.unwrap(),
                descriptor
            );
            let blob = store_root
                .path()
                .join("blobs")
                .join(format!("{}.zip", descriptor.digest.hex()));
            assert_eq!(
                fs::read(blob).unwrap(),
                bytes,
                "publication preserves exact inspected bytes"
            );
            let prepared = materialization.unwrap();
            assert_eq!(fs::read(prepared.root().join("app.py")).unwrap(), APP);
            assert!(prepared.root().join("empty-64").is_dir());
            assert!(prepared.root().join("deps").is_dir());
            let hit = cache.lookup(&descriptor).await.unwrap().unwrap();
            assert_eq!(hit.root(), prepared.root());
            drop(hit);
            drop(prepared);
            drop(cache);
            let reopened = FileArtifactCache::new(cache_root.path(), limits).unwrap();
            assert!(reopened.lookup(&descriptor).await.unwrap().is_some());
        } else {
            assert_eq!(inspection.unwrap_err().kind, PublicationErrorKind::TooLarge);
            assert_eq!(
                publication.unwrap_err().kind,
                PublicationErrorKind::TooLarge
            );
            assert_eq!(materialization.unwrap_err().kind, ErrorKind::InvalidInput);
            assert_eq!(
                fs::read_dir(store_root.path()).unwrap().count(),
                0,
                "invalid input exposed no descriptor or blob"
            );
            assert_eq!(
                fs::read_dir(cache_root.path()).unwrap().count(),
                1,
                "only the cache lock remains"
            );
        }
    }
}

#[test]
fn borrowed_inspection_honors_all_lower_peer_bounds_and_descriptor_identity() {
    let (descriptor, bytes, _) = package(3);
    let defaults = ProgramPackageProfile::Entries8192V1.limits();
    let actual = ZipProgramPackageInspector
        .inspect(&descriptor, &bytes, &defaults)
        .unwrap();
    assert_eq!(actual.entry_count, 3);
    assert_eq!(
        verify_program_package(bytes.clone(), &descriptor, &ArtifactLimits::default()).unwrap(),
        actual.manifest
    );
    for limits in [
        PublicationLimits {
            max_archive_bytes: descriptor.size - 1,
            ..defaults.clone()
        },
        PublicationLimits {
            max_expanded_bytes: 1,
            ..defaults.clone()
        },
        PublicationLimits {
            max_file_bytes: 1,
            ..defaults.clone()
        },
        PublicationLimits {
            max_entries: 2,
            ..defaults.clone()
        },
        PublicationLimits {
            max_manifest_bytes: 1,
            ..defaults.clone()
        },
        PublicationLimits {
            max_descriptor_bytes: 1,
            ..defaults.clone()
        },
        PublicationLimits {
            max_entries: u64::MAX,
            ..defaults.clone()
        },
        PublicationLimits {
            max_entries: 0,
            ..defaults.clone()
        },
    ] {
        assert!(
            ZipProgramPackageInspector
                .inspect(&descriptor, &bytes, &limits)
                .is_err()
        );
    }
    for wrong in [
        ProgramDescriptor {
            size: descriptor.size + 1,
            ..descriptor.clone()
        },
        ProgramDescriptor {
            digest: Digest(format!("sha256:{}", "0".repeat(64))),
            ..descriptor.clone()
        },
        ProgramDescriptor {
            program: ProgramRef {
                id: "other".into(),
                version: "v3".into(),
            },
            ..descriptor.clone()
        },
    ] {
        assert_eq!(
            ZipProgramPackageInspector
                .inspect(&wrong, &bytes, &defaults)
                .unwrap_err()
                .kind,
            PublicationErrorKind::InvalidArtifact
        );
    }
    assert_eq!(describe(descriptor.program.clone(), &bytes), descriptor);
}

#[test]
fn borrowed_inspection_rejects_crc_and_dishonest_raw_counts_with_matching_outer_digest() {
    let (descriptor, mut corrupt, _) = package(3);
    let offset = corrupt
        .windows(APP.len())
        .position(|window| window == APP)
        .unwrap();
    corrupt[offset] ^= 1;
    let descriptor = describe(descriptor.program, &corrupt);
    assert_eq!(
        ZipProgramPackageInspector
            .inspect(&descriptor, &corrupt, &PublicationLimits::default())
            .unwrap_err()
            .kind,
        PublicationErrorKind::InvalidArtifact
    );

    let (descriptor, mut dishonest, _) = package(8192);
    let eocd = dishonest.len() - 22;
    assert_eq!(&dishonest[eocd..eocd + 4], b"PK\x05\x06");
    dishonest[eocd + 8..eocd + 10].copy_from_slice(&4096u16.to_le_bytes());
    dishonest[eocd + 10..eocd + 12].copy_from_slice(&4096u16.to_le_bytes());
    let descriptor = describe(descriptor.program, &dishonest);
    for profile in [
        ProgramPackageProfile::Legacy4096,
        ProgramPackageProfile::Entries8192V1,
    ] {
        assert_eq!(
            ZipProgramPackageInspector
                .inspect(&descriptor, &dishonest, &profile.limits())
                .unwrap_err()
                .kind,
            PublicationErrorKind::InvalidArtifact
        );
    }
}

#[tokio::test]
async fn expanded_cache_preserves_pins_byte_accounting_and_cleanup() {
    let profile = ProgramPackageProfile::Entries8192V1;
    let (large, bytes, expanded) = package(4097);
    let (small, small_bytes, _) = package(2);
    let charge = large.size + expanded + serde_json::to_vec(&large).unwrap().len() as u64;
    let root = tempfile::tempdir().unwrap();
    let limits = ArtifactLimits {
        max_cache_bytes: charge,
        ..ArtifactLimits::for_profile(profile)
    };
    let cache = FileArtifactCache::new(root.path(), limits.clone()).unwrap();
    let pinned = cache.publish(&large, bytes).await.unwrap();
    assert_eq!(
        cache
            .publish(&small, small_bytes.clone())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Capacity
    );
    assert!(cache.lookup(&large).await.unwrap().is_some());
    drop(pinned);
    let replacement = cache.publish(&small, small_bytes).await.unwrap();
    assert!(cache.lookup(&large).await.unwrap().is_none());
    assert!(!root.path().join(large.digest.hex()).exists());
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 2);
    drop(replacement);
    drop(cache);
    let reopened = FileArtifactCache::new(root.path(), limits).unwrap();
    assert!(reopened.lookup(&small).await.unwrap().is_some());
    assert!(reopened.lookup(&large).await.unwrap().is_none());
}

#[test]
fn borrowed_inspection_preserves_raw_structure_and_path_rejections() {
    let (descriptor, bytes, _) = package(4);
    let eocd = bytes.len() - 22;
    let central = u32::from_le_bytes(bytes[eocd + 16..eocd + 20].try_into().unwrap()) as usize;
    let first_len =
        u16::from_le_bytes(bytes[central + 28..central + 30].try_into().unwrap()) as usize;
    let first_extra =
        u16::from_le_bytes(bytes[central + 30..central + 32].try_into().unwrap()) as usize;
    let first_comment =
        u16::from_le_bytes(bytes[central + 32..central + 34].try_into().unwrap()) as usize;
    let second = central + 46 + first_len + first_extra + first_comment;
    let mut cases = Vec::new();
    let mut malformed = bytes.clone();
    malformed[central] ^= 1;
    cases.push(("malformed central record", malformed));
    let mut zip64 = bytes.clone();
    zip64[central + 42..central + 46].copy_from_slice(&u32::MAX.to_le_bytes());
    cases.push(("ZIP64 offset sentinel", zip64));
    let mut overlapping = bytes.clone();
    overlapping[second + 42..second + 46].copy_from_slice(&bytes[central + 42..central + 46]);
    cases.push(("overlapping local records", overlapping));
    for (label, original, replacement) in [
        (
            "duplicate raw names",
            b"deps/file-3".as_slice(),
            b"deps/file-2".as_slice(),
        ),
        (
            "case collision",
            b"deps/file-3".as_slice(),
            b"deps/FILE-2".as_slice(),
        ),
        (
            "parent traversal",
            b"deps/file-2".as_slice(),
            b"../escape12".as_slice(),
        ),
    ] {
        assert_eq!(original.len(), replacement.len());
        let offsets: Vec<_> = bytes
            .windows(original.len())
            .enumerate()
            .filter_map(|(offset, value)| (value == original).then_some(offset))
            .collect();
        assert_eq!(offsets.len(), 2, "local and central names");
        let mut changed = bytes.clone();
        for offset in offsets {
            changed[offset..offset + replacement.len()].copy_from_slice(replacement);
        }
        cases.push((label, changed));
    }
    for (label, changed) in cases {
        let changed_descriptor = describe(descriptor.program.clone(), &changed);
        assert_eq!(
            ZipProgramPackageInspector
                .inspect(
                    &changed_descriptor,
                    &changed,
                    &ProgramPackageProfile::Entries8192V1.limits()
                )
                .unwrap_err()
                .kind,
            PublicationErrorKind::InvalidArtifact,
            "accepted {label}"
        );
    }
}
