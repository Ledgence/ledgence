use super::*;
use serde_json::json;

fn instance() -> SelfHostedInstanceContext {
    SelfHostedInstanceContext {
        instance_id: "test".into(),
        name: "Test".into(),
        scope: Scope {
            tenant_id: "acme".into(),
            namespace: "test".into(),
        },
    }
}
fn spec() -> CredentialSpec {
    CredentialSpec {
        subject: "alice".into(),
        class: PrincipalClass::Operator,
        expires_at_ms: 10_000,
        capabilities: vec![
            AccessCapability::MetadataRead,
            AccessCapability::SensitiveRead,
        ],
    }
}
fn issued() -> (AccessConfig, SecretCredential) {
    let mut config = AccessConfig::new(&instance()).unwrap();
    let secret = config.issue(spec(), 100).unwrap();
    (config, secret)
}
#[test]
fn provisioning_stores_only_a_verifier_and_redacts_the_secret() {
    let (config, secret) = issued();
    let config_bytes = serde_json::to_vec(&config).unwrap();
    assert!(
        !String::from_utf8(config_bytes.clone())
            .unwrap()
            .contains(secret.expose_secret())
    );
    assert!(!format!("{secret:?}").contains(secret.expose_secret()));
    assert!(format!("{secret:?}").contains("[REDACTED]"));
    assert_eq!(config.credentials[0].credential_id, secret.credential_id());
    let decoded = AccessConfig::decode(&config_bytes).unwrap();
    let verifier = OpaqueCredentialVerifier::new(decoded).unwrap();
    let principal = verifier.verify(secret.expose_secret(), 100).unwrap();
    assert_eq!(principal.identity().subject, "alice");
    assert_eq!(principal.identity().issuer, config.audience().issuer());
    assert_eq!(principal.audience(), &config.audience());
    assert_eq!(principal.credential_id(), secret.credential_id());
    principal.require(AccessCapability::SensitiveRead).unwrap();
    assert_eq!(
        principal.require(AccessCapability::ApprovalDecide),
        Err(ContractError::Forbidden)
    );
}
#[test]
fn verification_rejects_expired_tampered_unknown_and_malformed_credentials() {
    let (config, secret) = issued();
    let verifier = OpaqueCredentialVerifier::new(config).unwrap();
    verifier.verify(secret.expose_secret(), 9_999).unwrap();
    for now in [10_000, 10_001, u64::MAX] {
        assert_eq!(
            verifier.verify(secret.expose_secret(), now).unwrap_err(),
            ContractError::Unauthenticated
        );
    }
    let mut first = secret.expose_secret().as_bytes().to_vec();
    first[38] = if first[38] == b'0' { b'1' } else { b'0' };
    let mut last = secret.expose_secret().as_bytes().to_vec();
    last[101] = if last[101] == b'0' { b'1' } else { b'0' };
    let (_, another) = issued();
    for bad in [
        String::new(),
        "a".repeat(103),
        "é".repeat(51),
        secret.expose_secret().to_uppercase(),
        secret.expose_secret()[..101].into(),
        format!("{} ", secret.expose_secret()),
        String::from_utf8(first).unwrap(),
        String::from_utf8(last).unwrap(),
        another.expose_secret().into(),
    ] {
        assert_eq!(
            verifier.verify(&bad, 100).unwrap_err(),
            ContractError::Unauthenticated
        );
    }
}
#[test]
fn configuration_is_strict_bounded_and_class_separated() {
    let (config, _) = issued();
    let original = serde_json::to_value(&config).unwrap();
    let mut mutations = Vec::new();
    for (key, value) in [
        ("version", json!(2)),
        ("unknown", json!(true)),
        ("authority_id", json!(null)),
        ("credentials", json!(null)),
    ] {
        let mut value_copy = original.clone();
        value_copy[key] = value;
        mutations.push(value_copy);
    }
    for (key, value) in [
        ("credential_id", json!("cred_bad")),
        ("verifier_sha256", json!("a".repeat(63))),
        ("subject", json!("")),
        ("expires_at_ms", json!(0)),
        ("class", json!("worker")),
        ("capabilities", json!(["worker_execute"])),
        ("capabilities", json!(["metadata_read", "metadata_read"])),
        ("capabilities", json!(["future_permission"])),
        ("capabilities", json!([])),
        ("unknown", json!(true)),
    ] {
        let mut value_copy = original.clone();
        value_copy["credentials"][0][key] = value;
        mutations.push(value_copy);
    }
    let mut duplicate = original.clone();
    duplicate["credentials"]
        .as_array_mut()
        .unwrap()
        .push(original["credentials"][0].clone());
    mutations.push(duplicate);
    for value in mutations {
        assert!(AccessConfig::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    let bytes = serde_json::to_string(&config).unwrap();
    let duplicate = bytes.replacen("\"version\":1", "\"version\":1,\"version\":1", 1);
    assert!(AccessConfig::decode(duplicate.as_bytes()).is_err());
    assert!(AccessConfig::decode(&vec![b' '; ACCESS_CONFIG_MAX_BYTES + 1]).is_err());
    let mut worker = AccessConfig::new(&instance()).unwrap();
    let token = worker
        .issue(
            CredentialSpec {
                class: PrincipalClass::Worker,
                capabilities: vec![AccessCapability::WorkerExecute],
                ..spec()
            },
            100,
        )
        .unwrap();
    let principal = OpaqueCredentialVerifier::new(worker)
        .unwrap()
        .verify(token.expose_secret(), 100)
        .unwrap();
    principal.require(AccessCapability::WorkerExecute).unwrap();
    assert_eq!(
        principal.require(AccessCapability::SensitiveRead),
        Err(ContractError::Forbidden)
    );
}
#[test]
fn rotation_preserves_actor_but_reload_applies_revocation_and_permissions() {
    let (mut config, original) = issued();
    let first = OpaqueCredentialVerifier::new(config.clone()).unwrap();
    let replacement = config.issue(spec(), 100).unwrap();
    assert_ne!(original.credential_id(), replacement.credential_id());
    let overlap = OpaqueCredentialVerifier::new(config.clone()).unwrap();
    let a = overlap.verify(original.expose_secret(), 100).unwrap();
    let b = overlap.verify(replacement.expose_secret(), 100).unwrap();
    assert_eq!(a.identity(), b.identity());
    assert_eq!(a.audience(), b.audience());
    config.credentials.remove(0);
    config.credentials[0].capabilities = vec![AccessCapability::MetadataRead];
    let changed = OpaqueCredentialVerifier::new(config).unwrap();
    assert_eq!(
        changed.verify(original.expose_secret(), 100).unwrap_err(),
        ContractError::Unauthenticated
    );
    assert_eq!(
        changed
            .verify(replacement.expose_secret(), 100)
            .unwrap()
            .require(AccessCapability::SensitiveRead),
        Err(ContractError::Forbidden)
    );
    first.verify(original.expose_secret(), 100).unwrap(); // An old replica needs a restart.
}
#[test]
fn provisioning_failures_do_not_modify_the_config() {
    let (mut config, _) = issued();
    let before = serde_json::to_vec(&config).unwrap();
    for invalid in [
        CredentialSpec {
            expires_at_ms: 100,
            ..spec()
        },
        CredentialSpec {
            class: PrincipalClass::Worker,
            ..spec()
        },
        CredentialSpec {
            expires_at_ms: u64::MAX,
            ..spec()
        },
    ] {
        assert!(config.issue(invalid, 100).is_err());
        assert_eq!(serde_json::to_vec(&config).unwrap(), before);
    }
}
#[test]
fn loader_checks_regular_file_and_json_bounds() {
    let (config, _) = issued();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("access.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    assert_eq!(
        AccessConfig::load(&path).unwrap().authority_id,
        config.authority_id
    );
    assert!(AccessConfig::load(dir.path()).is_err());
    std::fs::write(&path, vec![b' '; ACCESS_CONFIG_MAX_BYTES + 1]).unwrap();
    assert!(AccessConfig::load(&path).is_err());
}
#[test]
fn verification_cost_is_bounded_by_token_size_not_config_scan() {
    let (mut config, secret) = issued();
    let record = config.credentials[0].clone();
    for index in 1..ACCESS_MAX_CREDENTIALS {
        let mut filler = record.clone();
        filler.credential_id = format!("cred_{index:032x}");
        config.credentials.push(filler);
    }
    let verifier = OpaqueCredentialVerifier::new(config).unwrap();
    let begin = std::time::Instant::now();
    for _ in 0..10_000 {
        verifier.verify(secret.expose_secret(), 100).unwrap();
    }
    println!(
        "opaque verifier: 10000 accepted requests / 1024 credentials in {:?} (no I/O)",
        begin.elapsed()
    );
}

#[cfg(unix)]
#[test]
fn loader_rejects_symlinks_and_fifo_without_waiting_for_a_writer() {
    use std::os::unix::fs::OpenOptionsExt;
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("access.json");
    let (config, _) = issued();
    std::fs::write(&target, serde_json::to_vec(&config).unwrap()).unwrap();
    let link = directory.path().join("link.json");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    assert!(AccessConfig::load(&link).is_err());
    let fifo = directory.path().join("fifo");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let (sender, receiver) = std::sync::mpsc::channel();
    let read_path = fifo.clone();
    let reader = std::thread::spawn(move || {
        let _ = sender.send(AccessConfig::load(&read_path));
    });
    let received = receiver.recv_timeout(std::time::Duration::from_secs(2));
    if received.is_err() {
        // Release an accidentally blocking reader before reporting a regression.
        let _ = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32)
            .open(&fifo);
    }
    assert!(
        received
            .expect("configuration loader waited for a FIFO writer")
            .is_err()
    );
    reader.join().unwrap();
}
