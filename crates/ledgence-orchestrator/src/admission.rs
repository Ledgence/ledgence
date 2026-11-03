//! Explicit resource admission policy and bounded durable readiness scans.
use ledgence_orchestration_api::{AccessAudience, AdmissionConfig, decode_unique_json};
use std::{io::Read, path::PathBuf};

const CONFIG_MAX_BYTES: usize = 16 * 1024;

/// Only configured instances run this bounded due-work scan. It does not poll
/// active attempts; ordinary terminal-work finalization rearms later checks.
pub async fn run(
    service: std::sync::Arc<ledgence_orchestration_service::ApplicationService>,
    health: crate::health::Health,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> ledgence_orchestration_api::Result<()> {
    use ledgence_orchestration_api::{
        ADMISSION_MAX_MAINTENANCE_BATCH, AdmissionMaintenanceQuery, ContractError,
    };
    use std::time::Duration;
    struct Lifetime(crate::health::Health);
    impl Drop for Lifetime {
        fn drop(&mut self) {
            self.0.admission_failure("admission_recovery_stopped");
        }
    }
    let _lifetime = Lifetime(health.clone());
    let mut backoff = Duration::from_millis(50);
    loop {
        let mut delay = Duration::from_millis(50);
        for _ in 0..64 {
            if *stop.borrow() {
                return Ok(());
            }
            let query = AdmissionMaintenanceQuery {
                limit: ADMISSION_MAX_MAINTENANCE_BATCH,
            };
            match tokio::time::timeout(
                Duration::from_secs(30),
                service.coordinate_admissions(&query),
            )
            .await
            {
                Ok(Ok(progress)) => {
                    health.admission_success();
                    backoff = Duration::from_millis(50);
                    if progress.examined < ADMISSION_MAX_MAINTENANCE_BATCH {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                Ok(Err(ContractError::Unavailable(_) | ContractError::OwnershipLost)) | Err(_) => {
                    health.admission_failure("admission_recovery_unavailable");
                    delay = backoff;
                    backoff = backoff.saturating_mul(2).min(Duration::from_secs(5));
                    break;
                }
                Ok(Err(error)) => {
                    health.admission_failure("admission_recovery_failed");
                    return Err(error);
                }
            }
        }
        tokio::select! {
            _=tokio::time::sleep(delay)=>{},
            changed=stop.changed()=>if changed.is_err()||*stop.borrow(){return Ok(());},
        }
    }
}

pub async fn load_optional(
    path: Option<PathBuf>,
    audience: Option<&AccessAudience>,
) -> Result<Option<AdmissionConfig>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    let audience = audience
        .ok_or("admission operations require configured authenticated instance access")?
        .clone();
    tokio::task::spawn_blocking(move || {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(
                (nix::fcntl::OFlag::O_NOFOLLOW | nix::fcntl::OFlag::O_NONBLOCK).bits(),
            );
        }
        #[cfg(not(unix))]
        if std::fs::symlink_metadata(&path)
            .map_err(|_| "could not inspect admission configuration")?
            .file_type()
            .is_symlink()
        {
            return Err("admission configuration must be a regular file".into());
        }
        let file = options
            .open(path)
            .map_err(|_| "could not open admission configuration")?;
        if !file
            .metadata()
            .map_err(|_| "could not inspect admission configuration")?
            .is_file()
        {
            return Err("admission configuration must be a regular file".into());
        }
        let mut bytes = Vec::new();
        file.take(CONFIG_MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "could not read admission configuration")?;
        let config: AdmissionConfig = decode_unique_json(&bytes, CONFIG_MAX_BYTES)
            .map_err(|_| "invalid admission configuration JSON")?;
        config
            .validate_for(&audience)
            .map_err(|_| "admission configuration audience or policy mismatch")?;
        Ok(Some(config))
    })
    .await
    .map_err(|_| "admission configuration loading failed".to_owned())?
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_orchestration_api::{AdmissionDispatchPolicy, Scope};
    fn audience() -> AccessAudience {
        AccessAudience {
            authority_id: "authority".into(),
            instance_id: "instance".into(),
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "namespace".into(),
            },
        }
    }
    #[tokio::test]
    async fn admission_policy_requires_exact_authority_and_bounded_regular_document() {
        assert!(load_optional(None, None).await.unwrap().is_none());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("admission.json");
        let config = AdmissionConfig {
            v: 1,
            audience: audience(),
            dispatch: AdmissionDispatchPolicy::Enabled,
        };
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        assert!(load_optional(Some(path.clone()), None).await.is_err());
        assert!(
            load_optional(Some(path.clone()), Some(&audience()))
                .await
                .unwrap()
                .is_some()
        );
        let mut wrong = audience();
        wrong.authority_id.push('x');
        assert!(
            load_optional(Some(path.clone()), Some(&wrong))
                .await
                .is_err()
        );
        for body in [
            b"{}".to_vec(),
            vec![b' '; CONFIG_MAX_BYTES + 1],
            b"{\"v\":1,\"v\":1}".to_vec(),
        ] {
            std::fs::write(&path, body).unwrap();
            assert!(
                load_optional(Some(path.clone()), Some(&audience()))
                    .await
                    .is_err()
            );
        }
        assert!(
            load_optional(Some(dir.path().to_owned()), Some(&audience()))
                .await
                .is_err()
        );
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn policy_symlink_and_fifo_are_rejected_without_blocking() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("policy");
        std::fs::write(&target, b"{}").unwrap();
        let link = dir.path().join("link");
        symlink(&target, &link).unwrap();
        assert!(load_optional(Some(link), Some(&audience())).await.is_err());
        let fifo = dir.path().join("fifo");
        nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::S_IRUSR).unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                load_optional(Some(fifo), Some(&audience()))
            )
            .await
            .unwrap()
            .is_err()
        );
    }
}
