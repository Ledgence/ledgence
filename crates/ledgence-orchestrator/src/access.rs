//! Explicit optional access configuration. Invalid configured access never falls
//! back to operator-trusted mode; the store separately rejects later omission.
use ledgence_adapter_access::{AccessConfig, OpaqueCredentialVerifier};
use ledgence_orchestration_api::{CredentialVerifier, SelfHostedInstanceConfig};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

pub async fn load_optional(
    path: Option<PathBuf>,
    instance: Option<&SelfHostedInstanceConfig>,
    bind: SocketAddr,
    allow_insecure_console: bool,
) -> Result<Option<Arc<dyn CredentialVerifier>>, String> {
    if allow_insecure_console && (path.is_none() || !bind.ip().is_loopback()) {
        return Err(
            "insecure Console access requires configured credentials and a loopback listener"
                .into(),
        );
    }
    let Some(path) = path else {
        return Ok(None);
    };
    let instance = instance.ok_or("credential access requires instance configuration")?;
    ledgence_adapter_http::server::console::validate_access_origin(
        instance,
        allow_insecure_console,
    )
    .map_err(|error| error.to_string())?;
    let verifier = tokio::task::spawn_blocking(move || {
        AccessConfig::load(&path).and_then(OpaqueCredentialVerifier::new)
    })
    .await
    .map_err(|_| "access configuration loading failed".to_owned())?
    .map_err(|error| error.to_string())?;
    verifier
        .audience()
        .require_instance(&instance.context())
        .map_err(|error| error.to_string())?;
    Ok(Some(Arc::new(verifier)))
}
#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_adapter_access::CredentialSpec;
    use ledgence_orchestration_api::{AccessCapability, PrincipalClass, Scope};
    fn instance() -> SelfHostedInstanceConfig {
        SelfHostedInstanceConfig {
            instance_id: "instance".into(),
            name: "Test".into(),
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "namespace".into(),
            },
            suggested_queues: vec![],
            allowed_origins: vec!["https://console.example".into()],
        }
    }
    #[tokio::test]
    async fn omission_is_legacy_but_invalid_config_and_wrong_scope_never_fall_back() {
        let bind = "127.0.0.1:8080".parse().unwrap();
        assert!(
            load_optional(None, None, bind, false)
                .await
                .unwrap()
                .is_none()
        );
        assert!(load_optional(None, None, bind, true).await.is_err());
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("access.json");
        let instance = instance();
        std::fs::write(&path, b"{malformed}").unwrap();
        assert!(
            load_optional(Some(path.clone()), Some(&instance), bind, false)
                .await
                .is_err()
        );
        let mut config = AccessConfig::new(&instance.context()).unwrap();
        let token = config
            .issue(
                CredentialSpec {
                    subject: "operator".into(),
                    class: PrincipalClass::Operator,
                    expires_at_ms: 4_102_444_800_000,
                    capabilities: vec![AccessCapability::MetadataRead],
                },
                1,
            )
            .unwrap();
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let loaded = load_optional(Some(path.clone()), Some(&instance), bind, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            loaded
                .verify(token.expose_secret(), 2)
                .unwrap()
                .identity()
                .subject,
            "operator"
        );
        assert!(
            load_optional(Some(path.clone()), None, bind, false)
                .await
                .is_err()
        );
        let mut wrong = instance.clone();
        wrong.scope.namespace.push('x');
        assert!(
            load_optional(Some(path.clone()), Some(&wrong), bind, false)
                .await
                .is_err()
        );
        let mut local = instance.clone();
        local.allowed_origins = vec!["http://127.0.0.1:8080".into()];
        assert!(
            load_optional(Some(path.clone()), Some(&local), bind, false)
                .await
                .is_err()
        );
        assert!(
            load_optional(Some(path.clone()), Some(&local), bind, true)
                .await
                .is_ok()
        );
        assert!(
            load_optional(
                Some(path),
                Some(&local),
                "0.0.0.0:8080".parse().unwrap(),
                true
            )
            .await
            .is_err()
        );
    }
}
