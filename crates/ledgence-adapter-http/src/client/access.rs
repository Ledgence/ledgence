//! One bounded startup discovery. An implemented local trait is not evidence
//! that the remote deployment supports the corresponding operation.
use super::*;
use serde::{Deserialize, Deserializer};

/// Stable identity negotiated for an authenticated worker connection. This is
/// a remote binding to check receipts against, not a trusted server principal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerEffectAccess {
    pub audience: AccessAudience,
    pub identity: PrincipalIdentity,
    pub runtime_features: ledgence_worker_api::RuntimeFeatures,
}

/// Authenticated audience and worker identity used to verify effect receipts.
/// A feature name alone does not establish this binding or authorize effects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRuntimeBinding {
    pub audience: AccessAudience,
    pub identity: PrincipalIdentity,
}

/// Legacy effect-specific binding retains its existing public name.
pub type WorkerEffectBinding = WorkerRuntimeBinding;

/// Validated startup discovery metadata. Runtime feature negotiation is
/// independent of the optional authenticated effect-receipt binding.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkerRuntimeAccess {
    pub runtime_features: ledgence_worker_api::RuntimeFeatures,
    pub effect_evidence: Option<WorkerEffectBinding>,
    pub authenticated_binding: Option<WorkerRuntimeBinding>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum AccessReply {
    OperatorTrusted,
    Credentials(Box<CredentialReply>),
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialReply {
    actor_key: String,
    issuer: String,
    subject: String,
    class: PrincipalClass,
    instance_id: String,
    scope: Scope,
    capabilities: Vec<AccessCapability>,
    expires_at_ms: u64,
    #[serde(default, deserialize_with = "present")]
    operator_audit: Option<bool>,
    #[serde(default, deserialize_with = "present")]
    authority_id: Option<String>,
    #[serde(default, deserialize_with = "capabilities")]
    runtime_capabilities: Option<RuntimeCapabilities>,
    #[serde(default, deserialize_with = "present")]
    worker_features: Option<ledgence_worker_api::RuntimeFeatures>,
}
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeCapabilities {
    #[serde(default)]
    approved_effect_evidence_v2: bool,
}
fn capabilities<'de, D: Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<RuntimeCapabilities>, D::Error> {
    let value = serde_json::Value::deserialize(decoder)?;
    if !value.is_object() {
        return Err(serde::de::Error::custom(
            "runtime capabilities must be an object",
        ));
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(serde::de::Error::custom)
}
fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    decoder: D,
) -> std::result::Result<Option<T>, D::Error> {
    T::deserialize(decoder).map(Some)
}
impl ResponseValue for AccessReply {
    const MAX_BYTES: usize = 16 * 1024;
}
impl AccessReply {
    fn worker(&self, expected: &Scope, credentials: bool) -> Result<WorkerRuntimeAccess> {
        let Self::Credentials(reply) = self else {
            return if credentials {
                Err(unavailable(
                    "credentialed worker requires authenticated access discovery",
                ))
            } else {
                Ok(WorkerRuntimeAccess::default())
            };
        };
        let CredentialReply {
            actor_key,
            issuer,
            subject,
            class,
            instance_id,
            scope,
            capabilities,
            expires_at_ms,
            operator_audit,
            authority_id,
            runtime_capabilities,
            worker_features,
        } = reply.as_ref();
        if !credentials || *class != PrincipalClass::Worker || scope != expected {
            return Err(unavailable(
                "worker access identity or scope disagrees with its connection",
            ));
        }
        class.validate_capabilities(capabilities)?;
        validate_text(actor_key, 128)?;
        validate_text(instance_id, 128)?;
        scope.validate()?;
        let identity = PrincipalIdentity {
            issuer: issuer.clone(),
            subject: subject.clone(),
        };
        identity.validate()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| unavailable("worker access clock unavailable"))?
            .as_millis();
        if u128::from(*expires_at_ms) <= now || *expires_at_ms > i64::MAX as u64 {
            return Err(unavailable(
                "worker access discovery credential has expired",
            ));
        }
        if let Some(authority) = authority_id {
            validate_text(authority, 128)?;
        }
        let enabled = runtime_capabilities
            .as_ref()
            .is_some_and(|capabilities| capabilities.approved_effect_evidence_v2);
        if let Some(features) = worker_features {
            features
                .validate()
                .map_err(|_| unavailable("invalid worker features"))?;
            if features.native_effect_resolution() && !enabled {
                return Err(unavailable(
                    "native effect resolution requires effect evidence v2",
                ));
            }
        }
        let runtime_features = worker_features.clone().unwrap_or_default();
        let authenticated_binding =
            authority_id
                .as_ref()
                .map(|authority_id| WorkerRuntimeBinding {
                    audience: AccessAudience {
                        authority_id: authority_id.clone(),
                        instance_id: instance_id.clone(),
                        scope: scope.clone(),
                    },
                    identity: identity.clone(),
                });
        if let Some(binding) = &authenticated_binding {
            binding.audience.validate()?;
        }
        if runtime_features.resource_admission() && authenticated_binding.is_none() {
            return Err(unavailable(
                "resource admission requires an authenticated audience",
            ));
        }
        if !enabled {
            return Ok(WorkerRuntimeAccess {
                runtime_features,
                effect_evidence: None,
                authenticated_binding,
            });
        }
        if *operator_audit != Some(true) {
            return Err(unavailable(
                "effect evidence requires complete audited access",
            ));
        }
        let audience = AccessAudience {
            authority_id: authority_id
                .clone()
                .ok_or_else(|| unavailable("effect evidence authority is missing"))?,
            instance_id: instance_id.clone(),
            scope: scope.clone(),
        };
        audience.validate()?;
        Ok(WorkerRuntimeAccess {
            runtime_features,
            effect_evidence: Some(WorkerEffectBinding { audience, identity }),
            authenticated_binding,
        })
    }
}

impl HttpTaskService {
    /// Negotiate the legacy effect-specific view in one startup exchange.
    /// Connections without effect evidence return `None`; use
    /// [`Self::discover_worker_runtime`] to retain independent runtime features.
    pub async fn discover_worker(&self, scope: &Scope) -> Result<Option<WorkerEffectAccess>> {
        let access = self.discover_worker_runtime(scope).await?;
        Ok(access.effect_evidence.map(|binding| WorkerEffectAccess {
            audience: binding.audience,
            identity: binding.identity,
            runtime_features: access.runtime_features,
        }))
    }

    /// Negotiate once before starting a worker, preserving valid generic
    /// runtime features independently of the optional effect-receipt binding.
    /// A credentialed connection never falls back on missing, malformed,
    /// anonymous or rejected discovery. Anonymous legacy servers yield empty
    /// features and no effect binding.
    pub async fn discover_worker_runtime(&self, scope: &Scope) -> Result<WorkerRuntimeAccess> {
        scope.validate()?;
        let expected = scope.clone();
        let credentials = self.authorization.is_some();
        let reply = self
            .get_validated("v1/access", &[], move |reply: &AccessReply| {
                reply
                    .worker(&expected, credentials)
                    .map(|_| ())
                    .map_err(|_| unavailable("invalid worker access discovery"))
            })
            .await;
        match reply {
            Ok(reply) => reply.worker(scope, credentials),
            Err(ContractError::NotFound) if !credentials => Ok(WorkerRuntimeAccess::default()),
            Err(error) => Err(error),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn scope() -> Scope {
        Scope {
            tenant_id: "tenant".into(),
            namespace: "billing".into(),
        }
    }
    fn snapshot() -> Value {
        json!({
            "mode":"credentials","actor_key":"actor","issuer":"urn:test","subject":"worker-one",
            "class":"worker","instance_id":"instance","scope":scope(),"capabilities":["worker_execute"],
            "expires_at_ms":i64::MAX as u64,"operator_audit":true,"authority_id":"authority",
            "runtime_capabilities":{"approved_effect_evidence_v2":true}
        })
    }
    async fn discovery_server(
        body: Value,
        status: u16,
        credentialed: bool,
    ) -> (HttpTaskService, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /v1/access HTTP/1.1\r\n"));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("ledgence-worker-features: 1\r\n")
            );
            assert_eq!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer test-token"),
                credentialed
            );
            let body = serde_json::to_vec(&body).unwrap();
            stream.write_all(format!("HTTP/1.1 {status} reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
            // One discovery exchange; errors must never send fallback requests.
            assert!(
                tokio::time::timeout(Duration::from_millis(30), listener.accept())
                    .await
                    .is_err()
            );
        });
        let mut client = HttpTaskService::new(&base).unwrap();
        if credentialed {
            client = client.with_bearer_token("test-token").unwrap();
        }
        (client, server)
    }
    async fn finish_discovery(server: tokio::task::JoinHandle<()>) {
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }
    async fn discover(
        body: Value,
        status: u16,
        credentialed: bool,
    ) -> Result<Option<WorkerEffectAccess>> {
        let (client, server) = discovery_server(body, status, credentialed).await;
        let result = client.discover_worker(&scope()).await;
        finish_discovery(server).await;
        result
    }
    async fn discover_runtime(
        body: Value,
        status: u16,
        credentialed: bool,
    ) -> Result<WorkerRuntimeAccess> {
        let (client, server) = discovery_server(body, status, credentialed).await;
        let result = client.discover_worker_runtime(&scope()).await;
        finish_discovery(server).await;
        result
    }
    fn features_only_snapshot() -> Value {
        let mut body = snapshot();
        for key in ["runtime_capabilities", "authority_id", "operator_audit"] {
            body.as_object_mut().unwrap().remove(key);
        }
        body["worker_features"] = json!({"v":1,"supported":["future.v9"]});
        body
    }
    #[tokio::test]
    async fn generic_runtime_features_survive_without_effect_evidence() {
        for capabilities in [
            None,
            Some(json!({})),
            Some(json!({"approved_effect_evidence_v2":false})),
        ] {
            let mut body = features_only_snapshot();
            if let Some(capabilities) = capabilities {
                body["runtime_capabilities"] = capabilities;
                body["operator_audit"] = json!(false);
            }
            let access = discover_runtime(body.clone(), 200, true).await.unwrap();
            assert_eq!(access.runtime_features.supported, ["future.v9"]);
            assert!(access.effect_evidence.is_none());
            // The compatibility API intentionally exposes only effect access.
            assert!(discover(body, 200, true).await.unwrap().is_none());
        }
    }
    #[tokio::test]
    async fn runtime_discovery_preserves_effect_binding_and_legacy_fallbacks() {
        let mut body = snapshot();
        body["worker_features"] = json!({"v":1,"supported":[
            "native_effect_resolution.v1", "provider_operation.v1", "future.v9"
        ]});
        let access = discover_runtime(body.clone(), 200, true).await.unwrap();
        let legacy = discover(body, 200, true).await.unwrap().unwrap();
        let binding = access.effect_evidence.unwrap();
        assert_eq!(binding.audience, legacy.audience);
        assert_eq!(binding.identity, legacy.identity);
        assert_eq!(access.runtime_features, legacy.runtime_features);
        assert!(access.runtime_features.native_effect_resolution());
        assert!(access.runtime_features.provider_operation());
        for (body, status) in [
            (json!({"mode":"operator_trusted"}), 200),
            (serde_json::to_value(ContractError::NotFound).unwrap(), 404),
        ] {
            assert_eq!(
                discover_runtime(body.clone(), status, false).await.unwrap(),
                WorkerRuntimeAccess::default()
            );
            assert!(discover_runtime(body, status, true).await.is_err());
        }
        assert!(
            discover_runtime(features_only_snapshot(), 200, false)
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn features_only_discovery_still_validates_identity_and_manifest() {
        for (key, value) in [
            ("actor_key", json!("")),
            ("issuer", json!("")),
            ("subject", json!("")),
            ("class", json!("operator")),
            ("instance_id", json!("")),
            ("scope", json!({"tenant_id":"other","namespace":"billing"})),
            ("capabilities", json!(["metadata_read"])),
            ("capabilities", json!(["worker_execute", "worker_execute"])),
            ("expires_at_ms", json!(1)),
            ("expires_at_ms", json!(u64::MAX)),
            ("operator_audit", Value::Null),
            ("authority_id", Value::Null),
            ("authority_id", json!("")),
            ("runtime_capabilities", Value::Null),
            ("worker_features", Value::Null),
            ("worker_features", json!({"v":2,"supported":[]})),
            (
                "worker_features",
                json!({"v":1,"supported":["same","same"]}),
            ),
            ("worker_features", json!({"v":1,"supported":["BAD"]})),
            ("worker_features", json!({"v":1,"supported":[true]})),
            ("worker_features", json!({"v":1,"supported":[],"unknown":1})),
        ] {
            let mut body = features_only_snapshot();
            body[key] = value;
            assert!(discover_runtime(body, 200, true).await.is_err(), "{key}");
        }
    }
    #[tokio::test]
    async fn generic_discovery_cannot_enable_protected_features_without_evidence() {
        for supported in [
            json!(["native_effect_resolution.v1"]),
            json!(["provider_operation.v1"]),
            json!([
                "native_effect_resolution.v1",
                "provider_operation.v1",
                "future.v9"
            ]),
        ] {
            let mut body = snapshot();
            body["worker_features"] = json!({"v":1,"supported":supported});
            for key in ["runtime_capabilities", "authority_id", "operator_audit"] {
                let mut broken = body.clone();
                broken.as_object_mut().unwrap().remove(key);
                assert!(discover_runtime(broken, 200, true).await.is_err(), "{key}");
            }
            body["runtime_capabilities"] = json!({"approved_effect_evidence_v2":false});
            assert!(discover_runtime(body, 200, true).await.is_err());
        }
    }
    #[tokio::test]
    async fn discovery_preserves_explicit_legacy_and_binds_new_worker_identity() {
        let access = discover(snapshot(), 200, true).await.unwrap().unwrap();
        assert_eq!(
            access.audience,
            AccessAudience {
                authority_id: "authority".into(),
                instance_id: "instance".into(),
                scope: scope()
            }
        );
        assert_eq!(access.identity.subject, "worker-one");
        assert!(!access.runtime_features.native_effect_resolution());
        let mut old = snapshot();
        for key in ["runtime_capabilities", "authority_id", "operator_audit"] {
            old.as_object_mut().unwrap().remove(key);
        }
        assert!(discover(old, 200, true).await.unwrap().is_none());
        assert!(
            discover(json!({"mode":"operator_trusted"}), 200, false)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            discover(
                serde_json::to_value(ContractError::NotFound).unwrap(),
                404,
                false
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            discover(json!({"mode":"operator_trusted"}), 200, true)
                .await
                .is_err()
        );
        assert!(
            discover(
                serde_json::to_value(ContractError::NotFound).unwrap(),
                404,
                true
            )
            .await
            .is_err()
        );
        assert!(discover(snapshot(), 200, false).await.is_err());
    }
    #[tokio::test]
    async fn native_features_require_valid_manifest_and_complete_authenticated_evidence() {
        let mut value = snapshot();
        value["worker_features"] =
            json!({"v":1,"supported":["native_effect_resolution.v1","future.v9"]});
        let access = discover(value.clone(), 200, true).await.unwrap().unwrap();
        assert!(access.runtime_features.native_effect_resolution());
        for malformed in [
            Value::Null,
            json!({"v":2,"supported":[]}),
            json!({"v":1,"supported":[true]}),
            json!({"v":1,"supported":["same","same"]}),
            json!({"v":1,"supported":["BAD"]}),
            json!({"v":1,"supported":[],"unknown":1}),
        ] {
            value["worker_features"] = malformed;
            assert!(discover(value.clone(), 200, true).await.is_err());
        }
        value["worker_features"] = json!({"v":1,"supported":["native_effect_resolution.v1"]});
        for key in ["runtime_capabilities", "authority_id", "operator_audit"] {
            let mut broken = value.clone();
            broken.as_object_mut().unwrap().remove(key);
            assert!(discover(broken, 200, true).await.is_err(), "{key}");
        }
    }
    #[tokio::test]
    async fn discovery_rejects_malformed_or_conflicting_capabilities_and_authentication() {
        let mutations = [
            ("runtime_capabilities", Value::Null),
            ("runtime_capabilities", json!([])),
            (
                "runtime_capabilities",
                json!({"approved_effect_evidence_v2":"true"}),
            ),
            (
                "runtime_capabilities",
                json!({"approved_effect_evidence_v2":null}),
            ),
            ("runtime_capabilities", json!({"unknown":true})),
            ("authority_id", Value::Null),
            ("authority_id", json!("")),
            ("class", json!("operator")),
            ("capabilities", json!(["metadata_read"])),
            ("capabilities", json!(["worker_execute", "worker_execute"])),
            ("scope", json!({"tenant_id":"other","namespace":"billing"})),
            ("instance_id", json!("")),
            ("subject", json!("")),
            ("issuer", json!("")),
            ("expires_at_ms", json!(1)),
            ("expires_at_ms", json!(u64::MAX)),
            ("operator_audit", json!(false)),
            ("operator_audit", Value::Null),
        ];
        for (key, value) in mutations {
            let mut body = snapshot();
            body[key] = value;
            assert!(discover(body, 200, true).await.is_err(), "{key}");
        }
        let mut missing = snapshot();
        missing.as_object_mut().unwrap().remove("authority_id");
        assert!(discover(missing, 200, true).await.is_err());
        for error in [ContractError::Unauthenticated, ContractError::Forbidden] {
            assert!(
                discover(
                    serde_json::to_value(&error).unwrap(),
                    error_status(&error),
                    true
                )
                .await
                .is_err()
            );
        }
    }
    #[tokio::test]
    async fn admission_negotiates_without_effect_evidence_but_requires_audience() {
        let mut body = features_only_snapshot();
        body["worker_features"] = json!({"v":1,"supported":["resource_admission.v1"]});
        assert!(discover_runtime(body.clone(), 200, true).await.is_err());
        body["authority_id"] = json!("authority");
        body["operator_audit"] = json!(false);
        let access = discover_runtime(body.clone(), 200, true).await.unwrap();
        assert!(access.runtime_features.resource_admission());
        assert!(access.effect_evidence.is_none());
        assert_eq!(
            access.authenticated_binding.unwrap().audience.authority_id,
            "authority"
        );
        assert!(discover(body, 200, true).await.unwrap().is_none());
    }
}
