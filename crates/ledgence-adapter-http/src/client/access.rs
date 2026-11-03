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
    fn worker(&self, expected: &Scope, credentials: bool) -> Result<Option<WorkerEffectAccess>> {
        let Self::Credentials(reply) = self else {
            return if credentials {
                Err(unavailable(
                    "credentialed worker requires authenticated access discovery",
                ))
            } else {
                Ok(None)
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
        if !enabled {
            return Ok(None);
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
        Ok(Some(WorkerEffectAccess { audience, identity }))
    }
}

impl HttpTaskService {
    /// Negotiate once before starting a worker. A credentialed connection never
    /// falls back on missing, malformed, anonymous or rejected discovery.
    pub async fn discover_worker(&self, scope: &Scope) -> Result<Option<WorkerEffectAccess>> {
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
            Err(ContractError::NotFound) if !credentials => Ok(None),
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
    async fn discover(
        body: Value,
        status: u16,
        credentialed: bool,
    ) -> Result<Option<WorkerEffectAccess>> {
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
        let result = client.discover_worker(&scope()).await;
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        result
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
}
