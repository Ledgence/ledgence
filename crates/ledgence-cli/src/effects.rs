//! A saved assessment is immutable and bound to its reviewed endpoint and scope.
use crate::{encode, submission};
use ledgence_adapter_http::HttpTaskService;
use ledgence_orchestration_api::*;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedResolution {
    format: String,
    base_url: String,
    body: String,
}

fn restore(bytes: &[u8], endpoint: &str, scope: &Scope) -> Result<Vec<u8>> {
    let saved: SavedResolution = decode_unique_json(bytes, EFFECT_RESOLUTION_SAVED_MAX_BYTES)?;
    if saved.format != EFFECT_RESOLUTION_SAVED_FORMAT || saved.base_url != endpoint {
        return Err(ContractError::InvalidInput(
            "saved assessment format or destination does not match this client".into(),
        ));
    }
    let command: EffectResolutionCommand =
        decode_unique_json(saved.body.as_bytes(), EFFECT_RESOLUTION_COMMAND_MAX_BYTES)?;
    command.validate()?;
    if command.occurrence.scope != *scope {
        return Err(ContractError::InvalidInput(
            "saved assessment belongs to another scope".into(),
        ));
    }
    Ok(saved.body.into_bytes())
}

pub async fn resolve(client: &HttpTaskService, scope: Scope, file: PathBuf) -> Result<Vec<u8>> {
    let endpoint = client.base_url().to_owned();
    let body = tokio::task::spawn_blocking(move || {
        let bytes = submission::read_bytes(
            &file,
            EFFECT_RESOLUTION_SAVED_MAX_BYTES,
            "saved effect assessment",
        )?;
        restore(&bytes, &endpoint, &scope)
    })
    .await
    .map_err(|_| ContractError::Unavailable("effect assessment preparation failed".into()))??;
    encode(client.resolve_effect_bytes(&body).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    fn scope() -> Scope {
        Scope {
            tenant_id: "acme".into(),
            namespace: "billing".into(),
        }
    }
    fn command() -> Value {
        json!({"occurrence":{"scope":scope(),"workflow_id":"workflow","activation_id":"original","approval_key":"refund"},
            "resolution_id":"resolve-1","expected_effect_revision":u64::MAX-1,"supersedes_resolution_id":null,
            "action":{"name":"refund","version":"1","arguments":{"amount":50.0}},
            "assessment":{"kind":"record_success","result":null},"reason":"  reviewed\n", "evidence_references":["receipt:1"]})
    }
    fn save(body: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({"format":EFFECT_RESOLUTION_SAVED_FORMAT,"base_url":"http://localhost:8080","body":body})).unwrap()
    }
    #[test]
    fn shared_console_export_restores_exact_body_and_numeric_kinds() {
        let bytes = include_bytes!(
            "../../../sdk/python-client/tests/fixtures/console-effect-resolution.json"
        );
        let envelope: SavedResolution = serde_json::from_slice(bytes).unwrap();
        let scope = Scope {
            tenant_id: "acme".into(),
            namespace: "demo".into(),
        };
        let restored = restore(bytes, &envelope.base_url, &scope).unwrap();
        assert_eq!(restored, envelope.body.as_bytes());
        let value: Value =
            decode_unique_json(&restored, EFFECT_RESOLUTION_COMMAND_MAX_BYTES).unwrap();
        assert_eq!(
            value["expected_effect_revision"].as_u64(),
            Some(u64::MAX - 1)
        );
        assert_eq!(
            value["assessment"]["result"]["integer"].as_u64(),
            Some(u64::MAX)
        );
        assert!(value["assessment"]["result"]["float"].is_f64());
    }

    #[test]
    fn saved_command_preserves_exact_body_bytes_and_u64_without_rebinding() {
        let body = serde_json::to_string_pretty(&command()).unwrap();
        assert_eq!(
            restore(&save(&body), "http://localhost:8080", &scope()).unwrap(),
            body.as_bytes()
        );
        for endpoint in [
            "http://127.0.0.1:8080",
            "http://localhost:8080/",
            "https://localhost:8080",
        ] {
            assert!(restore(&save(&body), endpoint, &scope()).is_err());
        }
        let mut foreign = scope();
        foreign.namespace = "other".into();
        assert!(restore(&save(&body), "http://localhost:8080", &foreign).is_err());
        assert!(restore(body.as_bytes(), "http://localhost:8080", &scope()).is_err());
    }
    #[test]
    fn saved_command_rejects_duplicate_fields_nulls_oversize_and_unknown_fields() {
        let body = serde_json::to_string(&command()).unwrap();
        for bytes in [
            save(&body.replacen('{', "{\"reason\":\"duplicate\",", 1)),
            save(&body).into_iter().take(10).collect(),
            vec![b' '; EFFECT_RESOLUTION_SAVED_MAX_BYTES + 1],
            save(&" ".repeat(EFFECT_RESOLUTION_COMMAND_MAX_BYTES + 1)),
        ] {
            assert!(restore(&bytes, "http://localhost:8080", &scope()).is_err());
        }
        for value in [
            json!({"format":EFFECT_RESOLUTION_SAVED_FORMAT,"base_url":"http://localhost:8080","body":null}),
            json!({"format":EFFECT_RESOLUTION_SAVED_FORMAT,"base_url":"http://localhost:8080","body":body,"credentials":"no"}),
        ] {
            assert!(
                restore(
                    &serde_json::to_vec(&value).unwrap(),
                    "http://localhost:8080",
                    &scope()
                )
                .is_err()
            );
        }
    }
}
