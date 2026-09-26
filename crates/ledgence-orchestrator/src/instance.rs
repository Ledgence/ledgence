//! Bounded loading of server-only instance configuration.
use ledgence_orchestration_api::SelfHostedInstanceConfig;
use ledgence_orchestration_api::decode_unique_json;
use std::{io::Read, path::PathBuf};
const CONFIG_MAX_BYTES: usize = 64 * 1024;
pub async fn load_optional(
    path: Option<PathBuf>,
) -> Result<Option<SelfHostedInstanceConfig>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(path)
            .map_err(|error| format!("could not open instance configuration: {error}"))?;
        let mut bytes = Vec::new();
        file.take(CONFIG_MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| format!("could not read instance configuration: {error}"))?;
        let config: SelfHostedInstanceConfig = decode_unique_json(&bytes, CONFIG_MAX_BYTES)
            .map_err(|_| "invalid instance configuration JSON".to_owned())?;
        config.validate().map_err(|error| error.to_string())?;
        Ok(Some(config))
    })
    .await
    .map_err(|_| "instance configuration loading failed".to_owned())?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn configuration_loader_rejects_unknown_duplicate_and_oversized_documents() {
        assert!(load_optional(None).await.unwrap().is_none());
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("instance.json");
        for bytes in [
            br#"{"instance_id":"local","name":"Local","unknown":true}"#.to_vec(),
            br#"{"instance_id":"local","name":"Local","name":"Changed"}"#.to_vec(),
            vec![b' '; CONFIG_MAX_BYTES + 1],
            br#"{"instance_id":"local","name":"Local","suggested_queues":["q","q"]}"#.to_vec(),
        ] {
            std::fs::write(&path, bytes).unwrap();
            assert!(load_optional(Some(path.clone())).await.is_err());
        }
        std::fs::write(&path, br#"{"instance_id":"local","name":"Local"}"#).unwrap();
        let config = load_optional(Some(path)).await.unwrap().unwrap();
        assert_eq!(config.scope.tenant_id, "default");
        assert_eq!(config.scope.namespace, "default");
    }
}
