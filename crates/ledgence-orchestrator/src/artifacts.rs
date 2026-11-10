//! Optional execution artifact composition. Program packages keep their own store.
use ledgence_adapter_blob::{FileArtifactSpoolFactory, FileExecutionArtifactStore};
use ledgence_adapter_postgres::PostgresStore;
use ledgence_orchestration_api::{
    ArtifactSpoolFactory, ExecutionArtifactBlobStore, ExecutionArtifactService, decode_unique_json,
};
use ledgence_orchestration_service::ArtifactService;
use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    store_id: String,
    backend: Backend,
    spool_directory: PathBuf,
}
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Backend {
    Filesystem {
        directory: PathBuf,
    },
    S3 {
        bucket: String,
        prefix: String,
        region: String,
        endpoint_url: Option<String>,
        #[serde(default)]
        force_path_style: bool,
        #[serde(default)]
        local_credentials: bool,
    },
}
fn local_path(base: &Path, path: &Path) -> Result<PathBuf, String> {
    if path.as_os_str().is_empty() {
        return Err("artifact paths must be nonempty".into());
    }
    Ok(if path.is_absolute() {
        path.to_owned()
    } else {
        base.join(path)
    })
}
async fn read(path: PathBuf) -> Result<(Config, PathBuf), String> {
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        if !std::fs::symlink_metadata(&path)
            .map_err(|_| "could not inspect artifact configuration")?
            .file_type()
            .is_file()
        {
            return Err("artifact configuration must be a regular file".into());
        }
        let file =
            std::fs::File::open(&path).map_err(|_| "could not open artifact configuration")?;
        if !file
            .metadata()
            .map_err(|_| "could not inspect artifact configuration")?
            .is_file()
        {
            return Err("artifact configuration must be a regular file".into());
        }
        let mut bytes = Vec::new();
        file.take(16 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "could not read artifact configuration")?;
        let config: Config = decode_unique_json(&bytes, 16 * 1024)
            .map_err(|_| "invalid artifact configuration JSON")?;
        if config.schema != "ledgence.artifact-store.v1" {
            return Err("unsupported artifact configuration schema".into());
        }
        ledgence_orchestration_api::validate_text(&config.store_id, 128)
            .map_err(|_| "invalid artifact store ID")?;
        let base = path.parent().unwrap_or(Path::new(".")).to_owned();
        Ok((config, base))
    })
    .await
    .map_err(|_| "artifact configuration reader failed".to_owned())?
}
pub(crate) async fn load(
    path: Option<PathBuf>,
    catalog: Arc<PostgresStore>,
    maintenance: bool,
) -> Result<Option<Arc<dyn ExecutionArtifactService>>, String> {
    let Some(path) = path else { return Ok(None) };
    let (config, base) = read(path).await?;
    let deadline = Instant::now() + Duration::from_secs(120);
    let blobs: Arc<dyn ExecutionArtifactBlobStore> = match config.backend {
        Backend::Filesystem { directory } => Arc::new(
            FileExecutionArtifactStore::open(config.store_id, local_path(&base, &directory)?)
                .await
                .map_err(|e| e.to_string())?,
        ),
        Backend::S3 {
            bucket,
            prefix,
            region,
            endpoint_url,
            force_path_style,
            local_credentials,
        } => {
            #[cfg(feature = "s3-artifacts")]
            {
                let mut options =
                    ledgence_adapter_blob::S3ArtifactOptions::new(config.store_id, bucket, region);
                options.prefix = prefix;
                options.endpoint_url = endpoint_url;
                options.force_path_style = force_path_style;
                options.local_credentials = local_credentials;
                Arc::new(
                    ledgence_adapter_blob::S3ExecutionArtifactStore::connect(options)
                        .await
                        .map_err(|e| e.to_string())?,
                )
            }
            #[cfg(not(feature = "s3-artifacts"))]
            {
                let _ = (
                    bucket,
                    prefix,
                    region,
                    endpoint_url,
                    force_path_style,
                    local_credentials,
                );
                return Err("S3 artifacts require a binary built with s3-artifacts".into());
            }
        }
    };
    let mut spool_path = local_path(&base, &config.spool_directory)?;
    // Fixed, separately locked namespaces permit restart cleanup of crashed
    // maintenance spools. A second concurrent retain process fails admission.
    spool_path = spool_path.join(if maintenance { "retain" } else { "serve" });
    let spools = Arc::new(
        FileArtifactSpoolFactory::open(spool_path)
            .await
            .map_err(|e| e.to_string())?,
    );
    match ArtifactService::new(catalog, blobs, spools.clone(), deadline).await {
        Ok(service) => Ok(Some(Arc::new(service))),
        Err(error) => {
            drain_spools(spools.as_ref()).await;
            Err(error.to_string())
        }
    }
}
async fn drain_spools(spools: &dyn ArtifactSpoolFactory) {
    loop {
        match spools.drain(Instant::now() + Duration::from_secs(30)).await {
            Ok(()) => break,
            Err(error) => {
                tracing::warn!(%error,"artifact spool cleanup still owns local IO; waiting");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
pub(crate) async fn drain(service: &dyn ExecutionArtifactService) {
    loop {
        match service
            .drain(Instant::now() + Duration::from_secs(30))
            .await
        {
            Ok(()) => break,
            Err(error) => {
                tracing::warn!(%error,"artifact service cleanup still owns local IO; waiting");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn config_is_strict_bounded_and_relative_to_its_file() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("artifact.json");
        std::fs::write(&path,r#"{"schema":"ledgence.artifact-store.v1","store_id":"local","backend":{"kind":"filesystem","directory":"objects"},"spool_directory":"spool"}"#).unwrap();
        let (config, base) = read(path.clone()).await.unwrap();
        assert_eq!(
            local_path(&base, &config.spool_directory).unwrap(),
            root.path().join("spool")
        );
        for text in [
            r#"{"schema":"ledgence.artifact-store.v1","store_id":"local","backend":{"kind":"filesystem","directory":"objects","secret":"no"},"spool_directory":"spool"}"#,
            r#"{"schema":"ledgence.artifact-store.v1","store_id":"a","store_id":"b","backend":{"kind":"filesystem","directory":"objects"},"spool_directory":"spool"}"#,
        ] {
            std::fs::write(&path, text).unwrap();
            assert!(read(path.clone()).await.is_err());
        }
        std::fs::write(&path, vec![b' '; 16 * 1024 + 1]).unwrap();
        assert!(read(path).await.is_err());
    }
}
