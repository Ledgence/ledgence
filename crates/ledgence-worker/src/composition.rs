//! Shared worker adapters for local fixtures and remote delivery.

use ledgence_adapter_artifact::{
    ArtifactLimits, FileArtifactCache, FileProgramStore, HttpProgramStore,
};
use ledgence_adapter_subprocess::SubprocessRuntime;
use ledgence_worker_api::{ProgramStore, Result};
use ledgence_worker_core::{Worker, WorkerConfig};
use std::{path::Path, sync::Arc};

pub struct WorkerParts {
    pub store: Arc<dyn ProgramStore>,
    cache: Arc<FileArtifactCache>,
    runtime: Arc<SubprocessRuntime>,
}

impl WorkerParts {
    /// Call from blocking preparation: the filesystem adapters inspect disk.
    pub fn new(store: &str, cache: &Path, python: &Path, runner: &Path) -> Result<Self> {
        let limits = ArtifactLimits::default();
        let store: Arc<dyn ProgramStore> =
            if store.starts_with("http://") || store.starts_with("https://") {
                Arc::new(HttpProgramStore::new(store, limits.clone())?)
            } else {
                Arc::new(FileProgramStore::new(store, limits.clone())?)
            };
        Ok(Self {
            store,
            cache: Arc::new(FileArtifactCache::new(cache, limits)?),
            runtime: Arc::new(SubprocessRuntime::new(python, runner)),
        })
    }

    pub fn worker(self, concurrency: usize) -> Result<Worker> {
        Worker::new(
            WorkerConfig {
                concurrency,
                ..WorkerConfig::default()
            },
            self.store,
            self.cache,
            self.runtime,
        )
    }
}
