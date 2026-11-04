//! Startup selection and bounded live reload for multi-queue polling.
use crate::{input, read_bounded};
use ledgence_orchestration_api::decode_unique_json;
use ledgence_worker_api::Result;
use ledgence_worker_delivery::poll_plan::{PollPlan, PollPlanner, PollStrategy};
use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

const MAX_BYTES: usize = 16 * 1024;
const REFRESH: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Strategy {
    Stride,
    Lottery,
}

impl From<Strategy> for PollStrategy {
    fn from(strategy: Strategy) -> Self {
        match strategy {
            Strategy::Stride => Self::Stride,
            Strategy::Lottery => Self::Lottery,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueWeight {
    queue: String,
    weight: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    schema_version: u32,
    revision: u64,
    strategy: Strategy,
    queues: Vec<QueueWeight>,
}

impl Document {
    fn decode(bytes: &[u8]) -> Result<Self> {
        let document: Self = decode_unique_json(bytes, MAX_BYTES).map_err(invalid_plan)?;
        if document.schema_version != 1 {
            return Err(input("poll plan schema_version must be 1"));
        }
        Ok(document)
    }

    fn targets(self) -> Result<PollPlan> {
        PollPlan::new(
            self.revision,
            self.queues
                .into_iter()
                .map(|route| (route.queue, route.weight))
                .collect(),
        )
        .map_err(invalid_plan)
    }
}

fn invalid_plan(error: impl std::fmt::Display) -> ledgence_worker_api::Error {
    input(format!("invalid poll plan: {error}"))
}

fn read(path: &Path) -> Result<Vec<u8>> {
    read_bounded(path, MAX_BYTES as u64, "poll plan")
}

pub(super) struct PlanFile {
    path: PathBuf,
    strategy: Strategy,
    revision: u64,
    planner: PollPlanner,
    last_bytes: Vec<u8>,
}

impl PlanFile {
    pub(super) fn load(path: PathBuf) -> Result<Self> {
        let bytes = read(&path)?;
        let document = Document::decode(&bytes)?;
        let strategy = document.strategy;
        let revision = document.revision;
        let planner = PollPlanner::new(document.targets()?, strategy.into());
        Ok(Self {
            path,
            strategy,
            revision,
            planner,
            last_bytes: bytes,
        })
    }

    pub(super) fn planner(&self) -> &PollPlanner {
        &self.planner
    }

    fn apply(&mut self, bytes: Vec<u8>) -> Result<bool> {
        if self.last_bytes == bytes {
            return Ok(false);
        }
        let document = Document::decode(&bytes)?;
        if document.strategy != self.strategy {
            return Err(input(
                "changing the poll strategy requires restarting the worker",
            ));
        }
        let revision = document.revision;
        self.planner
            .install(document.targets()?)
            .map_err(invalid_plan)?;
        let advanced = self.revision != revision;
        self.revision = revision;
        self.last_bytes = bytes;
        Ok(advanced)
    }

    pub(super) async fn watch(mut self) {
        tracing::info!(revision = self.revision, "worker poll plan loaded");
        let mut last_problem = None;
        loop {
            tokio::time::sleep(REFRESH).await;
            let path = self.path.clone();
            // File I/O must not occupy the runtime thread that renews leases or executes shutdown.
            let update = tokio::task::spawn_blocking(move || read(&path))
                .await
                .map_err(|_| input("poll plan reader failed"))
                .and_then(|data| data)
                .and_then(|data| self.apply(data));
            match update {
                Ok(advanced) => {
                    let recovered = last_problem.take().is_some();
                    if advanced || recovered {
                        tracing::info!(revision = self.revision, "worker poll plan loaded");
                    }
                }
                Err(error) => {
                    let reason = error.to_string();
                    if last_problem.as_ref() != Some(&reason) {
                        tracing::warn!(revision = self.revision, %reason, "worker poll plan rejected");
                        last_problem = Some(reason);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn config(revision: u64, strategy: &str, a: u32, b: u32) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schema_version":1, "revision":revision, "strategy":strategy,
            "queues":[{"queue":"a","weight":a},{"queue":"b","weight":b}]
        }))
        .unwrap()
    }

    #[test]
    fn rejected_edits_keep_the_live_plan_and_idle_can_resume() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plan.json");
        fs::write(&path, config(10, "stride", 1, 0)).unwrap();
        let mut file = PlanFile::load(path).unwrap();
        let shared = file.planner().clone();
        let mut different_routes: serde_json::Value =
            serde_json::from_slice(&config(11, "stride", 0, 1)).unwrap();
        different_routes["queues"][1]["queue"] = "not-subscribed".into();
        for invalid in [
            config(9, "stride", 0, 1),
            config(10, "stride", 0, 1),
            config(11, "lottery", 0, 1),
            b"{}".to_vec(),
            serde_json::to_vec(&different_routes).unwrap(),
        ] {
            assert!(file.apply(invalid).is_err());
            assert_eq!(shared.issue(), Some(0));
        }
        assert!(file.apply(config(11, "stride", 0, 0)).unwrap());
        assert_eq!(shared.issue(), None);
        assert!(file.apply(config(12, "stride", 0, 1)).unwrap());
        assert_eq!(shared.issue(), Some(1));
    }

    #[test]
    fn file_boundary_rejects_ambiguous_and_oversized_input() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plan.json");
        let json = String::from_utf8(config(1, "stride", 7, 3)).unwrap();
        fs::write(
            &path,
            json.replace("\"weight\":7", "\"weight\":7,\"weight\":2"),
        )
        .unwrap();
        assert!(PlanFile::load(path.clone()).is_err());
        fs::write(&path, vec![b' '; MAX_BYTES + 1]).unwrap();
        assert!(PlanFile::load(path).is_err());
    }
}
