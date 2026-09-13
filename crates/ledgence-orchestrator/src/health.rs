use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone)]
pub struct Health {
    pub stopping: Arc<AtomicBool>,
    inner: Arc<Mutex<Progress>>,
    freshness: Duration,
}

#[derive(Default)]
struct Progress {
    prerequisites: bool,
    last_success: Option<Instant>,
    failure: Option<&'static str>,
}

impl Health {
    pub fn new(freshness: Duration) -> Self {
        Self {
            stopping: Arc::new(AtomicBool::new(false)),
            inner: Arc::new(Mutex::new(Progress::default())),
            freshness,
        }
    }

    pub fn prerequisites_ready(&self) {
        self.progress().prerequisites = true;
    }

    pub fn recovery_success(&self) {
        let mut progress = self.progress();
        progress.last_success = Some(Instant::now());
        progress.failure = None;
    }

    pub fn recovery_failure(&self, reason: &'static str) {
        self.progress().failure = Some(reason);
    }

    pub fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
    }

    fn progress(&self) -> std::sync::MutexGuard<'_, Progress> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn readiness(&self) -> (StatusCode, serde_json::Value) {
        let progress = self.progress();
        let age = progress.last_success.map(|instant| instant.elapsed());
        let mut reasons = Vec::new();
        if self.stopping.load(Ordering::Acquire) {
            reasons.push("shutting_down");
        }
        if !progress.prerequisites {
            reasons.push("startup_incomplete");
        }
        if let Some(reason) = progress.failure {
            reasons.push(reason);
        }
        match age {
            None => reasons.push("recovery_pending"),
            Some(age) if age > self.freshness => reasons.push("recovery_stale"),
            _ => {}
        }
        let ready = reasons.is_empty();
        (
            if ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            json!({
                "status": if ready { "ok" } else { "not_ready" },
                "reasons": reasons,
                "recovery_last_success_age_ms": age.map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
            }),
        )
    }

    pub fn router(&self) -> Router {
        let ready = self.clone();
        Router::new()
            .route(
                "/health/live",
                get(|| async { reply(StatusCode::OK, json!({"status":"ok", "reasons":[]})) }),
            )
            .route(
                "/health/ready",
                get(move || {
                    let ready = ready.clone();
                    async move {
                        let (status, body) = ready.readiness();
                        reply(status, body)
                    }
                }),
            )
    }
}

fn reply(status: StatusCode, body: serde_json::Value) -> impl IntoResponse {
    (
        status,
        [
            ("content-type", "application/json"),
            ("cache-control", "no-store"),
        ],
        body.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn readiness_requires_prerequisites_and_fresh_success_and_stops_immediately() {
        let health = Health::new(Duration::from_secs(40));
        assert_eq!(health.readiness().0, StatusCode::SERVICE_UNAVAILABLE);
        health.prerequisites_ready();
        assert!(health.readiness().1["recovery_last_success_age_ms"].is_null());
        health.recovery_success();
        assert_eq!(health.readiness().0, StatusCode::OK);
        tokio::time::advance(Duration::from_secs(40)).await;
        assert_eq!(health.readiness().0, StatusCode::OK);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(health.readiness().1["reasons"], json!(["recovery_stale"]));
        health.recovery_success();
        health.recovery_failure("recovery_unavailable");
        assert_eq!(
            health.readiness().1["reasons"],
            json!(["recovery_unavailable"])
        );
        health.recovery_success();
        assert_eq!(health.readiness().0, StatusCode::OK);
        health.stop();
        health.recovery_success();
        assert_eq!(health.readiness().1["reasons"], json!(["shutting_down"]));
    }
}
