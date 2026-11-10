use super::*;
use std::time::{Duration, Instant};
pub(super) fn is_tool(name: &str) -> bool {
    matches!(name, "ledgence_artifact_inspect" | "ledgence_artifact_list")
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inspect {
    artifact_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct List {
    target: ExecutionTarget,
    #[serde(default = "default_limit")]
    limit: u32,
    cursor: Option<String>,
}
impl ToolDispatcher {
    pub(super) async fn call_artifacts(
        &self,
        name: &str,
        args: &Value,
    ) -> std::result::Result<Value, ToolError> {
        let reader = self.artifacts.as_ref().ok_or_else(|| ToolError {
            code: "unknown_tool",
            message: "Artifact metadata reader is not configured".into(),
            outcome_unknown: false,
        })?;
        let deadline = Instant::now() + Duration::from_secs(30);
        match name {
            "ledgence_artifact_inspect" => {
                let args: Inspect = parse(args)?;
                let query = ArtifactInspectQuery {
                    scope: self.scope.clone(),
                    artifact_id: args.artifact_id,
                };
                query.validate().map_err(ToolError::arguments)?;
                let reply = remote(reader.inspect(&query, deadline).await, false)?;
                reply.validate().map_err(|_| invalid_reply(false))?;
                if reply.scope != self.scope || reply.reference.id != query.artifact_id {
                    return Err(invalid_reply(false));
                }
                encode(reply, false)
            }
            "ledgence_artifact_list" => {
                let args: List = parse(args)?;
                let query = ArtifactListQuery {
                    scope: self.scope.clone(),
                    target: args.target,
                    limit: args.limit,
                    cursor: args.cursor,
                };
                query.validate().map_err(ToolError::arguments)?;
                let reply = remote(reader.list(&query, deadline).await, false)?;
                reply
                    .validate_for(&query)
                    .map_err(|_| invalid_reply(false))?;
                encode(reply, false)
            }
            _ => unreachable!("artifact tool selected"),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct Reader {
        calls: AtomicUsize,
    }
    impl ArtifactMetadataReader for Reader {
        fn inspect<'a>(
            &'a self,
            _: &'a ArtifactInspectQuery,
            _: Instant,
        ) -> ContractFuture<'a, ArtifactManifest> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err(ContractError::NotFound)
            })
        }
        fn list<'a>(
            &'a self,
            q: &'a ArtifactListQuery,
            _: Instant,
        ) -> ContractFuture<'a, ArtifactPage> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(q.scope.tenant_id, "acme");
                Ok(ArtifactPage {
                    items: vec![],
                    next_cursor: None,
                })
            })
        }
    }
    #[tokio::test]
    async fn metadata_tools_reject_scope_bytes_urls_and_transfer_operations() {
        let reader = Arc::new(Reader::default());
        let dispatcher = test_dispatcher(true).with_artifact_reader(reader.clone());
        for (name, args) in [
            (
                "ledgence_artifact_inspect",
                json!({"artifact_id":"art_one","scope":{}}),
            ),
            (
                "ledgence_artifact_list",
                json!({"target":{"kind":"task","id":"t"},"url":"https://example.com"}),
            ),
            (
                "ledgence_artifact_list",
                json!({"target":{"kind":"task","id":"t"},"limit":101}),
            ),
            (
                "ledgence_artifact_inspect",
                json!({"artifact_id":"art_one","content":"x"}),
            ),
        ] {
            assert_eq!(
                dispatcher.call(name, &args).await.unwrap_err().code,
                "invalid_arguments"
            );
        }
        assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            dispatcher
                .call(
                    "ledgence_artifact_list",
                    &json!({"target":{"kind":"workflow","id":"wf_one"},"limit":1})
                )
                .await
                .unwrap(),
            json!({"items":[],"next_cursor":null})
        );
        assert_eq!(reader.calls.load(Ordering::SeqCst), 1);

        for name in [
            "ledgence_artifact_put",
            "ledgence_artifact_download",
            "ledgence_artifact_replace",
        ] {
            assert_eq!(
                dispatcher.call(name, &json!({})).await.unwrap_err().code,
                "unknown_tool"
            );
        }
        let tools: Vec<_> = dispatcher
            .definitions()
            .filter(|tool| is_tool(tool.name))
            .collect();
        assert_eq!(tools.len(), 2);
        assert!(
            tools
                .iter()
                .all(|tool| tool.read_only && tool.idempotent && !tool.destructive)
        );
        assert!(
            !test_dispatcher(true)
                .definitions()
                .any(|tool| is_tool(tool.name))
        );
    }
}
