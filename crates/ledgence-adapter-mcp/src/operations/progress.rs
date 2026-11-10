use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    target: ExecutionProgressTarget,
    cursor: Option<String>,
    #[serde(default = "default_progress_limit")]
    limit: u32,
}
fn default_progress_limit() -> u32 {
    EXECUTION_PROGRESS_MAX_LIMIT
}

impl ToolDispatcher {
    pub(super) async fn call_progress(
        &self,
        arguments: &Value,
    ) -> std::result::Result<Value, ToolError> {
        let args: Arguments = parse(arguments)?;
        let query = ExecutionProgressQuery {
            scope: self.scope.clone(),
            target: args.target,
            cursor: args.cursor,
            limit: args.limit,
            wait_ms: 0,
        };
        query.validate().map_err(ToolError::arguments)?;
        let reader = remote(self.tasks.progress_reader(), false)?;
        let page = remote(reader.execution_progress(&query).await, false)?;
        page.validate_for(&query)
            .map_err(|_| invalid_reply(false))?;
        encode(page, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn progress_rejects_overrides_invalid_cursors_and_bounds_before_transport() {
        let dispatcher = test_dispatcher(true);
        for args in [
            json!({"target":{"kind":"task","id":"task"},"scope":{"tenant_id":"other","namespace":"other"}}),
            json!({"target":{"kind":"task","id":""}}),
            json!({"target":{"kind":"tasks","id":"task"}}),
            json!({"target":{"kind":"task","id":"task"},"cursor":"00"}),
            json!({"target":{"kind":"task","id":"task"},"limit":0}),
            json!({"target":{"kind":"task","id":"task"},"limit":101}),
            json!({"target":{"kind":"task","id":"task"},"follow":true}),
        ] {
            let error = dispatcher
                .call("ledgence_execution_progress", &args)
                .await
                .unwrap_err();
            assert_eq!(error.code, "invalid_arguments", "{args}: {error}");
            assert!(!error.outcome_unknown);
        }
    }
    #[test]
    fn progress_discovery_is_bounded_and_read_only() {
        let dispatcher = test_dispatcher(true);
        let definition = dispatcher
            .definitions()
            .find(|tool| tool.name == "ledgence_execution_progress")
            .unwrap();
        assert!(definition.read_only && definition.idempotent && !definition.destructive);
        assert_eq!(
            definition.input_schema["properties"]["limit"]["maximum"],
            100
        );
        assert_eq!(
            definition.input_schema["properties"]["cursor"]["maxLength"],
            4096
        );
        assert_eq!(definition.input_schema["additionalProperties"], false);
        assert!(definition.input_schema["properties"].get("scope").is_none());
    }
}
