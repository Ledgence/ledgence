use super::*;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream},
    task::JoinHandle,
};

struct Client {
    stream: BufReader<DuplexStream>,
    stop: CancellationToken,
    server: JoinHandle<Result<(), ServeError>>,
}
impl Client {
    fn new(read_only: bool) -> Self {
        let (client, server) = tokio::io::duplex(256 * 1024);
        let (input, output) = tokio::io::split(server);
        let stop = CancellationToken::new();
        let task = tokio::spawn(serve(
            operations::test_dispatcher(read_only),
            input,
            output,
            stop.clone(),
        ));
        Self {
            stream: BufReader::new(client),
            stop,
            server: task,
        }
    }
    async fn send(&mut self, request: Value) {
        let mut bytes = serde_json::to_vec(&request).unwrap();
        bytes.push(b'\n');
        self.stream.get_mut().write_all(&bytes).await.unwrap();
    }
    async fn read(&mut self) -> Value {
        let mut line = String::new();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), self.stream.read_line(&mut line))
                .await
                .unwrap()
                .unwrap()
                > 0
        );
        serde_json::from_str(&line).unwrap()
    }
    async fn initialize(&mut self, version: &str) -> Value {
        self.send(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":version,"capabilities":{},"clientInfo":{"name":"ledgence-test","version":"1"}}})).await;
        let response = self.read().await;
        self.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await;
        response
    }
    async fn finish(self) {
        drop(self.stream);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), self.server)
                .await
                .unwrap()
                .unwrap(),
            Ok(())
        );
    }
}
fn meta() -> Value {
    json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"ledgence-test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}})
}

#[tokio::test]
async fn legacy_initialize_lists_read_only_tools_and_negotiates_supported_versions() {
    for version in ["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"] {
        let mut client = Client::new(true);
        let response = client.initialize(version).await;
        assert_eq!(response["result"]["protocolVersion"], version);
        assert_eq!(response["result"]["serverInfo"]["name"], "ledgence");
        assert!(response["result"]["capabilities"]["tools"].is_object());
        assert!(response["result"]["capabilities"].get("tasks").is_none());
        client
            .send(json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
            .await;
        let response = client.read().await;
        let tools = response["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), schemas::tool_definitions(true).len());
        assert!(
            tools
                .iter()
                .all(|tool| tool["annotations"]["readOnlyHint"] == true)
        );
        assert!(response["result"].get("nextCursor").is_none());
        client.finish().await;
    }
}

#[tokio::test]
async fn modern_discover_and_per_request_metadata_use_current_lifecycle() {
    let mut client = Client::new(false);
    client.send(json!({"jsonrpc":"2.0","id":"discover","method":"server/discover","params":{"_meta":meta()}})).await;
    let response = client.read().await;
    assert_eq!(response["result"]["resultType"], "complete");
    assert!(
        response["result"]["supportedVersions"]
            .as_array()
            .unwrap()
            .contains(&json!("2026-07-28"))
    );
    client
        .send(json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{"_meta":meta()}}))
        .await;
    let response = client.read().await;
    assert_eq!(
        response["result"]["tools"].as_array().unwrap().len(),
        schemas::tool_definitions(false).len()
    );
    assert_eq!(response["result"]["resultType"], "complete");
    client
        .send(json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}))
        .await;
    assert_eq!(client.read().await["error"]["code"], -32602);
    client.finish().await;
}

#[tokio::test]
async fn invalid_tools_parameters_and_hidden_mutations_do_not_dispatch() {
    let mut client = Client::new(true);
    client.initialize("2025-11-25").await;
    for (id, params) in [
        (2, json!({"name":"does_not_exist","arguments":{}})),
        (3, json!({"name":"ledgence_task_status","arguments":{}})),
        (4, json!({"name":"ledgence_task_submit","arguments":{}})),
        (5, json!({"name":"ledgence_approval_decide","arguments":{}})),
        (
            6,
            json!({"name":"ledgence_task_status","arguments":{"task_id":"task_1","tenant":"other"}}),
        ),
        (
            7,
            json!({"name":"ledgence_task_status","arguments":{"task_id":"task_1"},"requestState":"never-issued"}),
        ),
    ] {
        client
            .send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":params}))
            .await;
        let response = client.read().await;
        assert_eq!(response["id"], id);
        assert_eq!(response["error"]["code"], -32602);
    }
    client
        .send(
            json!({"jsonrpc":"2.0","id":8,"method":"tools/list","params":{"cursor":"unexpected"}}),
        )
        .await;
    assert_eq!(client.read().await["error"]["code"], -32602);
    client.finish().await;
}

#[tokio::test]
async fn eof_before_initialize_and_explicit_shutdown_are_clean() {
    Client::new(false).finish().await;
    let client = Client::new(false);
    client.stop.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), client.server)
            .await
            .unwrap()
            .unwrap(),
        Ok(())
    );
}

#[tokio::test]
async fn malformed_frame_is_a_sanitized_session_failure() {
    let mut client = Client::new(false);
    client
        .stream
        .get_mut()
        .write_all(b"private-malformed-payload\n")
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(3), client.server)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error, ServeError::InvalidFrame);
    assert!(!error.to_string().contains("private-malformed-payload"));
}

#[tokio::test]
async fn malformed_tool_parameters_return_invalid_params_and_keep_both_lifecycles_usable() {
    for modern in [false, true] {
        let mut client = Client::new(false);
        if modern {
            client.send(json!({"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":meta()}})).await;
            assert_eq!(client.read().await["result"]["resultType"], "complete");
        } else {
            client.initialize("2025-11-25").await;
        }
        let malformed = [
            Value::Null,
            json!([]),
            json!({}),
            json!({"name":5,"arguments":{}}),
            json!({"name":"ledgence_program_list","arguments":[]}),
            json!({"name":"ledgence_program_list","arguments":null}),
            json!({"name":"ledgence_program_list","arguments":false}),
        ];
        for (index, mut params) in malformed.into_iter().enumerate() {
            if modern && let Some(object) = params.as_object_mut() {
                object.insert("_meta".into(), meta());
            }
            let id = 2 + index;
            client
                .send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":params}))
                .await;
            let response = client.read().await;
            assert_eq!(response["id"], id);
            assert_eq!(
                response["error"]["code"], -32602,
                "modern={modern}: {response}"
            );
        }
        // Unknown methods remain method-not-found rather than invalid params.
        let params = if modern {
            json!({"_meta":meta()})
        } else {
            json!({})
        };
        client
            .send(json!({"jsonrpc":"2.0","id":90,"method":"does/not/exist","params":params}))
            .await;
        assert_eq!(client.read().await["error"]["code"], -32601);
        client
            .send(json!({"jsonrpc":"2.0","id":91,"method":"tools/list","params":params}))
            .await;
        assert!(client.read().await["result"]["tools"].is_array());
        client.finish().await;
    }
}

#[tokio::test]
async fn malformed_tool_parameters_do_not_bypass_modern_metadata_or_version_checks() {
    let mut client = Client::new(false);
    client
        .send(json!({"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":meta()}}))
        .await;
    client.read().await;
    client
        .send(json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":5}}))
        .await;
    let missing_meta = client.read().await;
    assert_eq!(missing_meta["error"]["code"], -32602);
    assert!(
        missing_meta["error"]["message"]
            .as_str()
            .unwrap()
            .contains("_meta")
    );
    let mut invalid_version = meta();
    invalid_version["io.modelcontextprotocol/protocolVersion"] = json!("2099-01-01");
    client.send(json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":5,"_meta":invalid_version}})).await;
    let response = client.read().await;
    assert_eq!(
        response["error"]["code"],
        json!(rmcp::model::ErrorCode::UNSUPPORTED_PROTOCOL_VERSION)
    );
    client
        .send(json!({"jsonrpc":"2.0","id":4,"method":"tools/list","params":{"_meta":meta()}}))
        .await;
    assert!(client.read().await["result"]["tools"].is_array());
    client.finish().await;
}
