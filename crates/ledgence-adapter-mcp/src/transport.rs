//! Bounded newline transport. Protocol/lifecycle semantics remain in rmcp.
use std::{
    collections::HashMap,
    io::{self, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use ledgence_orchestration_api::decode_unique_json;
use rmcp::{
    RoleServer,
    model::{ClientRequest, CustomRequest, JsonRpcMessage, RequestId, RequestMetaObject},
    service::{RxJsonRpcMessage, TxJsonRpcMessage},
    transport::Transport,
};
use serde_json::{Map, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::Mutex as AsyncMutex,
};
use tokio_util::sync::CancellationToken;

use crate::ServeError;

pub(crate) const MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_OUTPUT_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_PENDING_REQUESTS: usize = 16;

struct Pending {
    cancelled: CancellationToken,
    cancellable: bool,
}

pub(crate) struct Connection {
    pending: Mutex<HashMap<RequestId, Pending>>,
    failure: Mutex<Option<ServeError>>,
    pub(crate) closed_input: AtomicBool,
    pub(crate) stop: CancellationToken,
}

impl Connection {
    pub(crate) fn new(stop: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(HashMap::new()),
            failure: Mutex::new(None),
            closed_input: AtomicBool::new(false),
            stop,
        })
    }

    pub(crate) fn fail(&self, error: ServeError) {
        let mut failure = self
            .failure
            .lock()
            .expect("connection failure lock poisoned");
        if failure.is_none() {
            *failure = Some(error);
        }
        self.stop.cancel();
    }

    pub(crate) fn failure(&self) -> Option<ServeError> {
        self.failure
            .lock()
            .expect("connection failure lock poisoned")
            .clone()
    }

    fn register(&self, id: RequestId, cancellable: bool) -> Result<(), ServeError> {
        let mut pending = self.pending.lock().expect("pending request lock poisoned");
        if pending.contains_key(&id) {
            return Err(ServeError::DuplicateRequest);
        }
        if pending.len() >= MAX_PENDING_REQUESTS {
            return Err(ServeError::TooManyRequests);
        }
        pending.insert(
            id,
            Pending {
                cancelled: self.stop.child_token(),
                cancellable,
            },
        );
        Ok(())
    }

    pub(crate) fn request_token(&self, id: &RequestId) -> CancellationToken {
        self.pending
            .lock()
            .expect("pending request lock poisoned")
            .get(id)
            .map(|pending| pending.cancelled.clone())
            .unwrap_or_else(|| self.stop.child_token())
    }

    fn cancel(&self, id: &RequestId) {
        if let Some(pending) = self
            .pending
            .lock()
            .expect("pending request lock poisoned")
            .get(id)
            && pending.cancellable
        {
            pending.cancelled.cancel();
        }
    }

    fn is_cancelled(&self, id: &RequestId) -> bool {
        self.pending
            .lock()
            .expect("pending request lock poisoned")
            .get(id)
            .is_none_or(|pending| pending.cancelled.is_cancelled())
    }

    fn finish(&self, id: &RequestId) {
        self.pending
            .lock()
            .expect("pending request lock poisoned")
            .remove(id);
    }
}

pub(crate) struct BoundedTransport<R, W> {
    input: BufReader<R>,
    line: Vec<u8>,
    output: Arc<AsyncMutex<W>>,
    connection: Arc<Connection>,
    initialized_seen: bool,
}

impl<R: AsyncRead, W: AsyncWrite> BoundedTransport<R, W> {
    pub(crate) fn new(input: R, output: W, connection: Arc<Connection>) -> Self {
        Self {
            input: BufReader::new(input),
            line: Vec::new(),
            output: Arc::new(AsyncMutex::new(output)),
            connection,
            initialized_seen: false,
        }
    }
}

impl<R, W> Transport<RoleServer> for BoundedTransport<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    type Error = ServeError;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleServer>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let output = self.output.clone();
        let connection = self.connection.clone();
        async move {
            let id = match &item {
                JsonRpcMessage::Response(response) => Some(response.id.clone()),
                JsonRpcMessage::Error(error) => error.id.clone(),
                _ => None,
            };
            // Consume cancellation in this transport, rather than in rmcp: rmcp
            // otherwise suppresses completion before send(), losing our bounded
            // outstanding-request slot. Keep the ID reserved until completion so
            // a late cancelled result cannot be mistaken for a newer request.
            if id.as_ref().is_some_and(|id| connection.is_cancelled(id)) {
                connection.finish(id.as_ref().expect("checked response ID"));
                return Ok(());
            }
            let mut bytes = LimitedWriter(Vec::new());
            if serde_json::to_writer(&mut bytes, &item).is_err() {
                connection.fail(ServeError::OutputTooLarge);
                return Err(ServeError::OutputTooLarge);
            }
            bytes.0.push(b'\n');
            let write = async {
                let mut output = output.lock().await;
                if id.as_ref().is_some_and(|id| connection.is_cancelled(id)) {
                    return Ok(());
                }
                output.write_all(&bytes.0).await?;
                output.flush().await
            };
            let result = tokio::select! {
                biased;
                () = connection.stop.cancelled() => return Ok(()),
                result = write => result,
            };
            if result.is_err() {
                connection.fail(ServeError::OutputFailed);
                return Err(ServeError::OutputFailed);
            }
            if let Some(id) = id {
                connection.finish(&id);
            }
            Ok(())
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleServer>> {
        loop {
            let available = tokio::select! {
                biased;
                () = self.connection.stop.cancelled() => return None,
                result = self.input.fill_buf() => match result {
                    Ok(bytes) => bytes,
                    Err(_) => { self.connection.fail(ServeError::InputFailed); return None; }
                },
            };
            if available.is_empty() {
                if !self.line.is_empty() {
                    self.connection.fail(ServeError::InvalidFrame);
                } else {
                    self.connection.closed_input.store(true, Ordering::Release);
                    self.connection.stop.cancel();
                }
                return None;
            }
            let delimiter = available.iter().position(|byte| *byte == b'\n');
            let amount = delimiter.unwrap_or(available.len());
            if self.line.len().saturating_add(amount) > MAX_FRAME_BYTES {
                self.connection.fail(ServeError::FrameTooLarge);
                return None;
            }
            self.line.extend_from_slice(&available[..amount]);
            self.input
                .consume(amount + usize::from(delimiter.is_some()));
            if delimiter.is_none() {
                continue;
            }
            let value = decode_unique_json::<Value>(&self.line, MAX_FRAME_BYTES);
            self.line.clear();
            let Value::Object(object) = (match value {
                Ok(value) => value,
                Err(_) => {
                    self.connection.fail(ServeError::InvalidFrame);
                    return None;
                }
            }) else {
                self.connection.fail(ServeError::InvalidFrame);
                return None;
            };
            if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
                self.connection.fail(ServeError::InvalidFrame);
                return None;
            }
            let method = object.get("method").and_then(Value::as_str);
            if method.is_none() {
                self.connection.fail(ServeError::InvalidFrame);
                return None;
            }
            if !object.contains_key("id") {
                if method == Some("notifications/cancelled") {
                    if let Some(id) = object
                        .get("params")
                        .and_then(|params| params.get("requestId"))
                        .and_then(|id| serde_json::from_value::<RequestId>(id.clone()).ok())
                    {
                        self.connection.cancel(&id);
                    }
                    continue;
                }
                if method != Some("notifications/initialized") || self.initialized_seen {
                    // No subscriptions or logging negotiation are advertised.
                    // Unknown notifications are ignored, without spawning an
                    // unbounded stream of SDK notification handler tasks.
                    continue;
                }
                self.initialized_seen = true;
            }
            let cancellable = method != Some("initialize");
            let decoded = if malformed_tool_envelope(&object) {
                invalid_tool_request(&object)
            } else {
                serde_json::from_value::<RxJsonRpcMessage<RoleServer>>(Value::Object(object))
            };
            let message = match decoded {
                Ok(message) => message,
                Err(_) => {
                    self.connection.fail(ServeError::InvalidFrame);
                    return None;
                }
            };
            match &message {
                JsonRpcMessage::Request(request) => {
                    if let Err(error) = self.connection.register(request.id.clone(), cancellable) {
                        self.connection.fail(error);
                        return None;
                    }
                }
                JsonRpcMessage::Notification(_) => {}
                _ => {
                    self.connection.fail(ServeError::InvalidFrame);
                    return None;
                }
            }
            return Some(message);
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.connection.stop.cancel();
        Ok(())
    }
}

struct LimitedWriter(Vec<u8>);
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > MAX_OUTPUT_BYTES {
            return Err(io::Error::other("MCP response exceeds transport limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// rmcp's catch-all CustomRequest turns malformed known-method parameters
// into method-not-found, and its Option fields collapse explicit null values.
// Preserve malformed tool envelopes as SDK custom requests so SDK lifecycle /
// per-request metadata validation still happens before our invalid-params reply.
fn malformed_tool_envelope(object: &Map<String, Value>) -> bool {
    let Some(method @ ("tools/call" | "tools/list")) = object.get("method").and_then(Value::as_str)
    else {
        return false;
    };
    if !object.contains_key("id") {
        return false;
    }
    let Some(params) = object.get("params") else {
        return method == "tools/call";
    };
    let Some(params) = params.as_object() else {
        return true;
    };
    if params.get("_meta").is_some_and(|meta| !meta.is_object()) {
        return true;
    }
    if method == "tools/call" {
        !params.get("name").is_some_and(Value::is_string)
            || params
                .get("arguments")
                .is_some_and(|args| !args.is_object())
            || params.contains_key("inputResponses")
            || params.contains_key("requestState")
    } else {
        params
            .get("cursor")
            .is_some_and(|cursor| !cursor.is_string())
    }
}
fn invalid_tool_request(
    object: &Map<String, Value>,
) -> Result<RxJsonRpcMessage<RoleServer>, serde_json::Error> {
    let id = serde_json::from_value::<RequestId>(object.get("id").cloned().unwrap_or(Value::Null))?;
    let mut request = CustomRequest::new(
        object["method"].as_str().expect("known tool method"),
        object.get("params").cloned(),
    );
    if let Some(meta) = object
        .get("params")
        .and_then(|params| params.get("_meta"))
        .and_then(Value::as_object)
    {
        request
            .extensions
            .insert(RequestMetaObject::from(meta.clone()));
    }
    Ok(JsonRpcMessage::request(
        ClientRequest::CustomRequest(request),
        id,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::ServerResult;
    use serde_json::json;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    fn state() -> Arc<Connection> {
        Connection::new(CancellationToken::new())
    }
    fn frames(values: &[Value]) -> Cursor<Vec<u8>> {
        Cursor::new(
            values
                .iter()
                .flat_map(|value| {
                    let mut bytes = serde_json::to_vec(value).unwrap();
                    bytes.push(b'\n');
                    bytes
                })
                .collect(),
        )
    }
    fn request(id: i64) -> Value {
        json!({"jsonrpc":"2.0","id":id,"method":"ping"})
    }
    fn response(id: i64) -> TxJsonRpcMessage<RoleServer> {
        JsonRpcMessage::response(ServerResult::empty(()), RequestId::Number(id))
    }

    #[tokio::test]
    async fn strict_decode_preserves_exact_integers_and_rejects_duplicate_keys() {
        let valid = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"test\",\"arguments\":{\"data\":18446744073709551615}}}\n";
        let connection = state();
        let mut transport =
            BoundedTransport::new(Cursor::new(valid), tokio::io::sink(), connection.clone());
        let message = transport.receive().await.unwrap();
        assert_eq!(
            serde_json::to_value(message).unwrap()["params"]["arguments"]["data"],
            json!(u64::MAX)
        );
        assert!(connection.failure().is_none());
        for invalid in [
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"id\":2,\"method\":\"ping\"}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"test\",\"arguments\":{\"x\":1,\"\\u0078\":2}}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"test\",\"arguments\":{\"x\":18446744073709551616}}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}",
            "[]\n",
            "{\"jsonrpc\":\"2.0\"}\n",
            "not json\n",
        ] {
            let connection = state();
            let mut transport =
                BoundedTransport::new(Cursor::new(invalid), tokio::io::sink(), connection.clone());
            assert!(transport.receive().await.is_none());
            assert_eq!(
                connection.failure(),
                Some(ServeError::InvalidFrame),
                "{invalid}"
            );
        }
    }

    #[tokio::test]
    async fn input_frame_and_outstanding_requests_are_bounded() {
        let connection = state();
        let mut bytes = vec![b' '; MAX_FRAME_BYTES + 1];
        bytes.push(b'\n');
        let mut transport =
            BoundedTransport::new(Cursor::new(bytes), tokio::io::sink(), connection.clone());
        assert!(transport.receive().await.is_none());
        assert_eq!(connection.failure(), Some(ServeError::FrameTooLarge));
        let connection = state();
        let input: Vec<_> = (0..=MAX_PENDING_REQUESTS as i64).map(request).collect();
        let mut transport =
            BoundedTransport::new(frames(&input), tokio::io::sink(), connection.clone());
        for _ in 0..MAX_PENDING_REQUESTS {
            assert!(transport.receive().await.is_some());
        }
        assert!(transport.receive().await.is_none());
        assert_eq!(connection.failure(), Some(ServeError::TooManyRequests));
    }

    #[tokio::test]
    async fn duplicate_outstanding_id_closes_connection() {
        let connection = state();
        let mut transport = BoundedTransport::new(
            frames(&[request(1), request(1)]),
            tokio::io::sink(),
            connection.clone(),
        );
        assert!(transport.receive().await.is_some());
        assert!(transport.receive().await.is_none());
        assert_eq!(connection.failure(), Some(ServeError::DuplicateRequest));
    }

    #[tokio::test]
    async fn repeated_cancellation_releases_slots_only_when_handlers_finish() {
        let connection = state();
        let (output, mut reader) = tokio::io::duplex(1024);
        let mut input = Vec::new();
        for id in 0..32 {
            input.push(request(id));
            input.push(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":id}}));
        }
        input.push(request(32));
        let mut transport = BoundedTransport::new(frames(&input), output, connection.clone());
        assert!(transport.receive().await.is_some());
        for id in 0..32 {
            let token = connection.request_token(&RequestId::Number(id));
            // Reads consume the cancellation and yield the next real request.
            assert!(transport.receive().await.is_some());
            assert!(token.is_cancelled());
            assert_eq!(connection.pending.lock().unwrap().len(), 2);
            transport.send(response(id)).await.unwrap();
            assert_eq!(connection.pending.lock().unwrap().len(), 1);
        }
        assert!(connection.failure().is_none());
        drop(transport);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert!(
            bytes.is_empty(),
            "cancelled results must not reach the client"
        );
    }

    #[tokio::test]
    async fn cancelled_id_cannot_be_reused_before_its_handler_finishes() {
        let connection = state();
        let input = [
            request(1),
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}),
            request(1),
        ];
        let mut transport =
            BoundedTransport::new(frames(&input), tokio::io::sink(), connection.clone());
        assert!(transport.receive().await.is_some());
        assert!(transport.receive().await.is_none());
        assert_eq!(connection.failure(), Some(ServeError::DuplicateRequest));
    }

    #[tokio::test]
    async fn unknown_and_malformed_cancellations_do_not_end_session() {
        let connection = state();
        let input = [
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":999}}),
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":null}}),
            request(1),
        ];
        let mut transport =
            BoundedTransport::new(frames(&input), tokio::io::sink(), connection.clone());
        assert!(transport.receive().await.is_some());
        assert!(
            !connection
                .request_token(&RequestId::Number(1))
                .is_cancelled()
        );
        transport.send(response(1)).await.unwrap();
        assert!(connection.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn backpressure_keeps_request_slot_reserved_and_shutdown_unblocks_writer() {
        let connection = state();
        let (output, _reader) = tokio::io::duplex(1);
        let input: Vec<_> = (0..=MAX_PENDING_REQUESTS as i64).map(request).collect();
        let mut transport = BoundedTransport::new(frames(&input), output, connection.clone());
        for _ in 0..MAX_PENDING_REQUESTS {
            assert!(transport.receive().await.is_some());
        }
        let writing = transport.send(response(0));
        tokio::pin!(writing);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut writing)
                .await
                .is_err()
        );
        assert_eq!(
            connection.pending.lock().unwrap().len(),
            MAX_PENDING_REQUESTS
        );
        assert!(transport.receive().await.is_none());
        assert_eq!(connection.failure(), Some(ServeError::TooManyRequests));
        tokio::time::timeout(std::time::Duration::from_secs(1), writing)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn broken_output_is_reported_without_echoing_data() {
        let connection = state();
        let (output, reader) = tokio::io::duplex(1024);
        drop(reader);
        let mut transport =
            BoundedTransport::new(frames(&[request(1)]), output, connection.clone());
        assert!(transport.receive().await.is_some());
        assert_eq!(
            transport.send(response(1)).await.unwrap_err(),
            ServeError::OutputFailed
        );
        assert_eq!(connection.failure(), Some(ServeError::OutputFailed));
    }

    #[test]
    fn response_serialization_is_bounded_before_allocating_an_unbounded_buffer() {
        let mut writer = LimitedWriter(Vec::new());
        writer.write_all(&vec![b'x'; MAX_OUTPUT_BYTES]).unwrap();
        assert!(writer.write_all(b"x").is_err());
        assert_eq!(writer.0.len(), MAX_OUTPUT_BYTES);
    }
}
