use super::*;
use ledgence_worker_api::TraceContext;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{SpanData, SpanExporter},
};
use prost::Message;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::Instant,
};

#[derive(Clone, Debug, Default)]
struct Memory(Arc<Mutex<Vec<SpanData>>>);
impl SpanExporter for Memory {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.lock().unwrap().extend(batch);
        Ok(())
    }
}
fn memory(ratio: f64) -> (Telemetry, Memory) {
    let exported = Memory::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exported.clone())
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            ratio,
        ))))
        .build();
    (
        Telemetry {
            provider: Some(provider),
            state: Arc::default(),
            metrics: None,
            metric_layer: None,
            metric_state: Arc::default(),
        },
        exported,
    )
}
fn origin(sampled: bool) -> TraceContext {
    TraceContext {
        traceparent: format!(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0{}",
            u8::from(sampled)
        ),
        tracestate: Some("vendor=one".into()),
    }
}
fn config(variables: &[(&str, &str)]) -> Result<Config, ConfigError> {
    Config::from_variables(
        "ledgence-test",
        "0.1.0",
        variables
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string())),
    )
}

#[test]
fn defaults_are_off_and_unsupported_settings_fail_clearly() {
    assert!(!config(&[]).unwrap().enabled());
    for values in [
        vec![("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc")],
        vec![("OTEL_TRACES_SAMPLER_ARG", "NaN")],
        vec![("OTEL_TRACES_SAMPLER_ARG", "1.1")],
        vec![("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")],
        vec![("OTEL_RESOURCE_ATTRIBUTES", "unknown=value")],
        vec![("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "file:///tmp/trace")],
        vec![("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "key=value")],
    ] {
        assert!(config(&values).is_err(), "{values:?}");
    }
    assert!(
        !config(&[
            (
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                "http://localhost:4318/v1/traces"
            ),
            ("OTEL_SDK_DISABLED", "true")
        ])
        .unwrap()
        .enabled()
    );
}

#[test]
fn parent_sampling_links_and_context_survive_error_only_log_filter() {
    let (telemetry, exported) = memory(0.0);
    let bridge = telemetry.bridge();
    let subscriber = telemetry.subscriber(std::io::sink, EnvFilter::new("error"));
    let parent = origin(true);
    let (producer_context, worker_context) = tracing::subscriber::with_default(subscriber, || {
        let producer = tracing::info_span!("ledgence.invocation.create", otel.kind = "producer");
        bridge.set_parent(&producer, Some(&parent));
        bridge.add_link(&producer, &origin(false));
        let p = bridge.context(&producer).unwrap();
        let worker = tracing::info_span!("ledgence.attempt.process", otel.kind = "consumer");
        bridge.set_parent(&worker, Some(&p));
        let w = bridge.context(&worker).unwrap();
        assert_eq!(p.tracestate, parent.tracestate);
        assert_eq!(&p.traceparent[3..35], &parent.traceparent[3..35]);
        assert_eq!(&w.traceparent[3..35], &p.traceparent[3..35]);
        assert!(w.traceparent.ends_with("01"));
        (p, w)
    });
    let spans = exported.0.lock().unwrap();
    let producer = spans
        .iter()
        .find(|s| s.name == "ledgence.invocation.create")
        .unwrap();
    let worker = spans
        .iter()
        .find(|s| s.name == "ledgence.attempt.process")
        .unwrap();
    assert_eq!(producer.parent_span_id.to_string(), "00f067aa0ba902b7");
    assert_eq!(producer.span_kind, opentelemetry::trace::SpanKind::Producer);
    assert_eq!(producer.links.len(), 1);
    assert_eq!(
        worker.parent_span_id.to_string(),
        &producer_context.traceparent[36..52]
    );
    assert_eq!(
        worker.span_context.span_id().to_string(),
        &worker_context.traceparent[36..52]
    );
    drop(spans);
    telemetry.shutdown().unwrap();
}

#[test]
fn unsampled_is_valid_and_explicit_root_does_not_inherit_ambient_origin() {
    let (telemetry, exported) = memory(1.0);
    let bridge = telemetry.bridge();
    tracing::subscriber::with_default(
        telemetry.subscriber(std::io::sink, EnvFilter::new("error")),
        || {
            let parent = tracing::info_span!("transport");
            bridge.set_parent(&parent, Some(&origin(true)));
            let _entered = parent.enter();
            let unsampled = tracing::info_span!("work");
            bridge.set_parent(&unsampled, Some(&origin(false)));
            let context = bridge.context(&unsampled).unwrap();
            assert!(context.traceparent.ends_with("00"));
            context.validate().unwrap();
            let root = tracing::info_span!("fresh_root");
            bridge.set_parent(&root, None);
            assert_ne!(
                &bridge.context(&root).unwrap().traceparent[3..35],
                &origin(true).traceparent[3..35]
            );
        },
    );
    assert!(
        !exported
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|span| span.name == "work")
    );
    telemetry.shutdown().unwrap();
}

#[test]
fn disabled_never_synthesizes_context() {
    let telemetry = Telemetry::new(config(&[]).unwrap()).unwrap();
    let bridge = telemetry.bridge();
    tracing::subscriber::with_default(
        telemetry.subscriber(std::io::sink, EnvFilter::new("info")),
        || {
            let span = tracing::info_span!("disabled");
            bridge.set_parent(&span, Some(&origin(true)));
            assert_eq!(bridge.context(&span), None);
        },
    );
    telemetry.shutdown().unwrap();
}

#[derive(Clone, Default)]
struct LogBytes(Arc<Mutex<Vec<u8>>>);
impl Write for LogBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> MakeWriter<'a> for LogBytes {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}
#[test]
fn logs_and_filtered_scopes_use_active_ids_but_parentless_python_keeps_its_snapshot() {
    let (telemetry, exported) = memory(1.0);
    let bridge = telemetry.bridge();
    let logs = LogBytes::default();
    let expected = tracing::subscriber::with_default(
        telemetry.subscriber(logs.clone(), EnvFilter::new("info")),
        || {
            let span = tracing::info_span!("work");
            bridge.set_parent(&span, Some(&origin(true)));
            let context = bridge.context(&span).unwrap();
            let _entered = span.enter();
            let diagnostic = tracing::info_span!(target: "ledgence::context", "invocation");
            assert_eq!(bridge.context(&diagnostic), Some(context.clone()));
            let _diagnostic = diagnostic.enter();
            tracing::info!("active Rust log");
            tracing::info!(target: "ledgence::program_log", parent: None, trace_id = "python-trace", span_id = "python-span", "Python log");
            context
        },
    );
    let lines: Vec<serde_json::Value> = String::from_utf8(logs.0.lock().unwrap().clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["trace_id"], expected.traceparent[3..35]);
    assert_eq!(lines[0]["span_id"], expected.traceparent[36..52]);
    assert!(lines[1].get("trace_id").is_none());
    assert_eq!(lines[1]["fields"]["trace_id"], "python-trace");
    assert_eq!(exported.0.lock().unwrap().len(), 1);
    telemetry.shutdown().unwrap();
}

fn read_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
    stream
        .set_read_timeout(Some(Duration::from_secs(8)))
        .unwrap();
    let mut bytes = Vec::new();
    let mut byte = [0];
    while !bytes.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).unwrap();
        bytes.push(byte[0]);
        assert!(bytes.len() < 16384);
    }
    let headers = String::from_utf8(bytes).unwrap();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            line.to_lowercase()
                .strip_prefix("content-length: ")
                .map(str::to_owned)
        })
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(length < 32 * 1024 * 1024);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).unwrap();
    (headers, body)
}
#[test]
fn real_http_protobuf_exports_the_tree_and_bounded_values_without_internal_spans() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
    let receiver = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let result = read_request(&mut stream);
        stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").unwrap();
        result
    });
    let telemetry =
        Telemetry::new(config(&[("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", &endpoint)]).unwrap())
            .unwrap();
    let bridge = telemetry.bridge();
    tracing::subscriber::with_default(
        telemetry.subscriber(std::io::sink, EnvFilter::new("error")),
        || {
            let producer = tracing::info_span!(
                "ledgence.invocation.create",
                otel.kind = "producer",
                ledgence.task.id = "task_001"
            );
            bridge.set_parent(&producer, Some(&origin(true)));
            let p = bridge.context(&producer).unwrap();
            let worker = tracing::info_span!(
                "ledgence.attempt.process",
                otel.kind = "consumer",
                test.large = "🙂".repeat(1000)
            );
            bridge.set_parent(&worker, Some(&p));
            let _entered = worker.enter();
            tracing::info!("this log is not a span event");
            let _external = tracing::info_span!(target: "reqwest", "external_client").entered();
        },
    );
    telemetry.shutdown().unwrap();
    let (headers, body) = receiver.join().unwrap();
    assert!(headers.starts_with("POST /v1/traces HTTP/1.1"));
    assert!(
        headers
            .to_lowercase()
            .contains("content-type: application/x-protobuf")
    );
    let decoded = ExportTraceServiceRequest::decode(body.as_slice()).unwrap();
    let spans: Vec<_> = decoded
        .resource_spans
        .iter()
        .flat_map(|r| r.scope_spans.iter())
        .flat_map(|s| &s.spans)
        .collect();
    assert_eq!(spans.len(), 2);
    let producer = spans
        .iter()
        .find(|s| s.name == "ledgence.invocation.create")
        .unwrap();
    let worker = spans
        .iter()
        .find(|s| s.name == "ledgence.attempt.process")
        .unwrap();
    assert_eq!(worker.parent_span_id, producer.span_id);
    assert_eq!(worker.trace_id, producer.trace_id);
    assert_eq!(
        producer.parent_span_id,
        [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]
    );
    assert!(worker.events.is_empty());
    let value = worker
        .attributes
        .iter()
        .find(|a| a.key == "test.large")
        .unwrap()
        .value
        .as_ref()
        .unwrap()
        .value
        .as_ref()
        .unwrap();
    match value {
        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(value) => {
            assert_eq!(value.len(), processor::VALUE_BYTES)
        }
        _ => panic!("string attribute expected"),
    }
    let resource = decoded.resource_spans[0].resource.as_ref().unwrap();
    assert!(
        resource
            .attributes
            .iter()
            .any(|attr| attr.key == "service.name")
    );
}

#[test]
fn stalled_exporter_has_finite_queue_and_shutdown_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
    let (arrived_tx, arrived_rx) = std::sync::mpsc::channel();
    let receiver = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        arrived_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(2400));
        // Remaining export attempts see a closed receiver rather than creating
        // an unbounded test fixture lifetime after the assertion.
    });
    let telemetry =
        Telemetry::new(config(&[("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", &endpoint)]).unwrap())
            .unwrap();
    let state = telemetry.state.clone();
    let started = Instant::now();
    tracing::subscriber::with_default(
        telemetry.subscriber(std::io::sink, EnvFilter::new("error")),
        || {
            for _ in 0..5000 {
                let span = tracing::info_span!("completed");
                drop(span);
            }
        },
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "span completion waited on network export"
    );
    arrived_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(state.statistics().queue_dropped_spans > 0);
    assert!(state.pending.load(std::sync::atomic::Ordering::Relaxed) <= processor::QUEUE_SPANS);
    let start = Instant::now();
    let _ = telemetry.shutdown();
    assert!(start.elapsed() < Duration::from_secs(4));
    receiver.join().unwrap();
    assert!(state.statistics().failed_batches > 0);
    assert!(state.statistics().failed_spans > 0);
}

#[test]
fn collector_partial_rejection_is_counted_without_retrying_accepted_spans() {
    use opentelemetry_proto::tonic::collector::trace::v1::{
        ExportTracePartialSuccess, ExportTraceServiceResponse,
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
    let receiver = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        let body = ExportTraceServiceResponse {
            partial_success: Some(ExportTracePartialSuccess {
                rejected_spans: 1,
                error_message: "rejected by collector".into(),
            }),
        }
        .encode_to_vec();
        write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len()).unwrap();
        stream.write_all(&body).unwrap();
        listener.set_nonblocking(true).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        assert!(listener.accept().is_err(), "partial success was retried");
    });
    let telemetry =
        Telemetry::new(config(&[("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", &endpoint)]).unwrap())
            .unwrap();
    let state = telemetry.state.clone();
    tracing::subscriber::with_default(
        telemetry.subscriber(std::io::sink, EnvFilter::new("error")),
        || {
            drop(tracing::info_span!("one_span"));
        },
    );
    telemetry.shutdown().unwrap();
    receiver.join().unwrap();
    assert_eq!(state.statistics().rejected_spans, 1);
    assert_eq!(state.statistics().collector_warnings, 1);
    assert_eq!(state.statistics().failed_batches, 0);
}

#[test]
fn invalid_and_oversized_acknowledgements_are_bounded_export_failures() {
    for body in [b"invalid protobuf".to_vec(), vec![0; 65 * 1024]] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
        let receiver = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len()).unwrap();
            let _ = stream.write_all(&body);
        });
        let telemetry =
            Telemetry::new(config(&[("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", &endpoint)]).unwrap())
                .unwrap();
        let state = telemetry.state.clone();
        tracing::subscriber::with_default(
            telemetry.subscriber(std::io::sink, EnvFilter::new("error")),
            || {
                drop(tracing::info_span!("one_span"));
            },
        );
        telemetry.shutdown().unwrap();
        receiver.join().unwrap();
        assert_eq!(state.statistics().failed_batches, 1);
        assert_eq!(state.statistics().failed_spans, 1);
    }
}

#[test]
fn dropping_enabled_telemetry_inside_tokio_does_not_block_or_destroy_its_client_there() {
    let telemetry = Telemetry::new(
        config(&[(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "http://127.0.0.1:9/v1/traces",
        )])
        .unwrap(),
    )
    .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let started = Instant::now();
        drop(telemetry);
        assert!(started.elapsed() < Duration::from_millis(250));
        tokio::task::yield_now().await;
    });
}

#[test]
fn outer_shutdown_budget_includes_a_blocked_exporter_destructor() {
    #[derive(Debug)]
    struct SlowDrop {
        entered: std::sync::mpsc::SyncSender<()>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl SpanExporter for SlowDrop {
        async fn export(&self, _: Vec<SpanData>) -> OTelSdkResult {
            Ok(())
        }
    }
    impl Drop for SlowDrop {
        fn drop(&mut self) {
            let _ = self.entered.send(());
            let _ = self
                .release
                .get_mut()
                .unwrap()
                .recv_timeout(Duration::from_secs(5));
        }
    }
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(SlowDrop {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        })
        .build();
    let started = Instant::now();
    let result = shutdown_provider(provider, Duration::from_millis(100));
    assert!(result.is_err());
    assert!(
        started.elapsed() < Duration::from_millis(600),
        "exporter destruction escaped total budget"
    );
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    release_tx.send(()).unwrap();
}

#[test]
fn completed_span_bounds_compact_large_backing_allocations() {
    use opentelemetry::{Array, Value};
    let (telemetry, exported) = memory(1.0);
    tracing::subscriber::with_default(
        telemetry.subscriber(std::io::sink, EnvFilter::new("error")),
        || {
            drop(tracing::info_span!("seed"));
        },
    );
    let mut span = exported.0.lock().unwrap()[0].clone();
    fn overallocated() -> String {
        let mut short = String::with_capacity(1024 * 1024);
        short.push_str("short");
        assert!(short.capacity() > processor::VALUE_BYTES);
        short
    }
    span.name = std::borrow::Cow::Owned(overallocated());
    span.attributes = vec![
        KeyValue::new(overallocated(), overallocated()),
        KeyValue::new("integers", Value::Array(Array::I64(vec![42; 1_000_000]))),
        KeyValue::new("floats", Value::Array(Array::F64(vec![1.0; 1_000_000]))),
        KeyValue::new("booleans", Value::Array(Array::Bool(vec![true; 1_000_000]))),
        KeyValue::new(
            "strings",
            Value::Array(Array::String(
                (0..100).map(|_| overallocated().into()).collect(),
            )),
        ),
    ];
    processor::bound_span(&mut span);
    match span.name {
        std::borrow::Cow::Owned(name) => assert!(name.capacity() <= 128),
        _ => panic!("bounded owned name expected"),
    }
    for attr in span.attributes {
        let key: String = attr.key.into();
        assert!(key.capacity() <= 128);
        match attr.value {
            Value::String(value) => {
                let value: String = value.into();
                assert!(value.capacity() <= processor::VALUE_BYTES);
            }
            Value::Array(Array::I64(values)) => assert!(values.capacity() <= 16),
            Value::Array(Array::F64(values)) => assert!(values.capacity() <= 16),
            Value::Array(Array::Bool(values)) => assert!(values.capacity() <= 16),
            Value::Array(Array::String(values)) => {
                assert!(values.capacity() <= 16);
                for value in values {
                    let value: String = value.into();
                    assert!(value.capacity() <= 16);
                }
            }
            _ => panic!("unexpected fixture value"),
        }
    }
    telemetry.shutdown().unwrap();
}

#[test]
fn completed_span_accounting_has_a_conservative_128_kib_budget() {
    use std::mem::size_of;
    // String arrays have the largest bounded attribute representation: sixteen
    // StringValue slots plus 256 UTF-8 bytes, a 128-byte key and KeyValue itself.
    let attribute =
        size_of::<KeyValue>() + 128 + 16 * size_of::<opentelemetry::StringValue>() + 256;
    let attributes = 32 + 4 * 8 + 8 * 4;
    // W3C context has at most 32 tracestate members / 512 combined header bytes.
    let state = 32 * size_of::<(String, String)>() + 512;
    let span_budget = size_of::<SpanData>()
        + attributes * attribute
        + 4 * (size_of::<opentelemetry::trace::Event>() + 128)
        + 8 * size_of::<opentelemetry::trace::Link>()
        + 9 * state
        + 128
        + 256
        + 256;
    assert!(
        span_budget <= 128 * 1024,
        "review retained payload budget: {span_budget}"
    );
    assert!(span_budget * processor::QUEUE_SPANS <= 128 * 1024 * 1024);
}

mod metrics;
