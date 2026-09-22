use super::*;
use ledgence_worker_api::metrics::{Metric, MetricGuard, MetricKind, MetricOutcome, MetricTimer};
use opentelemetry_proto::tonic::{
    collector::metrics::v1::{
        ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    },
    metrics::v1::{metric::Data, number_data_point::Value},
};

fn metrics_config(endpoint: &str) -> Config {
    config(&[("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)]).unwrap()
}
fn decode(body: &[u8]) -> Vec<opentelemetry_proto::tonic::metrics::v1::Metric> {
    ExportMetricsServiceRequest::decode(body)
        .unwrap()
        .resource_metrics
        .into_iter()
        .flat_map(|r| r.scope_metrics)
        .flat_map(|s| s.metrics)
        .collect()
}
fn sum(metrics: &[opentelemetry_proto::tonic::metrics::v1::Metric], name: &str) -> f64 {
    match metrics
        .iter()
        .find(|m| m.name == name)
        .unwrap()
        .data
        .as_ref()
        .unwrap()
    {
        Data::Sum(sum) => {
            assert_eq!(sum.aggregation_temporality, 2);
            sum.data_points
                .iter()
                .map(|point| match point.value.unwrap() {
                    Value::AsDouble(v) => v,
                    Value::AsInt(v) => v as f64,
                })
                .sum()
        }
        _ => panic!("expected sum"),
    }
}

#[test]
fn metrics_only_exports_closed_vocabulary_and_guard_lifetimes_without_logs_or_traces() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1/metrics", listener.local_addr().unwrap());
    let receiver = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
            .unwrap();
        request
    });
    let telemetry = Telemetry::new(metrics_config(&endpoint)).unwrap();
    assert!(telemetry.enabled());
    assert!(telemetry.metrics_enabled());
    assert!(telemetry.provider.is_none());
    let logs = LogBytes::default();
    let guard = tracing::subscriber::with_default(
        telemetry.subscriber(logs.clone(), EnvFilter::new("error")),
        || {
            for _ in 0..1000 {
                Metric::CacheLookup.record(1.0, MetricOutcome::Hit);
            }
            for id in 100..1100u64 {
                tracing::event!(target:"ledgence::metrics",tracing::Level::DEBUG, metric=id, value=1.0f64, outcome=1u64, task_id=%format!("task_{id}"));
            }
            // Unrecognized fields never become attributes on otherwise valid events.
            tracing::event!(target:"ledgence::metrics",tracing::Level::DEBUG,metric=Metric::CacheLookup as u64,value=1.0f64,outcome=MetricOutcome::Hit as u64,tenant="arbitrary",task_id="secret");
            Metric::CacheLookup.record(f64::NAN, MetricOutcome::Hit);
            Metric::CacheLookup.record(-1.0, MetricOutcome::Hit);
            Metric::CacheLookup.record(50.0, MetricOutcome::Ok);
            MetricTimer::start(Metric::ExecutionDuration).finish(MetricOutcome::Ok);
            drop(MetricTimer::start(Metric::PreparationDuration));
            MetricGuard::consumer()
        },
    );
    // The release occurs after the scoped subscriber is gone, using the guard's
    // original dispatch. Its series must balance instead of losing the decrement.
    drop(guard);
    telemetry.shutdown().unwrap();
    assert!(
        logs.0.lock().unwrap().is_empty(),
        "metric observations became logs"
    );
    let (headers, body) = receiver.join().unwrap();
    assert!(headers.starts_with("POST /v1/metrics HTTP/1.1"));
    assert!(
        headers
            .to_lowercase()
            .contains("content-type: application/x-protobuf")
    );
    let decoded = decode(&body);
    assert_eq!(sum(&decoded, Metric::CacheLookup.name()), 1001.0);
    assert_eq!(sum(&decoded, Metric::ConsumerSlots.name()), 0.0);
    for metric in &decoded {
        match metric.data.as_ref().unwrap() {
            Data::Histogram(histogram) => {
                assert_eq!(histogram.aggregation_temporality, 2);
                assert_eq!(histogram.data_points.len(), 1);
                let point = &histogram.data_points[0];
                assert_eq!(point.count, 1);
                assert_eq!(point.explicit_bounds.len(), 17);
                assert_eq!(point.bucket_counts.iter().sum::<u64>(), point.count);
                assert_eq!(point.attributes.len(), 1);
                assert_eq!(point.attributes[0].key, "outcome");
            }
            Data::Sum(sum) => {
                assert_eq!(sum.data_points.len(), 1);
                assert!(
                    sum.data_points[0]
                        .attributes
                        .iter()
                        .all(|a| a.key == "outcome")
                );
            }
            _ => panic!("unexpected metric kind"),
        }
    }
    assert!(Metric::ALL.iter().all(|metric| {
        MetricOutcome::ALL
            .iter()
            .filter(|outcome| metric.accepts(**outcome))
            .count()
            < crate::metrics::CARDINALITY_LIMIT
    }));
    assert_eq!(Metric::ConsumerSlots.kind(), MetricKind::UpDownCounter);
}

#[test]
fn cumulative_metrics_survive_failed_export_without_a_replay_queue() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1/metrics", listener.local_addr().unwrap());
    let receiver = std::thread::spawn(move || {
        let mut requests = Vec::new();
        for status in ["503 Unavailable", "200 OK", "200 OK"] {
            let (mut stream, _) = listener.accept().unwrap();
            requests.push(read_request(&mut stream).1);
            write!(
                stream,
                "HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            )
            .unwrap();
        }
        requests
    });
    let telemetry = Telemetry::new(metrics_config(&endpoint)).unwrap();
    let state = telemetry.metric_state.clone();
    let dispatch =
        tracing::Dispatch::new(telemetry.subscriber(std::io::sink, EnvFilter::new("off")));
    tracing::dispatcher::with_default(&dispatch, || {
        Metric::CacheLookup.record(1000.0, MetricOutcome::Hit)
    });
    let _ = telemetry.metrics.as_ref().unwrap().force_flush();
    assert_eq!(state.statistics().failed_exports, 1);
    tracing::dispatcher::with_default(&dispatch, || {
        Metric::CacheLookup.record(1.0, MetricOutcome::Hit)
    });
    telemetry.metrics.as_ref().unwrap().force_flush().unwrap();
    telemetry.shutdown().unwrap();
    let requests = receiver.join().unwrap();
    assert_eq!(
        sum(&decode(&requests[0]), Metric::CacheLookup.name()),
        1000.0
    );
    assert_eq!(
        sum(&decode(&requests[1]), Metric::CacheLookup.name()),
        1001.0
    );
    assert_eq!(
        sum(&decode(&requests[2]), Metric::CacheLookup.name()),
        1001.0
    );
}

#[test]
fn metric_acknowledgements_report_partial_rejection_and_malformed_or_oversized_bodies() {
    let partial = ExportMetricsServiceResponse {
        partial_success: Some(ExportMetricsPartialSuccess {
            rejected_data_points: 2,
            error_message: "partial".into(),
        }),
    }
    .encode_to_vec();
    for (body, failed, rejected) in [
        (partial, 0, 2),
        (b"bad protobuf".to_vec(), 1, 0),
        (vec![0; 65 * 1024], 1, 0),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1/metrics", listener.local_addr().unwrap());
        let receiver = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            let _ = stream.write_all(&body);
        });
        let telemetry = Telemetry::new(metrics_config(&endpoint)).unwrap();
        let state = telemetry.metric_state.clone();
        tracing::subscriber::with_default(
            telemetry.subscriber(std::io::sink, EnvFilter::new("error")),
            || Metric::CacheLookup.record(1.0, MetricOutcome::Hit),
        );
        let _ = telemetry.shutdown();
        receiver.join().unwrap();
        assert_eq!(state.statistics().failed_exports, failed);
        assert_eq!(state.statistics().rejected_points, rejected);
    }
}

#[test]
fn unavailable_metrics_export_cannot_block_observation_or_async_drop() {
    let telemetry = Telemetry::new(metrics_config("http://127.0.0.1:9/v1/metrics")).unwrap();
    let subscriber = telemetry.subscriber(std::io::sink, EnvFilter::new("off"));
    let started = Instant::now();
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..100_000 {
            Metric::CacheLookup.record(1.0, MetricOutcome::Hit);
        }
    });
    assert!(started.elapsed() < Duration::from_secs(2));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let started = Instant::now();
        drop(telemetry);
        assert!(started.elapsed() < Duration::from_millis(250));
    });
}

#[test]
fn metrics_configuration_is_independent_explicit_and_disabled_consistently() {
    assert!(config(&[("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "file:///tmp/metrics")]).is_err());
    assert!(config(&[("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")]).is_err());
    assert!(config(&[("OTEL_METRIC_EXPORT_INTERVAL", "1")]).is_err());
    let telemetry = Telemetry::new(
        config(&[
            (
                "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
                "http://127.0.0.1:9/v1/metrics",
            ),
            ("OTEL_SDK_DISABLED", "true"),
        ])
        .unwrap(),
    )
    .unwrap();
    assert!(!telemetry.enabled());
    assert!(!telemetry.metrics_enabled());
    let logs = LogBytes::default();
    tracing::subscriber::with_default(
        telemetry.subscriber(logs.clone(), EnvFilter::new("trace")),
        || {
            assert!(
                !tracing::enabled!(target: "ledgence::metrics", tracing::Level::DEBUG),
                "disabled metrics retained an enabled callsite"
            );
            Metric::CacheLookup.record(1.0, MetricOutcome::Hit);
        },
    );
    assert!(logs.0.lock().unwrap().is_empty());
    telemetry.shutdown().unwrap();
}

#[test]
fn an_inflight_slow_collector_does_not_hold_the_observation_path() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1/metrics", listener.local_addr().unwrap());
    let (arrived_tx, arrived_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let receiver = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        arrived_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
            .unwrap();
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
            .unwrap();
        request.1
    });
    let telemetry = Telemetry::new(metrics_config(&endpoint)).unwrap();
    let dispatch =
        tracing::Dispatch::new(telemetry.subscriber(std::io::sink, EnvFilter::new("off")));
    tracing::dispatcher::with_default(&dispatch, || {
        Metric::CacheLookup.record(1.0, MetricOutcome::Hit)
    });
    let provider = telemetry.metrics.as_ref().unwrap().clone();
    let export = std::thread::spawn(move || provider.force_flush());
    arrived_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let started = Instant::now();
    tracing::dispatcher::with_default(&dispatch, || {
        for _ in 0..1000 {
            Metric::CacheLookup.record(1.0, MetricOutcome::Hit)
        }
    });
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "observations waited for the collector"
    );
    release_tx.send(()).unwrap();
    export.join().unwrap().unwrap();
    let started = Instant::now();
    telemetry.shutdown().unwrap();
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(
        sum(
            &decode(&receiver.join().unwrap()),
            Metric::CacheLookup.name()
        ),
        1001.0
    );
}

#[test]
fn retained_metric_instruments_do_not_own_the_exporter_or_its_blocking_client() {
    use opentelemetry_sdk::metrics::{
        PeriodicReader, SdkMeterProvider, Temporality, data::ResourceMetrics,
        exporter::PushMetricExporter,
    };
    struct Exporter(std::sync::mpsc::Sender<bool>);
    impl PushMetricExporter for Exporter {
        async fn export(&self, _: &ResourceMetrics) -> OTelSdkResult {
            Ok(())
        }
        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }
        fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
            Ok(())
        }
        fn temporality(&self) -> Temporality {
            Temporality::Cumulative
        }
    }
    impl Drop for Exporter {
        fn drop(&mut self) {
            let _ = self.0.send(tokio::runtime::Handle::try_current().is_err());
        }
    }
    let (send, receive) = std::sync::mpsc::channel();
    let provider = SdkMeterProvider::builder()
        .with_reader(
            PeriodicReader::builder(Exporter(send))
                .with_interval(Duration::from_secs(60))
                .build(),
        )
        .build();
    let layer = crate::metrics::MetricsLayer::new(&provider);
    let retained = layer.clone();
    shutdown_providers(None, Some(provider), Duration::from_secs(1)).unwrap();
    assert!(
        receive.recv_timeout(Duration::from_secs(1)).unwrap(),
        "exporter destruction occurred on an async runtime"
    );
    // Instrument handles remain alive, but the exporter has already been freed.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        drop(layer);
        drop(retained);
    });
}
