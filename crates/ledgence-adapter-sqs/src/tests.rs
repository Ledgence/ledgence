use super::*;
use ledgence_orchestration_api::{DispatchRef, Scope};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex, atomic::AtomicBool},
    thread,
};

fn record(id: &str) -> PublishedDispatch {
    PublishedDispatch {
        dispatch: DispatchRef {
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "namespace".into(),
            },
            queue: "queue".into(),
            task_id: format!("task_{id}"),
            generation: 1,
        },
        publication_id: id.into(),
    }
}
fn options(endpoint: &str) -> SqsOptions {
    let mut value = SqsOptions::new("us-east-1", format!("{endpoint}/000000000000/queue"));
    value.endpoint_url = Some(endpoint.into());
    value.local_credentials = true;
    value
}
fn attributes() -> Value {
    json!({"Attributes":{"FifoQueue":"false","DelaySeconds":"0","MaximumMessageSize":"1048576"}})
}
fn success(id: &str) -> Value {
    json!({"Id":id,"MessageId":format!("message_{id}"),"MD5OfMessageBody":"md5"})
}

#[test]
fn rejects_unsupported_configuration() {
    let good = options("http://127.0.0.1:9324");
    assert!(good.validate().is_ok());
    for mutate in [
        |o: &mut SqsOptions| o.region.clear(),
        |o: &mut SqsOptions| o.endpoint_url = None,
        |o: &mut SqsOptions| o.endpoint_url = Some("https://sqs.us-east-1.amazonaws.com".into()),
        |o: &mut SqsOptions| o.queue_url = "http://user:password@localhost/q".into(),
        |o: &mut SqsOptions| o.operation_timeout = Duration::ZERO,
        |o: &mut SqsOptions| o.operation_timeout = Duration::from_nanos(1_000_001),
        |o: &mut SqsOptions| o.visibility_timeout = Duration::from_secs(29),
        |o: &mut SqsOptions| o.visibility_timeout = Duration::from_secs(43_201),
    ] {
        let mut o = good.clone();
        mutate(&mut o);
        assert!(o.validate().is_err());
    }
}

#[test]
fn batch_confirmation_requires_complete_disjoint_exact_identities() {
    assert_eq!(
        batch_confirmations(3, ["2", "0"].into_iter(), ["1"].into_iter()).unwrap(),
        vec![true, false, true]
    );
    for (ok, bad) in [
        (vec!["0", "0"], vec![]),
        (vec!["0"], vec!["0"]),
        (vec!["0"], vec![]),
        (vec!["0", "2"], vec![]),
        (vec!["0", "01"], vec![]),
        (vec!["0", "-1"], vec![]),
    ] {
        assert!(batch_confirmations(2, ok.into_iter(), bad.into_iter()).is_err());
    }
}

#[tokio::test]
async fn json_protocol_maps_partial_results_and_preserves_input_order() {
    let fixture = Fixture::new(vec![
        attributes(),
        json!({"Successful":[success("1")],"Failed":[{"Id":"0","Code":"InternalError","SenderFault":false}]}),
        json!({"Messages":[{"Body":"{}","ReceiptHandle":"receipt-a"},{"Body":"{}","ReceiptHandle":"receipt-b"}]}),
        json!({"Successful":[{"Id":"0"}],"Failed":[{"Id":"1","Code":"InternalError","SenderFault":false}]}),
    ]);
    let queue = SqsQueue::connect(options(&fixture.endpoint)).await.unwrap();
    let results = queue
        .publish(
            &[record("a"), record("b")],
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(
        results,
        vec![
            PublishResult {
                publication_id: "a".into(),
                outcome: PublicationOutcome::Retry
            },
            PublishResult {
                publication_id: "b".into(),
                outcome: PublicationOutcome::Confirmed
            }
        ]
    );
    let deliveries = queue
        .receive(2, Duration::ZERO, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let receipts = deliveries
        .iter()
        .map(|d| d.receipt.clone())
        .collect::<Vec<_>>();
    assert_ne!(receipts[0], "receipt-a");
    let acks = queue
        .acknowledge(&receipts, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(acks[0].confirmed);
    assert!(!acks[1].confirmed);
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests[1].0, "AmazonSQS.SendMessageBatch");
    assert_eq!(requests[1].1["Entries"][0]["Id"], "0");
    assert_eq!(requests[2].1["MaxNumberOfMessages"], 2);
    assert_eq!(requests[3].1["Entries"][0]["ReceiptHandle"], "receipt-a");
}

#[tokio::test]
async fn malformed_provider_batch_never_confirms_publication() {
    for response in [
        json!({"Successful":[success("0"),success("0")],"Failed":[]}),
        json!({"Successful":[],"Failed":[]}),
        json!({"Successful":[success("0")],"Failed":[{"Id":"0","Code":"error","SenderFault":true}]}),
    ] {
        let fixture = Fixture::new(vec![attributes(), response]);
        let queue = SqsQueue::connect(options(&fixture.endpoint)).await.unwrap();
        assert!(matches!(
            queue
                .publish(&[record("a")], Instant::now() + Duration::from_secs(2))
                .await,
            Err(ContractError::Unavailable(_))
        ));
    }
}

#[tokio::test]
async fn fifo_delay_and_small_message_capabilities_are_rejected() {
    for (name, value) in [
        ("FifoQueue", "true"),
        ("DelaySeconds", "1"),
        ("MaximumMessageSize", "1024"),
    ] {
        let mut attrs = attributes();
        attrs["Attributes"][name] = json!(value);
        let fixture = Fixture::new(vec![attrs]);
        assert!(SqsQueue::connect(options(&fixture.endpoint)).await.is_err());
    }
}

#[tokio::test]
async fn explicit_compatible_endpoint_can_omit_provider_size_metadata() {
    let fixture = Fixture::new(vec![json!({"Attributes":{"DelaySeconds":"0"}})]);
    let queue = SqsQueue::connect(options(&fixture.endpoint)).await.unwrap();
    assert_eq!(
        DispatchPublisher::limits(&queue).max_message_bytes,
        DISPATCH_MAX_BYTES
    );
}

#[tokio::test]
async fn bounds_and_receipt_binding_reject_before_network() {
    let fixture = Fixture::new(vec![attributes(), attributes()]);
    let queue = SqsQueue::connect(options(&fixture.endpoint)).await.unwrap();
    let other = SqsQueue::connect(options(&fixture.endpoint)).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    assert!(queue.publish(&[], deadline).await.is_err());
    assert!(
        queue
            .publish(&[record("a"), record("a")], deadline)
            .await
            .is_err()
    );
    assert!(queue.receive(11, Duration::ZERO, deadline).await.is_err());
    assert!(
        queue
            .receive(1, Duration::from_secs(21), deadline)
            .await
            .is_err()
    );
    assert!(
        queue
            .acknowledge(&[format!("{}raw", other.receipt_prefix)], deadline)
            .await
            .is_err()
    );
    assert!(
        queue
            .publish(&[record("a")], Instant::now() - Duration::from_secs(1))
            .await
            .is_err()
    );
    assert_eq!(fixture.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn receive_requires_reserved_count_and_bounded_distinct_receipts() {
    for messages in [
        json!([{"Body":"{}","ReceiptHandle":"a"},{"Body":"{}","ReceiptHandle":"b"}]),
        json!([{"Body":"{}"}]),
        json!([{"Body":"x".repeat(DISPATCH_MAX_BYTES+1),"ReceiptHandle":"a"}]),
        json!([{"Body":"{}","ReceiptHandle":"r".repeat(QUEUE_RECEIPT_MAX_BYTES+1)}]),
    ] {
        let fixture = Fixture::new(vec![attributes(), json!({"Messages":messages})]);
        let queue = SqsQueue::connect(options(&fixture.endpoint)).await.unwrap();
        assert!(matches!(
            queue
                .receive(1, Duration::ZERO, Instant::now() + Duration::from_secs(2))
                .await,
            Err(ContractError::InvalidQueueDelivery(_))
        ));
    }
    let fixture = Fixture::new(vec![
        attributes(),
        json!({"Messages":[{"Body":"{}","ReceiptHandle":"same"},{"Body":"{}","ReceiptHandle":"same"}]}),
    ]);
    let queue = SqsQueue::connect(options(&fixture.endpoint)).await.unwrap();
    assert!(matches!(
        queue
            .receive(2, Duration::ZERO, Instant::now() + Duration::from_secs(2))
            .await,
        Err(ContractError::InvalidQueueDelivery(_))
    ));
}

#[tokio::test]
async fn oversized_wire_response_is_rejected_before_sdk_decoding() {
    let fixture = Fixture::new(vec![
        attributes(),
        json!({"Messages":[{"Body":"x".repeat(bounded_http::MAX_RESPONSE_BYTES+1),"ReceiptHandle":"r"}]}),
    ]);
    let queue = SqsQueue::connect(options(&fixture.endpoint)).await.unwrap();
    assert!(matches!(
        queue
            .receive(1, Duration::ZERO, Instant::now() + Duration::from_secs(2))
            .await,
        Err(ContractError::InvalidQueueDelivery(_))
    ));
}

#[tokio::test]
async fn health_probe_requires_current_compatible_provider_evidence() {
    let mut changed = attributes();
    changed["Attributes"]["DelaySeconds"] = json!("1");
    let fixture = Fixture::new(vec![attributes(), attributes(), changed]);
    let queue = SqsQueue::connect(options(&fixture.endpoint)).await.unwrap();
    queue.check_configuration().await.unwrap();
    assert!(queue.check_configuration().await.is_err());
    assert_eq!(fixture.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn caller_deadline_bounds_unresponsive_broker() {
    let fixture = Fixture::new(vec![attributes()]);
    let queue = SqsQueue::connect(options(&fixture.endpoint)).await.unwrap();
    let start = Instant::now();
    assert!(
        queue
            .receive(
                1,
                Duration::from_secs(20),
                start + Duration::from_millis(100)
            )
            .await
            .is_err()
    );
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn synchronous_sdk_ready_after_operation_deadline_is_not_a_confirmation() {
    let fixture = Fixture::new(vec![attributes()]);
    let mut queue = SqsQueue::connect(options(&fixture.endpoint)).await.unwrap();
    queue.options.operation_timeout = Duration::from_millis(20);
    let caller_deadline = Instant::now() + Duration::from_secs(5);
    let deadline = queue.deadline(caller_deadline);
    let completed = AtomicBool::new(false);
    let result = run_until(deadline, "publish", async {
        thread::sleep(Duration::from_millis(50));
        completed.store(true, Ordering::SeqCst);
        Ok(PublicationOutcome::Confirmed)
    })
    .await;
    assert!(completed.load(Ordering::SeqCst));
    assert!(
        Instant::now() < caller_deadline,
        "the outer budget is still valid"
    );
    assert!(
        matches!(result, Err(ContractError::Unavailable(_))),
        "the shorter adapter budget rejects a synchronous late confirmation"
    );
}

#[tokio::test]
#[ignore = "requires LEDGENCE_TEST_SQS_ENDPOINT pointing to an owned local ElasticMQ service"]
async fn elasticmq_acceptance() {
    let endpoint = std::env::var("LEDGENCE_TEST_SQS_ENDPOINT").expect("explicit local endpoint");
    options(&endpoint).validate().unwrap();
    let client = Client::from_conf(
        aws_sdk_sqs::Config::builder()
            .behavior_version(BehaviorVersion::v2026_01_12())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("local", "local", None, None, "test"))
            .endpoint_url(&endpoint)
            .retry_config(RetryConfig::disabled())
            .timeout_config(timeout_config(Duration::from_secs(5)))
            .build(),
    );
    let name = format!("ledgence-sqs-acceptance-{}", std::process::id());
    let url = client
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    let mut returned = url::Url::parse(&url).unwrap();
    let target = url::Url::parse(&endpoint).unwrap();
    returned.set_host(target.host_str()).unwrap();
    returned.set_port(target.port()).unwrap();
    let mut config = options(&endpoint);
    config.queue_url = returned.to_string();
    let queue = SqsQueue::connect(config).await.unwrap();
    let records = (0..10)
        .map(|i| record(&format!("publication_{i}")))
        .collect::<Vec<_>>();
    let sent = queue
        .publish(&records, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        sent.iter()
            .all(|r| r.outcome == PublicationOutcome::Confirmed)
    );
    let received = queue
        .receive(
            10,
            Duration::from_secs(1),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(received.len(), 10);
    for message in &received {
        let decoded = PublishedDispatch::decode(&message.body).unwrap();
        assert!(records.contains(&decoded));
    }
    let receipts = received
        .iter()
        .map(|m| m.receipt.clone())
        .collect::<Vec<_>>();
    let acks = queue
        .acknowledge(&receipts, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(acks.iter().all(|r| r.confirmed));
    let start = Instant::now();
    assert!(
        queue
            .receive(1, Duration::from_secs(1), start + Duration::from_secs(3))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(start.elapsed() >= Duration::from_millis(900));
    client
        .delete_queue()
        .queue_url(returned.to_string())
        .send()
        .await
        .unwrap();
}

struct Fixture {
    endpoint: String,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}
impl Fixture {
    fn new(responses: Vec<Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let capture = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let handle = thread::spawn(move || {
            let mut responses = responses.into_iter();
            while !stopped.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        socket.set_nonblocking(false).unwrap();
                        socket
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        let (target, body) = read_request(&mut socket);
                        capture.lock().unwrap().push((target, body));
                        if let Some(response) = responses.next() {
                            let bytes = serde_json::to_vec(&response).unwrap();
                            let header = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                bytes.len()
                            );
                            let _ = socket.write_all(header.as_bytes());
                            let _ = socket.write_all(&bytes);
                        } else {
                            while !stopped.load(Ordering::Relaxed) {
                                thread::sleep(Duration::from_millis(5));
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(error) => panic!("fixture accept: {error}"),
                }
            }
        });
        Self {
            endpoint,
            requests,
            stop,
            handle: Some(handle),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().unwrap().join().unwrap();
    }
}
fn read_request(socket: &mut TcpStream) -> (String, Value) {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut b = [0];
        socket.read_exact(&mut b).unwrap();
        head.push(b[0]);
        assert!(head.len() < 65536);
    }
    let head = String::from_utf8(head).unwrap();
    let mut target = String::new();
    let mut length = 0;
    for line in head.lines() {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().unwrap();
            }
            if name.eq_ignore_ascii_case("x-amz-target") {
                target = value.trim().into();
            }
            if name.eq_ignore_ascii_case("content-type") {
                assert_eq!(value.trim(), "application/x-amz-json-1.0");
            }
        }
    }
    let mut bytes = vec![0; length];
    socket.read_exact(&mut bytes).unwrap();
    (target, serde_json::from_slice(&bytes).unwrap())
}
