//! Exercise the actual SDK adapter through the generic acquisition source.
use super::*;
use ledgence_adapter_sqs::{SqsOptions, SqsQueue};
use ledgence_worker_delivery::{AcquisitionSource, BrokerAcquisitionSource, SourceReply};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn attributes() -> Value {
    json!({"Attributes":{"FifoQueue":"false","DelaySeconds":"0","MaximumMessageSize":"1048576"}})
}

async fn source(
    fixture: &Fixture,
    service: Arc<Service>,
) -> (BrokerAcquisitionSource, AcquireCommand) {
    let mut options = SqsOptions::new("us-east-1", format!("{}/queue", fixture.endpoint));
    options.endpoint_url = Some(fixture.endpoint.clone());
    options.local_credentials = true;
    let queue = Arc::new(SqsQueue::connect(options).await.unwrap());
    let session = service.open_session(&scope(), "invoices", 2).await.unwrap();
    let source = BrokerAcquisitionSource::new(queue, service).unwrap();
    source.start_session(&session).unwrap();
    let command = AcquireCommand {
        scope: session.scope,
        queue: session.queue,
        worker_session_id: session.id,
        consumer_id: 0,
        sequence: 1,
    };
    (source, command)
}

fn options() -> AcquireOptions {
    AcquireOptions::immediate(Instant::now() + WAIT)
}

#[tokio::test]
async fn sdk_receive_protocol_rejections_stop_all_admission_without_claim_or_ack() {
    for (case, messages) in [
        json!([{"Body":"{}","ReceiptHandle":"a"},{"Body":"{}","ReceiptHandle":"b"}]),
        json!([{"ReceiptHandle":"a"}]),
        json!([{"Body":"{}"}]),
        json!([{"Body":"x".repeat(DISPATCH_MAX_BYTES + 1),"ReceiptHandle":"a"}]),
        json!([{"Body":"{}","ReceiptHandle":"r".repeat(QUEUE_RECEIPT_MAX_BYTES + 1)}]),
        // The connector must preserve its positive body-cap rejection through
        // Smithy's error chain; an ordinary network error stays retryable.
        json!([{"Body":"x".repeat(3 * 1024 * 1024),"ReceiptHandle":"a"}]),
    ]
    .into_iter()
    .enumerate()
    {
        let fixture = Fixture::new(vec![Some(attributes()), Some(json!({"Messages":messages}))]);
        let service = Service::new(0);
        let (source, mut command) = source(&fixture, service.clone()).await;
        for consumer in [0, 0, 1] {
            command.consumer_id = consumer;
            let result = source.acquire(&command, options()).await;
            assert!(
                matches!(
                    result,
                    Ok(SourceReply::Stopped {
                        error: ContractError::InvalidQueueDelivery(_)
                    })
                ),
                "receive case {case}: {result:?}"
            );
        }
        assert!(service.broker_commands.lock().unwrap().is_empty());
        assert_eq!(
            *fixture.requests.lock().unwrap(),
            ["AmazonSQS.GetQueueAttributes", "AmazonSQS.ReceiveMessage"]
        );
        source.finish_session(&command.worker_session_id);
    }
}

#[tokio::test]
async fn sdk_receive_network_failure_remains_retryable_on_the_same_sequence() {
    let record = PublishedDispatch {
        dispatch: DispatchRef {
            scope: scope(),
            queue: "invoices".into(),
            task_id: "task_1".into(),
            generation: 1,
        },
        publication_id: "publication_1".into(),
    };
    let fixture = Fixture::new(vec![
        Some(attributes()),
        None, // Close the accepted request without a response: outcome unknown.
        Some(
            json!({"Messages":[{"Body":serde_json::to_string(&record).unwrap(),"ReceiptHandle":"a"}]}),
        ),
        Some(json!({"Successful":[{"Id":"0"}],"Failed":[]})),
    ]);
    let service = Service::new(0);
    *service.broker_disposition.lock().unwrap() = Some(ClaimDisposition::TerminalOrSuperseded);
    let (source, command) = source(&fixture, service.clone()).await;
    assert!(matches!(
        source.acquire(&command, options()).await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(service.broker_commands.lock().unwrap().is_empty());
    assert!(matches!(
        source.acquire(&command, options()).await.unwrap(),
        SourceReply::Discarded { sequence: 1 }
    ));
    assert_eq!(
        service.broker_commands.lock().unwrap()[0].acquisition,
        command
    );
    assert_eq!(
        *fixture.requests.lock().unwrap(),
        [
            "AmazonSQS.GetQueueAttributes",
            "AmazonSQS.ReceiveMessage",
            "AmazonSQS.ReceiveMessage",
            "AmazonSQS.DeleteMessageBatch"
        ]
    );
    source.finish_session(&command.worker_session_id);
}

#[tokio::test]
async fn sdk_full_driver_deadline_delete_outage_does_not_block_durable_handoff() {
    let mut received = false;
    let fixture = Fixture::responding(move |target| match target {
        "AmazonSQS.GetQueueAttributes" => Response::Json(attributes()),
        "AmazonSQS.ReceiveMessage" if !received => {
            received = true;
            Response::Json(json!({"Messages":[{
                "Body":serde_json::to_string(&PublishedDispatch {
                    dispatch: DispatchRef {
                        scope: scope(), queue: "invoices".into(),
                        task_id: "task_1".into(), generation: 1,
                    },
                    publication_id: "publication_1".into(),
                }).unwrap(),
                "ReceiptHandle":"receipt_1"
            }]}))
        }
        "AmazonSQS.ReceiveMessage" => Response::Json(json!({"Messages":[]})),
        // Hold every delete connection open without replying. Other requests
        // remain available, so only acknowledgment is failing throughout.
        "AmazonSQS.DeleteMessageBatch" => Response::Hold,
        other => panic!("unexpected SDK request: {other}"),
    });
    let mut options = SqsOptions::new("us-east-1", format!("{}/queue", fixture.endpoint));
    options.endpoint_url = Some(fixture.endpoint.clone());
    options.local_credentials = true;
    options.operation_timeout = Duration::from_secs(30);
    let queue = Arc::new(SqsQueue::connect(options).await.unwrap());
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    let source = Arc::new(BrokerAcquisitionSource::new(queue, service.clone()).unwrap());
    let mut config = DeliveryConfig::new(scope(), "invoices");
    config.acquire_wait = Duration::ZERO;
    assert_eq!(config.request_timeout, Duration::from_secs(30));
    let mut handle = DeliveryDriver::new(worker, service.clone(), config)
        .unwrap()
        .with_acquisition_source(source)
        .start();
    tokio::time::timeout(Duration::from_secs(35), async {
        while service.accepted_count() != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a full-budget delete outage must not withhold a durable claim forever");
    assert!(handle.shutdown(WAIT).await.unwrap().finished);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|target| *target == "AmazonSQS.DeleteMessageBatch")
            .count(),
        1
    );
    let commands = service.broker_commands.lock().unwrap();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[0], commands[1]);
    assert_eq!(counts.peak.load(Ordering::SeqCst), 1);
    assert_eq!(service.state.lock().unwrap().capacity_violations, 0);
}

enum Response {
    Json(Value),
    Close,
    Hold,
}

struct Fixture {
    endpoint: String,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}
impl Fixture {
    fn new(responses: Vec<Option<Value>>) -> Self {
        let mut responses = responses.into_iter();
        Self::responding(move |_| match responses.next().flatten() {
            Some(value) => Response::Json(value),
            None => Response::Close,
        })
    }

    fn responding(mut respond: impl FnMut(&str) -> Response + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let capture = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let handle = thread::spawn(move || {
            let mut held = Vec::new();
            while !stopped.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        socket.set_nonblocking(false).unwrap();
                        socket
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        socket
                            .set_write_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        let target = read_request(&mut socket);
                        capture.lock().unwrap().push(target.clone());
                        let response = match respond(&target) {
                            Response::Json(value) => value,
                            Response::Close => continue,
                            Response::Hold => {
                                held.push(socket);
                                continue;
                            }
                        };
                        let bytes = serde_json::to_vec(&response).unwrap();
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            bytes.len()
                        );
                        let _ = socket.write_all(header.as_bytes());
                        let _ = socket.write_all(&bytes);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
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
        self.stop.store(true, Ordering::SeqCst);
        self.handle.take().unwrap().join().unwrap();
    }
}
fn read_request(socket: &mut TcpStream) -> String {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        socket.read_exact(&mut byte).unwrap();
        head.push(byte[0]);
        assert!(head.len() <= 16 * 1024);
    }
    let head = String::from_utf8(head).unwrap();
    let mut target = None;
    let mut length = 0;
    for line in head.lines() {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().unwrap();
            }
            if name.eq_ignore_ascii_case("x-amz-target") {
                target = Some(value.trim().to_owned());
            }
        }
    }
    assert!(length <= 64 * 1024);
    let mut body = vec![0; length];
    socket.read_exact(&mut body).unwrap();
    target.expect("SDK JSON request target")
}
