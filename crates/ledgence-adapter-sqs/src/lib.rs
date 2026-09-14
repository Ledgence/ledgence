//! Optional SQS Standard delivery adapter. Task authority remains in Ledgence.
//!
//! The official SDK supplies signing, credentials and JSON protocol handling.
//! This adapter adds finite deadlines, bounded transport data and complete
//! per-entry confirmation checks. A successful delete never proves exactly-once
//! execution or that a Standard queue can no longer redeliver a record.

mod bounded_http;
pub mod deployment;
#[cfg(test)]
mod tests;

use aws_sdk_sqs::{
    Client,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig},
    types::{DeleteMessageBatchRequestEntry, QueueAttributeName, SendMessageBatchRequestEntry},
};
use ledgence_orchestration_api::{
    AckQueue, AckResult, ContractError, ContractFuture, DISPATCH_MAX_BYTES, DispatchPublisher,
    PublicationOutcome, PublishResult, PublishedDispatch, QUEUE_RECEIPT_MAX_BYTES, QueueDelivery,
    QueueLimits, Result,
};
use std::{
    collections::HashSet,
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

const LIMITS: QueueLimits = QueueLimits {
    max_publish_batch: 10,
    max_receive_batch: 10,
    max_ack_batch: 10,
    max_message_bytes: DISPATCH_MAX_BYTES,
};
static NEXT_SOURCE_ID: AtomicU64 = AtomicU64::new(1);

/// Provider configuration; none of these limits adds execution concurrency.
#[derive(Debug, Clone)]
pub struct SqsOptions {
    pub region: String,
    pub queue_url: String,
    /// Explicit provider override, including ElasticMQ. No endpoint is inferred.
    pub endpoint_url: Option<String>,
    /// Use fixed nonsecret credentials only for an explicit loopback endpoint.
    /// False uses the standard environment/profile/role credential chain.
    pub local_credentials: bool,
    /// Whole send/delete/configuration operation budget (1 ms through 30 s).
    pub operation_timeout: Duration,
    /// Transport visibility through durable handoff, independent of task leases.
    pub visibility_timeout: Duration,
}
impl SqsOptions {
    pub fn new(region: impl Into<String>, queue_url: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            queue_url: queue_url.into(),
            endpoint_url: None,
            local_credentials: false,
            operation_timeout: Duration::from_secs(5),
            visibility_timeout: Duration::from_secs(60),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.region.is_empty()
            || self.region.len() > 128
            || !self
                .region
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(invalid("invalid SQS region"));
        }
        validate_url(&self.queue_url)?;
        if let Some(endpoint) = &self.endpoint_url {
            validate_url(endpoint)?;
        }
        if self.local_credentials {
            let endpoint = self.endpoint_url.as_deref().ok_or_else(|| {
                invalid("local SQS credentials require an explicit loopback endpoint")
            })?;
            let url = validate_url(endpoint)?;
            let loopback = url.host().is_some_and(|host| match host {
                url::Host::Domain(domain) => domain == "localhost",
                url::Host::Ipv4(ip) => ip.is_loopback(),
                url::Host::Ipv6(ip) => ip.is_loopback(),
            });
            if !loopback {
                return Err(invalid("local SQS credentials require a loopback endpoint"));
            }
        }
        if !(Duration::from_millis(1)..=Duration::from_secs(30)).contains(&self.operation_timeout)
            || !self
                .operation_timeout
                .subsec_nanos()
                .is_multiple_of(1_000_000)
        {
            return Err(invalid(
                "SQS operation timeout must be whole milliseconds between 1 and 30000",
            ));
        }
        if !(Duration::from_secs(30)..=Duration::from_secs(43_200))
            .contains(&self.visibility_timeout)
            || self.visibility_timeout.subsec_nanos() != 0
        {
            return Err(invalid(
                "SQS visibility must be whole seconds between 30 and 43200",
            ));
        }
        Ok(())
    }
}

/// Cloneable client for one verified Standard queue. Clones share connections
/// and the receipt namespace. Construct a separate adapter for another queue.
#[derive(Clone)]
pub struct SqsQueue {
    client: Client,
    options: SqsOptions,
    receipt_prefix: String,
}
impl SqsQueue {
    /// Load credentials and verify queue capabilities within one finite budget.
    pub async fn connect(options: SqsOptions) -> Result<Self> {
        options.validate()?;
        let deadline = Instant::now() + options.operation_timeout;
        let client = run_until(deadline, "configuration", async {
            let behavior = BehaviorVersion::v2026_01_12();
            let http = bounded_http::client(!options.local_credentials);
            let mut config = aws_config::defaults(behavior)
                .region(Region::new(options.region.clone()))
                .http_client(http.clone())
                .retry_config(RetryConfig::disabled())
                .timeout_config(timeout_config(options.operation_timeout));
            if options.local_credentials {
                config = config.credentials_provider(Credentials::new(
                    "ledgence-local",
                    "ledgence-local",
                    None,
                    None,
                    "explicit-loopback",
                ));
            }
            let sdk = config.load().await;
            let mut builder = aws_sdk_sqs::config::Builder::from(&sdk)
                .http_client(http)
                .retry_config(RetryConfig::disabled())
                .timeout_config(timeout_config(Duration::from_secs(30)));
            if let Some(endpoint) = &options.endpoint_url {
                builder = builder.endpoint_url(endpoint);
            }
            Ok(Client::from_conf(builder.build()))
        })
        .await?;
        verify_configuration(&client, &options, deadline).await?;
        let source = NEXT_SOURCE_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| unavailable("SQS source identity exhausted"))?;
        Ok(Self {
            client,
            options,
            receipt_prefix: format!("sqs:{source}:"),
        })
    }

    /// Recheck the selected queue with existing connections and credentials.
    /// Supervisors may use this bounded probe to recover health after an outage;
    /// an empty dispatch-intent scan alone provides no provider-health evidence.
    pub async fn check_configuration(&self) -> Result<()> {
        verify_configuration(
            &self.client,
            &self.options,
            Instant::now() + self.options.operation_timeout,
        )
        .await
    }

    fn deadline(&self, requested: Instant) -> Instant {
        requested.min(Instant::now() + self.options.operation_timeout)
    }

    fn receipt<'a>(&self, wrapped: &'a str) -> Result<&'a str> {
        if wrapped.len() > QUEUE_RECEIPT_MAX_BYTES {
            return Err(invalid("SQS receipt exceeds byte limit"));
        }
        wrapped
            .strip_prefix(&self.receipt_prefix)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid("SQS receipt belongs to another source or is empty"))
    }
}

impl DispatchPublisher for SqsQueue {
    fn limits(&self) -> QueueLimits {
        LIMITS
    }
    fn publish<'a>(
        &'a self,
        records: &'a [PublishedDispatch],
        deadline: Instant,
    ) -> ContractFuture<'a, Vec<PublishResult>> {
        Box::pin(async move {
            validate_batch(records.len())?;
            let mut identities = HashSet::new();
            let entries = records
                .iter()
                .enumerate()
                .map(|(index, record)| {
                    record.validate()?;
                    if !identities.insert(&record.publication_id) {
                        return Err(invalid("duplicate SQS publication identity"));
                    }
                    let body = serde_json::to_string(record)
                        .map_err(|_| invalid("invalid dispatch JSON"))?;
                    if body.len() > DISPATCH_MAX_BYTES {
                        return Err(invalid("dispatch exceeds SQS adapter byte limit"));
                    }
                    SendMessageBatchRequestEntry::builder()
                        .id(index.to_string())
                        .message_body(body)
                        .delay_seconds(0)
                        .build()
                        .map_err(|_| invalid("invalid SQS publish entry"))
                })
                .collect::<Result<Vec<_>>>()?;
            let output = run_until(self.deadline(deadline), "publish", async {
                self.client
                    .send_message_batch()
                    .queue_url(&self.options.queue_url)
                    .set_entries(Some(entries))
                    .send()
                    .await
                    .map_err(|_| unavailable("SQS publish outcome unavailable"))
            })
            .await?;
            let confirmed = batch_confirmations(
                records.len(),
                output.successful().iter().map(|entry| entry.id()),
                output.failed().iter().map(|entry| entry.id()),
            )?;
            Ok(records
                .iter()
                .zip(confirmed)
                .map(|(record, confirmed)| PublishResult {
                    publication_id: record.publication_id.clone(),
                    outcome: if confirmed {
                        PublicationOutcome::Confirmed
                    } else {
                        PublicationOutcome::Retry
                    },
                })
                .collect())
        })
    }
}

impl AckQueue for SqsQueue {
    fn limits(&self) -> QueueLimits {
        LIMITS
    }
    fn receive(
        &self,
        max: u32,
        wait: Duration,
        deadline: Instant,
    ) -> ContractFuture<'_, Vec<QueueDelivery>> {
        Box::pin(async move {
            validate_batch(max as usize)?;
            if wait > Duration::from_secs(20) {
                return Err(invalid("SQS long poll cannot exceed 20 seconds"));
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| unavailable("SQS receive deadline elapsed"))?;
            // Reserve network allowance beyond the broker's integer-second wait.
            let poll = wait
                .min(remaining.saturating_sub(Duration::from_secs(1)))
                .as_secs();
            let response = run_until(
                deadline.min(Instant::now() + wait + self.options.operation_timeout),
                "receive",
                async {
                    self.client
                        .receive_message()
                        .queue_url(&self.options.queue_url)
                        .max_number_of_messages(max as i32)
                        .wait_time_seconds(poll as i32)
                        .visibility_timeout(self.options.visibility_timeout.as_secs() as i32)
                        .send()
                        .await
                        .map_err(|error| {
                            if bounded_http::response_limit_reached(&error) {
                                invalid_delivery("SQS receive response exceeds wire byte limit")
                            } else {
                                unavailable("SQS receive outcome unavailable")
                            }
                        })
                },
            )
            .await?;
            if response.messages().len() > max as usize {
                return Err(invalid_delivery("SQS returned more records than reserved"));
            }
            let mut seen = HashSet::new();
            response
                .messages()
                .iter()
                .map(|message| {
                    let body = message
                        .body()
                        .ok_or_else(|| invalid_delivery("SQS message body missing"))?;
                    let receipt = message
                        .receipt_handle()
                        .filter(|r| !r.is_empty())
                        .ok_or_else(|| invalid_delivery("SQS message receipt missing"))?;
                    if !seen.insert(receipt) {
                        return Err(invalid_delivery("SQS returned duplicate receipts"));
                    }
                    if body.len() > DISPATCH_MAX_BYTES
                        || receipt.len() > QUEUE_RECEIPT_MAX_BYTES - self.receipt_prefix.len()
                    {
                        return Err(invalid_delivery(
                            "SQS message or receipt exceeds byte limit",
                        ));
                    }
                    Ok(QueueDelivery {
                        body: body.as_bytes().to_vec(),
                        receipt: format!("{}{receipt}", self.receipt_prefix),
                    })
                })
                .collect()
        })
    }
    fn acknowledge<'a>(
        &'a self,
        receipts: &'a [String],
        deadline: Instant,
    ) -> ContractFuture<'a, Vec<AckResult>> {
        Box::pin(async move {
            validate_batch(receipts.len())?;
            let mut seen = HashSet::new();
            let entries = receipts
                .iter()
                .enumerate()
                .map(|(index, receipt)| {
                    let raw = self.receipt(receipt)?;
                    if !seen.insert(raw) {
                        return Err(invalid("duplicate SQS acknowledgment receipt"));
                    }
                    DeleteMessageBatchRequestEntry::builder()
                        .id(index.to_string())
                        .receipt_handle(raw)
                        .build()
                        .map_err(|_| invalid("invalid SQS acknowledgment entry"))
                })
                .collect::<Result<Vec<_>>>()?;
            let output = run_until(self.deadline(deadline), "acknowledge", async {
                self.client
                    .delete_message_batch()
                    .queue_url(&self.options.queue_url)
                    .set_entries(Some(entries))
                    .send()
                    .await
                    .map_err(|_| unavailable("SQS acknowledgment outcome unavailable"))
            })
            .await?;
            let confirmed = batch_confirmations(
                receipts.len(),
                output.successful().iter().map(|entry| entry.id()),
                output.failed().iter().map(|entry| entry.id()),
            )?;
            Ok(receipts
                .iter()
                .zip(confirmed)
                .map(|(receipt, confirmed)| AckResult {
                    receipt: receipt.clone(),
                    confirmed,
                })
                .collect())
        })
    }
}

async fn verify_configuration(
    client: &Client,
    options: &SqsOptions,
    deadline: Instant,
) -> Result<()> {
    let attributes = run_until(deadline, "queue verification", async {
        client
            .get_queue_attributes()
            .queue_url(&options.queue_url)
            .attribute_names(QueueAttributeName::FifoQueue)
            .attribute_names(QueueAttributeName::MaximumMessageSize)
            .attribute_names(QueueAttributeName::DelaySeconds)
            .send()
            .await
            .map_err(|_| unavailable("SQS queue verification failed"))
    })
    .await?;
    let values = attributes
        .attributes()
        .ok_or_else(|| unavailable("SQS queue attributes missing"))?;
    if values
        .get(&QueueAttributeName::FifoQueue)
        .is_some_and(|v| v != "false")
        || options.queue_url.trim_end_matches('/').ends_with(".fifo")
    {
        return Err(invalid("only SQS Standard queues are supported"));
    }
    if values
        .get(&QueueAttributeName::DelaySeconds)
        .map(String::as_str)
        != Some("0")
    {
        return Err(invalid("SQS queue default delay must be zero"));
    }
    match values.get(&QueueAttributeName::MaximumMessageSize) {
        Some(value) => {
            let maximum = value
                .parse::<usize>()
                .map_err(|_| unavailable("invalid SQS maximum message size"))?;
            if maximum < DISPATCH_MAX_BYTES {
                return Err(invalid(
                    "SQS queue message size is below the dispatch limit",
                ));
            }
        }
        // ElasticMQ 1.7.1 omits this attribute. Explicit compatible
        // endpoints are qualified by conformance tests; our own 16 KiB
        // limit still applies. Absence is not invented provider metadata.
        None if options.endpoint_url.is_some() => {}
        None => return Err(unavailable("SQS maximum message size missing")),
    }
    Ok(())
}

fn batch_confirmations<'a>(
    count: usize,
    successes: impl Iterator<Item = &'a str>,
    failures: impl Iterator<Item = &'a str>,
) -> Result<Vec<bool>> {
    let mut results = vec![None; count];
    for (id, confirmed) in successes
        .map(|id| (id, true))
        .chain(failures.map(|id| (id, false)))
    {
        let index = id
            .parse::<usize>()
            .ok()
            .filter(|index| *index < count && index.to_string() == id)
            .ok_or_else(|| unavailable("SQS batch returned an unexpected identity"))?;
        if results[index].replace(confirmed).is_some() {
            return Err(unavailable("SQS batch repeated an identity"));
        }
    }
    results
        .into_iter()
        .map(|result| result.ok_or_else(|| unavailable("SQS batch omitted an identity")))
        .collect()
}
fn validate_batch(count: usize) -> Result<()> {
    if !(1..=10).contains(&count) {
        return Err(invalid("SQS batch size must be between 1 and 10"));
    }
    Ok(())
}
fn validate_url(value: &str) -> Result<url::Url> {
    if value.len() > 4096 {
        return Err(invalid("SQS URL exceeds byte limit"));
    }
    let url = url::Url::parse(value).map_err(|_| invalid("invalid SQS URL"))?;
    if !matches!(url.scheme(), "https" | "http")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
    {
        return Err(invalid(
            "SQS URL must be absolute HTTP(S) without credentials, query or fragment",
        ));
    }
    Ok(url)
}
fn timeout_config(timeout: Duration) -> TimeoutConfig {
    TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(2))
        .operation_timeout(timeout)
        .operation_attempt_timeout(timeout)
        .build()
}
async fn run_until<T>(
    deadline: Instant,
    operation: &str,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    if deadline <= Instant::now() {
        return Err(unavailable(&format!("SQS {operation} deadline elapsed")));
    }
    let result = tokio::time::timeout_at(deadline.into(), future)
        .await
        .map_err(|_| unavailable(&format!("SQS {operation} deadline elapsed")))?;
    // A synchronous SDK poll (including response decoding) cannot be preempted
    // by Tokio's timer. Its late Ready reply is still an unknown outcome, even
    // when an outer coordinator has a longer remaining exchange budget.
    if Instant::now() >= deadline {
        Err(unavailable(&format!("SQS {operation} deadline elapsed")))
    } else {
        result
    }
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn invalid_delivery(message: &str) -> ContractError {
    ContractError::InvalidQueueDelivery(message.into())
}
fn unavailable(message: &str) -> ContractError {
    ContractError::Unavailable(message.into())
}
