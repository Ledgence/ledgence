use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use std::io::Read;

/// OTLP replies contain acknowledgement/partial-success data, never task data.
/// Bound their body as well as the already bounded request batches.
#[derive(Debug)]
pub(crate) struct BoundedHttp {
    pub client: reqwest::blocking::Client,
    pub state: std::sync::Arc<crate::processor::State>,
    pub metric_state: Option<std::sync::Arc<crate::metrics::State>>,
}
const RESPONSE_BYTES: u64 = 64 * 1024;
#[async_trait::async_trait]
impl HttpClient for BoundedHttp {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let (parts, body) = request.into_parts();
        let response = self
            .client
            .request(parts.method, parts.uri.to_string())
            .headers(parts.headers)
            .body(body)
            .send()?;
        if response
            .content_length()
            .is_some_and(|length| length > RESPONSE_BYTES)
        {
            return Err(std::io::Error::other("OTLP response exceeds 64 KiB").into());
        }
        let status = response.status();
        let headers = response.headers().clone();
        let mut body = Vec::new();
        response.take(RESPONSE_BYTES + 1).read_to_end(&mut body)?;
        if body.len() as u64 > RESPONSE_BYTES {
            return Err(std::io::Error::other("OTLP response exceeds 64 KiB").into());
        }
        if status.is_success() {
            use prost::Message;
            if let Some(state) = &self.metric_state {
                let acknowledgement = opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse::decode(body.as_slice())?;
                if let Some(partial) = acknowledgement.partial_success {
                    state.partial(
                        partial.rejected_data_points.max(0) as u64,
                        !partial.error_message.is_empty(),
                    );
                }
            } else {
                let acknowledgement = opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse::decode(body.as_slice())?;
                if let Some(partial) = acknowledgement.partial_success {
                    self.state.partial_success(
                        partial.rejected_spans.max(0) as u64,
                        !partial.error_message.is_empty(),
                    );
                }
            }
        }
        let mut reply = Response::builder().status(status).body(Bytes::from(body))?;
        *reply.headers_mut() = headers;
        Ok(reply)
    }
}
