//! Bound response bodies before the Smithy runtime's unbounded collection.

use aws_smithy_http_client::{Builder, ConnectorBuilder, proxy::ProxyConfig, tls};
use aws_smithy_runtime_api::client::{
    http::{
        HttpClient, HttpConnector, HttpConnectorFuture, SharedHttpClient, SharedHttpConnector,
        http_client_fn,
    },
    orchestrator::HttpRequest,
    result::ConnectorError,
};
use aws_smithy_types::body::SdkBody;
use http_body_util::{BodyExt, Limited};

// A receive has at most ten escaped 16 KiB bodies and ten escaped 16 KiB
// receipts, plus bounded envelope fields. This is a hard wire-body limit, not
// a promise to accept oversized/malformed per-record fields after decoding.
pub(crate) const MAX_RESPONSE_BYTES: usize = 10 * (16 * 1024 + 16 * 1024) * 6 + 64 * 1024;

pub(crate) fn client(allow_environment_proxy: bool) -> SharedHttpClient {
    let inner = Builder::new().build_with_connector_fn(move |settings, components| {
        let mut connector = ConnectorBuilder::default().tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::AwsLc,
        ));
        connector.set_connector_settings(settings.cloned());
        if let Some(runtime) = components {
            connector.set_sleep_impl(runtime.sleep_impl());
        }
        // Match the pinned SDK behavior for production credentials. Explicit
        // loopback test credentials stay on the direct local connection.
        connector.set_proxy_config(Some(if allow_environment_proxy {
            ProxyConfig::from_env()
        } else {
            ProxyConfig::disabled()
        }));
        connector.build()
    });
    http_client_fn(move |settings, components| {
        SharedHttpConnector::new(BoundedConnector {
            inner: inner.http_connector(settings, components),
        })
    })
}

#[derive(Debug)]
struct BoundedConnector {
    inner: SharedHttpConnector,
}
impl HttpConnector for BoundedConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let pending = self.inner.call(request);
        HttpConnectorFuture::new(async move {
            let mut response = pending.await?;
            let body = std::mem::replace(response.body_mut(), SdkBody::taken());
            let bytes = Limited::new(body, MAX_RESPONSE_BYTES)
                .collect()
                .await
                .map_err(|error| ConnectorError::other(error, None))?
                .to_bytes();
            *response.body_mut() = SdkBody::from(bytes);
            Ok(response)
        })
    }
}

/// Only a positively identified body-cap violation is a protocol rejection.
/// Smithy preserves connector causes; ordinary I/O remains an unknown outcome.
pub(crate) fn response_limit_reached(mut error: &(dyn std::error::Error + 'static)) -> bool {
    for _ in 0..16 {
        if error.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        let Some(cause) = error.source() else {
            return false;
        };
        error = cause;
    }
    false
}
