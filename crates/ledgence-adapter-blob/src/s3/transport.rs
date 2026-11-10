//! Bound error aggregation while preserving streaming successful GET bodies.
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
#[derive(Clone, Copy, PartialEq)]
pub(super) enum Transport {
    Standard,
    LocalTls,
    LocalHttp,
}
pub(super) fn client(transport: Transport) -> SharedHttpClient {
    let inner = Builder::new().build_with_connector_fn(move |settings, components| {
        let mut connector = ConnectorBuilder::default();
        connector.set_connector_settings(settings.cloned());
        if let Some(runtime) = components {
            connector.set_sleep_impl(runtime.sleep_impl());
        }
        connector.set_proxy_config(Some(if transport == Transport::Standard {
            ProxyConfig::from_env()
        } else {
            ProxyConfig::disabled()
        }));
        if transport == Transport::LocalHttp {
            connector.build_http()
        } else {
            connector
                .tls_provider(tls::Provider::Rustls(
                    tls::rustls_provider::CryptoMode::AwsLc,
                ))
                .build()
        }
    });
    http_client_fn(move |settings, components| {
        SharedHttpConnector::new(Bounded {
            inner: inner.http_connector(settings, components),
        })
    })
}
#[derive(Debug)]
struct Bounded {
    inner: SharedHttpConnector,
}
impl HttpConnector for Bounded {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let pending = self.inner.call(request);
        HttpConnectorFuture::new(async move {
            let mut response = pending.await?;
            let body = std::mem::replace(response.body_mut(), SdkBody::taken());
            if (200..300).contains(&response.status().as_u16()) {
                *response.body_mut() =
                    SdkBody::from_body_1_x(Limited::new(body, super::MAX_BYTES as usize + 1));
            } else {
                let bytes = Limited::new(body, 64 * 1024)
                    .collect()
                    .await
                    .map_err(|error| ConnectorError::other(error, None))?
                    .to_bytes();
                *response.body_mut() = SdkBody::from(bytes);
            }
            Ok(response)
        })
    }
}
