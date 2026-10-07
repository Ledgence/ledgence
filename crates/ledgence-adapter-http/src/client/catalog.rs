use super::*;
use ledgence_orchestration_api::console::{
    CONSOLE_METADATA_MAX_BYTES, ConsoleRecord, RegisterProgram, RegisterProgramReply,
};
impl ResponseValue for RegisterProgramReply {
    const MAX_BYTES: usize = CONSOLE_METADATA_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.version.validate()
    }
}
impl HttpTaskService {
    /// Register one exact, already-published reference. One exchange, no retries.
    pub async fn register_program(
        &self,
        command: &RegisterProgram,
    ) -> Result<RegisterProgramReply> {
        command.validate()?;
        let expected = command.clone();
        self.post_validated(
            "v1/console/programs/register",
            command,
            SUBMISSION_MAX_BYTES,
            move |reply: &RegisterProgramReply| {
                if reply.version.descriptor.program != expected.program
                    || expected
                        .expected_descriptor
                        .as_ref()
                        .is_some_and(|descriptor| {
                            reply.version.descriptor != descriptor.clone().into()
                        })
                    || reply.version.metadata != expected.metadata
                    || (reply.metadata_updated
                        && (!expected.update_metadata || !reply.already_registered))
                {
                    return Err(unavailable(
                        "catalog registration reply does not match request",
                    ));
                }
                Ok(())
            },
        )
        .await
    }
}

/// Remote catalog reads constrained to an expected installation scope.
///
/// The server checks these guards in the same request; they never select or
/// override its configured scope. Older servers reject the unknown guards.
/// Registration remains available through `HttpTaskService::register_program`;
/// this read adapter deliberately does not expose unguarded registration.
#[derive(Clone)]
pub struct HttpProgramCatalogService {
    http: HttpTaskService,
    scope: Scope,
}
impl HttpProgramCatalogService {
    pub fn new(http: HttpTaskService, scope: Scope) -> Result<Self> {
        scope.validate()?;
        Ok(Self { http, scope })
    }
}

impl ledgence_orchestration_api::console::ProgramCatalogService for HttpProgramCatalogService {
    fn register_program<'a>(
        &'a self,
        _command: &'a RegisterProgram,
    ) -> ContractFuture<'a, RegisterProgramReply> {
        Box::pin(async {
            Err(ContractError::InvalidInput(
                "remote catalog adapter supports guarded reads only".into(),
            ))
        })
    }
    fn query_programs<'a>(
        &'a self,
        query: &'a ledgence_orchestration_api::console::ProgramCatalogQuery,
    ) -> ContractFuture<'a, ledgence_orchestration_api::console::ProgramCatalogReply> {
        use ledgence_orchestration_api::console::*;
        Box::pin(async move {
            query.validate(&self.scope)?;
            let mut fields = vec![
                ("expected_tenant_id", self.scope.tenant_id.clone()),
                ("expected_namespace", self.scope.namespace.clone()),
            ];
            let page = match query {
                ProgramCatalogQuery::Catalog { page, .. }
                | ProgramCatalogQuery::Programs(page)
                | ProgramCatalogQuery::Versions { page, .. } => Some(page),
                ProgramCatalogQuery::Inspect(_) => None,
            };
            if let Some(page) = page {
                fields.push(("limit", page.limit.to_string()));
                if let Some(cursor) = &page.cursor {
                    fields.push(("cursor", cursor.clone()));
                }
            }
            let scope = self.scope.clone();
            let expected = query.clone();
            match query {
                ProgramCatalogQuery::Catalog { kind, .. } => {
                    if let Some(kind) = kind {
                        let value = serde_json::to_value(kind)
                            .map_err(|_| unavailable("invalid catalog kind"))?;
                        fields.push((
                            "kind",
                            value
                                .as_str()
                                .ok_or_else(|| unavailable("invalid catalog kind"))?
                                .to_owned(),
                        ));
                    }
                    self.http
                        .get_validated(
                            "v1/console/programs/catalog",
                            &fields,
                            move |reply: &ConsolePage<ConsoleProgramCatalogEntry>| {
                                ProgramCatalogReply::Catalog(reply.clone())
                                    .validate(&scope, &expected)
                            },
                        )
                        .await
                        .map(ProgramCatalogReply::Catalog)
                }
                ProgramCatalogQuery::Programs(_) => self
                    .http
                    .get_validated(
                        "v1/console/programs",
                        &fields,
                        move |reply: &ConsolePage<ConsoleProgramSummary>| {
                            ProgramCatalogReply::Programs(reply.clone()).validate(&scope, &expected)
                        },
                    )
                    .await
                    .map(ProgramCatalogReply::Programs),
                ProgramCatalogQuery::Versions { program_id, .. } => {
                    fields.push(("program_id", program_id.clone()));
                    self.http
                        .get_validated(
                            "v1/console/programs/versions",
                            &fields,
                            move |reply: &ConsolePage<ConsoleProgramVersion>| {
                                ProgramCatalogReply::Versions(reply.clone())
                                    .validate(&scope, &expected)
                            },
                        )
                        .await
                        .map(ProgramCatalogReply::Versions)
                }
                ProgramCatalogQuery::Inspect(program) => {
                    fields.push(("program_id", program.id.clone()));
                    fields.push(("version", program.version.clone()));
                    self.http
                        .get_validated(
                            "v1/console/programs/inspect",
                            &fields,
                            move |reply: &ConsoleProgramDetail| {
                                ProgramCatalogReply::Inspect(Box::new(reply.clone()))
                                    .validate(&scope, &expected)
                            },
                        )
                        .await
                        .map(|reply| ProgramCatalogReply::Inspect(Box::new(reply)))
                }
            }
        })
    }
}
impl<
    T: ledgence_orchestration_api::console::ConsoleRecord
        + serde::de::DeserializeOwned
        + Send
        + 'static,
> ResponseValue for ledgence_orchestration_api::console::ConsolePage<T>
{
    const MAX_BYTES: usize = CONSOLE_METADATA_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        if self.items.len() > 100 {
            return Err(unavailable("catalog page exceeds item limit"));
        }
        for item in &self.items {
            item.validate()?;
        }
        Ok(())
    }
}
impl ResponseValue for ledgence_orchestration_api::console::ConsoleProgramDetail {
    const MAX_BYTES: usize = CONSOLE_METADATA_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.version.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_orchestration_api::console::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn response(
        status: u16,
        body: &str,
    ) -> (HttpProgramCatalogService, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let body = body.to_owned();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.windows(4).any(|v| v == b"\r\n\r\n") {
                let mut buffer = [0; 1024];
                let size = stream.read(&mut buffer).await.unwrap();
                assert_ne!(size, 0);
                request.extend_from_slice(&buffer[..size]);
                assert!(request.len() < 64 * 1024);
            }
            let reply = format!(
                "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(reply.as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        let http = HttpTaskService::new(&url).unwrap();
        (
            HttpProgramCatalogService::new(
                http,
                Scope {
                    tenant_id: "acme / north".into(),
                    namespace: "billing".into(),
                },
            )
            .unwrap(),
            handle,
        )
    }
    #[tokio::test]
    async fn every_catalog_read_sends_both_scope_guards_in_the_same_request() {
        for query in [
            ProgramCatalogQuery::Catalog {
                kind: None,
                page: ConsolePagination::default(),
            },
            ProgramCatalogQuery::Programs(ConsolePagination::default()),
            ProgramCatalogQuery::Versions {
                program_id: "invoice".into(),
                page: ConsolePagination::default(),
            },
        ] {
            let (client, request) =
                response(200, r#"{"items":[],"next_cursor":null,"observed_at":1}"#).await;
            client.query_programs(&query).await.unwrap();
            let request = request.await.unwrap();
            let line = request.lines().next().unwrap();
            assert!(line.starts_with("GET /v1/console/programs"));
            assert!(line.contains("expected_tenant_id=acme+%2F+north"));
            assert!(line.contains("expected_namespace=billing"));
            assert!(line.contains("limit=50"));
        }
        let query = ProgramCatalogQuery::Inspect(ledgence_worker_api::ProgramRef {
            id: "invoice".into(),
            version: "1".into(),
        });
        let (client, request) = response(404, r#"{"code":"not_found"}"#).await;
        assert_eq!(
            client.query_programs(&query).await.unwrap_err(),
            ContractError::NotFound
        );
        let request = request.await.unwrap();
        assert!(request.contains("expected_tenant_id=acme+%2F+north&expected_namespace=billing&program_id=invoice&version=1"));
    }
    #[tokio::test]
    async fn an_old_server_rejection_does_not_fall_back_to_unguarded_catalog() {
        let (client, request) = response(
            400,
            r#"{"code":"invalid_input","message":"unknown or duplicate console query field"}"#,
        )
        .await;
        let query = ProgramCatalogQuery::Programs(ConsolePagination::default());
        assert!(matches!(
            client.query_programs(&query).await,
            Err(ContractError::InvalidInput(_))
        ));
        assert!(
            request
                .await
                .unwrap()
                .contains("expected_namespace=billing")
        );
    }
}

#[cfg(all(test, feature = "server"))]
mod registration_precondition_tests {
    use super::*;
    use ledgence_orchestration_api::console::*;
    use ledgence_worker_api::{
        Digest, Platform, ProgramDescriptor, ProgramManifest, ProgramRef, PythonRuntime,
    };
    #[tokio::test]
    async fn registration_reply_must_match_the_expected_published_descriptor() {
        let descriptor = ProgramDescriptor {
            program: ProgramRef {
                id: "invoice".into(),
                version: "1".into(),
            },
            digest: Digest(format!("sha256:{}", "a".repeat(64))),
            size: 123,
        };
        let command = RegisterProgram {
            program: descriptor.program.clone(),
            expected_descriptor: Some(descriptor.clone()),
            metadata: ProgramDisplayMetadata::default(),
            update_metadata: false,
        };
        for wrong in [false, true] {
            let mut returned = descriptor.clone();
            if wrong {
                returned.size += 1;
            }
            let reply = RegisterProgramReply {
                version: ConsoleProgramVersion {
                    descriptor: returned.into(),
                    manifest: ProgramManifest {
                        schema_version: 1,
                        program: descriptor.program.clone(),
                        handler: "app:handle".into(),
                        runtime: PythonRuntime {
                            kind: "python".into(),
                            python: "3.14".into(),
                            protocol: 3,
                        },
                        platform: Platform {
                            os: "linux".into(),
                            arch: "aarch64".into(),
                        },
                    },
                    metadata: ProgramDisplayMetadata::default(),
                    registered_at: 1,
                    provenance: ProgramRegistrationProvenance::ConfiguredProgramStore,
                },
                already_registered: false,
                metadata_updated: false,
            };
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let router = axum::Router::new().route(
                "/v1/console/programs/register",
                axum::routing::post(move || async move {
                    axum::response::Response::builder()
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(serde_json::to_vec(&reply).unwrap()))
                        .unwrap()
                }),
            );
            let server = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });
            let result = HttpTaskService::new(&url)
                .unwrap()
                .register_program(&command)
                .await;
            server.abort();
            assert_eq!(result.is_err(), wrong);
        }
    }
}
