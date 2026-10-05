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
