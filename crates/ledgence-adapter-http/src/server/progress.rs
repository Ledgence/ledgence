//! Read-only lifecycle pages, using metadata authorization on every exchange.
use super::*;

pub(super) async fn post(
    server: &Server,
    bytes: Vec<u8>,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    let query: ExecutionProgressQuery = server
        .decode(bytes, EXECUTION_PROGRESS_QUERY_MAX_BYTES)
        .await?;
    read(server, query, context).await
}

/// The browser never chooses an installation scope.
pub(super) async fn console_post(
    server: &Server,
    bytes: Vec<u8>,
    scope: &Scope,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Query {
        target: ExecutionProgressTarget,
        #[serde(default)]
        cursor: Option<String>,
        #[serde(default = "limit")]
        limit: u32,
        #[serde(default)]
        wait_ms: u64,
    }
    fn limit() -> u32 {
        EXECUTION_PROGRESS_MAX_LIMIT
    }
    let query: Query = server
        .decode(bytes, EXECUTION_PROGRESS_QUERY_MAX_BYTES)
        .await?;
    read(
        server,
        ExecutionProgressQuery {
            scope: scope.clone(),
            target: query.target,
            cursor: query.cursor,
            limit: query.limit,
            wait_ms: query.wait_ms,
        },
        context,
    )
    .await
}

async fn read(
    server: &Server,
    query: ExecutionProgressQuery,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    query.validate()?;
    server.require_scope(&query.scope)?;
    record_scope(&query.scope);
    context.recheck(server)?;
    let reply = server
        .service
        .progress_reader()?
        .execution_progress(&query)
        .await?;
    context.recheck(server)?;
    server
        .blocking(move || {
            reply
                .validate_for(&query)
                .map_err(|_| unavailable("invalid execution progress response"))?;
            encode_bounded(&reply, EXECUTION_PROGRESS_PAGE_MAX_BYTES)
        })
        .await
}
