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
