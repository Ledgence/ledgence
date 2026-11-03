use super::*;

impl ResponseValue for AuditPage {
    const MAX_BYTES: usize = AUDIT_PAGE_MAX_BYTES;
}

impl AuditReader for HttpTaskService {
    fn query_audit<'a>(&'a self, query: &'a AuditQuery) -> ContractFuture<'a, AuditPage> {
        Box::pin(async move {
            query.validate()?;
            let expected = query.clone();
            self.post_validated(
                "v1/audit/events",
                query,
                AUDIT_CURSOR_MAX_BYTES + 4096,
                move |reply: &AuditPage| reply.validate_for_query(&expected),
            )
            .await
        })
    }
}
