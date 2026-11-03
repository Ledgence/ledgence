use super::*;

impl ResponseValue for EffectResolutionReceipt {
    const MAX_BYTES: usize = EFFECT_RESOLUTION_RECEIPT_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl EffectResolutionClient for HttpTaskService {
    fn resolve_effect<'a>(
        &'a self,
        command: &'a EffectResolutionCommand,
    ) -> ContractFuture<'a, EffectResolutionReceipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/workflows/approved-effects/resolve",
                command,
                EFFECT_RESOLUTION_COMMAND_MAX_BYTES,
                move |reply: &EffectResolutionReceipt| matches(reply.matches(&expected)),
            )
            .await
        })
    }
}
impl HttpTaskService {
    /// Send the exact frozen command bytes after local strict validation. The
    /// caller must separately validate its saved envelope's destination/scope.
    pub async fn resolve_effect_bytes(&self, bytes: &[u8]) -> Result<EffectResolutionReceipt> {
        let expected: EffectResolutionCommand =
            decode_unique_json(bytes, EFFECT_RESOLUTION_COMMAND_MAX_BYTES)?;
        expected.validate()?;
        let bytes = bytes.to_vec();
        let start = Instant::now();
        self.exchange(
            Method::POST,
            "v1/workflows/approved-effects/resolve",
            &[],
            ExchangeBudget {
                start,
                deadline: start + self.timeout,
            },
            async { Ok(Some(bytes)) },
            move |reply: &EffectResolutionReceipt| matches(reply.matches(&expected)),
        )
        .await
    }
}

impl ResponseValue for EffectClaimV2Receipt {
    const MAX_BYTES: usize = EFFECT_EVIDENCE_COMMAND_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for EffectConfirmV2Receipt {
    const MAX_BYTES: usize = 16 * 1024;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for EffectObservationReceipt {
    const MAX_BYTES: usize = 16 * 1024;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for EffectInboxPage {
    const MAX_BYTES: usize = EFFECT_EVIDENCE_PAGE_MAX_BYTES;
}
impl ResponseValue for EffectEvidenceDetail {
    const MAX_BYTES: usize = EFFECT_EVIDENCE_PAGE_MAX_BYTES;
}
fn matches(reply: Result<bool>) -> Result<()> {
    if matches!(reply, Ok(true)) {
        Ok(())
    } else {
        Err(unavailable("effect evidence response identity mismatch"))
    }
}
impl EffectEvidenceClient for HttpTaskService {
    fn claim_v2<'a>(
        &'a self,
        command: &'a EffectClaimV2Command,
    ) -> ContractFuture<'a, EffectClaimV2Receipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/workflows/approved-effects/v2/claim",
                command,
                EFFECT_EVIDENCE_COMMAND_MAX_BYTES,
                move |reply: &EffectClaimV2Receipt| matches(reply.matches(&expected)),
            )
            .await
        })
    }
    fn confirm_v2<'a>(
        &'a self,
        command: &'a EffectConfirmV2Command,
    ) -> ContractFuture<'a, EffectConfirmV2Receipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/workflows/approved-effects/v2/confirm",
                command,
                EFFECT_EVIDENCE_COMMAND_MAX_BYTES,
                move |reply: &EffectConfirmV2Receipt| matches(reply.matches(&expected)),
            )
            .await
        })
    }
    fn observe<'a>(
        &'a self,
        command: &'a EffectObservationCommand,
    ) -> ContractFuture<'a, EffectObservationReceipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/workflows/approved-effects/v2/observe",
                command,
                EFFECT_EVIDENCE_COMMAND_MAX_BYTES,
                move |reply: &EffectObservationReceipt| matches(reply.matches(&expected)),
            )
            .await
        })
    }
}
impl EffectEvidenceReader for HttpTaskService {
    fn effect_inbox<'a>(
        &'a self,
        query: &'a EffectInboxQuery,
    ) -> ContractFuture<'a, EffectInboxPage> {
        Box::pin(async move {
            query.validate()?;
            let expected = query.clone();
            self.post_validated(
                "v1/workflows/approved-effects/inbox",
                query,
                EFFECT_EVIDENCE_CURSOR_MAX_BYTES + 4096,
                move |reply: &EffectInboxPage| {
                    reply
                        .validate_for(&expected)
                        .map_err(|_| unavailable("invalid effect inbox response"))
                },
            )
            .await
        })
    }
    fn inspect_effect<'a>(
        &'a self,
        query: &'a EffectInspectQuery,
    ) -> ContractFuture<'a, EffectEvidenceDetail> {
        Box::pin(async move {
            query.validate()?;
            let expected = query.clone();
            self.post_validated(
                "v1/workflows/approved-effects/inspect",
                query,
                EFFECT_EVIDENCE_CURSOR_MAX_BYTES + 4096,
                move |reply: &EffectEvidenceDetail| {
                    reply
                        .validate_for(&expected)
                        .map_err(|_| unavailable("invalid effect detail response"))
                },
            )
            .await
        })
    }
}
