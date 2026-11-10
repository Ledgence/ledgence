use super::*;
impl PostgresStore {
    pub(super) async fn artifact_bind_store(&self, i: &ArtifactStoreIdentity) -> StoreResult<()> {
        i.validate()?;
        if self.instance_scope.get().is_none() {
            return Err(ContractError::Unavailable(
                "artifacts require an initialized bound instance".into(),
            )
            .into());
        }
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        sqlx::query("LOCK TABLE artifact_stores IN EXCLUSIVE MODE")
            .execute(&mut *tx)
            .await?;
        let existing: Option<(String, String)> =
            sqlx::query_as("SELECT store_id,fingerprint FROM artifact_stores WHERE singleton")
                .fetch_optional(&mut *tx)
                .await?;
        if existing
            .as_ref()
            .is_some_and(|(id, fp)| id != &i.store_id || fp != &i.fingerprint.0)
        {
            let retained:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_artifacts) OR EXISTS(SELECT 1 FROM artifact_uploads WHERE NOT deleted)").fetch_one(&mut *tx).await?;
            if retained {
                return Err(ContractError::Conflict.into());
            }
        }
        sqlx::query("INSERT INTO artifact_stores(singleton,store_id,fingerprint) VALUES(true,$1,$2) ON CONFLICT(singleton) DO UPDATE SET store_id=EXCLUDED.store_id,fingerprint=EXCLUDED.fingerprint").bind(&i.store_id).bind(&i.fingerprint.0).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    pub(super) async fn artifact_prepare(
        &self,
        a: &ArtifactAuthority,
        c: &ArtifactPrepareCommand,
    ) -> StoreResult<ArtifactPrepareReply> {
        c.validate()?;
        if &c.scope != a.scope() {
            return Err(ContractError::NotFound.into());
        }
        if let ArtifactAuthority::Worker { lease, .. } = a
            && c.target
                != (ExecutionTarget::Task {
                    id: lease.task_id.clone(),
                })
        {
            return Err(ContractError::Forbidden.into());
        }
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let r = canonical_root(&mut tx, &c.scope, &c.target).await?;
        let producer = authorize(self, &mut tx, a, &r, AccessCapability::ArtifactWrite)
            .await?
            .unwrap_or_else(|| external_producer(a));
        // Target visibility is rechecked after taking the canonical root lock.
        if canonical_root(&mut tx, &c.scope, &c.target).await? != r {
            return Err(ContractError::Conflict.into());
        }
        let prior:Option<(String,Vec<u8>)>=sqlx::query_as("SELECT selected_upload_id,metadata_bytes FROM execution_artifacts WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND producer_bytes=$5 AND artifact_key=$6 FOR UPDATE")
            .bind(&c.scope.tenant_id).bind(&c.scope.namespace).bind(r.kind()).bind(r.id()).bind(codec::encode(&producer)?).bind(&c.key).fetch_optional(&mut *tx).await?;
        if let Some((id, metadata)) = prior {
            if codec::decode::<ArtifactMetadata>(&metadata)? != c.metadata {
                return Err(ContractError::Conflict.into());
            }
            let row = upload(&mut tx, &id, false).await?;
            let upload = status(&mut tx, &row).await?;
            authorize(self, &mut tx, a, &r, AccessCapability::ArtifactWrite).await?;
            tx.commit().await?;
            return Ok(ArtifactPrepareReply { upload });
        }
        // Hold shared binding ownership until the first generation commits.
        // Rebinding takes EXCLUSIVE table ownership and must then observe this
        // committed manifest/ledger before deciding that the catalog is empty.
        let identity = sqlx::query(
            "SELECT store_id,fingerprint FROM artifact_stores WHERE singleton FOR SHARE",
        )
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| ContractError::Unavailable("artifact store is not configured".into()))?;
        let store = ArtifactStoreIdentity {
            store_id: identity.try_get("store_id")?,
            fingerprint: ledgence_worker_api::Digest(identity.try_get("fingerprint")?),
        };
        let now = db::now(&mut tx).await?;
        sqlx::query("INSERT INTO artifact_roots(root_kind,root_id,tenant_id,namespace) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
            .bind(r.kind()).bind(r.id()).bind(&c.scope.tenant_id).bind(&c.scope.namespace).execute(&mut *tx).await?;
        quota(&mut tx, &c.scope, &r, now).await?;
        let selected = ArtifactSelection {
            artifact_id: db::id(&mut tx, "art").await?,
            upload_id: db::id(&mut tx, "upl").await?,
            revision: 1,
        };
        sqlx::query("INSERT INTO execution_artifacts(artifact_id,tenant_id,namespace,root_kind,root_id,producer_bytes,artifact_key,metadata_bytes,selected_upload_id,selection_revision,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,1,$10)")
            .bind(&selected.artifact_id).bind(&c.scope.tenant_id).bind(&c.scope.namespace).bind(r.kind()).bind(r.id()).bind(codec::encode(&producer)?).bind(&c.key).bind(codec::encode(&c.metadata)?).bind(&selected.upload_id).bind(codec::ms(now)?).execute(&mut *tx).await?;
        insert_upload(&mut tx, &c.scope, &r, &selected, &c.metadata, &store, now).await?;
        let row = upload(&mut tx, &selected.upload_id, false).await?;
        let upload = status(&mut tx, &row).await?;
        authorize(self, &mut tx, a, &r, AccessCapability::ArtifactWrite).await?;
        tx.commit().await?;
        Ok(ArtifactPrepareReply { upload })
    }
    pub(super) async fn artifact_replace(
        &self,
        a: &ArtifactAuthority,
        c: &ArtifactReplaceCommand,
    ) -> StoreResult<ArtifactReplacementReceipt> {
        c.validate()?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let old = authorize_upload(self, &mut tx, a, &c.upload_id).await?;
        let selected = selection(&old)?;
        if selected.revision != c.expected_selection_revision {
            return Err(ContractError::Conflict.into());
        }
        if let Some(id) = old.try_get::<Option<String>, _>("replaced_by")? {
            let successor = upload(&mut tx, &id, false).await?;
            let replacement = selection(&successor)?;
            let current = status(&mut tx, &successor).await?;
            tx.commit().await?;
            return Ok(ArtifactReplacementReceipt {
                replaced: selected,
                replacement,
                current,
            });
        }
        let prior = status(&mut tx, &old).await?;
        if !prior.selected || prior.stage == ArtifactUploadStage::Ready {
            return Err(ContractError::Conflict.into());
        }
        let now = db::now(&mut tx).await?;
        let eligible = match prior.stage {
            ArtifactUploadStage::Prepared => now >= prior.prepare_expires_at,
            ArtifactUploadStage::Receiving => prior.ingress_expires_at.is_some_and(|t| now >= t),
            ArtifactUploadStage::Writing => prior.dispatch_expires_at.is_some_and(|t| now >= t),
            ArtifactUploadStage::Uncertain | ArtifactUploadStage::ExpiredNoWrite => true,
            _ => false,
        };
        if !eligible {
            return Err(ContractError::Busy.into());
        }
        let r = root(&old)?;
        quota(&mut tx, a.scope(), &r, now).await?;
        let obj = object(&old)?;
        let replacement = ArtifactSelection {
            artifact_id: selected.artifact_id.clone(),
            upload_id: db::id(&mut tx, "upl").await?,
            revision: selected.revision + 1,
        };
        insert_upload(
            &mut tx,
            a.scope(),
            &r,
            &replacement,
            &obj.metadata,
            &obj.store,
            now,
        )
        .await?;
        let affected=sqlx::query("UPDATE execution_artifacts SET selected_upload_id=$2,selection_revision=$3 WHERE artifact_id=$1 AND selected_upload_id=$4 AND selection_revision=$5")
            .bind(&selected.artifact_id).bind(&replacement.upload_id).bind(replacement.revision as i64).bind(&selected.upload_id).bind(selected.revision as i64).execute(&mut *tx).await?.rows_affected();
        if affected != 1 {
            return Err(ContractError::Conflict.into());
        }
        sqlx::query(
            "UPDATE artifact_uploads SET replaced_by=$2,cleanup_eligible=true WHERE upload_id=$1",
        )
        .bind(&selected.upload_id)
        .bind(&replacement.upload_id)
        .execute(&mut *tx)
        .await?;
        let row = upload(&mut tx, &replacement.upload_id, false).await?;
        let current = status(&mut tx, &row).await?;
        authorize(self, &mut tx, a, &r, AccessCapability::ArtifactWrite).await?;
        tx.commit().await?;
        Ok(ArtifactReplacementReceipt {
            replaced: selected,
            replacement,
            current,
        })
    }
    pub(super) async fn artifact_claim(
        &self,
        a: &ArtifactAuthority,
        s: &ArtifactSelection,
        d: Instant,
    ) -> StoreResult<ArtifactIngressClaim> {
        s.validate()?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let row = authorize_upload(self, &mut tx, a, &s.upload_id).await?;
        let prior = status(&mut tx, &row).await?;
        if prior.selection != *s || !prior.selected {
            return Err(ContractError::Conflict.into());
        }
        let now = db::now(&mut tx).await?;
        if prior.stage != ArtifactUploadStage::Prepared || now >= prior.prepare_expires_at {
            return Err(ContractError::Busy.into());
        }
        let duration = remaining_ms(d)?;
        let expires_at = now + duration;
        let token = db::id(&mut tx, "ing").await?;
        sqlx::query("UPDATE artifact_uploads SET stage='receiving',ingress_token=$2,ingress_expires_at_ms=$3 WHERE upload_id=$1 AND stage='prepared'").bind(&s.upload_id).bind(&token).bind(codec::ms(expires_at)?).execute(&mut *tx).await?;
        authorize(
            self,
            &mut tx,
            a,
            &root(&row)?,
            AccessCapability::ArtifactWrite,
        )
        .await?;
        let object = object(&row)?;
        tx.commit().await?;
        Ok(ArtifactIngressClaim {
            selection: s.clone(),
            token,
            expires_at,
            object,
        })
    }
    pub(super) async fn artifact_release(
        &self,
        c: &ArtifactIngressClaim,
    ) -> StoreResult<ArtifactUploadStatus> {
        c.selection.validate()?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        // Release owns only its no-write ingress token. It cannot resurrect a
        // retired root and it does not need a currently live worker lease.
        let row = upload(&mut tx, &c.selection.upload_id, true).await?;
        let prior = status(&mut tx, &row).await?;
        if prior.selection != c.selection
            || !prior.selected
            || prior.stage != ArtifactUploadStage::Receiving
            || row
                .try_get::<Option<String>, _>("ingress_token")?
                .as_deref()
                != Some(&c.token)
        {
            return Err(ContractError::Conflict.into());
        }
        let now = db::now(&mut tx).await?;
        sqlx::query("UPDATE artifact_uploads SET stage=CASE WHEN prepare_expires_at_ms<=$2 THEN 'expired_no_write' ELSE 'prepared' END,ingress_token=NULL,ingress_expires_at_ms=NULL WHERE upload_id=$1").bind(&c.selection.upload_id).bind(codec::ms(now)?).execute(&mut *tx).await?;
        let row = upload(&mut tx, &c.selection.upload_id, false).await?;
        let value = status(&mut tx, &row).await?;
        tx.commit().await?;
        Ok(value)
    }
    pub(super) async fn artifact_begin(
        &self,
        a: &ArtifactAuthority,
        c: &ArtifactIngressClaim,
        d: Instant,
    ) -> StoreResult<ArtifactBeginWrite> {
        c.selection.validate()?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let row = authorize_upload(self, &mut tx, a, &c.selection.upload_id).await?;
        let prior = status(&mut tx, &row).await?;
        if prior.selection != c.selection || !prior.selected {
            return Err(ContractError::Conflict.into());
        }
        // No replay can produce another dispatch capability, including the
        // caller retrying after a COMMIT acknowledgement was lost.
        if matches!(
            prior.stage,
            ArtifactUploadStage::Writing
                | ArtifactUploadStage::Uncertain
                | ArtifactUploadStage::Ready
        ) {
            tx.commit().await?;
            return Ok(ArtifactBeginWrite::ReconcileRequired(prior));
        }
        let now = db::now(&mut tx).await?;
        if prior.stage != ArtifactUploadStage::Receiving
            || row
                .try_get::<Option<String>, _>("ingress_token")?
                .as_deref()
                != Some(&c.token)
            || prior.ingress_expires_at.is_none_or(|t| now >= t)
        {
            return Err(ContractError::Conflict.into());
        }
        let dispatch_expires_at = now + remaining_ms(d)?;
        sqlx::query("UPDATE artifact_uploads SET stage='writing',quiescent=false,dispatch_expires_at_ms=$2,ingress_token=NULL,ingress_expires_at_ms=NULL WHERE upload_id=$1").bind(&c.selection.upload_id).bind(codec::ms(dispatch_expires_at)?).execute(&mut *tx).await?;
        authorize(
            self,
            &mut tx,
            a,
            &root(&row)?,
            AccessCapability::ArtifactWrite,
        )
        .await?;
        let permit = ArtifactWritePermit {
            selection: c.selection.clone(),
            object: object(&row)?,
            dispatch_expires_at,
        };
        tx.commit().await?;
        Ok(ArtifactBeginWrite::Dispatch(permit))
    }
    pub(super) async fn artifact_observe(
        &self,
        id: &str,
        o: ArtifactStorageObservation,
    ) -> StoreResult<()> {
        validate_text(id, 128)?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let row = upload(&mut tx, id, true).await?;
        let stage: String = row.try_get("stage")?;
        if row.try_get::<bool, _>("deleted")? {
            tx.commit().await?;
            return Ok(());
        }
        if !matches!(
            stage.as_str(),
            "writing" | "uncertain" | "ready" | "retired"
        ) {
            return Err(ContractError::Conflict.into());
        }
        match o {
            ArtifactStorageObservation::VerifiedComplete => {
                sqlx::query(
                    "UPDATE artifact_uploads SET verified=true,quiescent=true WHERE upload_id=$1",
                )
                .bind(id)
                .execute(&mut *tx)
                .await?;
            }
            ArtifactStorageObservation::ConfirmedNoWrite => {
                sqlx::query("UPDATE artifact_uploads SET quiescent=true,stage=CASE WHEN stage='ready' THEN stage ELSE 'uncertain' END WHERE upload_id=$1").bind(id).execute(&mut *tx).await?;
            }
            ArtifactStorageObservation::Uncertain => {
                sqlx::query("UPDATE artifact_uploads SET stage=CASE WHEN stage='ready' THEN stage ELSE 'uncertain' END WHERE upload_id=$1").bind(id).execute(&mut *tx).await?;
            }
            ArtifactStorageObservation::Corrupt => {
                // Completion/quiescence is a historical fact for safe deletion.
                // A later missing/corrupt read refutes acceptance evidence only;
                // concurrent acceptance must not use the superseded verification.
                sqlx::query("UPDATE artifact_uploads SET verified=false,stage=CASE WHEN stage='ready' THEN stage ELSE 'uncertain' END WHERE upload_id=$1")
                    .bind(id).execute(&mut *tx).await?;
            }
            ArtifactStorageObservation::Absent => {
                // Absence while a PUT is still in flight does not end that
                // dispatch or bring its replacement deadline forward.
                sqlx::query("UPDATE artifact_uploads SET verified=false WHERE upload_id=$1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
    pub(super) async fn artifact_accept(
        &self,
        a: &ArtifactAuthority,
        s: &ArtifactSelection,
    ) -> StoreResult<ArtifactManifest> {
        s.validate()?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let row = authorize_upload(self, &mut tx, a, &s.upload_id).await?;
        let prior = status(&mut tx, &row).await?;
        if prior.selection != *s || !prior.selected || row.try_get::<bool, _>("deleted")? {
            return Err(ContractError::Conflict.into());
        }
        if !row.try_get::<bool, _>("verified")? {
            return Err(ContractError::Busy.into());
        }
        let b = binding(&mut tx, &s.artifact_id).await?;
        if let Some(bytes) = b.try_get::<Option<Vec<u8>>, _>("manifest_bytes")? {
            let value = codec::decode(&bytes)?;
            tx.commit().await?;
            return Ok(value);
        }
        let obj = object(&row)?;
        let now = db::now(&mut tx).await?;
        let value = ArtifactManifest {
            reference: ArtifactRef {
                schema: "ledgence.artifact.v1".into(),
                id: s.artifact_id.clone(),
                digest: obj.metadata.digest,
                size: obj.metadata.size,
                media_type: obj.metadata.media_type,
            },
            scope: scope(&row)?,
            root: root(&row)?,
            producer: codec::decode(&b.try_get::<Vec<u8>, _>("producer_bytes")?)?,
            key: b.try_get("artifact_key")?,
            filename: obj.metadata.filename,
            created_at: b.try_get::<i64, _>("created_at_ms")? as u64,
            accepted_at: now,
            first_attempt_id: match a {
                ArtifactAuthority::Worker { lease, .. } => Some(lease.attempt_id.clone()),
                _ => None,
            },
        };
        value.validate()?;
        sqlx::query("UPDATE execution_artifacts SET manifest_bytes=$2 WHERE artifact_id=$1")
            .bind(&s.artifact_id)
            .bind(codec::encode(&value)?)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE artifact_uploads SET stage='ready' WHERE upload_id=$1")
            .bind(&s.upload_id)
            .execute(&mut *tx)
            .await?;
        authorize(
            self,
            &mut tx,
            a,
            &value.root,
            AccessCapability::ArtifactWrite,
        )
        .await?;
        tx.commit().await?;
        Ok(value)
    }
}
fn remaining_ms(d: Instant) -> StoreResult<u64> {
    let remaining = d
        .saturating_duration_since(Instant::now())
        .as_millis()
        .min(u128::from(ARTIFACT_TRANSFER_TIMEOUT_MS)) as u64;
    if remaining == 0 {
        Err(ContractError::Unavailable("artifact transfer deadline elapsed".into()).into())
    } else {
        Ok(remaining)
    }
}
