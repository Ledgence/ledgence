use super::*;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    scope: Scope,
    target: ExecutionTarget,
    after: String,
}
impl PostgresStore {
    pub(super) async fn artifact_status(
        &self,
        a: &ArtifactAuthority,
        id: &str,
    ) -> StoreResult<ArtifactUploadStatus> {
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let row = authorize_upload(self, &mut tx, a, id).await?;
        let value = status(&mut tx, &row).await?;
        tx.commit().await?;
        Ok(value)
    }
    pub(super) async fn artifact_resolve(
        &self,
        a: &ArtifactAuthority,
        id: &str,
        kind: ArtifactReadKind,
    ) -> StoreResult<ArtifactRecord> {
        validate_text(id, 128)?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let initial=sqlx::query("SELECT root_kind,root_id,tenant_id,namespace FROM execution_artifacts WHERE artifact_id=$1").bind(id).fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
        if scope(&initial)? != *a.scope() {
            return Err(ContractError::NotFound.into());
        }
        let capability = match kind {
            ArtifactReadKind::Metadata => AccessCapability::MetadataRead,
            ArtifactReadKind::Content => AccessCapability::SensitiveRead,
        };
        authorize(self, &mut tx, a, &root(&initial)?, capability).await?;
        let b = binding(&mut tx, id).await?;
        let manifest: ArtifactManifest = codec::decode(
            &b.try_get::<Option<Vec<u8>>, _>("manifest_bytes")?
                .ok_or(ContractError::NotFound)?,
        )?;
        manifest.validate().map_err(|_| corrupt())?;
        let u = upload(
            &mut tx,
            &b.try_get::<String, _>("selected_upload_id")?,
            false,
        )
        .await?;
        if u.try_get::<bool, _>("deleted")? {
            return Err(ContractError::NotFound.into());
        }
        let object = object(&u)?;
        authorize(self, &mut tx, a, &manifest.root, capability).await?;
        tx.commit().await?;
        Ok(ArtifactRecord { manifest, object })
    }
    pub(super) async fn artifact_list(
        &self,
        a: &ArtifactAuthority,
        q: &ArtifactListQuery,
    ) -> StoreResult<ArtifactPage> {
        q.validate()?;
        if &q.scope != a.scope() {
            return Err(ContractError::NotFound.into());
        }
        let after = if let Some(cursor) = &q.cursor {
            let c: Cursor =
                decode_unique_json(cursor.as_bytes(), 4096).map_err(ContractError::from)?;
            if c.scope != q.scope || c.target != q.target {
                return Err(ContractError::InvalidInput(
                    "artifact cursor does not match scope/target".into(),
                )
                .into());
            }
            validate_text(&c.after, 128)?;
            c.after
        } else {
            String::new()
        };
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let r = canonical_root(&mut tx, &q.scope, &q.target).await?;
        authorize(self, &mut tx, a, &r, AccessCapability::MetadataRead).await?;
        let rows:Vec<(String,Vec<u8>)>=sqlx::query_as("SELECT artifact_id,manifest_bytes FROM execution_artifacts WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND artifact_id>$5 AND manifest_bytes IS NOT NULL ORDER BY artifact_id LIMIT $6")
            .bind(&q.scope.tenant_id).bind(&q.scope.namespace).bind(r.kind()).bind(r.id()).bind(&after).bind(i64::from(q.limit)+1).fetch_all(&mut *tx).await?;
        let more = rows.len() > q.limit as usize;
        let items = rows
            .iter()
            .take(q.limit as usize)
            .map(|(_, bytes)| {
                let manifest: ArtifactManifest = codec::decode(bytes)?;
                manifest.validate().map_err(|_| corrupt())?;
                Ok(manifest)
            })
            .collect::<Result<Vec<_>>>()?;
        let next_cursor = if more {
            Some(
                serde_json::to_string(&Cursor {
                    scope: q.scope.clone(),
                    target: q.target.clone(),
                    after: rows[q.limit as usize - 1].0.clone(),
                })
                .map_err(|_| corrupt())?,
            )
        } else {
            None
        };
        let page = ArtifactPage { items, next_cursor };
        if codec::encode(&page)?.len() > ARTIFACT_PAGE_MAX_BYTES {
            return Err(corrupt().into());
        }
        authorize(self, &mut tx, a, &r, AccessCapability::MetadataRead).await?;
        tx.commit().await?;
        Ok(page)
    }
}
