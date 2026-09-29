//! Explicit registered references. Reads never contact the external program store.
use crate::{codec, persistence as db, *};
use ledgence_orchestration_api::console::*;
use ledgence_worker_api::{ProgramDescriptor, ProgramManifest};
use sqlx::{Postgres, QueryBuilder, Row, postgres::PgRow};

impl ProgramCatalogStore for PostgresStore {
    fn register_program<'a>(
        &'a self,
        scope: &'a Scope,
        command: &'a RegisterProgram,
        descriptor: &'a ProgramDescriptor,
        manifest: &'a ProgramManifest,
    ) -> ContractFuture<'a, RegisterProgramReply> {
        Box::pin(self.run(move || async move {
            self.require_scope(scope)?;scope.validate()?;command.validate()?;descriptor.validate().map_err(ContractError::from)?;manifest.validate().map_err(ContractError::from)?;
            if descriptor.program!=command.program || manifest.program!=command.program{return Err(ContractError::InvalidInput("catalog reference mismatch".into()).into());}
            let mut connection=self.transaction_connection().await?;
            let mut tx=connection.begin_write().await?;
            let at=db::now(&mut tx).await?;
            let metadata=codec::encode(&command.metadata)?;
            sqlx::query("INSERT INTO console_programs(tenant_id,namespace,program_id,metadata_bytes,last_registered_at_ms) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(&command.program.id).bind(&metadata).bind(codec::ms(at)?).execute(&mut *tx).await?;
            // Serialize versions of one program; no external verification is done
            // while holding this short transaction or the program row lock.
            sqlx::query("SELECT program_id FROM console_programs WHERE tenant_id=$1 AND namespace=$2 AND program_id=$3 FOR UPDATE")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(&command.program.id).fetch_one(&mut *tx).await?;
            let existing=sqlx::query("SELECT descriptor_bytes,manifest_bytes,metadata_bytes,registered_at_ms FROM console_program_versions WHERE tenant_id=$1 AND namespace=$2 AND program_id=$3 AND version=$4")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(&command.program.id).bind(&command.program.version).fetch_optional(&mut *tx).await?;
            let (version,already_registered,metadata_updated)=if let Some(row)=existing {
                let mut version=version(&row)?;
                if version.descriptor!=ConsoleProgramDescriptor::from(descriptor.clone()) || version.manifest!=*manifest {return Err(ContractError::Conflict.into());}
                let changed=version.metadata!=command.metadata;
                if changed && !command.update_metadata{return Err(ContractError::Conflict.into());}
                if changed {
                    sqlx::query("UPDATE console_program_versions SET metadata_bytes=$5 WHERE tenant_id=$1 AND namespace=$2 AND program_id=$3 AND version=$4")
                        .bind(&scope.tenant_id).bind(&scope.namespace).bind(&command.program.id).bind(&command.program.version).bind(&metadata).execute(&mut *tx).await?;
                    sqlx::query("UPDATE console_programs SET metadata_bytes=$4 WHERE tenant_id=$1 AND namespace=$2 AND program_id=$3")
                        .bind(&scope.tenant_id).bind(&scope.namespace).bind(&command.program.id).bind(&metadata).execute(&mut *tx).await?;
                    version.metadata=command.metadata.clone();
                }
                (version,true,changed)
            }else {
                sqlx::query("INSERT INTO console_program_versions(tenant_id,namespace,program_id,version,descriptor_bytes,manifest_bytes,metadata_bytes,registered_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
                    .bind(&scope.tenant_id).bind(&scope.namespace).bind(&command.program.id).bind(&command.program.version)
                    .bind(codec::encode(descriptor)?).bind(codec::encode(manifest)?).bind(&metadata).bind(codec::ms(at)?).execute(&mut *tx).await?;
                sqlx::query("UPDATE console_programs SET registered_versions=registered_versions+1,last_registered_at_ms=$4 WHERE tenant_id=$1 AND namespace=$2 AND program_id=$3")
                    .bind(&scope.tenant_id).bind(&scope.namespace).bind(&command.program.id).bind(codec::ms(at)?).execute(&mut *tx).await?;
                (ConsoleProgramVersion{descriptor:descriptor.clone().into(),manifest:manifest.clone(),metadata:command.metadata.clone(),registered_at:at,provenance:ProgramRegistrationProvenance::ConfiguredProgramStore},false,false)
            };
            version.validate()?;
            tx.commit().await?;
            Ok(RegisterProgramReply{version,already_registered,metadata_updated})
        }))
    }
    fn query_programs<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a ProgramCatalogQuery,
    ) -> ContractFuture<'a, ProgramCatalogReply> {
        Box::pin(self.run(move || async move {
            self.require_scope(scope)?;
            let position=query.validate(scope)?;
            let binding=query.binding(scope)?;
            let mut connection=self.transaction_connection().await?;
            let mut tx=connection.begin_read().await?;
            let at=db::now(&mut tx).await?;
            let reply=match query {
                ProgramCatalogQuery::Catalog {kind,page} => {
                    let mut sql=QueryBuilder::<Postgres>::new("SELECT program_id,metadata_bytes,registered_versions::text AS registered_versions,last_registered_at_ms,has_task,has_workflow,has_unspecified FROM console_programs WHERE tenant_id=");
                    sql.push_bind(&scope.tenant_id).push(" AND namespace=").push_bind(&scope.namespace);
                    if let Some(kind)=kind { sql.push(match kind { ConsoleProgramKind::Task=>" AND has_task",ConsoleProgramKind::Workflow=>" AND has_workflow",ConsoleProgramKind::Unspecified=>" AND has_unspecified" }); }
                    if let Some(position)=&position {sql.push(" AND program_id>").push_bind(text_key(position,0)?);}
                    sql.push(" ORDER BY program_id ASC LIMIT ").push_bind(i64::from(page.limit)+1);
                    let rows=sql.build().fetch_all(&mut *tx).await?;
                    let items=rows.iter().take(page.limit as usize).map(|row|{
                        let mut kinds=Vec::new();
                        for (key,kind) in [("has_task",ConsoleProgramKind::Task),("has_workflow",ConsoleProgramKind::Workflow),("has_unspecified",ConsoleProgramKind::Unspecified)] { if row.try_get::<bool,_>(key)? { kinds.push(kind); } }
                        Ok(ConsoleProgramCatalogEntry {program:ConsoleProgramSummary {
                            program_id:row.try_get("program_id")?,metadata:codec::decode(&row.try_get::<Vec<u8>,_>("metadata_bytes")?)?,
                            registered_versions:ConsoleU64(codec::u64_text(&row.try_get::<String,_>("registered_versions")?)?),last_registered_at:time(row,"last_registered_at_ms")?,
                        },kinds})
                    }).collect::<StoreResult<Vec<_>>>()?;
                    ProgramCatalogReply::Catalog(make_page(items,rows.len()>page.limit as usize,page,&binding,at)?)
                },
                ProgramCatalogQuery::Programs(page)=>{
                    let mut sql=QueryBuilder::<Postgres>::new("SELECT program_id,metadata_bytes,registered_versions::text AS registered_versions,last_registered_at_ms FROM console_programs WHERE tenant_id=");
                    sql.push_bind(&scope.tenant_id).push(" AND namespace=").push_bind(&scope.namespace);
                    if let Some(position)=&position {sql.push(" AND program_id>").push_bind(text_key(position,0)?);}
                    sql.push(" ORDER BY program_id ASC LIMIT ").push_bind(i64::from(page.limit)+1);
                    let rows=sql.build().fetch_all(&mut *tx).await?;
                    let items=rows.iter().take(page.limit as usize).map(|row|Ok(ConsoleProgramSummary{
                        program_id:row.try_get("program_id")?,metadata:codec::decode(&row.try_get::<Vec<u8>,_>("metadata_bytes")?)?,
                        registered_versions:ConsoleU64(codec::u64_text(&row.try_get::<String,_>("registered_versions")?)?),last_registered_at:time(row,"last_registered_at_ms")?,
                    })).collect::<StoreResult<Vec<_>>>()?;
                    ProgramCatalogReply::Programs(make_page(items,rows.len()>page.limit as usize,page,&binding,at)?)
                },
                ProgramCatalogQuery::Versions{program_id,page}=>{
                    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM console_programs WHERE tenant_id=$1 AND namespace=$2 AND program_id=$3)")
                        .bind(&scope.tenant_id).bind(&scope.namespace).bind(program_id).fetch_one(&mut *tx).await?;
                    if !exists{return Err(ContractError::NotFound.into());}
                    let mut sql=QueryBuilder::<Postgres>::new("SELECT descriptor_bytes,manifest_bytes,metadata_bytes,registered_at_ms FROM console_program_versions WHERE tenant_id=");
                    sql.push_bind(&scope.tenant_id).push(" AND namespace=").push_bind(&scope.namespace).push(" AND program_id=").push_bind(program_id);
                    if let Some(position)=&position {
                        sql.push(" AND (registered_at_ms,version)<(").push_bind(codec::ms(number_key(position,0)?)?).push(",").push_bind(text_key(position,1)?).push(")");
                    }
                    sql.push(" ORDER BY registered_at_ms DESC,version DESC LIMIT ").push_bind(i64::from(page.limit)+1);
                    let rows=sql.build().fetch_all(&mut *tx).await?;
                    let items=rows.iter().take(page.limit as usize).map(version).collect::<StoreResult<Vec<_>>>()?;
                    ProgramCatalogReply::Versions(make_page(items,rows.len()>page.limit as usize,page,&binding,at)?)
                },
                ProgramCatalogQuery::Inspect(program)=>{
                    let row=sqlx::query("SELECT descriptor_bytes,manifest_bytes,metadata_bytes,registered_at_ms FROM console_program_versions WHERE tenant_id=$1 AND namespace=$2 AND program_id=$3 AND version=$4")
                        .bind(&scope.tenant_id).bind(&scope.namespace).bind(&program.id).bind(&program.version).fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
                    ProgramCatalogReply::Inspect(Box::new(ConsoleProgramDetail{version:version(&row)?,observed_at:at}))
                },
            };
            reply.validate(scope,query)?;
            tx.commit().await?;
            Ok(reply)
        }))
    }
}
fn version(row: &PgRow) -> StoreResult<ConsoleProgramVersion> {
    let descriptor: ProgramDescriptor =
        codec::decode(&row.try_get::<Vec<u8>, _>("descriptor_bytes")?)?;
    let value = ConsoleProgramVersion {
        descriptor: descriptor.into(),
        manifest: codec::decode(&row.try_get::<Vec<u8>, _>("manifest_bytes")?)?,
        metadata: codec::decode(&row.try_get::<Vec<u8>, _>("metadata_bytes")?)?,
        registered_at: time(row, "registered_at_ms")?,
        provenance: ProgramRegistrationProvenance::ConfiguredProgramStore,
    };
    value.validate()?;
    Ok(value)
}
fn time(row: &PgRow, column: &str) -> StoreResult<u64> {
    let value: i64 = row.try_get(column)?;
    let at = u64::try_from(value)
        .map_err(|_| ContractError::Unavailable("invalid catalog timestamp".into()))?;
    if at > CONSOLE_MAX_TIMESTAMP {
        return Err(ContractError::Unavailable("invalid catalog timestamp".into()).into());
    }
    Ok(at)
}
fn text_key(position: &ConsolePosition, index: usize) -> Result<&str> {
    match position.get(index) {
        Some(ConsoleKey::Text(value)) => Ok(value),
        _ => Err(ContractError::InvalidInput("invalid catalog cursor".into())),
    }
}
fn number_key(position: &ConsolePosition, index: usize) -> Result<u64> {
    match position.get(index) {
        Some(ConsoleKey::Number(value)) => Ok(value.0),
        _ => Err(ContractError::InvalidInput("invalid catalog cursor".into())),
    }
}
fn make_page<T: ConsoleRecord>(
    mut items: Vec<T>,
    more: bool,
    page: &ConsolePagination,
    binding: &ConsoleCursorBinding,
    at: Timestamp,
) -> Result<ConsolePage<T>> {
    if more && items.is_empty() {
        return Err(ContractError::Unavailable(
            "empty catalog continuation".into(),
        ));
    }
    let encoding_error = || ContractError::Unavailable("could not encode catalog page".into());
    let mut retained = 0;
    let mut item_bytes = 0;
    let mut next_cursor = None;
    for (index, item) in items.iter().enumerate() {
        let cursor = if more || index + 1 < items.len() {
            Some(page.next_cursor(binding, &item.position())?)
        } else {
            None
        };
        // Count each item once and include the exact envelope/cursor for this
        // prefix. A shortened page must resume after its last retained item.
        let candidate_bytes = item_bytes
            + serde_json::to_vec(item)
                .map_err(|_| encoding_error())?
                .len()
            + usize::from(index > 0);
        let envelope_bytes = serde_json::to_vec(&ConsolePage::<()> {
            items: Vec::new(),
            next_cursor: cursor.clone(),
            observed_at: at,
        })
        .map_err(|_| encoding_error())?
        .len();
        if candidate_bytes + envelope_bytes > CONSOLE_METADATA_MAX_BYTES {
            if retained == 0 {
                return Err(ContractError::Unavailable(
                    "catalog item exceeds page byte limit".into(),
                ));
            }
            break;
        }
        retained = index + 1;
        item_bytes = candidate_bytes;
        next_cursor = cursor;
    }
    items.truncate(retained);
    let reply = ConsolePage {
        items,
        next_cursor,
        observed_at: at,
    };
    reply.validate(page, binding)?;
    Ok(reply)
}
