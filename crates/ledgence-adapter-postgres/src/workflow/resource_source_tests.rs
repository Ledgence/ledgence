use super::*;

fn reader(cap: AccessCapability, expiry: u64) -> VerifiedPrincipal {
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience().issuer(),
            subject: "investigator".into(),
        },
        audience(),
        "credential".into(),
        PrincipalClass::Operator,
        expiry,
        vec![cap],
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn resource_source_pages_bound_both_states_and_tables_under_skew() {
    let db = fixture(policy(2)).await;
    let s = &db.store;
    put(s, "other", policy(2), None, "other").await;
    // This query-plan fixture exercises only the scalar projection. Its unreadable
    // snapshot bytes also detect an accidental dependency on retained user payloads.
    sqlx::raw_sql("INSERT INTO admission_requests (tenant_id,namespace,resource_name,workflow_id,origin_activation_id,logical_key,state,revision,created_at_ms,updated_at_ms,deadline_at_ms,snapshot_bytes)
      SELECT 'acme','billing',CASE WHEN i>10000 THEN 'other' ELSE 'model' END,'w'||i,'a','k',
        CASE WHEN i>10000 OR i<=50 THEN 'possibly_dispatched' WHEN i<=53 THEN 'unknown_occupied' ELSE 'finished' END,1,0,0,0,'x'
      FROM generate_series(1,22000) i;
      INSERT INTO task_admission_requests (tenant_id,namespace,resource_name,task_id,generation,state,revision,created_at_ms,updated_at_ms,available_at_ms,snapshot_bytes)
      SELECT 'acme','billing',CASE WHEN i>10000 THEN 'other' ELSE 'model' END,'t'||i,1,
        CASE WHEN i>10000 OR i<=50 THEN 'possibly_dispatched' WHEN i<=53 THEN 'unknown_occupied' ELSE 'finished' END,1,0,0,0,'x'
      FROM generate_series(1,22000) i;
      ANALYZE admission_requests; ANALYZE task_admission_requests; ANALYZE provider_admission_links;")
        .execute(&s.pool).await.unwrap();
    let mut q = ResourceSourceQuery {
        scope: scope(),
        name: "model".into(),
        state: Some(ResourceSourceState::UnknownOccupied),
        cursor: None,
        limit: 2,
    };
    let sensitive = reader(AccessCapability::SensitiveRead, 253402300799999);
    assert!(matches!(
        s.list_resource_sources(&reader(AccessCapability::MetadataRead, 253402300799999), &q)
            .await,
        Err(ContractError::Forbidden)
    ));
    assert!(matches!(
        s.list_resource_sources(&reader(AccessCapability::SensitiveRead, 1), &q)
            .await,
        Err(ContractError::Unauthenticated)
    ));
    assert!(s.list_resource_sources(&worker(), &q).await.is_err());
    let mut found = Vec::new();
    loop {
        let page = s.list_resource_sources(&sensitive, &q).await.unwrap();
        page.validate_for(&q).unwrap();
        found.extend(page.items);
        q.cursor = page.next_cursor;
        if q.cursor.is_none() {
            break;
        }
        assert!(found.len() < 6);
        let mut foreign = q.clone();
        foreign.name = "other".into();
        assert!(s.list_resource_sources(&sensitive, &foreign).await.is_err());
        foreign = q.clone();
        foreign.state = None;
        assert!(s.list_resource_sources(&sensitive, &foreign).await.is_err());
    }
    assert_eq!(found.len(), 6);
    assert!(found.windows(2).all(|w| w[0].sequence > w[1].sequence));
    assert!(matches!(
        found[0].subject,
        ResourceSourceSubject::Task { .. }
    ));
    assert!(matches!(
        found[5].subject,
        ResourceSourceSubject::Workflow { .. }
    ));
    assert!(
        found
            .iter()
            .all(|i| i.state == ResourceSourceState::UnknownOccupied)
    );
    let mut tx = s.pool.begin().await.unwrap();
    let sql = include_str!("../../queries/admission_open_sources.sql");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "PREPARE retained(text,text,text,text[],bigint,bigint) AS {sql}"
    )))
    .execute(&mut *tx)
    .await
    .unwrap();
    for mode in ["force_custom_plan", "force_generic_plan"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET LOCAL plan_cache_mode={mode}"
        )))
        .execute(&mut *tx)
        .await
        .unwrap();
        for (states, through) in [
            ("ARRAY['unknown_occupied']", "9223372036854775807"),
            ("ARRAY['possibly_dispatched','unknown_occupied']", "25"),
        ] {
            let plan:serde_json::Value=sqlx::query_scalar(sqlx::AssertSqlSafe(format!("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE retained('acme','billing','model',{states},{through},3)")))
                .fetch_one(&mut *tx).await.unwrap();
            for index in [
                "task_admissions_open_sources",
                "admission_requests_open_sources",
            ] {
                assert!(plan.to_string().contains(index), "{plan}");
            }
            let root = &plan[0]["Plan"];
            let buffers = root["Shared Hit Blocks"].as_u64().unwrap()
                + root["Shared Read Blocks"].as_u64().unwrap();
            assert!(buffers < 150, "{plan}");
            eprintln!("{mode} {states} <= {through}: {buffers} buffers across 44,000 rows");
        }
    }
    tx.rollback().await.unwrap();
    db.finish().await;
}
