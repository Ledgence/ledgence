use super::*;
use std::collections::VecDeque;

fn query() -> ExecutionProgressQuery {
    ExecutionProgressQuery {
        scope: Scope {
            tenant_id: "acme".into(),
            namespace: "billing".into(),
        },
        target: ExecutionProgressTarget::Task { id: "task".into() },
        cursor: None,
        limit: 1,
        wait_ms: 0,
    }
}
fn options() -> Options {
    Options {
        server: "http://localhost".into(),
        query: ExecutionProgressQuery {
            wait_ms: EXECUTION_PROGRESS_MAX_WAIT_MS,
            ..query()
        },
        follow: true,
        keep_following: false,
        poll_interval: Duration::from_millis(250),
        timeout: None,
    }
}
fn page(after: u64, sequence: Option<u64>, tail: u64, state: &str) -> ExecutionProgressPage {
    let query = query();
    ExecutionProgressPage {
        scope: query.scope.clone(),
        target: query.target.clone(),
        state: state.into(),
        terminal: matches!(state, "succeeded" | "failed" | "cancelled"),
        high_watermark: ProgressSequence(tail),
        events: sequence
            .into_iter()
            .map(|sequence| ExecutionProgressEvent {
                sequence: ProgressSequence(sequence),
                at: 1,
                reason: "cleanup_confirmed".into(),
                activation_id: None,
                attempt_id: Some("attempt".into()),
            })
            .collect(),
        next_cursor: query.cursor_for(sequence.unwrap_or(after)).unwrap(),
        has_more: sequence.unwrap_or(after) < tail,
    }
}
struct Reader {
    pages: Mutex<VecDeque<Result<ExecutionProgressPage>>>,
    queries: Mutex<Vec<ExecutionProgressQuery>>,
    delays: Mutex<VecDeque<Duration>>,
}
impl Reader {
    fn new(pages: Vec<Result<ExecutionProgressPage>>) -> Self {
        Self {
            pages: Mutex::new(pages.into()),
            queries: Mutex::new(Vec::new()),
            delays: Mutex::new(VecDeque::new()),
        }
    }
    fn with_delays(self, delays: Vec<Duration>) -> Self {
        *self.delays.lock().unwrap() = delays.into();
        self
    }
}
impl ExecutionProgressReader for Reader {
    fn execution_progress<'a>(
        &'a self,
        query: &'a ExecutionProgressQuery,
    ) -> ContractFuture<'a, ExecutionProgressPage> {
        Box::pin(async move {
            self.queries.lock().unwrap().push(query.clone());
            let delay = self.delays.lock().unwrap().pop_front().unwrap_or_default();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            self.pages
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected additional read")
        })
    }
}
fn arguments(kind: &str) -> Vec<String> {
    vec![
        kind.into(),
        "progress".into(),
        "--server".into(),
        "http://localhost".into(),
        "--tenant".into(),
        "acme".into(),
        "--namespace".into(),
        "billing".into(),
        format!("--{kind}"),
        kind.into(),
    ]
}
#[test]
fn parser_preserves_cursor_and_rejects_ambiguous_or_unbounded_options() {
    for kind in ["task", "workflow"] {
        let parsed = parse(arguments(kind)).unwrap();
        assert_eq!(parsed.query.target.kind(), kind);
        assert!(!parsed.follow);
        assert_eq!(parsed.query.limit, 100);
        assert_eq!(parsed.query.wait_ms, 0);
        let mut args = arguments(kind);
        args.push("--follow".into());
        assert_eq!(
            parse(args).unwrap().query.wait_ms,
            EXECUTION_PROGRESS_MAX_WAIT_MS
        );
        let mut args = arguments(kind);
        args.extend(["--follow".into(), "--wait-ms".into(), "0".into()]);
        assert_eq!(parse(args).unwrap().query.wait_ms, 0);
    }
    let mut args = arguments("task");
    let cursor = query().cursor_for(u64::MAX).unwrap();
    args.extend([
        "--cursor".into(),
        cursor.clone(),
        "--follow".into(),
        "--keep-following".into(),
        "--timeout-ms".into(),
        "500".into(),
    ]);
    let parsed = parse(args).unwrap();
    assert_eq!(parsed.query.cursor.as_deref(), Some(cursor.as_str()));
    assert_eq!(parsed.query.validate().unwrap(), u64::MAX);
    assert!(parsed.follow && parsed.keep_following);
    for suffix in [
        vec!["--follow", "--follow"],
        vec!["--keep-following"],
        vec!["--follow", "--keep-following", "--keep-following"],
        vec!["--poll-interval-ms", "1000"],
        vec!["--follow", "--poll-interval-ms", "249"],
        vec!["--limit", "0"],
        vec!["--limit", "101"],
        vec!["--timeout-ms", "0"],
        vec!["--timeout-ms", "-1"],
        vec!["--cursor", "invalid"],
        vec!["--workflow", "wf"],
        vec!["--follow", "true"],
        vec!["--limit", "1", "--limit", "2"],
        vec!["--follow", "--timeout-ms"],
        vec!["--limit", "+1"],
        vec!["--wait-ms", "20001"],
        vec!["--wait-ms", "-1"],
        vec!["--wait-ms", "1.5"],
        vec!["--wait-ms", "1", "--wait-ms", "2"],
    ] {
        let mut args = arguments("task");
        args.extend(suffix.iter().map(|s| s.to_string()));
        assert!(parse(args).is_err(), "{suffix:?}");
    }
}
#[tokio::test]
async fn terminal_backlog_is_drained_before_follow_ends_without_poll_delay() {
    let reader = Reader::new(vec![
        Ok(page(0, Some(1), 3, "failed")),
        Ok(page(1, Some(2), 3, "failed")),
        Ok(page(2, Some(3), 3, "failed")),
    ]);
    let mut options = options();
    options.poll_interval = Duration::from_secs(10);
    let mut output = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(1),
        observe(&reader, options, |bytes| {
            output.push(serde_json::from_slice::<serde_json::Value>(&bytes).unwrap());
            std::future::ready(Ok(()))
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(output.len(), 3);
    let queries = reader.queries.lock().unwrap();
    assert_eq!(
        queries
            .iter()
            .map(|q| q.validate().unwrap())
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert!(queries.iter().all(|q| q.wait_ms == 0));
    assert_eq!(output[2]["high_watermark"], "3");
    assert_eq!(output[2]["has_more"], false);
}
#[tokio::test]
async fn timeout_and_backpressure_never_start_another_read() {
    let reader = Reader::new(vec![Ok(page(0, None, 0, "active"))]);
    let mut options = options();
    options.timeout = Some(Duration::from_millis(30));
    let result = observe(&reader, options, |_| std::future::pending::<Result<()>>()).await;
    assert!(
        matches!(result,Err(ContractError::Unavailable(message)) if message.contains("timed out"))
    );
    assert_eq!(reader.queries.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn output_failure_and_backend_failure_do_not_advance_or_retry() {
    let reader = Reader::new(vec![Ok(page(0, Some(1), 2, "active"))]);
    assert!(
        observe(&reader, options(), |_| std::future::ready(Err(
            unavailable("broken pipe")
        )))
        .await
        .is_err()
    );
    assert_eq!(reader.queries.lock().unwrap().len(), 1);
    let reader = Reader::new(vec![Err(ContractError::Forbidden)]);
    let result = observe(&reader, options(), |_| std::future::ready(Ok(()))).await;
    assert_eq!(result, Err(ContractError::Forbidden));
    assert_eq!(reader.queries.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn keep_following_can_observe_cleanup_after_terminal_and_suppresses_idle_pages() {
    let reader = Reader::new(vec![
        Ok(page(0, Some(1), 1, "failed")),
        Ok(page(1, None, 1, "failed")),
        Ok(page(1, Some(2), 2, "failed")),
        Err(ContractError::NotFound),
    ]);
    let mut options = options();
    options.keep_following = true;
    options.poll_interval = Duration::from_millis(1);
    let mut output = Vec::new();
    let result = observe(&reader, options, |bytes| {
        output.push(bytes);
        std::future::ready(Ok(()))
    })
    .await;
    assert_eq!(result, Err(ContractError::NotFound));
    assert_eq!(output.len(), 2);
    assert_eq!(
        reader
            .queries
            .lock()
            .unwrap()
            .iter()
            .map(|q| q.validate().unwrap())
            .collect::<Vec<_>>(),
        [0, 1, 1, 2]
    );
}
#[tokio::test]
async fn full_width_cursor_resume_and_bad_response_are_checked() {
    let after = 9_007_199_254_740_993;
    let reader = Reader::new(vec![Ok(page(
        after,
        Some(after + 1),
        after + 1,
        "succeeded",
    ))]);
    let mut options = options();
    options.query.cursor = Some(query().cursor_for(after).unwrap());
    observe(&reader, options, |bytes| {
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["events"][0]["sequence"], (after + 1).to_string());
        std::future::ready(Ok(()))
    })
    .await
    .unwrap();
    let mut wrong = page(0, Some(1), 1, "succeeded");
    wrong.scope.tenant_id = "other".into();
    let reader = Reader::new(vec![Ok(wrong)]);
    let mut wrote = false;
    assert!(
        observe(&reader, super::tests::options(), |_| {
            wrote = true;
            std::future::ready(Ok(()))
        })
        .await
        .is_err()
    );
    assert!(!wrote);
}

#[tokio::test]
async fn follow_rejects_watermark_and_terminal_regressions_before_output() {
    for (initial, changed) in [
        (page(0, Some(1), 3, "active"), page(1, Some(2), 2, "active")),
        (page(0, Some(1), 3, "failed"), page(1, Some(2), 3, "active")),
        (
            page(0, Some(1), 3, "failed"),
            page(1, Some(2), 3, "succeeded"),
        ),
    ] {
        let reader = Reader::new(vec![Ok(initial), Ok(changed)]);
        let mut output = Vec::new();
        assert!(
            observe(&reader, options(), |bytes| {
                output.push(bytes);
                std::future::ready(Ok(()))
            })
            .await
            .is_err()
        );
        assert_eq!(output.len(), 1);
        assert_eq!(reader.queries.lock().unwrap().len(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn caught_up_waits_share_budget_without_adding_poll_delay_after_a_long_read() {
    let reader = Reader::new(vec![
        Ok(page(0, None, 0, "active")),
        Ok(page(0, None, 0, "active")),
        Ok(page(0, Some(1), 1, "succeeded")),
    ])
    .with_delays(vec![Duration::ZERO, Duration::from_secs(20)]);
    let started = Instant::now();
    let mut output = Vec::new();
    observe(&reader, options(), |bytes| {
        output.push(bytes);
        std::future::ready(Ok(()))
    })
    .await
    .unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(started.elapsed(), Duration::from_millis(20_250));
    assert_eq!(
        reader
            .queries
            .lock()
            .unwrap()
            .iter()
            .map(|q| q.wait_ms)
            .collect::<Vec<_>>(),
        [0, 20_000, 20_000]
    );
}

#[tokio::test(start_paused = true)]
async fn remaining_observation_budget_bounds_wait_and_preserves_reply_allowance() {
    let reader = Reader::new(vec![
        Ok(page(0, None, 0, "active")),
        Ok(page(0, Some(1), 1, "succeeded")),
    ]);
    let mut settings = options();
    settings.timeout = Some(Duration::from_secs(2));
    observe(&reader, settings, |_| std::future::ready(Ok(())))
        .await
        .unwrap();
    {
        let queries = reader.queries.lock().unwrap();
        assert_eq!(queries[0].wait_ms, 0);
        assert_eq!(queries[1].wait_ms, 750);
        assert_eq!(queries[1].cursor, Some(queries[0].cursor_for(0).unwrap()));
    }

    let reader = Reader::new(vec![Ok(page(0, Some(1), 1, "succeeded"))]);
    let mut settings = options();
    settings.follow = false;
    settings.timeout = Some(Duration::from_millis(500));
    observe(&reader, settings, |_| std::future::ready(Ok(())))
        .await
        .unwrap();
    assert_eq!(reader.queries.lock().unwrap()[0].wait_ms, 0);
}

#[tokio::test(start_paused = true)]
async fn immediate_terminal_wait_replies_remain_paced_while_following_late_events() {
    let reader = Reader::new(vec![
        Ok(page(0, Some(1), 1, "failed")),
        Ok(page(1, None, 1, "failed")),
        Ok(page(1, Some(2), 2, "failed")),
        Err(ContractError::NotFound),
    ]);
    let mut settings = options();
    settings.keep_following = true;
    let started = Instant::now();
    let mut output = Vec::new();
    assert_eq!(
        observe(&reader, settings, |bytes| {
            output.push(bytes);
            std::future::ready(Ok(()))
        })
        .await,
        Err(ContractError::NotFound)
    );
    assert_eq!(output.len(), 2);
    assert_eq!(started.elapsed(), Duration::from_millis(750));
    assert_eq!(reader.queries.lock().unwrap().len(), 4);
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_server_wait_stops_observation_without_starting_another_read() {
    let reader = Arc::new(
        Reader::new(vec![
            Ok(page(0, None, 0, "active")),
            Ok(page(0, Some(1), 1, "succeeded")),
        ])
        .with_delays(vec![Duration::ZERO, Duration::from_secs(20)]),
    );
    let observed = reader.clone();
    let task = tokio::spawn(async move {
        observe(observed.as_ref(), options(), |_| std::future::ready(Ok(()))).await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(250)).await;
    tokio::task::yield_now().await;
    assert_eq!(reader.queries.lock().unwrap().len(), 2);
    assert_eq!(reader.queries.lock().unwrap()[1].wait_ms, 20_000);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(reader.queries.lock().unwrap().len(), 2);
}
