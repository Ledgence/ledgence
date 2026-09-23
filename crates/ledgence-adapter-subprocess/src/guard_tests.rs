use super::*;

#[tokio::test]
async fn guard_rejects_ready_result_crossing_invocation_deadline() {
    let control = RunControl::new(Duration::from_millis(100));
    let (mut reply, _receiver) = oneshot::channel();
    let mut polled = false;
    let result = guarded(
        async {
            polled = true;
            // One synchronous poll crosses the deadline; the timer cannot win
            // a select branch until this poll has already returned Ready.
            while Instant::now() < control.deadline() {
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        },
        &control,
        &mut reply,
        None,
    )
    .await;
    assert!(
        polled,
        "the operation must cross its deadline during polling"
    );
    assert_eq!(result.unwrap_err().kind, ErrorKind::TimedOut);
}

#[tokio::test]
async fn guard_rejects_ready_result_crossing_startup_deadline() {
    let control = RunControl::new(Duration::from_secs(5));
    let (mut reply, _receiver) = oneshot::channel();
    let mut polled = false;
    let result = guarded(
        async {
            polled = true;
            std::thread::sleep(Duration::from_millis(150));
            Ok(())
        },
        &control,
        &mut reply,
        Some(Duration::from_millis(100)),
    )
    .await;
    assert!(
        polled,
        "the operation must cross its deadline during polling"
    );
    assert!(
        control.check().is_ok(),
        "only the startup budget should expire"
    );
    let error = result.unwrap_err();
    assert_eq!(error.kind, ErrorKind::TimedOut);
    assert_eq!(error.message, "subprocess startup deadline expired");
}

#[tokio::test]
async fn guard_rejects_ready_result_cancelled_during_poll() {
    let control = RunControl::new(Duration::from_secs(5));
    let (mut reply, _receiver) = oneshot::channel();
    let result = guarded(
        async {
            control.cancel();
            Ok(())
        },
        &control,
        &mut reply,
        None,
    )
    .await;
    assert_eq!(result.unwrap_err().kind, ErrorKind::Cancelled);
}

#[tokio::test]
async fn guard_rejects_ready_result_when_caller_drops_during_poll() {
    let control = RunControl::new(Duration::from_secs(5));
    let (mut reply, receiver) = oneshot::channel();
    let result = guarded(
        async {
            drop(receiver);
            Ok(())
        },
        &control,
        &mut reply,
        None,
    )
    .await;
    assert_eq!(result.unwrap_err().kind, ErrorKind::Cancelled);
    assert!(control.check().is_ok());
}

#[tokio::test]
async fn guard_preserves_in_time_outcomes() {
    let control = RunControl::new(Duration::from_secs(5));
    let failure = Error::new(ErrorKind::Protocol, "invalid frame");
    for expected in [Ok(42), Err(failure)] {
        let (mut reply, _receiver) = oneshot::channel();
        let result = guarded(
            async { expected.clone() },
            &control,
            &mut reply,
            Some(Duration::from_secs(1)),
        )
        .await;
        assert_eq!(result, expected);
    }
}
