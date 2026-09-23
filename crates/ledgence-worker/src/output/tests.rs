#![cfg(unix)]

use super::*;
use nix::{
    fcntl::{FcntlArg, OFlag, fcntl},
    unistd::pipe,
};
use std::{
    io::{Read, Seek},
    os::fd::{AsFd, BorrowedFd},
};
use tracing_subscriber::fmt::MakeWriter;

fn local_outputs(stdout: BorrowedFd<'_>, stderr: BorrowedFd<'_>) -> Outputs {
    let (stdout, restore_stdout) = Destination::new(stdout).unwrap();
    let (stderr, restore_stderr) = Destination::new(stderr).unwrap();
    let (stdout, first) = start("stdout", stdout).unwrap();
    let (stderr, second) = start("stderr", stderr).unwrap();
    Outputs {
        stdout,
        stderr,
        writers: vec![first, second],
        _restore: vec![restore_stderr, restore_stdout],
    }
}

fn status_flags(fd: BorrowedFd<'_>) -> OFlag {
    OFlag::from_bits_retain(fcntl(fd, FcntlArg::F_GETFL).unwrap())
}

#[tokio::test]
async fn shared_descriptor_flags_survive_one_writer_finishing_then_restore_in_reverse_order() {
    // This is an owned test pipe, never the test process's stdout or stderr.
    let (_reader, writer) = pipe().unwrap();
    let original = status_flags(writer.as_fd());
    assert!(!original.contains(OFlag::O_NONBLOCK));
    let mut outputs = local_outputs(writer.as_fd(), writer.as_fd());
    assert!(status_flags(writer.as_fd()).contains(OFlag::O_NONBLOCK));

    outputs.stdout.line("first stream").await.unwrap();
    outputs.writers[0]
        .state
        .closing
        .store(true, Ordering::Release);
    outputs.writers[0]
        .thread
        .as_ref()
        .unwrap()
        .thread()
        .unpark();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !outputs.writers[0].thread.as_ref().unwrap().is_finished() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("closing one output must finish its idle writer");

    assert!(
        !outputs.writers[1].thread.as_ref().unwrap().is_finished(),
        "the other stream remains owned and open"
    );
    assert!(
        status_flags(writer.as_fd()).contains(OFlag::O_NONBLOCK),
        "finishing one stream must not make its shared peer blocking"
    );
    outputs.stderr.line("second stream").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), outputs.finish())
        .await
        .expect("both writers must finish")
        .unwrap();
    assert!(outputs.writers.iter().all(|writer| writer.thread.is_none()));
    drop(outputs);

    assert_eq!(
        status_flags(writer.as_fd()).contains(OFlag::O_NONBLOCK),
        original.contains(OFlag::O_NONBLOCK),
        "reverse restoration must preserve the original mode, not the second acquisition's temporary O_NONBLOCK"
    );
}

#[test]
fn log_queue_drops_whole_records_at_capacity_and_recovers_when_drained() {
    let capacity = 3;
    let (sender, mut receiver) = mpsc::channel(capacity);
    let state = Arc::new(State {
        name: "stderr",
        closing: AtomicBool::new(false),
        abort: AtomicBool::new(false),
        failure: Mutex::new(None),
        lost_logs: AtomicUsize::new(0),
    });
    let sink = Sink {
        sender,
        state: state.clone(),
        // No background consumer: capacity and overflow are deterministic.
        wake: thread::current(),
    };
    for sequence in 0..5 {
        let mut record = sink.make_writer();
        record.write_all(b"{\"sequence\":").unwrap();
        writeln!(record, "{sequence}}}").unwrap();
    }
    assert_eq!(receiver.len(), capacity);
    assert_eq!(sink.sender.capacity(), 0);
    assert_eq!(state.lost_logs.load(Ordering::Acquire), 2);

    let first = receiver.try_recv().unwrap();
    assert_eq!(first.bytes, b"{\"sequence\":0}\n");
    assert!(first.delivered.is_none());
    {
        let mut record = sink.make_writer();
        writeln!(record, "{{\"sequence\":5}}").unwrap();
    }
    assert_eq!(receiver.len(), capacity);
    for expected in [1, 2, 5] {
        let record = receiver.try_recv().unwrap();
        assert_eq!(
            record.bytes,
            format!("{{\"sequence\":{expected}}}\n").as_bytes()
        );
        assert!(record.delivered.is_none());
    }
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(state.lost_logs.load(Ordering::Acquire), 2);
    assert!(!state.closing.load(Ordering::Acquire));
}

#[tokio::test]
async fn finish_reports_optional_log_loss_without_failing_delivered_results() {
    let mut stdout = tempfile::tempfile().unwrap();
    let mut stderr = tempfile::tempfile().unwrap();
    let mut outputs = local_outputs(stdout.as_fd(), stderr.as_fd());
    outputs.stdout.line("{\"report\":true}").await.unwrap();
    outputs.stderr.log(b"{\"log\":\"retained\"}\n".to_vec());
    outputs.stderr.state.lost_logs.store(2, Ordering::Release);

    tokio::time::timeout(Duration::from_secs(2), outputs.finish())
        .await
        .expect("a working output destination must finish promptly")
        .expect("optional log loss must not fail successfully delivered results");
    assert!(outputs.writers.iter().all(|writer| writer.thread.is_none()));
    drop(outputs);

    stdout.rewind().unwrap();
    stderr.rewind().unwrap();
    let mut reports = String::new();
    let mut logs = String::new();
    stdout.read_to_string(&mut reports).unwrap();
    stderr.read_to_string(&mut logs).unwrap();
    assert_eq!(reports, "{\"report\":true}\n");
    let records: Vec<serde_json::Value> = logs
        .lines()
        .map(|line| serde_json::from_str(line).expect("every diagnostic must be a JSON record"))
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0], serde_json::json!({"log": "retained"}));
    assert_eq!(
        records[1],
        serde_json::json!({
            "level": "WARN",
            "message": "optional log records were not delivered",
            "dropped_log_records": 2,
        }),
        "retained logs and the final loss count must reach the working destination"
    );
}
