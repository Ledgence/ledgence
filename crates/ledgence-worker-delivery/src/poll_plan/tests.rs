use super::*;

fn plan(revision: u64, weights: &[u32]) -> PollPlan {
    let routes = weights
        .iter()
        .enumerate()
        .map(|(i, &w)| (format!("q{i}"), w))
        .collect();
    PollPlan::new(revision, routes).unwrap()
}

#[test]
fn rejects_unknown_duplicate_and_invalid_queues_before_use() {
    assert!(PollPlan::new(1, vec![]).is_err());
    assert!(PollPlan::new(1, vec![("q".into(), 1), ("q".into(), 0)]).is_err());
    assert!(PollPlan::new(1, vec![("q".repeat(129), 1)]).is_err());
    let oversized = (0..=MAX_POLL_QUEUES)
        .map(|i| (format!("q{i}"), 0))
        .collect();
    assert!(PollPlan::new(1, oversized).is_err());
    assert_eq!(
        plan(1, &[21, 9, 0]).routes(),
        &[("q0".into(), 7), ("q1".into(), 3), ("q2".into(), 0)]
    );
}

#[test]
fn stride_tracks_each_prefix_including_impossible_to_reduce_ratios() {
    for (a, b) in [
        (7, 3),
        (1, 7),
        (3, 1),
        (1, 1),
        (u32::MAX, u32::MAX - 2),
        (1, u32::MAX),
    ] {
        let planner = PollPlanner::new(plan(0, &[a, b]), PollStrategy::Stride);
        let total = i128::from(a) + i128::from(b);
        let mut count = 0;
        for n in 1..=1000 {
            count += i128::from(planner.issue() == Some(0));
            assert!(
                (count * total - n * i128::from(a)).abs() < total,
                "{a}:{b} at poll {n}"
            );
        }
    }
}

#[test]
fn many_independent_large_weights_match_exact_virtual_finish_oracle() {
    // The LCM of these weights exceeds u64. Compare to exact cumulative
    // service/weight ordering using a separate, unnormalized integer oracle.
    let weights = [
        4294967291, 4294967279, 4294967231, 251, 97, 0, 877, 4294967197,
    ];
    let p = PollPlanner::new(plan(1, &weights), PollStrategy::Stride);
    let mut count = [0_u64; 8];
    for _ in 0..3000 {
        let next = (0..weights.len())
            .filter(|&i| weights[i] != 0)
            .min_by(|&a, &b| {
                (u128::from(count[a]) * u128::from(weights[b]))
                    .cmp(&(u128::from(count[b]) * u128::from(weights[a])))
            })
            .unwrap();
        assert_eq!(p.issue(), Some(next));
        count[next] += 1;
    }
}

#[test]
fn equivalent_updates_replays_and_rejections_leave_the_decision_stream_unchanged() {
    for strategy in [PollStrategy::Stride, PollStrategy::SeededLottery(20261103)] {
        let live = PollPlanner::new(plan(0, &[7, 3]), strategy);
        let control = PollPlanner::new(plan(0, &[7, 3]), strategy);
        assert_eq!(live.queues(), &["q0", "q1"]);
        for rev in 1..=100 {
            // Reordering input fields cannot change route identity or tie-breaking.
            let scaled = PollPlan::new(rev, vec![("q1".into(), 30), ("q0".into(), 70)]).unwrap();
            live.install(scaled.clone()).unwrap();
            live.install(scaled).unwrap();
            assert!(live.install(plan(rev - 1, &[3, 7])).is_err());
            assert!(live.install(plan(rev, &[0, 1])).is_err());
            assert!(
                live.install(
                    PollPlan::new(rev + 1, vec![("q0".into(), 7), ("other".into(), 3)]).unwrap()
                )
                .is_err()
            );
            assert!(live.install(plan(rev + 1, &[1])).is_err());
            assert_eq!(live.revision(), rev);
            let choice = live.issue_with_revision().unwrap();
            assert_eq!(choice.revision, rev);
            assert_eq!(Some(choice.route), control.issue());
        }
    }
}

#[test]
fn idle_refresh_and_resume_consume_neither_rng_words_nor_stride_turns() {
    for strategy in [PollStrategy::Stride, PollStrategy::SeededLottery(20261103)] {
        let live = PollPlanner::new(plan(0, &[7, 3]), strategy);
        let control = PollPlanner::new(plan(0, &[7, 3]), strategy);
        for _ in 0..9 {
            assert_eq!(live.issue(), control.issue());
        }
        live.install(plan(1, &[0, 0])).unwrap();
        assert!(live.is_idle());
        for i in 2..100 {
            live.install(plan(i, &[0, 0])).unwrap();
            assert!(live.install(plan(1, &[7, 3])).is_err());
            assert_eq!(live.issue(), None);
        }
        live.install(plan(100, &[7, 3])).unwrap();
        for _ in 0..100 {
            assert_eq!(live.issue(), control.issue());
        }
    }
}

#[test]
fn stride_handles_weight_replacement_and_rejoining_without_idle_catch_up() {
    let p = PollPlanner::new(plan(0, &[1, 1, 1]), PollStrategy::Stride);
    assert_eq!(p.issue(), Some(0));
    // q0 already had its turn. A weight change must not erase its lead.
    p.install(plan(1, &[3, 1, 1])).unwrap();
    assert_eq!(p.issue(), Some(1));
    assert_eq!(p.issue(), Some(2));
    // q0 still has a residual lead above the old zero frontier. Immediately
    // leave and rejoin on equivalent new weights, without any intervening poll.
    p.install(plan(2, &[0, 1, 1])).unwrap();
    p.install(plan(3, &[3, 1, 1])).unwrap();
    // Check many reconfigurations at extreme weights without overflow.
    for rev in 4..80 {
        p.install(plan(rev, &[1, u32::MAX, 0])).unwrap();
        for _ in 0..50 {
            assert_ne!(p.issue(), Some(2));
        }
    }
    p.install(plan(80, &[0, 0, 1])).unwrap();
    for _ in 0..500 {
        assert_eq!(p.issue(), Some(2));
    }
    p.install(plan(81, &[1, 1, 1])).unwrap();
    // No long burst for a route that was inactive for 500 polls.
    let mut seen = [0; 3];
    for _ in 0..12 {
        seen[p.issue().unwrap()] += 1;
    }
    assert!(seen.iter().all(|&x| (3..=5).contains(&x)), "{seen:?}");
}

#[test]
fn lottery_matches_reference_and_distributes_service_across_many_seeds() {
    let p = PollPlanner::new(plan(1, &[7, 3]), PollStrategy::SeededLottery(20261103));
    assert_eq!((0..1000).filter(|_| p.issue() == Some(0)).count(), 730);
    let selected_a: usize = (0..64)
        .map(|seed| {
            let p = PollPlanner::new(plan(1, &[7, 3, 0]), PollStrategy::SeededLottery(seed));
            (0..1000)
                .map(|_| match p.issue() {
                    Some(0) => 1,
                    Some(1) => 0,
                    route => panic!("unexpected route: {route:?}"),
                })
                .sum::<usize>()
        })
        .sum();
    // Well outside normal sampling error, yet sensitive to systematic bias.
    assert!((44_000..=45_600).contains(&selected_a), "{selected_a}");
}

#[tokio::test]
async fn idle_consumers_observe_installs_before_and_after_waiting() {
    use std::time::Duration;
    let p = PollPlanner::new(plan(0, &[0, 0]), PollStrategy::Stride);
    let mut listener = p.subscribe();
    assert!(p.is_idle());
    p.install(plan(1, &[0, 1])).unwrap();
    listener.changed().await.unwrap();
    assert_eq!(*listener.borrow_and_update(), 1);
    p.wait_active().await;
    p.install(plan(2, &[0, 0])).unwrap();
    let waiters: Vec<_> = (0..8)
        .map(|_| {
            let p = p.clone();
            tokio::spawn(async move {
                p.wait_active().await;
                p.issue_with_revision()
            })
        })
        .collect();
    tokio::task::yield_now().await;
    p.install(plan(3, &[1, 0])).unwrap();
    for waiter in waiters {
        let choice = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            choice,
            Some(PollSelection {
                revision: 3,
                route: 0
            })
        );
    }
}

#[test]
fn concurrent_consumers_share_one_issue_history() {
    let planner = PollPlanner::new(plan(42, &[7, 3]), PollStrategy::Stride);
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let p = planner.clone();
            std::thread::spawn(move || (0..125).filter(|_| p.issue() == Some(0)).count())
        })
        .collect();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .sum::<usize>(),
        700
    );
}
