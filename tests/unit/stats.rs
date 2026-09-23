use super::*;

fn skipped(n: u64) -> RestBookSkipCounts {
    RestBookSkipCounts {
        revision_changed: n,
        ..Default::default()
    }
}

#[test]
fn snapshot_returns_counts_and_resets() {
    let stats = MinuteStats::new();
    stats.wakeup();
    stats.wakeup();
    stats.add_missing_book(3);
    stats.orders();
    stats.take_profit_candidate();
    stats.settled();
    stats.settlement_finalize_fail();
    stats.settlement_skipped_before_end();
    stats.settlement_skipped_before_end();
    stats.settlement_end_date_missing();
    stats.lifecycle_busy();
    stats.outcome_fee_refresh_ok();
    stats.outcome_fee_refresh_failed();
    stats.outcome_fee_unavailable();

    let first = stats.snapshot_and_reset();
    assert_eq!(first.wakeup, 2);
    assert_eq!(first.missing_book, 3);
    assert_eq!(first.orders, 1);
    assert_eq!(first.take_profit_candidate, 1);
    assert_eq!(first.settled, 1);
    assert_eq!(first.settlement_finalize_fail, 1);
    assert_eq!(first.settlement_skipped_before_end, 2);
    assert_eq!(first.settlement_end_date_missing, 1);
    assert_eq!(first.lifecycle_busy, 1);
    assert_eq!(first.outcome_fee_refresh_ok, 1);
    assert_eq!(first.outcome_fee_refresh_failed, 1);
    assert_eq!(first.outcome_fee_unavailable, 1);
    assert_eq!(first.calc, 0);

    let second = stats.snapshot_and_reset();
    assert_eq!(second, MinuteSnapshot::default());
}

#[test]
fn book_refresh_stats_count_results_latency_and_reset() {
    let stats = MinuteStats::new();
    stats.record_pm_book_resync(4, Some((3, 2, skipped(1))), 250);
    stats.record_pm_book_resync(2, None, 500);
    stats.record_pm_book_resync(1, Some((0, 0, skipped(0))), 10);
    for platform in [crate::config::POLYMARKET, crate::config::OUTCOME] {
        stats.record_hedge_book(platform, Some(true), 100);
        stats.record_hedge_book(platform, Some(false), 200);
        stats.record_hedge_book(platform, None, 300);
    }
    let s = stats.snapshot_and_reset();
    assert_eq!(s.pm_book_resync_batches, 3);
    assert_eq!(s.pm_book_resync_requested, 7);
    assert_eq!(s.pm_book_resync_returned, 3);
    assert_eq!(s.pm_book_resync_applied, 2);
    assert_eq!(s.pm_book_resync_skipped, 1);
    assert_eq!(s.pm_book_resync_failed, 1);
    assert_eq!(s.pm_book_resync_elapsed_ms, 760);
    assert_eq!(s.pm_book_resync_max_ms, 500);
    assert_eq!(
        (s.hedge_pm_book_requests, s.hedge_out_book_requests),
        (3, 3)
    );
    assert_eq!(
        (s.hedge_pm_book_accepted, s.hedge_out_book_accepted),
        (1, 1)
    );
    assert_eq!(
        (s.hedge_pm_book_discarded, s.hedge_out_book_discarded),
        (1, 1)
    );
    assert_eq!((s.hedge_pm_book_failed, s.hedge_out_book_failed), (1, 1));
    assert_eq!(
        (s.hedge_pm_book_elapsed_ms, s.hedge_out_book_elapsed_ms),
        (600, 600)
    );
    assert_eq!(
        (s.hedge_pm_book_max_ms, s.hedge_out_book_max_ms),
        (300, 300)
    );
    assert_eq!(stats.snapshot_and_reset(), MinuteSnapshot::default());
    stats.record_pm_book_resync(1, Some((1, 1, skipped(0))), 5);
    stats.record_hedge_book(crate::config::POLYMARKET, Some(true), 2);
    let next = stats.snapshot_and_reset();
    assert_eq!(next.pm_book_resync_max_ms, 5);
    assert_eq!(next.hedge_pm_book_max_ms, 2);
    assert_eq!(next.hedge_out_book_max_ms, 0);
}

#[test]
fn book_refresh_stats_accumulate_concurrent_updates() {
    let stats = MinuteStats::new();
    std::thread::scope(|scope| {
        for elapsed_ms in 1..=8 {
            let stats = &stats;
            scope.spawn(move || {
                for _ in 0..100 {
                    stats.record_pm_book_resync(2, Some((2, 1, skipped(1))), elapsed_ms);
                    stats.record_hedge_book(crate::config::OUTCOME, Some(true), elapsed_ms);
                }
            });
        }
    });
    let s = stats.snapshot_and_reset();
    assert_eq!(s.pm_book_resync_batches, 800);
    assert_eq!(s.pm_book_resync_requested, 1600);
    assert_eq!(s.pm_book_resync_elapsed_ms, 3600);
    assert_eq!(s.pm_book_resync_max_ms, 8);
    assert_eq!(s.hedge_out_book_requests, 800);
    assert_eq!(s.hedge_out_book_accepted, 800);
    assert_eq!(s.hedge_out_book_elapsed_ms, 3600);
    assert_eq!(s.hedge_out_book_max_ms, 8);
}

#[test]
fn actuals_counters_accumulate_concurrently_and_reset() {
    let stats = MinuteStats::new();
    stats.add_submitted_pending_promoted(0);
    assert_eq!(stats.snapshot_and_reset(), MinuteSnapshot::default());
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let stats = &stats;
            scope.spawn(move || {
                for _ in 0..100 {
                    stats.actuals_gate_blocked();
                    stats.add_submitted_pending_promoted(3);
                }
            });
        }
    });
    let snapshot = stats.snapshot_and_reset();
    assert_eq!(snapshot.actuals_gate_blocked, 800);
    assert_eq!(snapshot.submitted_pending_promoted, 2400);
    assert_eq!(stats.snapshot_and_reset(), MinuteSnapshot::default());
}

#[test]
fn add_zero_is_noop() {
    let stats = MinuteStats::new();
    stats.add_stale_book(0);
    assert_eq!(stats.snapshot_and_reset().stale_book, 0);
}

fn stale_pair(kind: crate::calc::StaleKind) -> CalcPairSample {
    use crate::book::BookSource;
    use crate::calc::CalcBookState;
    CalcPairSample {
        pm_label: "yes".into(),
        out_label: "no".into(),
        pm_ask: None,
        pm_sz: None,
        out_ask: None,
        out_sz: None,
        unit_cost: None,
        unit_expected_revenue: None,
        reason: "stale_book",
        stale: Some(CalcStaleDetail {
            kind,
            threshold_ms: 5000,
            pm: CalcBookState {
                source: BookSource::Ws,
                age_ms: 6000,
                invalid: kind != crate::calc::StaleKind::OutOnly,
            },
            out: CalcBookState {
                source: BookSource::Rest,
                age_ms: 7000,
                invalid: kind != crate::calc::StaleKind::PmOnly,
            },
        }),
    }
}

fn topic() -> TopicKey {
    TopicKey::new(uuid::Uuid::nil(), 1)
}

#[test]
fn stale_samples_are_bounded_by_kind_and_reset() {
    use crate::calc::StaleKind::*;
    let stats = MinuteStats::new();
    for _ in 0..10 {
        stats.record_calc_samples(topic(), vec![stale_pair(PmOnly)], false);
    }
    stats.record_calc_samples(topic(), vec![stale_pair(OutOnly), stale_pair(Both)], true);
    let samples = stats.take_samples();
    assert_eq!(samples.stale.iter().flatten().count(), 3);
    assert_eq!(samples.last_miss.unwrap().pairs.len(), 2);
    assert_eq!(stats.stale_sample_mask.load(Ordering::Relaxed), 0);
    assert!(stats.take_samples().stale.iter().all(Option::is_none));
    stats.record_calc_samples(topic(), vec![stale_pair(Both)], false);
    assert!(stats.take_samples().stale[Both.index()].is_some());
}

#[test]
fn stale_logs_are_emitted_with_found_and_not_replayed() {
    #[derive(Clone)]
    struct Buffer(std::sync::Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let buffer = Buffer(Default::default());
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        use crate::calc::StaleKind::*;
        let stats = MinuteStats::new();
        stats.found();
        stats.record_calc_samples(
            topic(),
            vec![stale_pair(PmOnly), stale_pair(OutOnly), stale_pair(Both)],
            false,
        );
        stats.log_and_reset();
        stats.log_and_reset();
    });
    let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    assert_eq!(output.matches("stale book sample").count(), 3);
    assert!(!output.contains("calc miss sample"));
    assert!(output.contains("pm_source=\"ws\"") || output.contains("pm_source=ws"));
    assert!(output.contains("threshold_ms=5000"));
}

#[test]
fn concurrent_sampling_and_drain_preserve_kind_slots() {
    use crate::calc::StaleKind::*;
    let stats = MinuteStats::new();
    let barrier = std::sync::Barrier::new(5);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let stats = &stats;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    stats.record_calc_samples(
                        topic(),
                        vec![stale_pair(PmOnly), stale_pair(OutOnly), stale_pair(Both)],
                        false,
                    );
                    stats.record_calc_skips(&CalcSkipCounts {
                        stale_book: 3,
                        stale_pm_only: 1,
                        stale_out_only: 1,
                        stale_both: 1,
                        stale_pm_expired: 2,
                        stale_out_invalid: 2,
                        ..Default::default()
                    });
                }
            });
        }
        barrier.wait();
        for _ in 0..100 {
            let samples = stats.take_samples();
            for (index, sample) in samples.stale.iter().enumerate() {
                if let Some(sample) = sample {
                    assert_eq!(sample.detail.kind.index(), index);
                }
            }
        }
    });
    let s = stats.snapshot_and_reset();
    assert_eq!(s.stale_book, 1200);
    assert_eq!(
        s.stale_book,
        s.stale_pm_only + s.stale_out_only + s.stale_both
    );
    assert_eq!(
        s.stale_pm_invalid + s.stale_pm_expired,
        s.stale_pm_only + s.stale_both
    );
    assert_eq!(
        s.stale_out_invalid + s.stale_out_expired,
        s.stale_out_only + s.stale_both
    );
    assert_eq!(stats.snapshot_and_reset(), MinuteSnapshot::default());
}

#[test]
fn resync_skip_breakdown_preserves_total_and_reset() {
    let stats = MinuteStats::new();
    stats.record_pm_book_resync(
        10,
        Some((
            10,
            2,
            RestBookSkipCounts {
                missing_token: 1,
                no_ticket: 1,
                parse_error: 1,
                invalid_payload: 1,
                older_timestamp: 1,
                timestamp_conflict: 1,
                epoch_changed: 1,
                revision_changed: 1,
            },
        )),
        20,
    );
    let s = stats.snapshot_and_reset();
    let counts = [
        s.pm_book_resync_skip_missing_token,
        s.pm_book_resync_skip_no_ticket,
        s.pm_book_resync_skip_parse_error,
        s.pm_book_resync_skip_invalid_payload,
        s.pm_book_resync_skip_older_timestamp,
        s.pm_book_resync_skip_timestamp_conflict,
        s.pm_book_resync_skip_epoch_changed,
        s.pm_book_resync_skip_revision_changed,
    ];
    assert_eq!(counts, [1; 8]);
    assert_eq!(s.pm_book_resync_skipped, counts.iter().sum::<u64>());
    assert_eq!(
        s.pm_book_resync_returned,
        s.pm_book_resync_applied + s.pm_book_resync_skipped
    );
    assert_eq!(stats.snapshot_and_reset(), MinuteSnapshot::default());
}

#[test]
fn records_and_clears_calc_miss() {
    let stats = MinuteStats::new();
    stats.record_calc_miss(crate::calc::CalcMissSnapshot {
        topic: "t".into(),
        pairs: Vec::new(),
    });
    stats.log_and_reset();
    stats.log_and_reset();
}
