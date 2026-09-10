use crate::calc::{CalcMissSnapshot, CalcPairSample, CalcSkipCounts, CalcStaleDetail};
use crate::domain::TopicKey;
use crate::platforms::polymarket::RestBookSkipCounts;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;

#[derive(Default)]
struct CalcSamples {
    last_miss: Option<CalcMissSnapshot>,
    stale: [Option<StaleSample>; 3],
}

struct StaleSample {
    topic: TopicKey,
    pm_label: String,
    out_label: String,
    detail: CalcStaleDetail,
}

impl StaleSample {
    fn log(&self) {
        let d = self.detail;
        tracing::info!(
            topic = %self.topic.as_str(),
            pm_label = %self.pm_label,
            out_label = %self.out_label,
            stale_side = d.kind.as_str(),
            pm_source = d.pm.source.as_str(),
            pm_age_ms = d.pm.age_ms,
            pm_invalid = d.pm.invalid,
            out_source = d.out.source.as_str(),
            out_age_ms = d.out.age_ms,
            out_invalid = d.out.invalid,
            threshold_ms = d.threshold_ms,
            "stale book sample"
        );
    }
}

macro_rules! minute_stats {
    ($($name:ident),+ $(,)?) => {
        pub struct MinuteStats {
            $($name: AtomicU64,)+
            samples: Mutex<CalcSamples>,
            stale_sample_mask: AtomicU8,
        }

        impl Default for MinuteStats {
            fn default() -> Self {
                Self {
                    $($name: AtomicU64::new(0),)+
                    samples: Mutex::new(CalcSamples::default()),
                    stale_sample_mask: AtomicU8::new(0),
                }
            }
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
        pub struct MinuteSnapshot {
            $(pub $name: u64,)+
        }

        impl MinuteStats {
            pub fn new() -> Self {
                Self::default()
            }

            $(
                pub fn $name(&self) {
                    self.$name.fetch_add(1, Ordering::Relaxed);
                }
            )+

            pub fn snapshot_and_reset(&self) -> MinuteSnapshot {
                MinuteSnapshot {
                    $($name: self.$name.swap(0, Ordering::Relaxed),)+
                }
            }

            pub fn log_and_reset(&self) {
                let s = self.snapshot_and_reset();
                tracing::info!(
                    $( $name = s.$name, )+
                    "minute stats"
                );
                let samples = self.take_samples();
                if s.found == 0 {
                    if let Some(miss) = samples.last_miss {
                        miss.log();
                    }
                }
                for sample in samples.stale.into_iter().flatten() {
                    sample.log();
                }
            }
        }
    };
}

minute_stats! {
    wakeup,
    coalesced,
    calc,
    found,
    missing_book,
    stale_book,
    stale_pm_only,
    stale_out_only,
    stale_both,
    stale_pm_invalid,
    stale_pm_expired,
    stale_out_invalid,
    stale_out_expired,
    unit_cost,
    unprofitable,
    no_topic,
    active_topic,
    stale_unknown,
    max_orders,
    max_loss,
    arb_disabled,
    rebalance_disabled,
    take_profit_disabled,
    pm_bal,
    pm_balance_call,
    pm_balance_cache_hit,
    pm_balance_refresh,
    pm_balance_refresh_fail,
    out_bal,
    outcome_fee_refresh_ok,
    outcome_fee_refresh_failed,
    outcome_fee_unavailable,
    http_fail,
    skew,
    no_longer,
    exceed_bal,
    claimed,
    orders,
    pm_ok,
    pm_fail,
    out_ok,
    out_fail,
    exec_err,
    take_profit_scan,
    take_profit_candidate,
    take_profit_confirmed,
    take_profit_cancelled,
    take_profit_pm_ok,
    take_profit_pm_fail,
    take_profit_out_ok,
    take_profit_out_fail,
    settlement_scan,
    settlement_skipped_before_end,
    settlement_end_date_missing,
    settlement_pending_entered,
    settlement_pending_scan,
    settlement_finalize_fail,
    outcome_fractional_settlement,
    settled,
    unavailable,
    lifecycle_busy,
    pm_book_resync_batches,
    pm_book_resync_requested,
    pm_book_resync_returned,
    pm_book_resync_applied,
    pm_book_resync_skipped,
    pm_book_resync_skip_missing_token,
    pm_book_resync_skip_no_ticket,
    pm_book_resync_skip_parse_error,
    pm_book_resync_skip_invalid_payload,
    pm_book_resync_skip_older_timestamp,
    pm_book_resync_skip_timestamp_conflict,
    pm_book_resync_skip_epoch_changed,
    pm_book_resync_skip_revision_changed,
    pm_book_resync_failed,
    pm_book_resync_elapsed_ms,
    pm_book_resync_max_ms,
    hedge_pm_book_requests,
    hedge_pm_book_accepted,
    hedge_pm_book_discarded,
    hedge_pm_book_failed,
    hedge_pm_book_elapsed_ms,
    hedge_pm_book_max_ms,
    hedge_out_book_requests,
    hedge_out_book_accepted,
    hedge_out_book_discarded,
    hedge_out_book_failed,
    hedge_out_book_elapsed_ms,
    hedge_out_book_max_ms,
}

impl MinuteStats {
    /// 按完成的批次记录；None 表示请求失败，而非空响应。
    pub fn record_pm_book_resync(
        &self,
        requested: usize,
        result: Option<(usize, usize, RestBookSkipCounts)>,
        elapsed_ms: u64,
    ) {
        self.pm_book_resync_batches();
        self.pm_book_resync_requested
            .fetch_add(requested as u64, Ordering::Relaxed);
        if let Some((returned, applied, skipped)) = result {
            self.pm_book_resync_returned
                .fetch_add(returned as u64, Ordering::Relaxed);
            self.pm_book_resync_applied
                .fetch_add(applied as u64, Ordering::Relaxed);
            self.pm_book_resync_skipped
                .fetch_add(skipped.total(), Ordering::Relaxed);
            for (counter, count) in [
                (
                    &self.pm_book_resync_skip_missing_token,
                    skipped.missing_token,
                ),
                (&self.pm_book_resync_skip_no_ticket, skipped.no_ticket),
                (&self.pm_book_resync_skip_parse_error, skipped.parse_error),
                (
                    &self.pm_book_resync_skip_invalid_payload,
                    skipped.invalid_payload,
                ),
                (
                    &self.pm_book_resync_skip_older_timestamp,
                    skipped.older_timestamp,
                ),
                (
                    &self.pm_book_resync_skip_timestamp_conflict,
                    skipped.timestamp_conflict,
                ),
                (
                    &self.pm_book_resync_skip_epoch_changed,
                    skipped.epoch_changed,
                ),
                (
                    &self.pm_book_resync_skip_revision_changed,
                    skipped.revision_changed,
                ),
            ] {
                if count > 0 {
                    counter.fetch_add(count, Ordering::Relaxed);
                }
            }
        } else {
            self.pm_book_resync_failed();
        }
        self.pm_book_resync_elapsed_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);
        self.pm_book_resync_max_ms
            .fetch_max(elapsed_ms, Ordering::Relaxed);
    }

    /// Some 表示 HTTP/解析成功后盘口是否被接受；None 表示请求或解析失败。
    pub fn record_hedge_book(&self, platform: &str, accepted: Option<bool>, elapsed_ms: u64) {
        let (requests, applied, discarded, failed, total, max) = match platform {
            crate::config::POLYMARKET => (
                &self.hedge_pm_book_requests,
                &self.hedge_pm_book_accepted,
                &self.hedge_pm_book_discarded,
                &self.hedge_pm_book_failed,
                &self.hedge_pm_book_elapsed_ms,
                &self.hedge_pm_book_max_ms,
            ),
            crate::config::OUTCOME => (
                &self.hedge_out_book_requests,
                &self.hedge_out_book_accepted,
                &self.hedge_out_book_discarded,
                &self.hedge_out_book_failed,
                &self.hedge_out_book_elapsed_ms,
                &self.hedge_out_book_max_ms,
            ),
            _ => return,
        };
        requests.fetch_add(1, Ordering::Relaxed);
        match accepted {
            Some(true) => applied,
            Some(false) => discarded,
            None => failed,
        }
        .fetch_add(1, Ordering::Relaxed);
        total.fetch_add(elapsed_ms, Ordering::Relaxed);
        max.fetch_max(elapsed_ms, Ordering::Relaxed);
    }

    pub fn add_missing_book(&self, n: u64) {
        if n > 0 {
            self.missing_book.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn add_stale_book(&self, n: u64) {
        if n > 0 {
            self.stale_book.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn add_unit_cost(&self, n: u64) {
        if n > 0 {
            self.unit_cost.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn add_unprofitable(&self, n: u64) {
        if n > 0 {
            self.unprofitable.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn record_calc_skips(&self, counts: &CalcSkipCounts) {
        self.add_missing_book(counts.missing_book);
        self.add_stale_book(counts.stale_book);
        self.add_unit_cost(counts.unit_cost);
        self.add_unprofitable(counts.unprofitable);
        for (counter, count) in [
            (&self.stale_pm_only, counts.stale_pm_only),
            (&self.stale_out_only, counts.stale_out_only),
            (&self.stale_both, counts.stale_both),
            (&self.stale_pm_invalid, counts.stale_pm_invalid),
            (&self.stale_pm_expired, counts.stale_pm_expired),
            (&self.stale_out_invalid, counts.stale_out_invalid),
            (&self.stale_out_expired, counts.stale_out_expired),
        ] {
            if count > 0 {
                counter.fetch_add(count, Ordering::Relaxed);
            }
        }
    }

    pub fn record_calc_samples(&self, topic: TopicKey, pairs: Vec<CalcPairSample>, missed: bool) {
        let mask = self.stale_sample_mask.load(Ordering::Relaxed);
        let needs_stale = pairs.iter().any(|pair| {
            pair.stale
                .is_some_and(|d| mask & (1 << d.kind.index()) == 0)
        });
        let needs_miss = missed && !pairs.is_empty();
        if !needs_stale && !needs_miss {
            return;
        }
        let mut samples = self.samples.lock().unwrap_or_else(|err| err.into_inner());
        // 位图只是快速提示；提交与清空同锁，窗口按提交时刻归属。
        for pair in &pairs {
            let Some(detail) = pair.stale else { continue };
            let index = detail.kind.index();
            if samples.stale[index].is_none() {
                samples.stale[index] = Some(StaleSample {
                    topic,
                    pm_label: pair.pm_label.clone(),
                    out_label: pair.out_label.clone(),
                    detail,
                });
                self.stale_sample_mask
                    .fetch_or(1 << index, Ordering::Relaxed);
            }
        }
        if needs_miss {
            samples.last_miss = Some(CalcMissSnapshot {
                topic: topic.as_str(),
                pairs,
            });
        }
    }

    pub fn record_calc_miss(&self, snapshot: CalcMissSnapshot) {
        self.samples
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .last_miss = Some(snapshot);
    }

    fn take_samples(&self) -> CalcSamples {
        let mut samples = self.samples.lock().unwrap_or_else(|err| err.into_inner());
        let taken = std::mem::take(&mut *samples);
        self.stale_sample_mask.store(0, Ordering::Relaxed);
        taken
    }
}

#[cfg(test)]
mod tests {
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
                    invalid: false,
                },
                out: CalcBookState {
                    source: BookSource::Rest,
                    age_ms: 7000,
                    invalid: true,
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
}
