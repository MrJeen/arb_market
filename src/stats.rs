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
    actuals_gate_blocked,
    submitted_pending_promoted,
    arb_disabled,
    rebalance_disabled,
    rebalance_loss_cooldown,
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

    pub fn add_submitted_pending_promoted(&self, n: u64) {
        if n > 0 {
            self.submitted_pending_promoted
                .fetch_add(n, Ordering::Relaxed);
        }
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
#[path = "../tests/unit/stats.rs"]
mod tests;
