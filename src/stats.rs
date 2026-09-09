use crate::calc::CalcMissSnapshot;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

macro_rules! minute_stats {
    ($($name:ident),+ $(,)?) => {
        pub struct MinuteStats {
            $($name: AtomicU64,)+
            last_miss: Mutex<Option<CalcMissSnapshot>>,
        }

        impl Default for MinuteStats {
            fn default() -> Self {
                Self {
                    $($name: AtomicU64::new(0),)+
                    last_miss: Mutex::new(None),
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
                let miss = self
                    .last_miss
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .take();
                if s.found == 0 {
                    if let Some(miss) = miss {
                        miss.log();
                    }
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
        result: Option<(usize, usize, usize)>,
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
                .fetch_add(skipped as u64, Ordering::Relaxed);
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

    pub fn record_calc_miss(&self, snapshot: CalcMissSnapshot) {
        *self.last_miss.lock().unwrap_or_else(|err| err.into_inner()) = Some(snapshot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(first.calc, 0);

        let second = stats.snapshot_and_reset();
        assert_eq!(second, MinuteSnapshot::default());
    }

    #[test]
    fn book_refresh_stats_count_results_latency_and_reset() {
        let stats = MinuteStats::new();
        stats.record_pm_book_resync(4, Some((3, 2, 1)), 250);
        stats.record_pm_book_resync(2, None, 500);
        stats.record_pm_book_resync(1, Some((0, 0, 0)), 10);
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
        stats.record_pm_book_resync(1, Some((1, 1, 0)), 5);
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
                        stats.record_pm_book_resync(2, Some((2, 1, 1)), elapsed_ms);
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
