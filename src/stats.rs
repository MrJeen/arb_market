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
}

impl MinuteStats {
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
