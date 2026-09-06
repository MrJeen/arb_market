use std::sync::atomic::{AtomicU64, Ordering};

macro_rules! minute_stats {
    ($($name:ident),+ $(,)?) => {
        #[derive(Default)]
        pub struct MinuteStats {
            $($name: AtomicU64,)+
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
    buy_disabled,
    pm_bal,
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

        let first = stats.snapshot_and_reset();
        assert_eq!(first.wakeup, 2);
        assert_eq!(first.missing_book, 3);
        assert_eq!(first.orders, 1);
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
}
