//! Explicit, bounded maintenance run; never started by the normal service.
use crate::config::RecomputeActualsConfig;
use crate::error::Result;
use crate::platforms::outcome::{fees::FEE_REFRESH_INTERVAL, OutcomeVenue};
use crate::store::{actuals::Projection, Store};
use std::time::Instant;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub upper_id: i64,
    pub scanned: u64,
    pub repaired: u64,
    pub still_unknown: u64,
    pub skipped: u64,
    pub errors: u64,
}

impl Summary {
    pub fn success(&self) -> bool {
        self.still_unknown == 0 && self.errors == 0
    }
}

// A small test seam around I/O; pricing remains in the existing locked transaction.
trait Backend {
    async fn upper_id(&self) -> Result<i64>;
    async fn page(&self, after: i64, upper: i64) -> Result<Vec<i64>>;
    async fn refresh_fees(&self) -> Result<()>;
    async fn repair(&self, id: i64) -> Result<Option<Projection>>;
}

struct LiveBackend {
    store: Store,
    venue: OutcomeVenue,
}

impl Backend for LiveBackend {
    async fn upper_id(&self) -> Result<i64> {
        self.store.unknown_actuals_upper_id().await
    }
    async fn page(&self, after: i64, upper: i64) -> Result<Vec<i64>> {
        self.store.unknown_actuals_page(after, upper).await
    }
    async fn refresh_fees(&self) -> Result<()> {
        self.venue.refresh_fees().await
    }
    async fn repair(&self, id: i64) -> Result<Option<Projection>> {
        self.store
            .refresh_unknown_order_actuals_with_fallback(id, &|wallet, token| {
                self.venue.latest_actuals_fee(wallet, token)
            })
            .await
    }
}

async fn scan(backend: &impl Backend) -> Summary {
    let mut summary = Summary::default();
    summary.upper_id = match backend.upper_id().await {
        Ok(id) => id,
        Err(_) => {
            summary.errors += 1;
            tracing::error!(
                stage = "upper_id",
                reason = "database query failed",
                "recompute stopped"
            );
            return summary;
        }
    };
    let mut after = 0;
    let mut last_attempt = None;
    loop {
        let ids = match backend.page(after, summary.upper_id).await {
            Ok(ids) => ids,
            Err(_) => {
                summary.errors += 1;
                tracing::error!(
                    after,
                    stage = "page",
                    reason = "database query failed",
                    "recompute stopped"
                );
                break;
            }
        };
        if ids.is_empty() {
            break;
        }
        if refresh_due(last_attempt, Instant::now()) {
            // Failed attempts also throttle retries; the cache itself retains its original ages.
            last_attempt = Some(Instant::now());
            if backend.refresh_fees().await.is_err() {
                summary.errors += 1;
                tracing::warn!(
                    service = "outcome",
                    api = "fee_snapshot",
                    reason = "fee refresh failed",
                    "recompute continues with available frozen evidence"
                );
            }
        }
        for id in ids {
            after = id; // Advance even on failure; each ID is attempted only once.
            summary.scanned += 1;
            match backend.repair(id).await {
                Ok(Some(Projection::Ready { .. })) => summary.repaired += 1,
                Ok(Some(Projection::Unknown { reason })) => {
                    summary.still_unknown += 1;
                    tracing::warn!(order_id = id, reason, "recompute order remains unknown");
                }
                Ok(None) => summary.skipped += 1,
                Err(_) => {
                    summary.errors += 1;
                    // Do not log arbitrary SQL errors: they may contain row data or credentials.
                    tracing::error!(
                        order_id = id,
                        reason = "projection transaction failed",
                        "recompute order failed; continuing"
                    );
                }
            }
        }
    }
    summary
}

fn refresh_due(last: Option<Instant>, now: Instant) -> bool {
    last.is_none_or(|last| now.duration_since(last) >= FEE_REFRESH_INTERVAL)
}

pub async fn run() -> Summary {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let summary = match prepare().await {
        Ok(backend) => scan(&backend).await,
        Err(stage) => {
            tracing::error!(
                stage,
                reason = "maintenance initialization failed",
                "recompute stopped"
            );
            Summary {
                errors: 1,
                ..Summary::default()
            }
        }
    };
    println!(
        "upper_id={} scanned={} repaired={} still_unknown={} skipped={} errors={}",
        summary.upper_id,
        summary.scanned,
        summary.repaired,
        summary.still_unknown,
        summary.skipped,
        summary.errors
    );
    summary
}

async fn prepare() -> std::result::Result<LiveBackend, &'static str> {
    let cfg = RecomputeActualsConfig::from_env().map_err(|_| "configuration")?;
    let venue = OutcomeVenue::connect_read_only(&cfg).map_err(|_| "read_only_client")?;
    let store = Store::connect(&cfg.app_postgres_uri)
        .await
        .map_err(|_| "database_connection")?;
    Ok(LiveBackend { store, venue })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use rust_decimal::Decimal;
    use std::cell::RefCell;

    struct Fake {
        ids: Vec<i64>,
        attempts: RefCell<Vec<i64>>,
        refreshes: RefCell<usize>,
        fail_page: bool,
        fail_fees: bool,
    }
    impl Backend for Fake {
        async fn upper_id(&self) -> Result<i64> {
            Ok(45)
        }
        async fn page(&self, after: i64, upper: i64) -> Result<Vec<i64>> {
            if self.fail_page && after >= 20 {
                return Err(Error::msg("test page"));
            }
            Ok(self
                .ids
                .iter()
                .copied()
                .filter(|id| *id > after && *id <= upper)
                .take(20)
                .collect())
        }
        async fn refresh_fees(&self) -> Result<()> {
            *self.refreshes.borrow_mut() += 1;
            if self.fail_fees {
                Err(Error::msg("test fees"))
            } else {
                Ok(())
            }
        }
        async fn repair(&self, id: i64) -> Result<Option<Projection>> {
            self.attempts.borrow_mut().push(id);
            match id {
                1 => Err(Error::msg("test order")),
                2 => Ok(None),
                3 => Ok(Some(Projection::Unknown {
                    reason: "missing evidence".into(),
                })),
                _ => Ok(Some(Projection::Ready {
                    actuals: (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
                    evidence: serde_json::json!({}),
                })),
            }
        }
    }
    fn fake(ids: Vec<i64>) -> Fake {
        Fake {
            ids,
            attempts: RefCell::new(vec![]),
            refreshes: RefCell::new(0),
            fail_page: false,
            fail_fees: false,
        }
    }
    #[tokio::test]
    async fn bounded_pages_advance_on_error_and_keep_frozen_repairs() {
        let mut backend = fake((1..=60).collect());
        backend.fail_fees = true;
        let summary = scan(&backend).await;
        assert_eq!(
            summary,
            Summary {
                upper_id: 45,
                scanned: 45,
                repaired: 42,
                skipped: 1,
                still_unknown: 1,
                errors: 2
            }
        );
        assert_eq!(*backend.attempts.borrow(), (1..=45).collect::<Vec<_>>());
        assert_eq!(*backend.refreshes.borrow(), 1);
        assert!(!summary.success());
    }
    #[tokio::test]
    async fn empty_does_not_query_fees_and_page_failure_keeps_counts() {
        let backend = fake(vec![]);
        assert!(scan(&backend).await.success());
        assert_eq!(*backend.refreshes.borrow(), 0);
        let mut backend = fake((1..=45).collect());
        backend.fail_page = true;
        let summary = scan(&backend).await;
        assert_eq!(summary.scanned, 20);
        assert_eq!(summary.errors, 2);
        assert_eq!(summary.repaired, 17);
    }
    #[test]
    fn refresh_interval_and_exit_status() {
        let now = Instant::now();
        assert!(refresh_due(None, now));
        assert!(!refresh_due(
            Some(now),
            now + std::time::Duration::from_secs(299)
        ));
        assert!(refresh_due(Some(now), now + FEE_REFRESH_INTERVAL));
        assert!(Summary {
            skipped: 10,
            ..Summary::default()
        }
        .success());
        assert!(!Summary {
            still_unknown: 1,
            ..Summary::default()
        }
        .success());
        assert!(!Summary {
            errors: 1,
            ..Summary::default()
        }
        .success());
    }
}
