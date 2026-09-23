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
#[path = "../tests/unit/recompute_actuals.rs"]
mod tests;
