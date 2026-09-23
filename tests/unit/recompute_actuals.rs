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
