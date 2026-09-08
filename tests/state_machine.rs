//! PostgreSQL integration coverage for lifecycle state transitions.
//!
//! These tests are intentionally ignored. Run them explicitly against a database named by
//! `APP_POSTGRES_URI`:
//! `cargo test --test state_machine -- --ignored --nocapture`
//!
//! The tests run migrations, create orders with unique event UUIDs, and delete only the rows they
//! created. They never truncate or otherwise reset the configured database.

use anyhow::Result;
use market_arb::domain::{MarketIdentity, TopicKey};
use market_arb::store::{NewLeg, Store};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

const POSTGRES_REQUIRED: &str = "requires APP_POSTGRES_URI; run manually against a test database";

struct Fixture {
    store: Store,
    order_id: i64,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let uri = std::env::var("APP_POSTGRES_URI")
            .expect("set APP_POSTGRES_URI before running ignored PostgreSQL tests");
        assert!(!uri.trim().is_empty(), "APP_POSTGRES_URI must not be empty");

        let store = Store::connect(&uri).await?;
        store.migrate().await?;

        // A unique event_id isolates this row from application data and concurrently running tests.
        let order_id = sqlx::query_scalar(
            "INSERT INTO arb_orders (event_id, unified_index, status)\n             VALUES ($1, 0, 'completed')\n             RETURNING id",
        )
        .bind(Uuid::new_v4())
        .fetch_one(&store.pool)
        .await?;

        Ok(Self { store, order_id })
    }

    async fn cleanup(&self) -> Result<()> {
        let mut tx = self.store.pool.begin().await?;
        sqlx::query("DELETE FROM legs WHERE order_id = $1")
            .bind(self.order_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM arb_orders WHERE id = $1")
            .bind(self.order_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn concurrent_lifecycle_claim_has_exactly_one_token() {
    let fixture = Fixture::new().await.expect(POSTGRES_REQUIRED);

    let exercised: Result<(Option<Uuid>, Option<Uuid>, Option<String>, Option<Uuid>)> = async {
        let (take_profit, rebalance) = tokio::join!(
            fixture
                .store
                .try_claim_lifecycle(fixture.order_id, "take_profit"),
            fixture
                .store
                .try_claim_lifecycle(fixture.order_id, "rebalance")
        );
        let (stored_action, stored_claim_id) = sqlx::query_as(
            "SELECT lifecycle_action, lifecycle_claim_id FROM arb_orders WHERE id = $1",
        )
        .bind(fixture.order_id)
        .fetch_one(&fixture.store.pool)
        .await?;
        Ok((take_profit?, rebalance?, stored_action, stored_claim_id))
    }
    .await;

    fixture.cleanup().await.expect("clean up test order");
    let (take_profit, rebalance, stored_action, stored_claim_id) =
        exercised.expect("exercise concurrent claims");
    assert_ne!(take_profit.is_some(), rebalance.is_some());
    let winning_token = take_profit.or(rebalance).expect("one claim token");
    assert_eq!(stored_claim_id, Some(winning_token));
    assert_eq!(
        stored_action.as_deref(),
        Some(if take_profit.is_some() {
            "take_profit"
        } else {
            "rebalance"
        })
    );
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn stale_token_cannot_release_or_insert_under_new_claim() {
    let fixture = Fixture::new().await.expect(POSTGRES_REQUIRED);

    let exercised: Result<(bool, bool, bool, i64)> = async {
        let old = fixture
            .store
            .try_claim_lifecycle(fixture.order_id, "rebalance")
            .await?
            .expect("initial claim");
        let first_release = fixture
            .store
            .release_lifecycle(fixture.order_id, "rebalance", old)
            .await?;
        let new = fixture
            .store
            .try_claim_lifecycle(fixture.order_id, "rebalance")
            .await?
            .expect("replacement claim");
        let stale_release = fixture
            .store
            .release_lifecycle(fixture.order_id, "rebalance", old)
            .await?;
        let leg = NewLeg {
            platform: "outcome",
            token_id: "stale-token",
            label: "yes",
            side: "SELL",
            intent: "rebalance",
            funder: None,
            wallet: None,
            service: None,
            req_price: Decimal::ONE,
            req_shares: Decimal::ONE,
            req_fee: Decimal::ZERO,
            client_order_id: None,
        };
        let stale_insert_failed = fixture
            .store
            .insert_leg_for_claim(fixture.order_id, "rebalance", old, &leg)
            .await
            .is_err();
        let leg_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM legs WHERE order_id = $1")
            .bind(fixture.order_id)
            .fetch_one(&fixture.store.pool)
            .await?;
        let current: Option<Uuid> =
            sqlx::query_scalar("SELECT lifecycle_claim_id FROM arb_orders WHERE id = $1")
                .bind(fixture.order_id)
                .fetch_one(&fixture.store.pool)
                .await?;
        Ok((
            first_release,
            stale_release,
            stale_insert_failed,
            leg_count + i64::from(current == Some(new)),
        ))
    }
    .await;

    fixture.cleanup().await.expect("clean up test order");
    let (first_release, stale_release, stale_insert_failed, count_plus_owner) =
        exercised.expect("exercise stale ownership");
    assert!(first_release);
    assert!(!stale_release, "old token must not clear the new claim");
    assert!(stale_insert_failed, "old token must not insert a leg");
    assert_eq!(
        count_plus_owner, 1,
        "no leg inserted and new owner retained"
    );
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn settlement_finalization_is_atomic_and_idempotent() {
    let fixture = Fixture::new().await.expect(POSTGRES_REQUIRED);

    let exercised: Result<_> = async {
        sqlx::query(
            "INSERT INTO legs
             (order_id, platform, token_id, label, side, intent, status,
              actual_shares, actual_price, actual_fee)
             VALUES
             ($1, 'polymarket', 'pm-yes', 'yes', 'BUY', 'arb_buy', 'matched', 10, 0.4, 0),
             ($1, 'outcome', '#5161', 'no', 'BUY', 'arb_buy', 'matched', 10, 0.5, 0)",
        )
        .bind(fixture.order_id)
        .execute(&fixture.store.pool)
        .await?;
        let payouts = std::collections::HashMap::from([
            (
                ("polymarket".to_string(), "pm-yes".to_string()),
                Decimal::ONE,
            ),
            (("outcome".to_string(), "#5161".to_string()), Decimal::ONE),
        ]);
        let first = fixture
            .store
            .finalize_position_settlement(
                fixture.order_id,
                "polymarket+outcome",
                &json!({"both": "settled"}),
                &payouts,
            )
            .await?;
        let second = fixture
            .store
            .finalize_position_settlement(
                fixture.order_id,
                "polymarket+outcome",
                &json!({"both": "settled"}),
                &payouts,
            )
            .await?;
        let row = sqlx::query(
            "SELECT position_status, actual_cost, actual_rev, actual_profit, settled_at
             FROM arb_orders WHERE id = $1",
        )
        .bind(fixture.order_id)
        .fetch_one(&fixture.store.pool)
        .await?;
        Ok::<_, anyhow::Error>((first, second, row))
    }
    .await;

    if let Err(err) = fixture.cleanup().await {
        eprintln!("cleanup failed: {err:#}");
    }
    let (first, second, row) = exercised.expect("settlement finalization");
    assert_eq!(
        first,
        Some((
            Decimal::new(90, 1),
            Decimal::new(200, 1),
            Decimal::new(110, 1)
        ))
    );
    assert_eq!(second, None);
    assert_eq!(
        row.try_get::<String, _>("position_status").unwrap(),
        "settled"
    );
    assert_eq!(
        row.try_get::<Decimal, _>("actual_cost").unwrap(),
        Decimal::new(90, 1)
    );
    assert_eq!(
        row.try_get::<Decimal, _>("actual_rev").unwrap(),
        Decimal::new(200, 1)
    );
    assert_eq!(
        row.try_get::<Decimal, _>("actual_profit").unwrap(),
        Decimal::new(110, 1)
    );
    assert!(row
        .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("settled_at")
        .unwrap()
        .is_some());
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn settlement_waits_for_claim_release_and_preserves_first_evidence() {
    let fixture = Fixture::new().await.expect(POSTGRES_REQUIRED);
    let first_result = json!({"winner": "yes", "round": 1});
    let second_result = json!({"winner": "no", "round": 2});

    let exercised: Result<_> = async {
        let claim_id = fixture
            .store
            .try_claim_lifecycle(fixture.order_id, "take_profit")
            .await?
            .expect("initial claim");
        let refused_while_claimed = fixture
            .store
            .mark_position_settled(fixture.order_id, "polymarket", &first_result)
            .await?;
        let retained: (Option<String>, Option<Uuid>) = sqlx::query_as(
            "SELECT lifecycle_action, lifecycle_claim_id FROM arb_orders WHERE id = $1",
        )
        .bind(fixture.order_id)
        .fetch_one(&fixture.store.pool)
        .await?;
        let released = fixture
            .store
            .release_lifecycle(fixture.order_id, "take_profit", claim_id)
            .await?;
        let first_settled = fixture
            .store
            .mark_position_settled(fixture.order_id, "polymarket", &first_result)
            .await?;
        let second_settled = fixture
            .store
            .mark_position_settled(fixture.order_id, "outcome", &second_result)
            .await?;
        let row = sqlx::query(
            "SELECT position_status, lifecycle_action, lifecycle_claim_id, settlement_source, settlement_result
             FROM arb_orders WHERE id = $1",
        )
        .bind(fixture.order_id)
        .fetch_one(&fixture.store.pool)
        .await?;
        Ok((
            refused_while_claimed,
            retained,
            released,
            first_settled,
            second_settled,
            row.try_get::<String, _>("position_status")?,
            row.try_get::<Option<String>, _>("lifecycle_action")?,
            row.try_get::<Option<Uuid>, _>("lifecycle_claim_id")?,
            row.try_get::<Option<String>, _>("settlement_source")?,
            row.try_get::<Option<Value>, _>("settlement_result")?,
        ))
    }
    .await;

    fixture.cleanup().await.expect("clean up test order");
    let (refused, retained, released, first, second, status, action, claim, source, result) =
        exercised.expect("exercise settlement claim exclusion");
    assert!(!refused, "settlement must not clear an active claim");
    assert_eq!(retained.0.as_deref(), Some("take_profit"));
    assert!(retained.1.is_some(), "claim token must be retained");
    assert!(released);
    assert!(first, "settlement may succeed after release");
    assert!(!second, "later settlement must not overwrite evidence");
    assert_eq!(status, "settled");
    assert_eq!(action, None);
    assert_eq!(claim, None);
    assert_eq!(source.as_deref(), Some("polymarket"));
    assert_eq!(result, Some(first_result));
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn closing_a_position_does_not_clear_an_active_claim() {
    let fixture = Fixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<_> = async {
        let claim_id = fixture
            .store
            .try_claim_lifecycle(fixture.order_id, "rebalance")
            .await?
            .expect("claim");
        let refused = fixture.store.mark_position_closed(fixture.order_id).await?;
        let retained: Option<Uuid> =
            sqlx::query_scalar("SELECT lifecycle_claim_id FROM arb_orders WHERE id = $1")
                .bind(fixture.order_id)
                .fetch_one(&fixture.store.pool)
                .await?;
        fixture
            .store
            .release_lifecycle(fixture.order_id, "rebalance", claim_id)
            .await?;
        let closed = fixture.store.mark_position_closed(fixture.order_id).await?;
        Ok((refused, retained, claim_id, closed))
    }
    .await;
    fixture.cleanup().await.expect("clean up test order");
    let (refused, retained, claim_id, closed) = exercised.expect("exercise close claim exclusion");
    assert!(!refused);
    assert_eq!(retained, Some(claim_id));
    assert!(closed);
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn lifecycle_counts_are_scoped_to_claim_token() {
    let fixture = Fixture::new().await.expect(POSTGRES_REQUIRED);

    let exercised: Result<((i64, i64), (i64, i64))> = async {
        let claim_id = fixture
            .store
            .try_claim_lifecycle(fixture.order_id, "rebalance")
            .await?
            .expect("claim");
        let other_claim_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO legs (order_id, platform, token_id, label, side, intent, lifecycle_claim_id, status)
             VALUES ($1, 'polymarket', $2, 'yes', 'sell', 'rebalance', $3, 'pending'),
                    ($1, 'outcome', $4, 'no', 'sell', 'rebalance', $5, 'completed')",
        )
        .bind(fixture.order_id)
        .bind(format!("test-token-{}", Uuid::new_v4()))
        .bind(claim_id)
        .bind(format!("test-token-{}", Uuid::new_v4()))
        .bind(other_claim_id)
        .execute(&fixture.store.pool)
        .await?;
        Ok((
            fixture
                .store
                .lifecycle_leg_counts(fixture.order_id, "rebalance", claim_id)
                .await?,
            fixture
                .store
                .lifecycle_leg_counts(fixture.order_id, "rebalance", other_claim_id)
                .await?,
        ))
    }
    .await;

    fixture.cleanup().await.expect("clean up test order");
    let (current, other) = exercised.expect("exercise claim-scoped counts");
    assert_eq!(current, (1, 1));
    assert_eq!(other, (1, 0));
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn order_market_identities_insert_read_backfill_and_conflict() {
    let uri = std::env::var("APP_POSTGRES_URI")
        .expect("set APP_POSTGRES_URI before running ignored PostgreSQL tests");
    assert!(!uri.trim().is_empty(), "APP_POSTGRES_URI must not be empty");
    let store = Store::connect(&uri).await.expect(POSTGRES_REQUIRED);
    store.migrate().await.expect("run migrations");

    let mut identity = MarketIdentity::new("polymarket", "condition-1").unwrap();
    identity.insert("outcome", "516").unwrap();
    let order_id = store
        .insert_order(
            TopicKey::new(Uuid::new_v4(), 0),
            &identity,
            "identity integration test",
            "identity integration test",
            None,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            &json!([]),
        )
        .await
        .expect("insert order and identities");

    let exercised: Result<_> = async {
        let stored = store.market_identities_for_order(order_id).await?;
        let idempotent = store.backfill_market_identity(order_id, &identity).await?;
        let mut extra = MarketIdentity::new("future-platform", "market-9")?;
        extra.insert("outcome", "516")?;
        let inserted = store.backfill_market_identity(order_id, &extra).await?;
        let after = store.market_identities_for_order(order_id).await?;
        let conflict = store
            .backfill_market_identity(order_id, &MarketIdentity::new("outcome", "999")?)
            .await;
        Ok((stored, idempotent, inserted, after, conflict))
    }
    .await;

    sqlx::query("DELETE FROM arb_orders WHERE id = $1")
        .bind(order_id)
        .execute(&store.pool)
        .await
        .expect("clean up test order with cascading identities");

    let (stored, idempotent, inserted, after, conflict) = exercised.expect("exercise identities");
    assert_eq!(stored.require("polymarket").unwrap(), "condition-1");
    assert_eq!(stored.require("outcome").unwrap(), "516");
    assert!(!idempotent, "same identities must be idempotent");
    assert!(inserted, "missing platform identity must be inserted");
    assert_eq!(after.require("future-platform").unwrap(), "market-9");
    assert!(conflict.is_err(), "different market_id must conflict");
}
