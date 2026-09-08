//! PostgreSQL integration coverage for lifecycle state transitions.
//!
//! These tests are intentionally ignored. Run them explicitly against a database named by
//! `APP_POSTGRES_URI`:
//! `cargo test --test state_machine -- --ignored --nocapture`
//!
//! The tests run migrations and delete only their own orders. Migration tests use private schemas
//! and drop only those schemas. They never truncate or otherwise reset the configured database.

use anyhow::{ensure, Result};
use market_arb::domain::{MarketIdentity, TopicKey};
use market_arb::store::{ArbOrderRow, NewLeg, Store};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, PgPool, Row};
use std::borrow::Cow;
use std::collections::HashMap;
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

        let order_id = insert_completed_order(&store.pool).await?;
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

async fn insert_completed_order(pool: &PgPool) -> Result<i64> {
    // A unique event_id isolates this row from application data and concurrently running tests.
    Ok(sqlx::query_scalar(
        "INSERT INTO arb_orders (event_id, unified_index, status)
         VALUES ($1, 0, 'completed') RETURNING id",
    )
    .bind(Uuid::new_v4())
    .fetch_one(pool)
    .await?)
}

async fn order_snapshot(pool: &PgPool, order_id: i64) -> Result<Value> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(o) FROM arb_orders o WHERE id = $1")
            .bind(order_id)
            .fetch_one(pool)
            .await?,
    )
}

struct MigrationFixture {
    store: Store,
    admin: PgPool,
    schema: String,
}

impl MigrationFixture {
    async fn new() -> Result<Self> {
        let uri = std::env::var("APP_POSTGRES_URI")
            .expect("set APP_POSTGRES_URI before running ignored PostgreSQL tests");
        assert!(!uri.trim().is_empty(), "APP_POSTGRES_URI must not be empty");
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&uri)
            .await?;
        // The identifier is generated here, never taken from configuration or database data.
        let schema = format!("settlement_test_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await?;
        let connection_schema = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(move |connection, _| {
                let schema = connection_schema.clone();
                Box::pin(async move {
                    // Every connection isolates both application tables and _sqlx_migrations.
                    sqlx::query("SELECT set_config('search_path', $1, false)")
                        .bind(schema)
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&uri)
            .await;
        match pool {
            Ok(pool) => Ok(Self {
                store: Store { pool },
                admin,
                schema,
            }),
            Err(err) => {
                if let Err(cleanup) = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
                    .execute(&admin)
                    .await
                {
                    eprintln!("migration schema cleanup failed: {cleanup}");
                }
                admin.close().await;
                Err(err.into())
            }
        }
    }

    async fn cleanup(self) -> Result<()> {
        self.store.pool.close().await;
        let result = sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await;
        self.admin.close().await;
        result?;
        Ok(())
    }
}

async fn position_status_width(pool: &PgPool) -> Result<i32> {
    Ok(sqlx::query_scalar(
        "SELECT character_maximum_length FROM information_schema.columns
         WHERE table_schema = current_schema()
           AND table_name = 'arb_orders' AND column_name = 'position_status'",
    )
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn fresh_migrations_support_settlement_pending() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let evidence = json!({"polymarket": "settled", "outcome": "unsettled"});
    let exercised: Result<_> = async {
        fixture.store.migrate().await?;
        let width = position_status_width(&fixture.store.pool).await?;
        let order_id = insert_completed_order(&fixture.store.pool).await?;
        let before = order_snapshot(&fixture.store.pool, order_id).await?;
        let entered = fixture
            .store
            .mark_settlement_pending(order_id, "polymarket", &evidence)
            .await?;
        let after = order_snapshot(&fixture.store.pool, order_id).await?;
        fixture.store.migrate().await?;
        let repeated = order_snapshot(&fixture.store.pool, order_id).await?;
        Ok((width, before, entered, after, repeated))
    }
    .await;
    fixture.cleanup().await.expect("clean up migration schema");
    let (width, before, entered, after, repeated) = exercised.expect("fresh migration lifecycle");
    assert_eq!(width, 32);
    assert_eq!(before["position_status"], "watching");
    assert!(entered.expect("pending transition").1);
    assert_eq!(after["position_status"], "settlement_pending");
    assert!(!after["settlement_pending_since"].is_null());
    assert_eq!(after["settlement_pending_source"], "polymarket");
    assert_eq!(after["settlement_pending_result"], evidence);
    assert_eq!(repeated, after);
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn migration_0009_upgrades_0008_preserving_rows() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let all = sqlx::migrate!("./migrations");
    let prefix = Migrator {
        migrations: Cow::Owned(all.iter().filter(|m| m.version <= 8).cloned().collect()),
        ..Migrator::DEFAULT
    };
    let evidence = json!({"polymarket": "settled", "outcome": "unsettled"});
    let exercised: Result<_> = async {
        prefix.run(&fixture.store.pool).await?;
        let old_width = position_status_width(&fixture.store.pool).await?;
        let order_id = insert_completed_order(&fixture.store.pool).await?;
        let before = order_snapshot(&fixture.store.pool, order_id).await?;
        let old_pending = fixture
            .store
            .mark_settlement_pending(order_id, "polymarket", &evidence)
            .await;
        ensure!(
            matches!(&old_pending, Err(market_arb::error::Error::Sqlx(sqlx::Error::Database(err)))
                if err.code().as_deref() == Some("22001")),
            "0008 must reject the overlong pending status: {old_pending:?}"
        );
        ensure!(
            order_snapshot(&fixture.store.pool, order_id).await? == before,
            "failed pending transition must preserve the original order"
        );
        let old_migrations: Vec<(i64, Vec<u8>)> =
            sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&fixture.store.pool)
                .await?;
        fixture.store.migrate().await?;
        let new_width = position_status_width(&fixture.store.pool).await?;
        let upgraded = order_snapshot(&fixture.store.pool, order_id).await?;
        let preserved: Vec<(i64, Vec<u8>)> = sqlx::query_as(
            "SELECT version, checksum FROM _sqlx_migrations WHERE version <= 8 ORDER BY version",
        )
        .fetch_all(&fixture.store.pool)
        .await?;
        let entered = fixture
            .store
            .mark_settlement_pending(order_id, "polymarket", &evidence)
            .await?;
        let after = order_snapshot(&fixture.store.pool, order_id).await?;
        fixture.store.migrate().await?;
        let repeated = order_snapshot(&fixture.store.pool, order_id).await?;
        let rollback = prefix.run(&fixture.store.pool).await;
        Ok((
            old_width,
            new_width,
            before,
            upgraded,
            old_migrations,
            preserved,
            entered,
            after,
            repeated,
            rollback,
        ))
    }
    .await;
    fixture.cleanup().await.expect("clean up migration schema");
    let (
        old_width,
        new_width,
        before,
        upgraded,
        old_migrations,
        preserved,
        entered,
        after,
        repeated,
        rollback,
    ) = exercised.expect("upgrade from original 0008");
    assert_eq!(old_width, 16);
    assert_eq!(new_width, 32);
    assert_eq!(before["position_status"], "watching");
    assert_eq!(upgraded, before);
    assert_eq!(old_migrations.len(), 8);
    assert_eq!(preserved, old_migrations);
    assert!(entered.expect("pending transition after upgrade").1);
    assert_eq!(after["position_status"], "settlement_pending");
    assert_eq!(after["settlement_pending_result"], evidence);
    assert_eq!(repeated, after);
    assert!(matches!(
        rollback,
        Err(sqlx::migrate::MigrateError::VersionMissing(9))
    ));
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn watching_and_pending_scans_partition_the_position_set() {
    // A private schema keeps the scan sets deterministic; the shared database holds live orders.
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let evidence = json!({"outcome": "settled", "polymarket": "unavailable"});
    let exercised: Result<_> = async {
        fixture.store.migrate().await?;
        let watching = insert_completed_order(&fixture.store.pool).await?;
        let pending = insert_completed_order(&fixture.store.pool).await?;
        let entered = fixture
            .store
            .mark_settlement_pending(pending, "outcome", &evidence)
            .await?;
        ensure!(
            entered.is_some_and(|(_, entered)| entered),
            "second order must enter pending"
        );
        let watching_ids = scan_ids(fixture.store.completed_unbalanced_orders(0, 10).await?);
        let pending_ids = scan_ids(fixture.store.settlement_pending_orders(0, 10).await?);
        // 每个集合各自回绕：一侧的游标不会跳过或提前消耗另一侧的行。
        let watching_wrapped = scan_ids(
            fixture
                .store
                .completed_unbalanced_orders(watching, 10)
                .await?,
        );
        let pending_wrapped = scan_ids(fixture.store.settlement_pending_orders(pending, 10).await?);
        Ok((
            watching,
            pending,
            watching_ids,
            pending_ids,
            watching_wrapped,
            pending_wrapped,
        ))
    }
    .await;
    fixture.cleanup().await.expect("clean up migration schema");
    let (watching, pending, watching_ids, pending_ids, watching_wrapped, pending_wrapped) =
        exercised.expect("partition watching and pending scans");
    assert_eq!(watching_ids, vec![watching]);
    assert_eq!(pending_ids, vec![pending]);
    assert_eq!(watching_wrapped, vec![watching]);
    assert_eq!(pending_wrapped, vec![pending]);
}

fn scan_ids(rows: Vec<ArbOrderRow>) -> Vec<i64> {
    rows.into_iter().map(|row| row.id).collect()
}

fn identity_probe_leg() -> NewLeg<'static> {
    NewLeg {
        platform: "polymarket",
        token_id: "probe-token",
        label: "yes",
        side: "BUY",
        intent: "arb_buy",
        funder: None,
        wallet: None,
        service: None,
        req_price: Decimal::ONE,
        req_shares: Decimal::ONE,
        req_fee: Decimal::ZERO,
        client_order_id: None,
    }
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn actived_order_and_initial_legs_are_never_visible_without_each_other() {
    // `mark_orders_complete` 扫全表，必须在私有 schema 里跑，避免碰到真实订单。
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let identity = MarketIdentity::new("polymarket", "condition-atomic").unwrap();
    let exercised: Result<_> = async {
        fixture.store.migrate().await?;
        let legs = [
            NewLeg {
                platform: "polymarket",
                token_id: "pm-yes",
                label: "yes",
                side: "BUY",
                intent: "arb_buy",
                funder: Some("0xfunder"),
                wallet: Some("0xfunder"),
                service: None,
                req_price: Decimal::new(4, 1),
                req_shares: Decimal::from(10),
                req_fee: Decimal::ZERO,
                client_order_id: None,
            },
            NewLeg {
                platform: "outcome",
                token_id: "#5161",
                label: "no",
                side: "BUY",
                intent: "arb_buy",
                funder: None,
                wallet: Some("0xwallet"),
                service: None,
                req_price: Decimal::new(55, 2),
                req_shares: Decimal::from(10),
                req_fee: Decimal::ZERO,
                client_order_id: None,
            },
        ];
        let (order_id, leg_ids) = fixture
            .store
            .insert_actived_order_with_legs(
                TopicKey::new(Uuid::new_v4(), 0),
                &identity,
                "atomic creation",
                "atomic creation",
                None,
                Decimal::from(10),
                Decimal::new(5, 1),
                Decimal::new(95, 1),
                &json!([]),
                &legs,
            )
            .await?;
        let created = order_snapshot(&fixture.store.pool, order_id).await?;
        // 回填任务在建档后立刻跑一轮：新单必须因为持有未完成腿而不被取消。
        market_arb::exec::mark_orders_complete(&fixture.store).await?;
        let after_reconcile = order_snapshot(&fixture.store.pool, order_id).await?;
        let leg_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM legs WHERE order_id = $1 AND status = 'pending'",
        )
        .bind(order_id)
        .fetch_one(&fixture.store.pool)
        .await?;
        let rejected_without_legs = fixture
            .store
            .insert_actived_order_with_legs(
                TopicKey::new(Uuid::new_v4(), 0),
                &identity,
                "atomic creation",
                "atomic creation",
                None,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                &json!([]),
                &[],
            )
            .await;
        let orphan_actived: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM arb_orders o
             WHERE o.status = 'actived'
               AND NOT EXISTS (SELECT 1 FROM legs l WHERE l.order_id = o.id)",
        )
        .fetch_one(&fixture.store.pool)
        .await?;
        Ok((
            leg_ids,
            created,
            after_reconcile,
            leg_count,
            rejected_without_legs,
            orphan_actived,
        ))
    }
    .await;
    fixture.cleanup().await.expect("clean up migration schema");
    let (leg_ids, created, after_reconcile, leg_count, rejected_without_legs, orphan_actived) =
        exercised.expect("atomic order creation");
    assert_eq!(leg_ids.len(), 2);
    assert_eq!(created["status"], "actived");
    assert_eq!(leg_count, 2);
    assert_eq!(
        after_reconcile, created,
        "a freshly created order must survive an immediate reconcile pass"
    );
    assert!(
        rejected_without_legs.is_err(),
        "an actived order without legs must not be creatable"
    );
    assert_eq!(orphan_actived, 0);
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
async fn settlement_pending_is_durable_blocks_claim_and_can_finalize() {
    let fixture = Fixture::new().await.expect(POSTGRES_REQUIRED);
    let first_result = json!({"polymarket": "settled", "outcome": "unsettled"});
    let second_result = json!({"polymarket": "settled", "outcome": "timeout"});
    let exercised: Result<_> = async {
        sqlx::query(
            "INSERT INTO legs
             (order_id, platform, token_id, label, side, intent, status,
              actual_shares, actual_price, actual_fee)
             VALUES
             ($1, 'polymarket', 'pm-yes', 'yes', 'BUY', 'arb_buy', 'matched', 10, 0.4, 0),
             ($1, 'outcome', '#5161', 'no', 'BUY', 'arb_buy', 'matched', 10, 0.55, 0)",
        )
        .bind(fixture.order_id)
        .execute(&fixture.store.pool)
        .await?;
        let first = fixture
            .store
            .mark_settlement_pending(fixture.order_id, "polymarket", &first_result)
            .await?;
        let second = fixture
            .store
            .mark_settlement_pending(fixture.order_id, "polymarket", &second_result)
            .await?;
        let claim = fixture
            .store
            .try_claim_lifecycle(fixture.order_id, "rebalance")
            .await?;
        // pending 订单只出现在结算清扫集合里，不再占用活跃订单的扫描配额。
        let pending_scanned = fixture.store.settlement_pending_orders(0, 20).await?;
        let watching_scanned = fixture.store.completed_unbalanced_orders(0, 20).await?;
        let before: (
            String,
            Option<Decimal>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<Value>,
        ) = sqlx::query_as(
            "SELECT position_status, NULLIF(actual_rev, 0), settled_at, settlement_pending_result
             FROM arb_orders WHERE id = $1",
        )
        .bind(fixture.order_id)
        .fetch_one(&fixture.store.pool)
        .await?;
        let payouts = std::collections::HashMap::from([
            (
                ("polymarket".to_string(), "pm-yes".to_string()),
                Decimal::ONE,
            ),
            (
                ("outcome".to_string(), "#5161".to_string()),
                Decimal::new(5, 1),
            ),
        ]);
        let finalized = fixture
            .store
            .finalize_position_settlement(
                fixture.order_id,
                "polymarket+outcome",
                &json!({"both": "settled"}),
                &payouts,
            )
            .await?;
        let after: (String, Option<chrono::DateTime<chrono::Utc>>, Option<Value>) = sqlx::query_as(
            "SELECT position_status, settlement_pending_since, settlement_pending_result
             FROM arb_orders WHERE id = $1",
        )
        .bind(fixture.order_id)
        .fetch_one(&fixture.store.pool)
        .await?;
        Ok((
            first,
            second,
            claim,
            pending_scanned,
            watching_scanned,
            before,
            finalized,
            after,
        ))
    }
    .await;
    fixture.cleanup().await.expect("clean up test order");
    let (first, second, claim, pending_scanned, watching_scanned, before, finalized, after) =
        exercised.expect("settlement pending lifecycle");
    let (first_since, entered) = first.expect("entered pending");
    assert!(entered);
    assert_eq!(second, Some((first_since, false)));
    assert_eq!(claim, None);
    assert!(
        pending_scanned
            .iter()
            .any(|order| order.id == fixture.order_id
                && order.position_status == "settlement_pending")
    );
    assert!(!watching_scanned
        .iter()
        .any(|order| order.id == fixture.order_id));
    assert_eq!(
        before,
        (
            "settlement_pending".into(),
            None,
            None,
            Some(first_result.clone())
        )
    );
    assert_eq!(
        finalized,
        Some((
            Decimal::new(95, 1),
            Decimal::new(150, 1),
            Decimal::new(55, 1)
        ))
    );
    assert_eq!(
        after,
        ("settled".into(), Some(first_since), Some(first_result))
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
async fn settlement_finalization_errors_preserve_state_and_can_retry() {
    for pending in [false, true] {
        for negative in [false, true] {
            let fixture = Fixture::new().await.expect(POSTGRES_REQUIRED);
            let pending_evidence = json!({"outcome": "settled", "polymarket": "unavailable"});
            let final_evidence = json!({"both": "settled"});
            let exercised: Result<_> = async {
                sqlx::query(
                    "INSERT INTO legs
                     (order_id, platform, token_id, label, side, intent, status,
                      actual_shares, actual_price, actual_fee)
                     VALUES ($1, 'outcome', '#5160', 'yes', $2, 'arb_buy', 'matched', 1, 0.4, 0)",
                )
                .bind(fixture.order_id)
                .bind(if negative { "SELL" } else { "BUY" })
                .execute(&fixture.store.pool)
                .await?;
                if pending {
                    let entered = fixture
                        .store
                        .mark_settlement_pending(fixture.order_id, "outcome", &pending_evidence)
                        .await?;
                    ensure!(
                        entered.is_some_and(|(_, entered)| entered),
                        "must enter pending"
                    );
                }
                let before = order_snapshot(&fixture.store.pool, fixture.order_id).await?;
                let payout_key = ("outcome".to_string(), "#5160".to_string());
                let mut payouts = HashMap::new();
                if negative {
                    payouts.insert(payout_key.clone(), Decimal::new(5, 1));
                }
                let rejected = fixture
                    .store
                    .finalize_position_settlement(
                        fixture.order_id,
                        "polymarket+outcome",
                        &final_evidence,
                        &payouts,
                    )
                    .await;
                let expected_error = if negative {
                    "negative settled position for outcome:#5160"
                } else {
                    "missing settlement payout for outcome:#5160"
                };
                ensure!(
                    matches!(&rejected, Err(err) if err.to_string().contains(expected_error)),
                    "expected {expected_error}, got {rejected:?}"
                );
                let after_error = order_snapshot(&fixture.store.pool, fixture.order_id).await?;
                ensure!(
                    after_error == before,
                    "rejected finalization must not update the order"
                );

                if negative {
                    sqlx::query("UPDATE legs SET side = 'BUY' WHERE order_id = $1")
                        .bind(fixture.order_id)
                        .execute(&fixture.store.pool)
                        .await?;
                } else {
                    payouts.insert(payout_key, Decimal::new(5, 1));
                }
                let finalized = fixture
                    .store
                    .finalize_position_settlement(
                        fixture.order_id,
                        "polymarket+outcome",
                        &final_evidence,
                        &payouts,
                    )
                    .await?;
                let after = order_snapshot(&fixture.store.pool, fixture.order_id).await?;
                let repeated = fixture
                    .store
                    .finalize_position_settlement(
                        fixture.order_id,
                        "polymarket+outcome",
                        &json!({"retry": "must not replace evidence"}),
                        &payouts,
                    )
                    .await?;
                let after_repeat = order_snapshot(&fixture.store.pool, fixture.order_id).await?;
                Ok((before, finalized, after, repeated, after_repeat))
            }
            .await;
            fixture.cleanup().await.expect("clean up test order");
            let (before, finalized, after, repeated, after_repeat) =
                exercised.unwrap_or_else(|err| {
                    panic!("finalization pending={pending} negative={negative}: {err:#}")
                });
            assert_eq!(
                before["position_status"],
                if pending {
                    "settlement_pending"
                } else {
                    "watching"
                }
            );
            assert_eq!(
                finalized,
                Some((Decimal::new(4, 1), Decimal::new(5, 1), Decimal::new(1, 1)))
            );
            assert_eq!(after["position_status"], "settled");
            assert!(!after["settled_at"].is_null());
            assert_eq!(after["settlement_source"], "polymarket+outcome");
            assert_eq!(after["settlement_result"], final_evidence);
            for field in [
                "settlement_pending_since",
                "settlement_pending_source",
                "settlement_pending_result",
            ] {
                assert_eq!(
                    after[field], before[field],
                    "first pending evidence: {field}"
                );
            }
            if pending {
                assert_eq!(after["settlement_pending_result"], pending_evidence);
            }
            assert_eq!(repeated, None);
            assert_eq!(after_repeat, after);
        }
    }
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
        let refused = fixture
            .store
            .finalize_closed_position(fixture.order_id)
            .await?;
        let retained: Option<Uuid> =
            sqlx::query_scalar("SELECT lifecycle_claim_id FROM arb_orders WHERE id = $1")
                .bind(fixture.order_id)
                .fetch_one(&fixture.store.pool)
                .await?;
        fixture
            .store
            .release_lifecycle(fixture.order_id, "rebalance", claim_id)
            .await?;
        let closed = fixture
            .store
            .finalize_closed_position(fixture.order_id)
            .await?;
        Ok((refused, retained, claim_id, closed))
    }
    .await;
    fixture.cleanup().await.expect("clean up test order");
    let (refused, retained, claim_id, closed) = exercised.expect("exercise close claim exclusion");
    assert_eq!(refused, None);
    assert_eq!(retained, Some(claim_id));
    assert_eq!(closed, Some((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)));
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn closing_after_rebalance_sell_refreshes_actuals_and_completes_rebalance() {
    let fixture = Fixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<_> = async {
        sqlx::query(
            "INSERT INTO legs
             (order_id, platform, token_id, label, side, intent, status,
              actual_shares, actual_price, actual_fee)
             VALUES
             ($1, 'outcome', '#5160', 'yes', 'BUY', 'arb_buy', 'matched', 10, 0.4, 0),
             ($1, 'outcome', '#5160', 'yes', 'SELL', 'rebalance', 'matched', 10, 0.5, 0)",
        )
        .bind(fixture.order_id)
        .execute(&fixture.store.pool)
        .await?;
        fixture
            .store
            .update_actuals(
                fixture.order_id,
                Decimal::from(4),
                Decimal::ZERO,
                Decimal::from(-4),
            )
            .await?;
        fixture
            .store
            .mark_rebalance(fixture.order_id, "actived")
            .await?;
        let finalized = fixture
            .store
            .finalize_closed_position(fixture.order_id)
            .await?;
        let row: (
            String,
            String,
            Decimal,
            Decimal,
            Decimal,
            Option<chrono::DateTime<chrono::Utc>>,
        ) = sqlx::query_as(
            "SELECT position_status, rebalance_status, actual_cost, actual_rev,
                        actual_profit, rebalanced_at
                 FROM arb_orders WHERE id = $1",
        )
        .bind(fixture.order_id)
        .fetch_one(&fixture.store.pool)
        .await?;
        Ok((finalized, row))
    }
    .await;
    fixture.cleanup().await.expect("clean up test order");
    let (finalized, row) = exercised.expect("finalize zero position after rebalance sell");
    assert_eq!(
        finalized,
        Some((Decimal::from(4), Decimal::from(5), Decimal::ONE))
    );
    assert_eq!(row.0, "closed");
    assert_eq!(row.1, "completed");
    assert_eq!(
        (row.2, row.3, row.4),
        (Decimal::from(4), Decimal::from(5), Decimal::ONE)
    );
    assert!(row.5.is_some());
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
    let (order_id, _) = store
        .insert_actived_order_with_legs(
            TopicKey::new(Uuid::new_v4(), 0),
            &identity,
            "identity integration test",
            "identity integration test",
            None,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            &json!([]),
            &[identity_probe_leg()],
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

    sqlx::query("DELETE FROM legs WHERE order_id = $1")
        .bind(order_id)
        .execute(&store.pool)
        .await
        .expect("clean up test legs");
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
