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
use market_arb::platforms::{FillFinality, OrderPoll, TradeFill};
use market_arb::reconcile::{FillEvidence, LegResolution};
use market_arb::store::{ArbOrderRow, LegRow, NewLeg, Store};
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

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_pm_calculated_fee_retains_snapshot_without_fabricating_reported_fee() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        let (order_id, legs) = submitted_reconciliation_order(store, &["polymarket"]).await?;
        let mut evidence = reconciliation_evidence("pm-fee-oid", "matched");
        evidence.poll.associated_trades = vec!["fee-trade".into()];
        evidence.poll.raw = json!({"associate_trades":["fee-trade"]});
        let current = store
            .record_order_poll(&legs[0], &evidence.poll)
            .await?
            .ok_or_else(|| anyhow::anyhow!("fee fixture lookup rejected"))?;
        let mut fill = reconciliation_trade("pm-fee-oid", "fee-trade", 10, Decimal::ZERO);
        fill.fee = None;
        fill.fee_token = None;
        fill.raw = json!({"role":"taker","fee_calculation":{
            "source":"clob-markets","condition_id":"reconciliation-market","rate":"0.07",
            "observed_at_ms":123,"currency":"pUSD","valuation":"1 USD",
            "rounding":"midpoint_away_from_zero_5dp"
        }});
        let result = store
            .record_reconciliation(&current, &[fill], &evidence, &json!({}))
            .await?;
        ensure!(matches!(result,LegResolution::Terminal{fee,..} if fee==Decimal::new(168,3)));
        let stored = fill_snapshots(&store.pool, current.id).await?;
        ensure!(stored.len() == 1 && stored[0]["fee"].is_null());
        ensure!(stored[0].pointer("/raw/accounting/source") == Some(&json!("calculated")));
        ensure!(
            stored[0].pointer("/raw/reconciliation_v1/raw/fee_calculation/rate")
                == Some(&json!("0.07"))
        );
        let parent = order_snapshot(&store.pool, order_id).await?;
        ensure!(parent["status"] == "completed" && parent["actual_cost"] == json!(4.168));
        Ok(())
    }
    .await;
    fixture.cleanup().await.expect("clean up fee schema");
    exercised.expect("calculated fee uses documented rate-only formula and preserves provenance");
}

// Reconciliation fixtures always use MigrationFixture's private schema, including fault injection.
async fn submitted_reconciliation_order(
    store: &Store,
    platforms: &[&str],
) -> Result<(i64, Vec<LegRow>)> {
    let mut identity = MarketIdentity::new(platforms[0], "reconciliation-market")?;
    for platform in &platforms[1..] {
        identity.insert(*platform, "reconciliation-market")?;
    }
    let legs: Vec<_> = platforms
        .iter()
        .enumerate()
        .map(|(index, platform)| NewLeg {
            platform,
            token_id: if index == 0 { "test-yes" } else { "test-no" },
            label: if index == 0 { "yes" } else { "no" },
            req_price: Decimal::new(4, 1),
            req_shares: Decimal::from(10),
            // Deliberately unrelated to actual trade fees: request estimates must not enter accounting.
            req_fee: Decimal::from(99),
            ..identity_probe_leg()
        })
        .collect();
    let (order_id, ids) = store
        .insert_actived_order_with_legs(
            TopicKey::new(Uuid::new_v4(), 0),
            &identity,
            "reconciliation integration test",
            "reconciliation integration test",
            None,
            Decimal::from(10),
            Decimal::ONE,
            Decimal::from(9),
            &json!([]),
            &legs,
        )
        .await?;
    for id in &ids {
        let client_id = format!("reconciliation-client-{id}");
        store
            .insert_envelope(
                *id,
                &client_id,
                &json!({"cloid": client_id}),
                &json!({"test": "unsigned fixture"}),
                None,
            )
            .await?;
    }
    let rows = store
        .open_legs()
        .await?
        .into_iter()
        .filter(|leg| ids.contains(&leg.id))
        .collect();
    Ok((order_id, rows))
}

async fn reconciliation_open_leg(store: &Store, id: i64) -> Result<LegRow> {
    store
        .open_legs()
        .await?
        .into_iter()
        .find(|leg| leg.id == id)
        .ok_or_else(|| anyhow::anyhow!("expected open leg {id}"))
}

async fn leg_snapshot(pool: &PgPool, leg_id: i64) -> Result<Value> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(l) FROM legs l WHERE id = $1")
            .bind(leg_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn fill_snapshots(pool: &PgPool, leg_id: i64) -> Result<Vec<Value>> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(f) FROM fills f WHERE leg_id = $1 ORDER BY trade_id")
            .bind(leg_id)
            .fetch_all(pool)
            .await?,
    )
}

fn reconciliation_trade(oid: &str, trade_id: &str, shares: i64, fee: Decimal) -> TradeFill {
    TradeFill {
        trade_id: trade_id.into(),
        order_id: Some(oid.into()),
        order_ids: vec![oid.into()],
        coin: None,
        shares: Decimal::from(shares),
        price: Decimal::new(4, 1),
        fee: Some(fee),
        fee_rate_bps: None,
        fee_token: Some("USDC".into()),
        finality: FillFinality::Confirmed,
        raw: json!({"oid": oid, "tid": trade_id}),
    }
}

fn reconciliation_evidence(oid: &str, status: &str) -> FillEvidence {
    FillEvidence {
        poll: OrderPoll {
            found: true,
            status: status.into(),
            order_id: Some(oid.into()),
            shares: Some(Decimal::from(10)),
            original_shares: Some(Decimal::from(10)),
            raw: json!({"status": status}),
            ..OrderPoll::default()
        },
        page_complete: true,
        history_complete: true,
        expected_shares: None,
    }
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_ack_waits_for_real_outcome_fills_and_actual_fees() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        let (order_id, legs) =
            submitted_reconciliation_order(store, &["outcome", "polymarket"]).await?;
        for leg in &legs {
            store
                .record_submission(
                    leg.id,
                    "actived",
                    Some(&format!("oid-{}", leg.id)),
                    &json!({"kind":"ack","expected_shares":"10"}),
                    &json!({"filled":{"totalSz":"10","avgPx":"0.4"}}),
                )
                .await?;
            let ack = leg_snapshot(&store.pool, leg.id).await?;
            ensure!(ack["status"] == "actived" && ack["actual_shares"].is_null());
            ensure!(!ack["submitted_at"].is_null());
            ensure!(
                fill_snapshots(&store.pool, leg.id).await?.is_empty(),
                "ACK must not create a fill"
            );
        }
        store.complete_orders().await?;
        ensure!(store.open_legs().await?.len() == 2);
        ensure!(order_snapshot(&store.pool, order_id).await?["status"] == "actived");

        // Emulate an old release's placeholder only inside the private test schema.
        sqlx::query(
            "INSERT INTO fills(leg_id,third_order_id,trade_id,shares,price,fee,raw)
                     VALUES($1,$2,'ack:legacy',999,0.99,99,'{}')",
        )
        .bind(legs[0].id)
        .bind(format!("oid-{}", legs[0].id))
        .execute(&store.pool)
        .await?;
        for (index, leg) in legs.iter().enumerate() {
            let oid = format!("oid-{}", leg.id);
            let mut evidence = reconciliation_evidence(&oid, "filled");
            if leg.platform == "polymarket" {
                evidence.poll.status = "matched".into();
                evidence.poll.associated_trades = vec!["real-trade".into()];
                evidence.poll.raw = json!({"associate_trades":["real-trade"]});
            }
            let current = reconciliation_open_leg(store, leg.id).await?;
            let mut current = store
                .record_order_poll(&current, &evidence.poll)
                .await?
                .ok_or_else(|| anyhow::anyhow!("fresh order poll rejected"))?;
            let fee = if index == 0 {
                Decimal::new(17, 2)
            } else {
                Decimal::new(3, 2)
            };
            let mut fill = reconciliation_trade(&oid, "real-trade", 10, fee);
            if index == 0 {
                fill.fee = None;
                fill.fee_rate_bps = Some(Decimal::from(500));
                let missing_fee = store
                    .record_reconciliation(
                        &current,
                        &[fill.clone()],
                        &evidence,
                        &json!({"cursor":"fees-pending"}),
                    )
                    .await?;
                ensure!(missing_fee == LegResolution::Pending("fee_evidence_missing"));
                ensure!(leg_snapshot(&store.pool, leg.id).await?["actual_shares"].is_null());
                current = reconciliation_open_leg(store, leg.id).await?;
                fill.fee = Some(fee);
            } else {
                fill.price = Decimal::new(55, 2);
            }
            let resolved = store
                .record_reconciliation(&current, &[fill], &evidence, &json!({"complete":true}))
                .await?;
            ensure!(matches!(resolved, LegResolution::Terminal {
                status: "matched", shares, fee: actual_fee, ref fee_sources, ..
            } if shares == Decimal::from(10) && actual_fee == fee && fee_sources == &["actual"]));
            if index == 0 {
                ensure!(
                    order_snapshot(&store.pool, order_id).await?["status"] == "actived",
                    "parent must wait for its second leg"
                );
            }
        }
        let parent = order_snapshot(&store.pool, order_id).await?;
        ensure!(parent["status"] == "completed" && !parent["completed_at"].is_null());
        ensure!(
            parent["actual_cost"] == json!(9.7)
                && parent["actual_rev"] == json!(10.0)
                && parent["actual_profit"] == json!(0.3),
            "actual accounting: {parent}"
        );
        ensure!(store.open_legs().await?.is_empty());
        let fills = fill_snapshots(&store.pool, legs[0].id).await?;
        ensure!(fills.len() == 2 && fills[0]["trade_id"] == "ack:legacy");
        let terminal = leg_snapshot(&store.pool, legs[0].id).await?;
        ensure!(terminal["actual_shares"] == json!(10.0) && terminal["actual_fee"] == json!(0.17));
        store.complete_orders().await?;
        ensure!(order_snapshot(&store.pool, order_id).await? == parent);
        Ok(())
    }
    .await;
    fixture
        .cleanup()
        .await
        .expect("clean up reconciliation schema");
    exercised.expect("ACK remains open until real fills and actual fees close both legs");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_pm_accumulates_pages_and_only_accounts_confirmed_trades() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        let (order_id, legs) = submitted_reconciliation_order(store, &["polymarket"]).await?;
        let id = legs[0].id;
        store.record_submission(id, "actived", Some("pm-oid"), &json!({"kind":"ack"}), &json!({"ack":true})).await?;
        let mut evidence = reconciliation_evidence("pm-oid", "matched");
        evidence.poll.associated_trades = vec!["a".into(), "b".into(), "c".into()];
        evidence.poll.raw = json!({"associate_trades":["a","b","c"]});
        evidence.page_complete = false;
        let current = reconciliation_open_leg(store, id).await?;
        let current = store.record_order_poll(&current, &evidence.poll).await?
            .ok_or_else(|| anyhow::anyhow!("fresh PM poll rejected"))?;
        let mut a = reconciliation_trade("pm-oid", "a", 4, Decimal::new(2, 2));
        let mut b = reconciliation_trade("pm-oid", "b", 3, Decimal::from(99));
        let mut c = reconciliation_trade("pm-oid", "c", 3, Decimal::from(99));
        a.finality = FillFinality::Pending;
        b.finality = FillFinality::Pending;
        let pending_a = a.clone();
        ensure!(matches!(store.record_reconciliation(&current, &[a.clone(), b.clone()], &evidence,
            &json!({"cursor":"page-2"})).await?, LegResolution::Pending(_)));
        ensure!(leg_snapshot(&store.pool, id).await?["actual_shares"].is_null());
        a.finality = FillFinality::Confirmed;
        let current = reconciliation_open_leg(store, id).await?;
        ensure!(current.last_order_info.as_ref().unwrap()["fill_progress"]["cursor"] == "page-2");
        ensure!(matches!(store.record_reconciliation(&current, &[a.clone(), a.clone(), b.clone()], &evidence,
            &json!({"cursor":"page-3"})).await?, LegResolution::Pending(_)));
        ensure!(fill_snapshots(&store.pool, id).await?.len() == 2, "repeated page/trade must upsert once");
        c.finality = FillFinality::Failed;
        evidence.page_complete = true;
        let current = reconciliation_open_leg(store, id).await?;
        ensure!(matches!(store.record_reconciliation(&current, &[c], &evidence,
            &json!({"complete":true})).await?, LegResolution::Pending(_)));
        ensure!(order_snapshot(&store.pool, order_id).await?["status"] == "actived");
        b.finality = FillFinality::Failed;
        let current = reconciliation_open_leg(store, id).await?;
        let resolved = store.record_reconciliation(&current, &[b, pending_a], &evidence,
            &json!({"complete":true})).await?;
        ensure!(matches!(resolved, LegResolution::Terminal {status:"matched", shares, fee, ..}
            if shares == Decimal::from(4) && fee == Decimal::new(2, 2)));
        let fills = fill_snapshots(&store.pool, id).await?;
        ensure!(fills.len() == 3);
        ensure!(fills[0]["raw"]["reconciliation_v1"]["finality"] == "confirmed",
            "late pending observation must not downgrade confirmed trade");
        ensure!(fills[1]["raw"]["reconciliation_v1"]["finality"] == "failed"
            && fills[2]["raw"]["reconciliation_v1"]["finality"] == "failed");
        let parent = order_snapshot(&store.pool, order_id).await?;
        ensure!(parent["status"] == "completed" && parent["actual_cost"] == json!(1.62));

        let (failed_parent, failed_legs) = submitted_reconciliation_order(store, &["polymarket"]).await?;
        let mut evidence = reconciliation_evidence("pm-failed", "matched");
        evidence.poll.associated_trades = vec!["failed".into()];
        evidence.poll.raw = json!({"associate_trades":["failed"]});
        let leg = store.record_order_poll(&failed_legs[0], &evidence.poll).await?
            .ok_or_else(|| anyhow::anyhow!("all-failed poll rejected"))?;
        let mut fill = reconciliation_trade("pm-failed", "failed", 10, Decimal::from(99));
        fill.finality = FillFinality::Failed;
        ensure!(matches!(store.record_reconciliation(&leg, &[fill], &evidence, &json!({})).await?,
            LegResolution::Terminal {status:"failed", shares, fee, ..} if shares.is_zero() && fee.is_zero()));
        let parent = order_snapshot(&store.pool, failed_parent).await?;
        ensure!(parent["status"] == "cancelled" && parent["actual_cost"] == json!(0.0));
        Ok(())
    }.await;
    fixture
        .cleanup()
        .await
        .expect("clean up reconciliation schema");
    exercised.expect(
        "PM observations accumulate monotonically without counting failed or duplicate fills",
    );
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_rejects_stale_snapshots_and_audits_late_submission_responses() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        let (order_id, legs) = submitted_reconciliation_order(store, &["outcome"]).await?;
        let stale = &legs[0];
        let evidence = reconciliation_evidence("durable-oid", "filled");
        let current = store
            .record_order_poll(stale, &evidence.poll)
            .await?
            .ok_or_else(|| anyhow::anyhow!("fresh poll rejected"))?;
        let before = leg_snapshot(&store.pool, stale.id).await?;
        ensure!(current.updated_at != stale.updated_at);
        let mut old_poll = evidence.poll.clone();
        old_poll.status = "open".into();
        ensure!(store.record_order_poll(stale, &old_poll).await?.is_none());
        let old_fill =
            reconciliation_trade("durable-oid", "old-observation", 10, Decimal::from(99));
        ensure!(
            store
                .record_reconciliation(stale, &[old_fill], &evidence, &json!({"cursor":"old"}))
                .await?
                == LegResolution::Pending("stale_leg_snapshot")
        );
        ensure!(leg_snapshot(&store.pool, stale.id).await? == before);
        ensure!(fill_snapshots(&store.pool, stale.id).await?.is_empty());
        let fill = reconciliation_trade("durable-oid", "confirmed", 10, Decimal::new(5, 2));
        ensure!(matches!(
            store
                .record_reconciliation(&current, &[fill], &evidence, &json!({"complete":true}))
                .await?,
            LegResolution::Terminal {
                status: "matched",
                ..
            }
        ));
        let terminal = leg_snapshot(&store.pool, stale.id).await?;
        let parent = order_snapshot(&store.pool, order_id).await?;
        let fills = fill_snapshots(&store.pool, stale.id).await?;
        for (status, kind) in [
            ("actived", "ack"),
            ("unknown", "unknown"),
            ("cancelled", "no_match"),
            ("failed", "rejected"),
        ] {
            let response = json!({"late":kind,"raw_response":"retained"});
            store
                .record_submission(
                    stale.id,
                    status,
                    Some("durable-oid"),
                    &json!({"kind":kind}),
                    &response,
                )
                .await?;
            let audited: Value =
                sqlx::query_scalar("SELECT submit_response FROM signed_envelopes WHERE leg_id=$1")
                    .bind(stale.id)
                    .fetch_one(&store.pool)
                    .await?;
            ensure!(
                audited == response,
                "late {kind} response must remain auditable"
            );
            ensure!(
                leg_snapshot(&store.pool, stale.id).await? == terminal,
                "late {kind} changed terminal leg"
            );
            ensure!(order_snapshot(&store.pool, order_id).await? == parent);
            ensure!(fill_snapshots(&store.pool, stale.id).await? == fills);
        }
        Ok(())
    }
    .await;
    fixture
        .cleanup()
        .await
        .expect("clean up reconciliation schema");
    exercised
        .expect("stale workers and late submission responses cannot overwrite terminal accounting");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_recovered_oid_is_durable_and_matches_same_round_fills_without_cloid() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        let (order_id, legs) = submitted_reconciliation_order(store, &["outcome"]).await?;
        let id = legs[0].id;
        store
            .record_submission(
                id,
                "unknown",
                None,
                &json!({"kind":"unknown"}),
                &json!({"timeout":true}),
            )
            .await?;
        let before = reconciliation_open_leg(store, id).await?;
        ensure!(before.third_order_id.is_none());
        let mut evidence = reconciliation_evidence("recovered-oid", "filled");
        evidence.poll.client_order_id = before.client_order_id.clone();
        evidence.poll.coin = Some(before.token_id.clone());
        let recovered = store
            .record_order_poll(&before, &evidence.poll)
            .await?
            .ok_or_else(|| anyhow::anyhow!("recovery poll rejected"))?;
        // A later fill HTTP failure cannot roll back the already committed order lookup.
        let reloaded = reconciliation_open_leg(
            &Store {
                pool: store.pool.clone(),
            },
            id,
        )
        .await?;
        ensure!(reloaded.third_order_id.as_deref() == Some("recovered-oid"));
        ensure!(reloaded.submitted_at == before.submitted_at && reloaded.status == "actived");
        ensure!(
            reloaded.last_order_info.as_ref().unwrap()["order_poll"]["order_id"] == "recovered-oid"
        );
        let fill = reconciliation_trade("recovered-oid", "oid-only-trade", 10, Decimal::new(7, 2));
        ensure!(fill.raw.get("cloid").is_none());
        ensure!(!fill.matches(
            before.third_order_id.as_deref(),
            before.client_order_id.as_deref()
        ));
        let matched: Vec<_> = [fill]
            .into_iter()
            .filter(|fill| {
                fill.matches(
                    recovered.third_order_id.as_deref(),
                    recovered.client_order_id.as_deref(),
                )
            })
            .collect();
        ensure!(
            matched.len() == 1,
            "same round must use recovered oid, not its pre-poll snapshot"
        );
        ensure!(matches!(
            store
                .record_reconciliation(&recovered, &matched, &evidence, &json!({"complete":true}))
                .await?,
            LegResolution::Terminal {
                status: "matched",
                ..
            }
        ));
        ensure!(order_snapshot(&store.pool, order_id).await?["status"] == "completed");
        ensure!(fill_snapshots(&store.pool, id).await?.len() == 1);
        Ok(())
    }
    .await;
    fixture
        .cleanup()
        .await
        .expect("clean up reconciliation schema");
    exercised.expect("recovered order id survives and associates oid-only fills in the same round");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_sql_failures_roll_back_fills_leg_and_parent_before_retry() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        for table in ["legs", "arb_orders"] {
            let (order_id, legs) = submitted_reconciliation_order(store, &["outcome"]).await?;
            let id = legs[0].id;
            let evidence = reconciliation_evidence(&format!("fault-oid-{id}"), "filled");
            let current = store.record_order_poll(&legs[0], &evidence.poll).await?
                .ok_or_else(|| anyhow::anyhow!("fault fixture poll rejected"))?;
            let fill = reconciliation_trade(evidence.poll.order_id.as_deref().unwrap(), "real", 10, Decimal::new(7, 2));
            let leg_before = leg_snapshot(&store.pool, id).await?;
            let parent_before = order_snapshot(&store.pool, order_id).await?;
            let fills_before = fill_snapshots(&store.pool, id).await?;
            // These generated identifiers and row ids belong exclusively to this private schema.
            // First fail after fill insertion; then fail after the leg's actuals were updated.
            let constraint = format!("reconciliation_fault_{table}");
            let check = if table == "legs" {
                format!("id <> {id} OR status <> 'matched'")
            } else {
                format!("id <> {order_id} OR actual_cost = 0")
            };
            sqlx::query(&format!("ALTER TABLE {table} ADD CONSTRAINT {constraint} CHECK ({check})"))
                .execute(&store.pool).await?;
            let rejected = store.record_reconciliation(&current, &[fill.clone()], &evidence, &json!({"cursor":"must-roll-back"})).await;
            ensure!(matches!(&rejected, Err(market_arb::error::Error::Sqlx(sqlx::Error::Database(err)))
                if err.code().as_deref() == Some("23514") && err.constraint() == Some(constraint.as_str())),
                "expected injected {table} constraint failure, got {rejected:?}");
            ensure!(fill_snapshots(&store.pool, id).await? == fills_before, "{table} failure leaked fill");
            ensure!(leg_snapshot(&store.pool, id).await? == leg_before, "{table} failure leaked leg/progress update");
            ensure!(order_snapshot(&store.pool, order_id).await? == parent_before, "{table} failure leaked parent actuals");
            sqlx::query(&format!("ALTER TABLE {table} DROP CONSTRAINT {constraint}"))
                .execute(&store.pool).await?;
            // The original snapshot must still be valid because every preceding write rolled back.
            ensure!(matches!(store.record_reconciliation(&current, &[fill], &evidence, &json!({"complete":true})).await?,
                LegResolution::Terminal {status:"matched", ..}));
            ensure!(fill_snapshots(&store.pool, id).await?.len() == 1);
            let parent = order_snapshot(&store.pool, order_id).await?;
            ensure!(parent["status"] == "completed" && parent["actual_cost"] == json!(4.07));
            store.complete_orders().await?;
            ensure!(order_snapshot(&store.pool, order_id).await? == parent);
        }
        Ok(())
    }.await;
    fixture
        .cleanup()
        .await
        .expect("clean up reconciliation schema");
    exercised.expect("SQL faults at both accounting stages roll back and permit exact retry");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_timeouts_use_first_submission_and_history_stays_frozen() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        let (order_id, legs) = submitted_reconciliation_order(store, &["outcome", "outcome"]).await?;
        // Time travel only this fixture's rows; recent creation/updates must not reset submission age.
        sqlx::query("UPDATE legs SET submitted_at=NOW()-INTERVAL '2 hours', created_at=NOW(), updated_at=NOW() WHERE order_id=$1")
            .bind(order_id).execute(&store.pool).await?;
        let first = reconciliation_open_leg(store, legs[0].id).await?.submitted_at;
        store.record_submission(legs[0].id, "unknown", None, &json!({"kind":"unknown"}), &json!({"timeout":true})).await?;
        store.record_submission(legs[1].id, "actived", Some("partial-oid"), &json!({"kind":"ack"}), &json!({"ack":true})).await?;
        let evidence = reconciliation_evidence("partial-oid", "filled");
        let current = reconciliation_open_leg(store, legs[1].id).await?;
        let current = store.record_order_poll(&current, &evidence.poll).await?
            .ok_or_else(|| anyhow::anyhow!("partial fill poll rejected"))?;
        let partial = reconciliation_trade("partial-oid", "partial", 2, Decimal::new(1, 2));
        ensure!(matches!(store.record_reconciliation(&current, &[partial], &evidence, &json!({"cursor":"remaining"})).await?,
            LegResolution::Pending(_)));
        for leg in &legs {
            store.record_submission(leg.id, "unknown", None, &json!({"kind":"unknown","retry":true}), &json!({"retry":true})).await?;
            ensure!(reconciliation_open_leg(store, leg.id).await?.submitted_at == first);
        }
        ensure!(store.insert_envelope(legs[0].id, "retry-must-not-reset", &json!({}), &json!({}), None).await.is_err());
        let (recent_id, _) = submitted_reconciliation_order(store, &["outcome"]).await?;
        sqlx::query("UPDATE legs SET created_at=NOW()-INTERVAL '2 hours' WHERE order_id=$1")
            .bind(recent_id).execute(&store.pool).await?;
        ensure!(store.fail_stale_pending_unsubmitted(std::time::Duration::from_secs(60)).await? == 0);
        ensure!(store.promote_submitted_pending_to_unknown().await? == 1);
        let before: Vec<_> = store.open_legs().await?;
        let timeout = std::time::Duration::from_secs(60);
        let stale = store.stale_unknown_legs(timeout).await?;
        ensure!(stale.iter().map(|leg| leg.id).collect::<Vec<_>>() == legs.iter().map(|leg| leg.id).collect::<Vec<_>>());
        ensure!(store.count_stale_unknown_legs(timeout).await? == 2);
        ensure!(before[0].status == "unknown" && before[1].status == "actived");
        ensure!(fill_snapshots(&store.pool, legs[1].id).await?.len() == 1, "partial observation must survive timeout");
        let parent = order_snapshot(&store.pool, order_id).await?;
        store.complete_orders().await?;
        ensure!(order_snapshot(&store.pool, order_id).await? == parent && parent["status"] == "actived");

        for position in ["closed", "settled"] {
            let (historical_id, historical_legs) = submitted_reconciliation_order(store, &["outcome"]).await?;
            let leg = &historical_legs[0];
            // Historical settlement evidence must satisfy the same all-or-none constraint as production.
            sqlx::query("UPDATE arb_orders SET position_status=$2, settled_at=CASE WHEN $2='settled' THEN NOW() ELSE NULL END,
                         settlement_source=CASE WHEN $2='settled' THEN 'historical-fixture' ELSE NULL END,
                         settlement_result=CASE WHEN $2='settled' THEN '{\"settled\":true}'::jsonb ELSE NULL END,
                         actual_cost=123, actual_rev=456, actual_profit=333 WHERE id=$1")
                .bind(historical_id).bind(position).execute(&store.pool).await?;
            sqlx::query("INSERT INTO fills(leg_id,third_order_id,trade_id,shares,price,fee) VALUES($1,'historical-oid','ack:historical',999,0.9,99)")
                .bind(leg.id).execute(&store.pool).await?;
            let historical_parent = order_snapshot(&store.pool, historical_id).await?;
            let historical_leg = leg_snapshot(&store.pool, leg.id).await?;
            let historical_fills = fill_snapshots(&store.pool, leg.id).await?;
            let evidence = reconciliation_evidence("historical-oid", "filled");
            store.record_submission(leg.id, "actived", Some("historical-oid"), &json!({"kind":"ack"}), &json!({"late":true})).await?;
            ensure!(store.record_order_poll(leg, &evidence.poll).await?.is_none());
            ensure!(matches!(store.record_reconciliation(leg, &[reconciliation_trade("historical-oid", "late-trade", 10, Decimal::ZERO)],
                &evidence, &json!({})).await?, LegResolution::Pending(_)));
            store.migrate().await?;
            store.complete_orders().await?;
            ensure!(leg_snapshot(&store.pool, leg.id).await? == historical_leg);
            ensure!(order_snapshot(&store.pool, historical_id).await? == historical_parent);
            ensure!(fill_snapshots(&store.pool, leg.id).await? == historical_fills);
            // Also force the completion scanner's all-terminal path over a historical parent.
            sqlx::query("UPDATE legs SET status='matched',actual_shares=10,actual_price=0.4,actual_fee=0.1 WHERE id=$1")
                .bind(leg.id).execute(&store.pool).await?;
            store.complete_orders().await?;
            ensure!(order_snapshot(&store.pool, historical_id).await? == historical_parent,
                "{position} historical actuals must never be recomputed");
        }
        Ok(())
    }.await;
    fixture
        .cleanup()
        .await
        .expect("clean up reconciliation schema");
    exercised.expect("submission age controls alerts including partial fills, without rewriting closed/settled history");
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
