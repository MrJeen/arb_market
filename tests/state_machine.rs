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
use market_arb::reconcile::{FillEvidence, LegResolution, PmTradeScan};
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
        fill.coin = Some(current.token_id.clone());
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
            funder: (*platform == "polymarket")
                .then_some("0x0000000000000000000000000000000000000001"),
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
            0,
            std::time::Instant::now() + std::time::Duration::from_secs(30),
        )
        .await?;
    for (id, platform) in ids.iter().zip(platforms) {
        let client_id = if *platform == "polymarket" {
            format!("0x{id:064x}")
        } else {
            format!("reconciliation-client-{id}")
        };
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
        pm_scan: None,
        pm_order_constraints: None,
        outcome_scan: None,
    }
}

fn pm_missing_evidence(
    leg: &LegRow,
    missing: &str,
    cursor: &str,
    trade_ids: &[&str],
) -> FillEvidence {
    let oid = leg
        .third_order_id
        .as_ref()
        .or(leg.client_order_id.as_ref())
        .unwrap();
    let after = leg.submitted_at.unwrap().timestamp();
    FillEvidence {
        poll: OrderPoll {
            found: false,
            status: "not_found".into(),
            order_id: Some(oid.clone()),
            raw: json!({"lookup_missing": missing}),
            ..OrderPoll::default()
        },
        page_complete: cursor == "LTE=",
        history_complete: cursor == "LTE=",
        expected_shares: None,
        pm_scan: Some(PmTradeScan {
            version: 2,
            funder: leg.funder_address.clone().unwrap(),
            asset_id: leg.token_id.clone(),
            order_id: oid.clone(),
            after,
            before: after + 300,
            next_cursor: cursor.into(),
            trade_ids: trade_ids.iter().map(|id| (*id).into()).collect(),
        }),
        pm_order_constraints: None,
        outcome_scan: None,
    }
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn outcome_final_probe_is_fresh_and_retains_observed_fills_atomically() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store=&fixture.store;
        store.migrate().await?;
        let (order_id,legs)=submitted_reconciliation_order(store,&["outcome"]).await?;
        let leg_id=legs[0].id;
        sqlx::query("UPDATE legs SET wallet_address='0xaccount' WHERE id=$1")
            .bind(leg_id).execute(&store.pool).await?;
        let mut evidence=reconciliation_evidence("987654", "canceled");
        evidence.poll.shares=None;
        evidence.poll.original_shares=Some(Decimal::from(10));
        evidence.poll.remaining_shares=Some(Decimal::from(4));
        let current=reconciliation_open_leg(store,leg_id).await?;
        let current=store.record_order_poll(&current,&evidence.poll).await?.unwrap();
        let info=current.last_order_info.as_ref().unwrap();
        let observed=info["outcome_terminal_observed_at_ms"].as_u64().unwrap();
        let start=current.submitted_at.unwrap().timestamp_millis().saturating_sub(30_000).max(0) as u64;
        let before=json!({"version":2,"tokenId":current.token_id,"submittedAtMs":start,
            "endTime":observed,"cursor":start,"seenIds":[],"historyChecked":true,
            "historyLowerBound":start.saturating_sub(1),"historyComplete":false,
            "scannedCount":0,"complete":false,"phase":"finalProbe","account":"0xaccount",
            "terminalObservedAtMs":observed,"scanValid":true});
        let mut fill=reconciliation_trade("987654","probe-only-fill",6,Decimal::new(1,2));
        fill.coin=Some(current.token_id.clone());
        // 旧布尔覆盖证明不能授权零成交/部分成交终态，但 probe 真成交仍必须被保存。
        evidence.page_complete=true;
        evidence.history_complete=true;
        let pending=store.record_reconciliation(&current,&[fill.clone()],&evidence,&before).await?;
        ensure!(matches!(pending,LegResolution::Pending(_)));
        ensure!(fill_snapshots(&store.pool,leg_id).await?.len()==1);
        ensure!(order_snapshot(&store.pool,order_id).await?["status"]=="actived");
        let current=reconciliation_open_leg(store,leg_id).await?;
        let mut complete=before.clone();
        complete["phase"]=json!("complete");
        complete["complete"]=json!(true);
        complete["historyComplete"]=json!(true);
        evidence.outcome_scan=Some(serde_json::from_value(complete.clone())?);
        let restored:FillEvidence=serde_json::from_value(serde_json::to_value(&evidence)?)?;
        ensure!(restored.outcome_scan.is_none(), "persisted evidence cannot restore fresh coverage");
        let result=store.record_reconciliation(&current,&[fill],&evidence,&complete).await?;
        ensure!(matches!(result,LegResolution::Terminal{status:"matched",shares,..} if shares==Decimal::from(6)));
        ensure!(fill_snapshots(&store.pool,leg_id).await?.len()==1);
        ensure!(order_snapshot(&store.pool,order_id).await?["status"]=="completed");
        ensure!(store.open_legs().await?.is_empty());
        Ok(())
    }.await;
    fixture
        .cleanup()
        .await
        .expect("clean up outcome final probe schema");
    exercised.expect("fresh final coverage and durable probe fill");
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
            if leg.platform == "polymarket" {
                fill.coin = Some(current.token_id.clone());
            }
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
        for fill in [&mut a, &mut b, &mut c] {
            fill.coin = Some(current.token_id.clone());
        }
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
        fill.coin = Some(leg.token_id.clone());
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
async fn reconciliation_pm_empty_missing_scan_preserves_other_venue_position() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        for source in ["http_404", "null_body"] {
            let (parent_id, legs) = submitted_reconciliation_order(store, &["polymarket", "outcome"]).await?;
            let pm = &legs[0];
            store.record_submission(pm.id, "unknown", None, &json!({"kind":"unknown"}), &json!({})).await?;
            let mut out = reconciliation_evidence(&format!("out-{}", legs[1].id), "filled");
            out.expected_shares = Some(Decimal::from(9));
            store.record_submission(legs[1].id, "actived", out.poll.order_id.as_deref(),
                &json!({"kind":"ack", "expected_shares":"9"}), &json!({"ack":true})).await?;
            let current = reconciliation_open_leg(store, legs[1].id).await?;
            let current = store.record_order_poll(&current, &out.poll).await?.unwrap();
            let fill = reconciliation_trade(out.poll.order_id.as_deref().unwrap(), "out-fill", 9, Decimal::ZERO);
            ensure!(matches!(store.record_reconciliation(&current, &[fill], &out, &json!({})).await?,
                LegResolution::Terminal {status:"matched", shares, ..} if shares == Decimal::from(9)));
            ensure!(order_snapshot(&store.pool, parent_id).await?["status"] == "actived");
            let current = reconciliation_open_leg(store, pm.id).await?;
            let incomplete = pm_missing_evidence(&current, source, "page-2", &[]);
            let current = store.record_order_poll(&current, &incomplete.poll).await?.unwrap();
            ensure!(matches!(store.record_reconciliation(&current, &[], &incomplete,
                &serde_json::to_value(incomplete.pm_scan.as_ref().unwrap())?).await?, LegResolution::Pending(_)));
            let current = reconciliation_open_leg(store, pm.id).await?;
            let complete = pm_missing_evidence(&current, source, "LTE=", &[]);
            let progress = serde_json::to_value(complete.pm_scan.as_ref().unwrap())?;
            ensure!(matches!(store.record_reconciliation(&current, &[], &complete, &progress).await?,
                LegResolution::Terminal {status:"failed", shares, price, fee, ..}
                    if shares.is_zero() && price.is_zero() && fee.is_zero()));
            let terminal = leg_snapshot(&store.pool, pm.id).await?;
            ensure!(terminal["status"] == "failed" && terminal["last_order_info"]["waiting_reason"].is_null());
            ensure!(terminal["last_order_info"]["fill_progress"]["trade_ids"] == json!([]));
            ensure!(terminal["last_order_info"].get("fill_evidence").is_none());
            let parent = order_snapshot(&store.pool, parent_id).await?;
            ensure!(parent["status"] == "completed" && parent["actual_cost"] == json!(3.6));
            let positions = store.positions_for_order(parent_id).await?;
            ensure!(positions["outcome"]["no"] == Decimal::from(9));
            ensure!(positions["polymarket"]["yes"].is_zero());
            ensure!(!store.open_legs().await?.iter().any(|leg| leg.order_id == parent_id));
            ensure!(store.record_order_poll(&current, &complete.poll).await?.is_none());
            ensure!(matches!(store.record_reconciliation(&current, &[], &complete, &progress).await?, LegResolution::Pending(_)));
            store.record_submission(pm.id, "actived", None, &json!({"kind":"unknown"}), &json!({"late":true})).await?;
            ensure!(leg_snapshot(&store.pool, pm.id).await? == terminal);
            ensure!(order_snapshot(&store.pool, parent_id).await? == parent);
        }
        Ok(())
    }.await;
    fixture.cleanup().await.expect("clean up empty scan schema");
    exercised.expect("empty PM scan fails atomically without losing Outcome position");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_pm_missing_order_pages_reload_with_fixed_window_and_exact_accounting() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        // Cover both missing responses and selectors, with mixed and all-failed terminal sets.
        for (missing, ack, all_failed) in [
            ("http_404", false, false),
            ("http_404", true, true),
            ("null_body", false, true),
            ("null_body", true, false),
        ] {
            let (order_id, legs) = submitted_reconciliation_order(store, &["polymarket"]).await?;
            let id = legs[0].id;
            sqlx::query("UPDATE legs SET submitted_at=to_timestamp(1700000000), updated_at=clock_timestamp() WHERE id=$1")
                .bind(id).execute(&store.pool).await?;
            let ack_oid = format!("pm-ack-{id}");
            store.record_submission(id, if ack { "actived" } else { "unknown" },
                ack.then_some(ack_oid.as_str()), &json!({"kind": if ack { "ack" } else { "unknown" }}),
                &json!({"test":true})).await?;
            let before = reconciliation_open_leg(store, id).await?;
            let first = pm_missing_evidence(&before, missing, "page-2", &["a"]);
            let oid = first.poll.order_id.as_deref().unwrap();
            ensure!(oid == if ack { ack_oid.as_str() } else { before.client_order_id.as_deref().unwrap() });
            let current = store.record_order_poll(&before, &first.poll).await?
                .ok_or_else(|| anyhow::anyhow!("missing {missing} poll rejected"))?;
            ensure!(current.third_order_id.as_deref() == Some(oid));
            ensure!(current.status == before.status && current.submitted_at == before.submitted_at);
            let mut a = reconciliation_trade(oid, "a", 4, Decimal::new(2, 2));
            a.coin = Some(current.token_id.clone());
            a.finality = FillFinality::Pending;
            let first_progress = serde_json::to_value(first.pm_scan.as_ref().unwrap())?;
            ensure!(matches!(store.record_reconciliation(&current, &[a.clone()], &first, &first_progress).await?,
                LegResolution::Pending(_)));

            // A new Store handle must resume entirely from the persisted scan, not local page state.
            let reopened = Store { pool: store.pool.clone() };
            let reloaded = reconciliation_open_leg(&reopened, id).await?;
            let info = reloaded.last_order_info.as_ref().unwrap();
            let mut scan: PmTradeScan = serde_json::from_value(info["fill_progress"].clone())?;
            ensure!(Some(&scan) == first.pm_scan.as_ref());
            ensure!(scan.after == 1_700_000_000 && scan.before == scan.after + 300);
            ensure!(reloaded.third_order_id.as_deref() == Some(oid) && reloaded.status == before.status);
            ensure!(info["order_poll"]["raw"] == json!({"lookup_missing":missing}));
            scan.next_cursor = "LTE=".into();
            scan.trade_ids.push("b".into());
            let mut complete = first.clone();
            complete.page_complete = true;
            complete.history_complete = true;
            complete.pm_scan = Some(scan);
            let complete_progress = serde_json::to_value(complete.pm_scan.as_ref().unwrap())?;
            a.finality = if all_failed { FillFinality::Failed } else { FillFinality::Confirmed };
            let mut b = reconciliation_trade(oid, "b", 6, Decimal::from(99));
            b.coin = Some(reloaded.token_id.clone());
            b.finality = FillFinality::Failed;
            let final_fills = [a.clone(), b, a];
            let leg_before = leg_snapshot(&store.pool, id).await?;
            let fills_before = fill_snapshots(&store.pool, id).await?;
            let parent_before = order_snapshot(&store.pool, order_id).await?;
            ensure!(reopened.record_reconciliation(&current, &final_fills, &complete, &complete_progress).await?
                == LegResolution::Pending("stale_leg_snapshot"));
            ensure!(leg_snapshot(&store.pool, id).await? == leg_before, "stale page advanced cursor");
            ensure!(fill_snapshots(&store.pool, id).await? == fills_before);
            ensure!(order_snapshot(&store.pool, order_id).await? == parent_before);

            let current = reopened.record_order_poll(&reloaded, &complete.poll).await?
                .ok_or_else(|| anyhow::anyhow!("resumed missing poll rejected"))?;
            ensure!(current.submitted_at == before.submitted_at && current.status == before.status);
            ensure!(current.last_order_info.as_ref().unwrap()["fill_progress"] == first_progress,
                "poll must neither reset the fixed after nor advance the scan");
            let resolved = reopened.record_reconciliation(&current, &final_fills, &complete, &complete_progress).await?;
            let (status, shares, price, fee, fee_sources) = if all_failed {
                ("failed", Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, vec![])
            } else {
                ("matched", Decimal::from(4), Decimal::new(4, 1), Decimal::new(2, 2), vec!["actual"])
            };
            ensure!(resolved == LegResolution::Terminal { status, shares, price, fee, fee_sources });
            let terminal = leg_snapshot(&store.pool, id).await?;
            let fills = fill_snapshots(&store.pool, id).await?;
            let parent = order_snapshot(&store.pool, order_id).await?;
            ensure!(terminal["status"] == status && terminal["actual_shares"] == json!(if all_failed { 0.0 } else { 4.0 }));
            ensure!(terminal["actual_fee"] == json!(if all_failed { 0.0 } else { 0.02 }));
            ensure!(terminal["submitted_at"] == leg_before["submitted_at"]);
            ensure!(terminal["last_order_info"]["fill_progress"] == complete_progress);
            ensure!(terminal["last_order_info"].get("fill_evidence").is_none());
            ensure!(fills.len() == 2 && fills.iter().all(|fill| fill["third_order_id"] == oid));
            ensure!(fills[0]["raw"]["reconciliation_v1"]["finality"] == if all_failed { "failed" } else { "confirmed" });
            ensure!(fills[1]["raw"]["reconciliation_v1"]["finality"] == "failed");
            ensure!(parent["status"] == if all_failed { "cancelled" } else { "completed" });
            ensure!(parent["actual_cost"] == json!(if all_failed { 0.0 } else { 1.62 }));
            ensure!(reopened.record_reconciliation(&current, &final_fills, &complete, &complete_progress).await?
                == LegResolution::Pending("stale_leg_snapshot"));
            reopened.complete_orders().await?;
            ensure!(leg_snapshot(&store.pool, id).await? == terminal);
            ensure!(fill_snapshots(&store.pool, id).await? == fills);
            ensure!(order_snapshot(&store.pool, order_id).await? == parent, "terminal retry recounted fills");
        }
        Ok(())
    }.await;
    fixture
        .cleanup()
        .await
        .expect("clean up missing-order page schema");
    exercised
        .expect("missing PM orders resume fixed scans and account terminal trades exactly once");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_pm_known_execution_survives_weaker_polls_missing_orders_and_reload() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        // Separate ID coverage from the quantity lower bound; both must survive a missing lookup.
        for known_b in [true, false] {
            for b_failed in [true, false] {
                let (order_id, legs) =
                    submitted_reconciliation_order(store, &["polymarket"]).await?;
                let id = legs[0].id;
                let oid = format!("known-execution-{id}");
                let mut evidence = reconciliation_evidence(&oid, "matched");
                evidence.poll.associated_trades = if known_b {
                    vec!["a".into(), "b".into()]
                } else {
                    vec!["a".into()]
                };
                evidence.poll.raw = json!({"associate_trades": evidence.poll.associated_trades});
                let current = store
                    .record_order_poll(&legs[0], &evidence.poll)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("known execution poll rejected"))?;
                let constraints =
                    current.last_order_info.as_ref().unwrap()["pm_order_constraints"].clone();
                ensure!(
                    constraints["matched_shares_lower_bound"]
                        == serde_json::to_value(Decimal::from(10))?
                );
                let parent_before = order_snapshot(&store.pool, order_id).await?;
                let submitted_at = leg_snapshot(&store.pool, id).await?["submitted_at"].clone();
                let mut a = reconciliation_trade(&oid, "a", 6, Decimal::new(2, 2));
                a.coin = Some(current.token_id.clone());
                ensure!(evidence.pm_order_constraints.is_none());
                ensure!(
                    matches!(
                        store
                            .record_reconciliation(
                                &current,
                                &[a],
                                &evidence,
                                &json!({"complete":true})
                            )
                            .await?,
                        LegResolution::Pending(_)
                    ),
                    "complete trade page concealed known execution"
                );

                // The latest normal lookup alone would permit a terminal six-share result.
                evidence.poll.associated_trades = vec!["a".into()];
                evidence.poll.raw = json!({"associate_trades":["a"]});
                evidence.poll.shares = Some(Decimal::from(6));
                let current = reconciliation_open_leg(store, id).await?;
                let current = store
                    .record_order_poll(&current, &evidence.poll)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("weaker normal poll rejected"))?;
                ensure!(
                    matches!(
                        store
                            .record_reconciliation(
                                &current,
                                &[],
                                &evidence,
                                &json!({"complete":true})
                            )
                            .await?,
                        LegResolution::Pending(_)
                    ),
                    "normal PM branch bypassed known constraints"
                );

                for missing in ["http_404", "null_body"] {
                    let current = reconciliation_open_leg(store, id).await?;
                    let missing_evidence = pm_missing_evidence(&current, missing, "LTE=", &["a"]);
                    let progress =
                        serde_json::to_value(missing_evidence.pm_scan.as_ref().unwrap())?;
                    let current = store
                        .record_order_poll(&current, &missing_evidence.poll)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("missing {missing} poll rejected"))?;
                    ensure!(missing_evidence.pm_order_constraints.is_none());
                    ensure!(
                        matches!(
                            store
                                .record_reconciliation(&current, &[], &missing_evidence, &progress)
                                .await?,
                            LegResolution::Pending(_)
                        ),
                        "{missing} erased known execution"
                    );
                    let saved = leg_snapshot(&store.pool, id).await?;
                    ensure!(saved["actual_shares"].is_null() && saved["actual_fee"].is_null());
                    ensure!(saved["last_order_info"]["pm_order_constraints"] == constraints);
                    ensure!(
                        saved["last_order_info"].get("fill_evidence").is_none(),
                        "fill_evidence layer should be omitted"
                    );
                    ensure!(order_snapshot(&store.pool, order_id).await? == parent_before);
                    ensure!(fill_snapshots(&store.pool, id).await?.len() == 1);
                }

                let reopened = Store {
                    pool: store.pool.clone(),
                };
                let reloaded = reconciliation_open_leg(&reopened, id).await?;
                ensure!(
                    reloaded.last_order_info.as_ref().unwrap()["pm_order_constraints"]
                        == constraints
                );
                let incomplete = pm_missing_evidence(&reloaded, "null_body", "page-2", &["a", "b"]);
                let mut b = reconciliation_trade(
                    &oid,
                    "b",
                    4,
                    if b_failed {
                        Decimal::from(99)
                    } else {
                        Decimal::new(3, 2)
                    },
                );
                b.coin = Some(reloaded.token_id.clone());
                b.finality = if b_failed {
                    FillFinality::Failed
                } else {
                    FillFinality::Confirmed
                };
                ensure!(
                    matches!(
                        reopened
                            .record_reconciliation(
                                &reloaded,
                                &[b],
                                &incomplete,
                                &serde_json::to_value(incomplete.pm_scan.as_ref().unwrap())?
                            )
                            .await?,
                        LegResolution::Pending(_)
                    ),
                    "all terminal trades still require a completed fixed-window scan"
                );
                let current = reconciliation_open_leg(&reopened, id).await?;
                let complete = pm_missing_evidence(&current, "http_404", "LTE=", &["a", "b"]);
                let progress = serde_json::to_value(complete.pm_scan.as_ref().unwrap())?;
                let shares = Decimal::from(if b_failed { 6 } else { 10 });
                let fee = Decimal::new(if b_failed { 2 } else { 5 }, 2);
                ensure!(
                    reopened
                        .record_reconciliation(&current, &[], &complete, &progress)
                        .await?
                        == LegResolution::Terminal {
                            status: "matched",
                            shares,
                            price: Decimal::new(4, 1),
                            fee,
                            fee_sources: vec!["actual"]
                        }
                );
                let terminal = leg_snapshot(&store.pool, id).await?;
                ensure!(terminal["actual_shares"] == json!(if b_failed { 6.0 } else { 10.0 }));
                ensure!(terminal["actual_fee"] == json!(if b_failed { 0.02 } else { 0.05 }));
                ensure!(terminal["submitted_at"] == submitted_at);
                ensure!(terminal["last_order_info"]["pm_order_constraints"] == constraints);
                let parent = order_snapshot(&store.pool, order_id).await?;
                ensure!(parent["status"] == "completed");
                ensure!(parent["actual_cost"] == json!(if b_failed { 2.42 } else { 4.05 }));
                ensure!(parent["actual_rev"] == json!(0.0));
                ensure!(parent["actual_profit"] == json!(if b_failed { -2.42 } else { -4.05 }));
                let fills = fill_snapshots(&store.pool, id).await?;
                ensure!(
                    fills.len() == 2
                        && fills[0]["raw"]["reconciliation_v1"]["finality"] == "confirmed"
                );
                ensure!(
                    fills[1]["raw"]["reconciliation_v1"]["finality"]
                        == if b_failed { "failed" } else { "confirmed" }
                );
                reopened.complete_orders().await?;
                ensure!(order_snapshot(&store.pool, order_id).await? == parent);
                ensure!(fill_snapshots(&store.pool, id).await? == fills);
            }
        }
        Ok(())
    }
    .await;
    fixture
        .cleanup()
        .await
        .expect("clean up known-execution schema");
    exercised.expect("known IDs and matched lower bounds survive normal and missing lookup rounds");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_pm_poll_constraints_only_accumulate_valid_same_order_evidence() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        let (_, legs) = submitted_reconciliation_order(store, &["polymarket"]).await?;
        let mut poll = reconciliation_evidence("monotonic-oid", "matched").poll;
        poll.associated_trades = vec!["b".into(), "a".into(), "b".into()];
        poll.raw = json!({"associate_trades":["b","a","b"]});
        poll.original_shares = Some(Decimal::from(100));
        let mut current = store
            .record_order_poll(&legs[0], &poll)
            .await?
            .ok_or_else(|| anyhow::anyhow!("monotonic fixture poll rejected"))?;
        let mut expected = json!({
            "version":1,"order_id":"monotonic-oid","asset_id":current.token_id,
            "funder":current.funder_address,"associated_trade_ids":["a","b"],
            "matched_shares_lower_bound":Decimal::from(10)
        });
        ensure!(current.last_order_info.as_ref().unwrap()["pm_order_constraints"] == expected);
        for case in [
            "smaller",
            "additional",
            "missing",
            "negative",
            "malformed",
            "not_found",
        ] {
            poll.found = true;
            poll.shares = Some(Decimal::from(6));
            poll.associated_trades = vec!["a".into()];
            poll.raw = json!({"associate_trades":["a"]});
            match case {
                "additional" => {
                    poll.associated_trades = vec!["c".into(), "a".into()];
                    poll.raw = json!({"associate_trades":["c","a"]});
                    poll.shares = Some(Decimal::from(12));
                    expected["associated_trade_ids"] = json!(["a", "b", "c"]);
                    expected["matched_shares_lower_bound"] =
                        serde_json::to_value(Decimal::from(12))?;
                }
                "missing" => {
                    poll.raw = json!({});
                    poll.associated_trades.clear();
                    poll.shares = None;
                }
                "negative" => {
                    poll.raw = json!({"associate_trades":[]});
                    poll.associated_trades.clear();
                    poll.shares = Some(-Decimal::ONE);
                }
                "malformed" => {
                    poll.raw = json!({"associate_trades":["poison",17]});
                    poll.associated_trades = vec!["poison".into()];
                    poll.shares = None;
                }
                "not_found" => {
                    poll.found = false;
                    poll.raw =
                        json!({"lookup_missing":"http_404","associate_trades":["not-found-id"]});
                    poll.associated_trades = vec!["not-found-id".into()];
                    poll.shares = Some(Decimal::from(99));
                }
                _ => {}
            }
            current = store
                .record_order_poll(&current, &poll)
                .await?
                .ok_or_else(|| anyhow::anyhow!("{case} poll rejected"))?;
            ensure!(
                current.last_order_info.as_ref().unwrap()["pm_order_constraints"] == expected,
                "{case} reduced constraints or admitted malformed evidence"
            );
        }
        let before = leg_snapshot(&store.pool, current.id).await?;
        poll.found = true;
        poll.order_id = Some("other-order".into());
        ensure!(store.record_order_poll(&current, &poll).await.is_err());
        ensure!(leg_snapshot(&store.pool, current.id).await? == before);
        ensure!(fill_snapshots(&store.pool, current.id).await?.is_empty());
        Ok(())
    }
    .await;
    fixture
        .cleanup()
        .await
        .expect("clean up monotonic constraint schema");
    exercised.expect(
        "valid association unions and matched lower bounds never decrease or cross order IDs",
    );
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_pm_legacy_poll_constraints_restore_before_missing_lookup_overwrite() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        for source in ["order_poll", "fill_evidence", "both"] {
            for case in ["valid", "other_order", "not_found", "malformed_ids"] {
                let (order_id, legs) =
                    submitted_reconciliation_order(store, &["polymarket"]).await?;
                let id = legs[0].id;
                let oid = format!("legacy-constraints-{id}");
                store
                    .record_submission(
                        id,
                        "actived",
                        Some(&oid),
                        &json!({"kind":"ack"}),
                        &json!({"ack":true}),
                    )
                    .await?;
                let mut previous = reconciliation_evidence(&oid, "matched").poll;
                previous.associated_trades = vec!["b".into(), "a".into(), "b".into()];
                previous.raw = json!({"associate_trades":["b","a","b"]});
                match case {
                    "other_order" => previous.order_id = Some("unrelated-legacy-order".into()),
                    "not_found" => previous.found = false,
                    "malformed_ids" => {
                        previous.associated_trades = vec!["poison".into()];
                        previous.raw = json!({"associate_trades":["poison",7]});
                        previous.shares = None;
                    }
                    _ => {}
                }
                let mut info = json!({});
                if source != "fill_evidence" {
                    info["order_poll"] = serde_json::to_value(&previous)?;
                }
                if source != "order_poll" {
                    info["fill_evidence"] = json!({"poll":previous});
                }
                sqlx::query(
                    "UPDATE legs SET last_order_info=$2,updated_at=clock_timestamp() WHERE id=$1",
                )
                .bind(id)
                .bind(info)
                .execute(&store.pool)
                .await?;
                let current = reconciliation_open_leg(store, id).await?;
                let missing = pm_missing_evidence(&current, "http_404", "LTE=", &["a"]);
                let current = store
                    .record_order_poll(&current, &missing.poll)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("legacy {source}/{case} lookup rejected"))?;
                let constraints =
                    current.last_order_info.as_ref().unwrap()["pm_order_constraints"].clone();
                ensure!(
                    constraints["order_id"] == oid && constraints["asset_id"] == current.token_id
                );
                ensure!(
                    constraints["associated_trade_ids"]
                        == if case == "valid" {
                            json!(["a", "b"])
                        } else {
                            json!([])
                        }
                );
                ensure!(
                    constraints["matched_shares_lower_bound"]
                        == if case == "valid" {
                            serde_json::to_value(Decimal::from(10))?
                        } else {
                            Value::Null
                        },
                    "{source}/{case} recovered invalid or cross-order shares"
                );
                ensure!(
                    current.last_order_info.as_ref().unwrap()["order_poll"]["raw"]
                        == json!({"lookup_missing":"http_404"})
                );
                ensure!(current.submitted_at == legs[0].submitted_at);
                if case == "valid" {
                    let mut a = reconciliation_trade(&oid, "a", 6, Decimal::new(2, 2));
                    a.coin = Some(current.token_id.clone());
                    ensure!(
                        matches!(
                            store
                                .record_reconciliation(
                                    &current,
                                    &[a],
                                    &missing,
                                    &serde_json::to_value(missing.pm_scan.as_ref().unwrap())?
                                )
                                .await?,
                            LegResolution::Pending(_)
                        ),
                        "legacy {source} was overwritten before recovering missing B"
                    );
                    let reopened = Store {
                        pool: store.pool.clone(),
                    };
                    let reloaded = reconciliation_open_leg(&reopened, id).await?;
                    ensure!(
                        reloaded.last_order_info.as_ref().unwrap()["pm_order_constraints"]
                            == constraints
                    );
                    ensure!(order_snapshot(&store.pool, order_id).await?["status"] == "actived");
                }
            }
        }
        Ok(())
    }
    .await;
    fixture
        .cleanup()
        .await
        .expect("clean up legacy constraints schema");
    exercised.expect(
        "both legacy poll locations recover same-order facts before missing polls overwrite them",
    );
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_pm_corrupt_typed_constraints_reject_all_writes_without_leaking_audit() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        for nested in [false, true] {
            for case in [
                "shape",
                "version",
                "missing_ids",
                "invalid_id",
                "negative_bound",
                "other_order",
                "other_asset",
                "other_funder",
            ] {
                let (order_id, legs) =
                    submitted_reconciliation_order(store, &["polymarket"]).await?;
                let id = legs[0].id;
                let oid = format!("corrupt-constraints-{id}");
                let mut poll = reconciliation_evidence(&oid, "live").poll;
                poll.shares = Some(Decimal::ZERO);
                poll.raw = json!({"associate_trades":[]});
                let current = store
                    .record_order_poll(&legs[0], &poll)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("corrupt constraints setup rejected"))?;
                let mut info = current.last_order_info.clone().unwrap();
                let mut corrupt = info["pm_order_constraints"].clone();
                match case {
                    "shape" => corrupt = json!([]),
                    "version" => corrupt["version"] = json!(2),
                    "missing_ids" => {
                        corrupt
                            .as_object_mut()
                            .unwrap()
                            .remove("associated_trade_ids");
                    }
                    "invalid_id" => corrupt["associated_trade_ids"] = json!(["ack:placeholder"]),
                    "negative_bound" => corrupt["matched_shares_lower_bound"] = json!("-1"),
                    "other_order" => corrupt["order_id"] = json!("different-order"),
                    "other_asset" => corrupt["asset_id"] = json!("different-asset"),
                    "other_funder" => {
                        corrupt["funder"] = json!("0x0000000000000000000000000000000000000002")
                    }
                    _ => unreachable!(),
                }
                if nested {
                    info["fill_evidence"] = json!({"pm_order_constraints":corrupt});
                } else {
                    info["pm_order_constraints"] = corrupt;
                }
                sqlx::query(
                    "UPDATE legs SET last_order_info=$2,updated_at=clock_timestamp() WHERE id=$1",
                )
                .bind(id)
                .bind(info)
                .execute(&store.pool)
                .await?;
                let current = reconciliation_open_leg(store, id).await?;
                let before = leg_snapshot(&store.pool, id).await?;
                let parent_before = order_snapshot(&store.pool, order_id).await?;
                let envelope_before: Value = sqlx::query_scalar(
                    "SELECT to_jsonb(e) FROM signed_envelopes e WHERE leg_id=$1",
                )
                .bind(id)
                .fetch_one(&store.pool)
                .await?;
                ensure!(
                    store.record_order_poll(&current, &poll).await.is_err(),
                    "{nested}/{case} poll replaced corrupt typed evidence"
                );
                ensure!(
                    store
                        .record_submission(
                            id,
                            "actived",
                            Some(&oid),
                            &json!({"kind":"ack"}),
                            &json!({"corrupt_retry":true})
                        )
                        .await
                        .is_err(),
                    "{nested}/{case} ACK silently reset corrupt typed evidence"
                );
                let missing = pm_missing_evidence(&current, "null_body", "LTE=", &["new-fill"]);
                let mut fill = reconciliation_trade(&oid, "new-fill", 1, Decimal::ZERO);
                fill.coin = Some(current.token_id.clone());
                ensure!(
                    store
                        .record_reconciliation(
                            &current,
                            &[fill],
                            &missing,
                            &serde_json::to_value(missing.pm_scan.as_ref().unwrap())?
                        )
                        .await
                        .is_err(),
                    "{nested}/{case} reconciliation accepted corrupt evidence"
                );
                ensure!(leg_snapshot(&store.pool, id).await? == before);
                ensure!(fill_snapshots(&store.pool, id).await?.is_empty());
                ensure!(order_snapshot(&store.pool, order_id).await? == parent_before);
                let envelope_after: Value = sqlx::query_scalar(
                    "SELECT to_jsonb(e) FROM signed_envelopes e WHERE leg_id=$1",
                )
                .bind(id)
                .fetch_one(&store.pool)
                .await?;
                ensure!(
                    envelope_after == envelope_before,
                    "corrupt typed rejection leaked submission audit"
                );
            }
        }
        Ok(())
    }
    .await;
    fixture
        .cleanup()
        .await
        .expect("clean up corrupt constraints schema");
    exercised
        .expect("corrupt typed evidence fails closed with no leg, fill, parent or audit writes");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_pm_missing_order_does_not_finish_from_cached_confirmed_fills_alone() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        let (order_id, legs) = submitted_reconciliation_order(store, &["polymarket"]).await?;
        let id = legs[0].id;
        let evidence = pm_missing_evidence(&legs[0], "null_body", "page-2", &["cached"]);
        let current = store
            .record_order_poll(&legs[0], &evidence.poll)
            .await?
            .ok_or_else(|| anyhow::anyhow!("cache fixture poll rejected"))?;
        let mut fill = reconciliation_trade(
            evidence.poll.order_id.as_deref().unwrap(),
            "cached",
            4,
            Decimal::new(2, 2),
        );
        fill.coin = Some(current.token_id.clone());
        let progress = serde_json::to_value(evidence.pm_scan.as_ref().unwrap())?;
        ensure!(
            store
                .record_reconciliation(&current, &[fill], &evidence, &progress)
                .await?
                == LegResolution::Pending("trade_pages_incomplete")
        );
        let fills = fill_snapshots(&store.pool, id).await?;
        let parent = order_snapshot(&store.pool, order_id).await?;
        ensure!(
            fills.len() == 1 && fills[0]["raw"]["reconciliation_v1"]["finality"] == "confirmed"
        );
        for (case, reason) in [
            ("empty", "trade_scan_snapshot_mismatch"),
            ("different_ids", "trade_scan_snapshot_mismatch"),
            ("legacy", "trade_scan_evidence_missing"),
        ] {
            let current = reconciliation_open_leg(store, id).await?;
            let ids: &[&str] = if case == "different_ids" {
                &["not-cached"]
            } else {
                &[]
            };
            let mut complete = pm_missing_evidence(&current, "null_body", "LTE=", ids);
            let progress = serde_json::to_value(complete.pm_scan.as_ref().unwrap())?;
            if case == "legacy" {
                complete.pm_scan = None;
            }
            ensure!(
                store
                    .record_reconciliation(&current, &[], &complete, &progress)
                    .await?
                    == LegResolution::Pending(reason),
                "{case} reused unproven cached fills"
            );
            let leg = leg_snapshot(&store.pool, id).await?;
            ensure!(
                leg["status"] == current.status
                    && leg["actual_shares"].is_null()
                    && leg["actual_fee"].is_null()
            );
            ensure!(leg["last_order_info"]["fill_progress"] == progress);
            ensure!(fill_snapshots(&store.pool, id).await? == fills);
            ensure!(order_snapshot(&store.pool, order_id).await? == parent);
        }
        // Cached observations become usable only when this scan proves the exact nonempty ID set.
        let current = reconciliation_open_leg(store, id).await?;
        let complete = pm_missing_evidence(&current, "null_body", "LTE=", &["cached"]);
        ensure!(
            matches!(store.record_reconciliation(&current, &[], &complete,
            &serde_json::to_value(complete.pm_scan.as_ref().unwrap())?).await?,
            LegResolution::Terminal { status: "matched", shares, fee, .. }
                if shares == Decimal::from(4) && fee == Decimal::new(2, 2))
        );
        ensure!(fill_snapshots(&store.pool, id).await? == fills);
        ensure!(order_snapshot(&store.pool, order_id).await?["actual_cost"] == json!(1.62));
        Ok(())
    }
    .await;
    fixture
        .cleanup()
        .await
        .expect("clean up cached-scan schema");
    exercised.expect("empty, mismatched and legacy scans cannot close a missing order from cache");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_pm_known_execution_without_fills_locks_hash_at_both_recovery_entries() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        for source in ["typed", "order_poll", "fill_evidence"] {
            for has_ids in [true, false] {
                let (order_id, legs) = submitted_reconciliation_order(store, &["polymarket"]).await?;
                let id = legs[0].id;
                let oid = legs[0].client_order_id.as_deref().unwrap();
                let mut poll = reconciliation_evidence(oid, "live").poll;
                poll.associated_trades = if has_ids { vec!["known-but-unfetched".into()] } else { vec![] };
                poll.raw = json!({"associate_trades":poll.associated_trades});
                poll.shares = Some(if has_ids { Decimal::ZERO } else { Decimal::from(10) });
                let current = store.record_order_poll(&legs[0], &poll).await?
                    .ok_or_else(|| anyhow::anyhow!("known hash fixture rejected"))?;
                if source != "typed" {
                    let info = if source == "order_poll" { json!({"order_poll":poll}) }
                        else { json!({"fill_evidence":{"poll":poll}}) };
                    sqlx::query("UPDATE legs SET last_order_info=$2,updated_at=clock_timestamp() WHERE id=$1")
                        .bind(id).bind(info).execute(&store.pool).await?;
                }
                let current = reconciliation_open_leg(store, current.id).await?;
                let leg_before = leg_snapshot(&store.pool, id).await?;
                let parent_before = order_snapshot(&store.pool, order_id).await?;
                let envelope_before: Value = sqlx::query_scalar("SELECT to_jsonb(e) FROM signed_envelopes e WHERE leg_id=$1")
                    .bind(id).fetch_one(&store.pool).await?;
                ensure!(fill_snapshots(&store.pool, id).await?.is_empty());
                ensure!(current.third_order_id == current.client_order_id);
                ensure!(store.record_submission(id, "actived", Some("replacement-ack"),
                    &json!({"kind":"ack"}), &json!({"replacement":true})).await.is_err(),
                    "{source}/{has_ids} ACK ignored known execution without fills");
                let envelope_after: Value = sqlx::query_scalar("SELECT to_jsonb(e) FROM signed_envelopes e WHERE leg_id=$1")
                    .bind(id).fetch_one(&store.pool).await?;
                ensure!(envelope_after == envelope_before, "rejected replacement ACK leaked audit");
                let mut replacement = reconciliation_evidence("replacement-poll", "live").poll;
                replacement.shares = Some(Decimal::ZERO);
                replacement.raw = json!({"associate_trades":[]});
                for found in [true, false] {
                    replacement.found = found;
                    ensure!(store.record_order_poll(&current, &replacement).await.is_err(),
                        "{source}/{has_ids}/{found} lookup replaced hash with known execution");
                    ensure!(leg_snapshot(&store.pool, id).await? == leg_before);
                    ensure!(fill_snapshots(&store.pool, id).await?.is_empty());
                    ensure!(order_snapshot(&store.pool, order_id).await? == parent_before);
                }
            }
        }
        Ok(())
    }.await;
    fixture
        .cleanup()
        .await
        .expect("clean up known hash execution schema");
    exercised.expect("known associations or positive matched shares lock hash identity even before any fill is fetched");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_pm_hash_fills_lock_identity_and_reject_inconsistent_scan_writes() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        let (order_id, legs) = submitted_reconciliation_order(store, &["polymarket"]).await?;
        let id = legs[0].id;
        let evidence = pm_missing_evidence(&legs[0], "http_404", "page-2", &["hash-fill"]);
        let current = store
            .record_order_poll(&legs[0], &evidence.poll)
            .await?
            .ok_or_else(|| anyhow::anyhow!("hash fixture poll rejected"))?;
        let mut fill = reconciliation_trade(
            evidence.poll.order_id.as_deref().unwrap(),
            "hash-fill",
            4,
            Decimal::new(2, 2),
        );
        fill.coin = Some(current.token_id.clone());
        fill.finality = FillFinality::Pending;
        ensure!(matches!(
            store
                .record_reconciliation(
                    &current,
                    &[fill.clone()],
                    &evidence,
                    &serde_json::to_value(evidence.pm_scan.as_ref().unwrap())?
                )
                .await?,
            LegResolution::Pending(_)
        ));
        let current = reconciliation_open_leg(store, id).await?;
        ensure!(current.third_order_id == current.client_order_id);
        let leg_before = leg_snapshot(&store.pool, id).await?;
        let fills_before = fill_snapshots(&store.pool, id).await?;
        let parent_before = order_snapshot(&store.pool, order_id).await?;
        let response_before: Value =
            sqlx::query_scalar("SELECT to_jsonb(e) FROM signed_envelopes e WHERE leg_id=$1")
                .bind(id)
                .fetch_one(&store.pool)
                .await?;
        ensure!(store
            .record_submission(
                id,
                "actived",
                Some("different-ack-oid"),
                &json!({"kind":"ack"}),
                &json!({"late":true})
            )
            .await
            .is_err());
        let response_after: Value =
            sqlx::query_scalar("SELECT to_jsonb(e) FROM signed_envelopes e WHERE leg_id=$1")
                .bind(id)
                .fetch_one(&store.pool)
                .await?;
        ensure!(
            response_after == response_before,
            "rejected ACK leaked its transaction"
        );
        ensure!(leg_snapshot(&store.pool, id).await? == leg_before);
        for found in [true, false] {
            let mut poll = if found {
                reconciliation_evidence("different-poll-oid", "matched").poll
            } else {
                evidence.poll.clone()
            };
            poll.order_id = Some("different-poll-oid".into());
            ensure!(store.record_order_poll(&current, &poll).await.is_err());
            ensure!(leg_snapshot(&store.pool, id).await? == leg_before);
        }

        // Each malformed write must reject before either an incoming fill or its cursor can persist.
        for case in [
            "progress",
            "after",
            "funder",
            "asset",
            "order",
            "page_complete",
            "history_complete",
            "coin_missing",
            "coin_other",
            "trade_id",
        ] {
            let mut next =
                pm_missing_evidence(&current, "http_404", "page-3", &["hash-fill", "second"]);
            let scan = next.pm_scan.as_mut().unwrap();
            let mut incoming = fill.clone();
            incoming.trade_id = "second".into();
            match case {
                "after" => {
                    scan.after += 1;
                    scan.before += 1;
                }
                "funder" => scan.funder = "0x0000000000000000000000000000000000000002".into(),
                "asset" => scan.asset_id = "other-token".into(),
                "order" => scan.order_id = "other-order".into(),
                "page_complete" => next.page_complete = true,
                "history_complete" => next.history_complete = true,
                "coin_missing" => incoming.coin = None,
                "coin_other" => incoming.coin = Some("other-token".into()),
                "trade_id" => incoming.trade_id = "outside-scan".into(),
                _ => {}
            }
            let mut progress = serde_json::to_value(next.pm_scan.as_ref().unwrap())?;
            if case == "progress" {
                progress["next_cursor"] = json!("different-cursor");
            }
            ensure!(
                store
                    .record_reconciliation(&current, &[incoming], &next, &progress)
                    .await
                    .is_err(),
                "inconsistent {case} unexpectedly accepted"
            );
            ensure!(
                leg_snapshot(&store.pool, id).await? == leg_before,
                "{case} advanced progress"
            );
            ensure!(
                fill_snapshots(&store.pool, id).await? == fills_before,
                "{case} inserted a fill"
            );
            ensure!(order_snapshot(&store.pool, order_id).await? == parent_before);
        }
        // Without observations, either recovery entry point may replace the hash, but not its scan.
        for via_ack in [true, false] {
            let (parent_id, legs) = submitted_reconciliation_order(store, &["polymarket"]).await?;
            let mut empty_poll = reconciliation_evidence(legs[0].client_order_id.as_deref().unwrap(), "live").poll;
            empty_poll.shares = Some(Decimal::ZERO);
            empty_poll.raw = json!({"associate_trades":[]});
            let current = store.record_order_poll(&legs[0], &empty_poll).await?
                .ok_or_else(|| anyhow::anyhow!("zero-execution hash poll rejected"))?;
            let evidence = pm_missing_evidence(&current, "http_404", "page-2", &[]);
            let current = store
                .record_order_poll(&current, &evidence.poll)
                .await?
                .ok_or_else(|| anyhow::anyhow!("empty hash scan poll rejected"))?;
            ensure!(matches!(
                store
                    .record_reconciliation(
                        &current,
                        &[],
                        &evidence,
                        &serde_json::to_value(evidence.pm_scan.as_ref().unwrap())?
                    )
                    .await?,
                LegResolution::Pending(_)
            ));
            let current = reconciliation_open_leg(store, current.id).await?;
            ensure!(current.third_order_id == current.client_order_id);
            let info = current.last_order_info.as_ref().unwrap();
            ensure!(info["fill_progress"].is_object() && info.get("fill_evidence").is_none());
            ensure!(info["pm_order_constraints"]["order_id"] == current.client_order_id.as_deref().unwrap());
            ensure!(info["pm_order_constraints"]["associated_trade_ids"] == json!([]));
            ensure!(info["pm_order_constraints"]["matched_shares_lower_bound"] == serde_json::to_value(Decimal::ZERO)?);
            // Keep the old found poll so ACK recovery must filter it by oid, not resurrect its zero bound.
            sqlx::query("UPDATE legs SET last_order_info=jsonb_set(last_order_info,'{order_poll}',$2),updated_at=clock_timestamp() WHERE id=$1")
                .bind(current.id).bind(serde_json::to_value(&empty_poll)?).execute(&store.pool).await?;
            let current = reconciliation_open_leg(store, current.id).await?;
            let oid = format!("recovered-without-fills-{}", current.id);
            let submitted_at = leg_snapshot(&store.pool, current.id).await?["submitted_at"].clone();
            if via_ack {
                store
                    .record_submission(
                        current.id,
                        "actived",
                        Some(&oid),
                        &json!({"kind":"ack"}),
                        &json!({"ack":true}),
                    )
                    .await?;
            } else {
                let mut replacement = reconciliation_evidence(&oid, "live").poll;
                replacement.shares = Some(Decimal::ZERO);
                replacement.raw = json!({"associate_trades":[]});
                ensure!(store.record_order_poll(&current, &replacement).await?.is_some());
            }
            let recovered = reconciliation_open_leg(store, current.id).await?;
            ensure!(recovered.third_order_id.as_deref() == Some(oid.as_str()));
            ensure!(
                recovered.status == "actived" && recovered.submitted_at == current.submitted_at
            );
            let info = recovered.last_order_info.as_ref().unwrap();
            ensure!(
                info.get("fill_progress") == Some(&Value::Null)
                    && info.get("fill_evidence").is_none_or(Value::is_null),
                "identity recovery retained the old hash scan"
            );
            if via_ack {
                ensure!(info["pm_order_constraints"].is_null(), "ACK retained old hash constraints");
            } else {
                ensure!(info["pm_order_constraints"]["order_id"] == oid);
                ensure!(info["pm_order_constraints"]["associated_trade_ids"] == json!([]));
            }
            let missing = pm_missing_evidence(&recovered, "null_body", "LTE=", &[]);
            let recovered = store.record_order_poll(&recovered, &missing.poll).await?
                .ok_or_else(|| anyhow::anyhow!("recovered missing poll rejected"))?;
            let constraints = &recovered.last_order_info.as_ref().unwrap()["pm_order_constraints"];
            ensure!(constraints["order_id"] == oid && constraints["associated_trade_ids"] == json!([]));
            ensure!(constraints["matched_shares_lower_bound"] == if via_ack { Value::Null } else { serde_json::to_value(Decimal::ZERO)? },
                "old hash order_poll was restored under the recovered oid");
            ensure!(matches!(store.record_reconciliation(&recovered, &[], &missing,
                &serde_json::to_value(missing.pm_scan.as_ref().unwrap())?).await?,
                LegResolution::Terminal {status:"failed", shares, price, fee, ..}
                    if shares.is_zero() && price.is_zero() && fee.is_zero()));
            ensure!(!store.open_legs().await?.iter().any(|leg| leg.id == recovered.id));
            let snapshot = leg_snapshot(&store.pool, recovered.id).await?;
            ensure!(snapshot["last_order_info"]["waiting_reason"].is_null());
            ensure!(store.record_order_poll(&recovered, &missing.poll).await?.is_none());
            ensure!(fill_snapshots(&store.pool, current.id).await?.is_empty());
            let parent = order_snapshot(&store.pool, parent_id).await?;
            ensure!(parent["status"] == "cancelled" && parent["actual_cost"] == json!(0.0));
            ensure!(leg_snapshot(&store.pool, recovered.id).await?["submitted_at"] == submitted_at);
        }
        Ok(())
    }
    .await;
    fixture
        .cleanup()
        .await
        .expect("clean up hash identity schema");
    exercised
        .expect("PM hash fills lock their order identity; empty scans are cleared on recovery");
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
async fn reconciliation_pm_stale_and_parent_sql_failure_roll_back_constraint_growth_with_fills() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        let (order_id, legs) = submitted_reconciliation_order(store, &["polymarket"]).await?;
        let id = legs[0].id;
        let oid = format!("atomic-constraints-{id}");
        let mut evidence = reconciliation_evidence(&oid, "matched");
        evidence.poll.shares = Some(Decimal::from(6));
        evidence.poll.associated_trades = vec!["a".into()];
        evidence.poll.raw = json!({"associate_trades":["a"]});
        let stale = store.record_order_poll(&legs[0], &evidence.poll).await?
            .ok_or_else(|| anyhow::anyhow!("atomic constraints fixture poll rejected"))?;
        let mut pending = reconciliation_trade(&oid, "a", 6, Decimal::new(2, 2));
        pending.coin = Some(stale.token_id.clone());
        pending.finality = FillFinality::Pending;
        ensure!(matches!(store.record_reconciliation(&stale, &[pending], &evidence,
            &json!({"cursor":"page-2"})).await?, LegResolution::Pending(_)));
        let current = reconciliation_open_leg(store, id).await?;
        // This poll is committed separately and must survive either stale work or a later SQL failure.
        let current = store.record_order_poll(&current, &evidence.poll).await?
            .ok_or_else(|| anyhow::anyhow!("separately committed poll rejected"))?;
        let leg_before = leg_snapshot(&store.pool, id).await?;
        let fills_before = fill_snapshots(&store.pool, id).await?;
        let parent_before = order_snapshot(&store.pool, order_id).await?;
        let known = leg_before["last_order_info"]["pm_order_constraints"].clone();
        ensure!(known["associated_trade_ids"] == json!(["a"]));
        ensure!(known["matched_shares_lower_bound"] == serde_json::to_value(Decimal::from(6))?);
        ensure!(leg_before["last_order_info"]["order_poll"] == serde_json::to_value(&evidence.poll)?);
        let mut a = reconciliation_trade(&oid, "a", 6, Decimal::new(2, 2));
        let mut b = reconciliation_trade(&oid, "b", 4, Decimal::from(99));
        a.coin = Some(current.token_id.clone());
        b.coin = Some(current.token_id.clone());
        b.finality = FillFinality::Failed;
        evidence.poll.associated_trades.push("b".into());
        evidence.poll.raw = json!({"associate_trades":["a","b"]});
        evidence.poll.shares = Some(Decimal::from(10));
        let fills = [a, b];
        let progress = json!({"complete":true,"cursor":"LTE="});
        ensure!(evidence.pm_order_constraints.is_none());
        ensure!(store.record_reconciliation(&stale, &fills, &evidence, &progress).await?
            == LegResolution::Pending("stale_leg_snapshot"));
        ensure!(store.record_order_poll(&stale, &evidence.poll).await?.is_none());
        ensure!(leg_snapshot(&store.pool, id).await? == leg_before);
        ensure!(fill_snapshots(&store.pool, id).await? == fills_before);
        ensure!(order_snapshot(&store.pool, order_id).await? == parent_before);

        let constraint = "reconciliation_constraint_growth_fault";
        sqlx::query(&format!("ALTER TABLE arb_orders ADD CONSTRAINT {constraint} CHECK (id <> {order_id} OR actual_cost = 0)"))
            .execute(&store.pool).await?;
        let rejected = store.record_reconciliation(&current, &fills, &evidence, &progress).await;
        ensure!(matches!(&rejected, Err(market_arb::error::Error::Sqlx(sqlx::Error::Database(err)))
            if err.code().as_deref() == Some("23514") && err.constraint() == Some(constraint)),
            "expected parent SQL failure after typed constraint growth, got {rejected:?}");
        ensure!(leg_snapshot(&store.pool, id).await? == leg_before,
            "constraint/progress update leaked or previously committed poll was lost");
        ensure!(fill_snapshots(&store.pool, id).await? == fills_before, "fill update or insertion leaked");
        ensure!(order_snapshot(&store.pool, order_id).await? == parent_before);
        sqlx::query(&format!("ALTER TABLE arb_orders DROP CONSTRAINT {constraint}"))
            .execute(&store.pool).await?;
        ensure!(store.record_reconciliation(&current, &fills, &evidence, &progress).await? == LegResolution::Terminal {
            status:"matched", shares:Decimal::from(6), price:Decimal::new(4,1), fee:Decimal::new(2,2), fee_sources:vec!["actual"]
        });
        let terminal = leg_snapshot(&store.pool, id).await?;
        let info = &terminal["last_order_info"];
        ensure!(info["pm_order_constraints"]["associated_trade_ids"] == json!(["a","b"]));
        ensure!(info["pm_order_constraints"]["matched_shares_lower_bound"] == serde_json::to_value(Decimal::from(10))?);
        ensure!(info.get("fill_evidence").is_none());
        ensure!(info["fill_progress"] == progress && info["order_poll"] == leg_before["last_order_info"]["order_poll"]);
        ensure!(terminal["submitted_at"] == leg_before["submitted_at"]);
        ensure!(fill_snapshots(&store.pool, id).await?.len() == 2);
        let parent = order_snapshot(&store.pool, order_id).await?;
        ensure!(parent["status"] == "completed" && parent["actual_cost"] == json!(2.42));
        Ok(())
    }.await;
    fixture
        .cleanup()
        .await
        .expect("clean up atomic constraints schema");
    exercised.expect("stale work and parent SQL failures roll back constraints, progress and fills without losing committed polls");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn reconciliation_sql_failures_roll_back_fills_leg_and_parent_before_retry() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        let store = &fixture.store;
        store.migrate().await?;
        for (platform, table) in [("outcome", "legs"), ("outcome", "arb_orders"), ("polymarket", "arb_orders")] {
            let (order_id, legs) = submitted_reconciliation_order(store, &[platform]).await?;
            let id = legs[0].id;
            let evidence = if platform == "polymarket" {
                pm_missing_evidence(&legs[0], "http_404", "page-2", &["real"])
            } else {
                reconciliation_evidence(&format!("fault-oid-{id}"), "filled")
            };
            let mut current = store.record_order_poll(&legs[0], &evidence.poll).await?
                .ok_or_else(|| anyhow::anyhow!("fault fixture poll rejected"))?;
            let mut fill = reconciliation_trade(evidence.poll.order_id.as_deref().unwrap(), "real", 10, Decimal::new(7, 2));
            fill.coin = Some(current.token_id.clone());
            let (evidence, progress) = if platform == "polymarket" {
                let mut pending = fill.clone();
                pending.finality = FillFinality::Pending;
                ensure!(matches!(store.record_reconciliation(&current, &[pending], &evidence,
                    &serde_json::to_value(evidence.pm_scan.as_ref().unwrap())?).await?, LegResolution::Pending(_)));
                current = reconciliation_open_leg(store, id).await?;
                let evidence = pm_missing_evidence(&current, "http_404", "LTE=", &["real"]);
                let progress = serde_json::to_value(evidence.pm_scan.as_ref().unwrap())?;
                (evidence, progress)
            } else {
                (evidence, json!({"complete":true}))
            };
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
            let rejected = store.record_reconciliation(&current, &[fill.clone()], &evidence, &progress).await;
            ensure!(matches!(&rejected, Err(market_arb::error::Error::Sqlx(sqlx::Error::Database(err)))
                if err.code().as_deref() == Some("23514") && err.constraint() == Some(constraint.as_str())),
                "expected injected {table} constraint failure, got {rejected:?}");
            ensure!(fill_snapshots(&store.pool, id).await? == fills_before, "{table} failure leaked fill");
            ensure!(leg_snapshot(&store.pool, id).await? == leg_before, "{table} failure leaked leg/progress update");
            ensure!(order_snapshot(&store.pool, order_id).await? == parent_before, "{table} failure leaked parent actuals");
            sqlx::query(&format!("ALTER TABLE {table} DROP CONSTRAINT {constraint}"))
                .execute(&store.pool).await?;
            // The original snapshot must still be valid because every preceding write rolled back.
            ensure!(matches!(store.record_reconciliation(&current, &[fill], &evidence, &progress).await?,
                LegResolution::Terminal {status:"matched", ..}));
            ensure!(leg_snapshot(&store.pool, id).await?["last_order_info"]["fill_progress"] == progress);
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

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn outcome_fee_estimates_survive_abort_timeout_and_lifecycle_insert() {
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        fixture.store.migrate().await?;
        let estimate = json!({
            "model": "outcome_spot_close_v1", "taker_rate": "0.001344",
            "settlement_policy": "estimated_from_taker_close", "actual": false
        });
        let legs: Vec<_> = (0..3).map(|_| NewLeg {
            fee_estimate: Some(estimate.clone()),
            ..identity_probe_leg()
        }).collect();
        let (order_id, ids) = fixture.store.insert_actived_order_with_legs(
            TopicKey::new(Uuid::new_v4(), 0),
            &MarketIdentity::new("polymarket", "fee-estimate-test")?,
            "fee estimate", "fee estimate", None,
            Decimal::ONE, Decimal::ZERO, Decimal::ONE, &json!([]), &legs, 0,
            std::time::Instant::now() + std::time::Duration::from_secs(30),
        ).await?;
        // 签名证据存在时，费用快照失效不能撤销可能已经提交的腿。
        sqlx::query("INSERT INTO signed_envelopes (leg_id, order_hash, payload) VALUES ($1,'test-envelope','{}')")
            .bind(ids[1]).execute(&fixture.store.pool).await?;
        fixture.store.abort_unsubmitted_legs(&ids[..2], "fee_snapshot_changed").await?;
        let aborted = leg_snapshot(&fixture.store.pool, ids[0]).await?;
        ensure!(aborted["status"] == "failed");
        ensure!(aborted["last_order_info"]["fee_estimate"] == estimate);
        let signed = leg_snapshot(&fixture.store.pool, ids[1]).await?;
        ensure!(signed["status"] == "pending");
        sqlx::query("UPDATE legs SET created_at=NOW()-INTERVAL '1 hour' WHERE id=$1")
            .bind(ids[2]).execute(&fixture.store.pool).await?;
        fixture.store.fail_stale_pending_unsubmitted(std::time::Duration::from_secs(300)).await?;
        let expired = leg_snapshot(&fixture.store.pool, ids[2]).await?;
        ensure!(expired["status"] == "failed");
        ensure!(expired["last_order_info"]["fee_estimate"] == estimate);
        ensure!(expired["actual_fee"].is_null());
        let claim = Uuid::new_v4();
        sqlx::query("UPDATE arb_orders SET lifecycle_action='rebalance', lifecycle_claim_id=$2, lifecycle_claimed_at=NOW() WHERE id=$1")
            .bind(order_id).bind(claim).execute(&fixture.store.pool).await?;
        let added = fixture.store.insert_legs_atomic(order_id, "rebalance", claim, &legs[..1]).await?;
        let added = leg_snapshot(&fixture.store.pool, added[0]).await?;
        ensure!(added["last_order_info"]["fee_estimate"] == estimate);
        ensure!(added["actual_fee"].is_null());
        Ok(())
    }.await;
    fixture
        .cleanup()
        .await
        .expect("clean up fee estimate schema");
    exercised.expect("estimate remains separate from actual fee and survives lifecycle writes");
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
        fee_estimate: None,
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
                fee_estimate: None,
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
                fee_estimate: None,
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
                0,
                std::time::Instant::now() + std::time::Duration::from_secs(30),
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
                0,
                std::time::Instant::now() + std::time::Duration::from_secs(30),
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

async fn admission_order(
    store: &Store,
    key: TopicKey,
    limit: usize,
    deadline: std::time::Instant,
    legs: &[NewLeg<'_>],
) -> market_arb::error::Result<(i64, Vec<i64>)> {
    store
        .insert_actived_order_with_legs(
            key,
            &MarketIdentity::new("polymarket", "admission-condition")?,
            "admission test",
            "admission test",
            None,
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::ONE,
            &json!([]),
            legs,
            limit,
            deadline,
        )
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn admission_serializes_last_slot_and_preserves_counting_policy() {
    use market_arb::error::Error;
    use std::time::{Duration, Instant};
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        fixture.store.migrate().await?;
        for index in 0..19 {
            let status = ["pending", "actived", "completed"][index % 3];
            sqlx::query("INSERT INTO arb_orders(event_id,unified_index,status) VALUES ($1,0,$2)")
                .bind(Uuid::new_v4())
                .bind(status)
                .execute(&fixture.store.pool)
                .await?;
        }
        // completed 的已平仓/结算行仍沿用原计数口径，不顺手改变风控定义。
        sqlx::query("UPDATE arb_orders SET position_status='closed' WHERE status='completed'")
            .execute(&fixture.store.pool)
            .await?;
        for status in ["cancelled", "failed"] {
            sqlx::query("INSERT INTO arb_orders(event_id,unified_index,status) VALUES ($1,0,$2)")
                .bind(Uuid::new_v4())
                .bind(status)
                .execute(&fixture.store.pool)
                .await?;
        }
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let attempt = || {
            let store = fixture.store.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                admission_order(
                    &store,
                    TopicKey::new(Uuid::new_v4(), 0),
                    20,
                    Instant::now() + Duration::from_secs(30),
                    &[identity_probe_leg()],
                )
                .await
            })
        };
        let (a, b) = tokio::join!(attempt(), attempt());
        let results = [a?, b?];
        ensure!(
            results.iter().filter(|r| r.is_ok()).count() == 1,
            "exactly one admission must win"
        );
        ensure!(
            results
                .iter()
                .filter(|r| matches!(r, Err(Error::OrderCapacityReached)))
                .count()
                == 1
        );
        ensure!(fixture.store.count_active_orders().await? == 20);
        let child_counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM legs),(SELECT COUNT(*) FROM arb_order_market_identities)",
        )
        .fetch_one(&fixture.store.pool)
        .await?;
        ensure!(
            child_counts == (1, 1),
            "rejected admission must leave no children"
        );
        let (unlimited, _) = admission_order(
            &fixture.store,
            TopicKey::new(Uuid::new_v4(), 0),
            0,
            Instant::now() + Duration::from_secs(30),
            &[identity_probe_leg()],
        )
        .await?;
        ensure!(unlimited > 0 && fixture.store.count_active_orders().await? == 21);
        let expired = admission_order(
            &fixture.store,
            TopicKey::new(Uuid::new_v4(), 0),
            0,
            Instant::now(),
            &[identity_probe_leg()],
        )
        .await;
        ensure!(matches!(expired, Err(Error::OrderConfirmationExpired)));
        ensure!(fixture.store.count_active_orders().await? == 21);
        Ok(())
    }
    .await;
    fixture.cleanup().await.expect("clean up admission schema");
    exercised.expect("atomic admission and counting policy");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn admission_rolls_back_failed_legs_and_preserves_topic_uniqueness() {
    use market_arb::error::Error;
    use std::time::{Duration, Instant};
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        fixture.store.migrate().await?;
        let mut bad = identity_probe_leg();
        bad.side="too-long-side";
        let deadline = || Instant::now()+Duration::from_secs(30);
        ensure!(admission_order(&fixture.store,TopicKey::new(Uuid::new_v4(),0),1,deadline(),&[bad]).await.is_err());
        ensure!(fixture.store.count_active_orders().await?==0);
        let key=TopicKey::new(Uuid::new_v4(),0);
        admission_order(&fixture.store,key,1,deadline(),&[identity_probe_leg()]).await?;
        let duplicate=admission_order(&fixture.store,key,0,deadline(),&[identity_probe_leg()]).await;
        ensure!(matches!(duplicate,Err(Error::Sqlx(sqlx::Error::Database(ref db))) if db.code().as_deref()==Some("23505")));
        ensure!(fixture.store.count_active_orders().await?==1);
        let counts:(i64,i64)=sqlx::query_as("SELECT (SELECT COUNT(*) FROM legs),(SELECT COUNT(*) FROM arb_order_market_identities)")
            .fetch_one(&fixture.store.pool).await?;
        ensure!(counts==(1,1));
        Ok(())
    }.await;
    fixture.cleanup().await.expect("clean up admission schema");
    exercised.expect("admission rollback and uniqueness");
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn admission_deadline_bounds_lock_wait_without_pending_legs() {
    use market_arb::error::Error;
    use std::time::{Duration, Instant};
    let fixture = MigrationFixture::new().await.expect(POSTGRES_REQUIRED);
    let exercised: Result<()> = async {
        fixture.store.migrate().await?;
        let mut blocker = fixture.store.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(0x6d61726b_61726201_i64)
            .execute(&mut *blocker)
            .await?;
        let store = fixture.store.clone();
        let mut attempt = tokio::spawn(async move {
            admission_order(
                &store,
                TopicKey::new(Uuid::new_v4(), 0),
                1,
                Instant::now() + Duration::from_millis(100),
                &[identity_probe_leg()],
            )
            .await
        });
        // 保持锁直到调用超时；取消中的SQL可以等待事务回滚，因此先释放阻塞者再收尾。
        let before_release = tokio::time::timeout(Duration::from_millis(200), &mut attempt).await;
        blocker.rollback().await?;
        let result = match before_release {
            Ok(result) => result?,
            Err(_) => tokio::time::timeout(Duration::from_secs(5), attempt).await??,
        };
        ensure!(
            matches!(result, Err(Error::OrderConfirmationExpired)),
            "unexpected admission result: {result:?}"
        );
        ensure!(fixture.store.count_active_orders().await? == 0);
        admission_order(
            &fixture.store,
            TopicKey::new(Uuid::new_v4(), 0),
            1,
            Instant::now() + Duration::from_secs(30),
            &[identity_probe_leg()],
        )
        .await?;
        Ok(())
    }
    .await;
    fixture.cleanup().await.expect("clean up admission schema");
    exercised.expect("admission deadline and lock release");
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
            fee_estimate: None,
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
            0,
            std::time::Instant::now() + std::time::Duration::from_secs(30),
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
