//! Settlement fee allocation and PostgreSQL integration regressions.
//!
//! DB tests are ignored by default. Explicitly run with APP_POSTGRES_URI configured:
//! cargo test --test settlement_fees -- --ignored --nocapture
//! No dotenv loading: a missing/empty URI fails explicitly selected DB tests.
//! Every connection (including DDL connections) has only a unique private schema in
//! search_path. Migrations and cleanup never target public or another test's schema.

use anyhow::{ensure, Result};
use market_arb::store::settlement_fees::{
    preview_allocations, FeeEvent, GroupKey, GroupSnapshot, SnapshotFill, SnapshotLeg,
};
use market_arb::store::{LegRow, Store};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;

fn dec(value: &str) -> Decimal {
    Decimal::from_str(value).expect("literal decimal")
}

fn key() -> GroupKey {
    GroupKey {
        network: "mainnet".into(),
        wallet: "0x0000000000000000000000000000000000000001".into(),
        token: "#100000001".into(),
    }
}

fn payouts() -> HashMap<(String, String), Decimal> {
    HashMap::from([(("outcome".into(), key().token), Decimal::ONE)])
}

fn event(quantity: Decimal, fee: Decimal) -> FeeEvent {
    FeeEvent {
        tid: "9001".into(),
        quantity,
        payout: Decimal::ONE,
        fee,
        fee_token: "USDC".into(),
        evidence: json!({"source":"integration_fixture","tid":9001}),
    }
}

fn proof() -> Value {
    json!({"version":1,"source":"integration_fixture", "ownership_verified":true,"transfers_checked":true})
}

// Both pure and SQL fixtures use explicit quantities, durable identities and
// matching fill economics; no missing quantity is interpreted as zero.
fn leg(order_id: i64, id: i64, side: &str, quantity: Decimal) -> SnapshotLeg {
    let price = dec("0.54383222"); // 9 * price = the reported trade cost 4.89448998.
    let now = chrono::Utc::now();
    SnapshotLeg {
        leg: LegRow {
            id,
            order_id,
            platform: "outcome".into(),
            token_id: key().token,
            label: "YES".into(),
            side: side.into(),
            intent: if side == "SELL" {
                "rebalance_sell"
            } else {
                "arb_buy"
            }
            .into(),
            funder_address: None,
            wallet_address: Some(key().wallet),
            service: None,
            req_price: Some(price),
            req_shares: Some(quantity),
            client_order_id: Some(format!("fixture-{id}")),
            third_order_id: Some(id.to_string()),
            status: "completed".into(),
            submitted_at: Some(now),
            last_order_info: None,
            updated_at: now,
        },
        actual_shares: Some(quantity),
        actual_price: Some(price),
        actual_fee: Some(Decimal::ZERO),
        fills: if quantity.is_zero() {
            vec![]
        } else {
            vec![SnapshotFill {
                trade_id: id.to_string(),
                third_order_id: id.to_string(),
                shares: quantity,
                price,
                fee: Decimal::ZERO,
                raw: json!({"reconciliation_v1":{"raw":{
                    "tid":id,"oid":id,"coin":key().token,
                    "side":if side == "SELL" {"A"} else {"B"},
                    "sz":quantity.to_string(),"px":price.to_string(),"fee":"0","feeToken":"USDC"
                }}}),
            }]
        },
    }
}

fn snapshot(legs: Vec<SnapshotLeg>) -> GroupSnapshot {
    GroupSnapshot {
        key: key(),
        legs,
        allocations: vec![],
        sealed: false,
    }
}

#[tokio::test]
async fn completed_partial_window_rescans_with_expanded_end_time() -> Result<()> {
    use market_arb::platforms::outcome::settlement::SettlementScan;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = snapshot(vec![leg(1, 1, "BUY", dec("9"))]);
    s.legs[0].leg.submitted_at = chrono::DateTime::from_timestamp_millis(2000);
    let mut previous = SettlementScan::new(&key().wallet, &key().token, 1000, 3000)?;
    previous.complete = true;
    previous.history_complete = true;
    let mut value = serde_json::to_value(previous)?;
    value["events"] = json!([{ "tid":9001,"hash":"0xabc","oid":0,"coin":key().token,"time":2500,"sz":"3","px":"1","fee":"0.004032","feeToken":"USDC","startPosition":"9","dir":"Settlement","side":"A" }]);
    // Build event through the production parser so this remains schema-accurate.
    let partial = market_arb::platforms::outcome::settlement::apply_page(
        &json!([{ "tid":9001,"hash":"0xabc","oid":0,"coin":key().token,"time":2500,"sz":"3","px":"1","fee":"0.004032","feeToken":"USDC","startPosition":"9","dir":"Settlement","side":"A" }]),
        &SettlementScan::new(&key().wallet, &key().token, 1000, 3000)?,
    )?;
    value["events"] = serde_json::to_value(partial.progress.events)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = vec![0; 8192];
        let n = socket.read(&mut bytes).await.unwrap();
        let request = String::from_utf8_lossy(&bytes[..n]);
        assert!(request.contains("\"endTime\":4000"));
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]")
            .await
            .unwrap();
    });
    let next =
        market_arb::settlement_fees::advance(&reqwest::Client::new(), &url, &s, Some(&value), 4000)
            .await?;
    server.await?;
    ensure!(next.end_time == 4000 && next.events.is_empty());
    Ok(())
}

#[test]
fn allocation_conserves_sample_fee_and_assigns_rounding_remainder_by_order_id() -> Result<()> {
    let mut s = snapshot(vec![
        leg(20, 2, "BUY", dec("6")),
        leg(10, 1, "BUY", dec("3")),
    ]);
    let events = [event(dec("9"), dec("0.012096"))];
    let rows = preview_allocations(&s, Decimal::ONE, &events)?;
    ensure!(rows.len() == 2 && rows[0].order_id == 10 && rows[1].order_id == 20);
    ensure!(rows[0].fee == dec("0.004032") && rows[1].fee == dec("0.008064"));
    ensure!(rows.iter().map(|r| r.fee).sum::<Decimal>() == events[0].fee);
    ensure!(rows.iter().all(|r| r.status == "verified"));
    let tiny = [event(dec("9"), dec("0.000001"))];
    let rounded = preview_allocations(&s, Decimal::ONE, &tiny)?;
    ensure!(rounded[0].fee.is_zero() && rounded[1].fee == dec("0.000001"));
    s.legs.reverse();
    ensure!(
        serde_json::to_value(preview_allocations(&s, Decimal::ONE, &tiny)?)?
            == serde_json::to_value(rounded)?
    );
    Ok(())
}

#[test]
fn allocation_zero_net_position_needs_no_fee_event() -> Result<()> {
    let s = snapshot(vec![
        leg(1, 1, "BUY", dec("9")),
        leg(1, 2, "SELL", dec("9")),
        leg(2, 3, "BUY", Decimal::ZERO),
    ]);
    let rows = preview_allocations(&s, Decimal::ONE, &[])?;
    ensure!(rows.len() == 2);
    ensure!(rows
        .iter()
        .all(|r| r.quantity.is_zero() && r.fee.is_zero() && r.status == "not_applicable"));
    Ok(())
}

#[test]
fn allocation_rejects_missing_quantities_inconsistent_fills_and_invalid_events() -> Result<()> {
    let s = snapshot(vec![leg(1, 1, "BUY", dec("9"))]);
    let valid = event(dec("9"), dec("0.012096"));
    ensure!(preview_allocations(&s, Decimal::ONE, &[]).is_err());
    ensure!(preview_allocations(&s, Decimal::ONE, &[valid.clone(), valid.clone()]).is_err());
    for invalid in [event(dec("8"), valid.fee), event(dec("9"), dec("-0.1"))] {
        ensure!(preview_allocations(&s, Decimal::ONE, &[invalid]).is_err());
    }
    ensure!(preview_allocations(&s, Decimal::ZERO, &[valid.clone()]).is_err());
    let mut missing = s.clone();
    missing.legs[0].actual_shares = None;
    ensure!(preview_allocations(&missing, Decimal::ONE, &[valid.clone()]).is_err());
    let mut inconsistent = s.clone();
    inconsistent.legs[0].fills[0].shares = dec("8");
    ensure!(preview_allocations(&inconsistent, Decimal::ONE, &[valid.clone()]).is_err());
    let mut pending = s;
    pending.legs[0].leg.status = "submitted".into();
    ensure!(preview_allocations(&pending, Decimal::ONE, &[valid]).is_err());
    Ok(())
}

struct Fixture {
    store: Store,
    ddl: PgPool,
    schema: String,
}

async fn private_pool(uri: &str, schema: &str, size: u32) -> Result<PgPool> {
    let schema = schema.to_owned();
    Ok(PgPoolOptions::new()
        .max_connections(size)
        .after_connect(move |connection, _| {
            let schema = schema.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('search_path', $1, false)")
                    .bind(schema)
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(uri)
        .await?)
}

impl Fixture {
    async fn new() -> Result<Option<Self>> {
        let Some(uri) = std::env::var("APP_POSTGRES_URI")
            .ok()
            .filter(|v| !v.trim().is_empty())
        else {
            anyhow::bail!("BLOCKED: explicit PostgreSQL test requires nonempty process APP_POSTGRES_URI; no dotenv loading");
        };
        // Generated identifier only. PostgreSQL permits search_path to name a
        // not-yet-created schema; even the DDL pool never includes public.
        let schema = format!("settlement_fees_test_{}", Uuid::new_v4().simple());
        let ddl = private_pool(&uri, &schema, 1).await?;
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&ddl)
            .await?;
        let pool = match private_pool(&uri, &schema, 4).await {
            Ok(pool) => pool,
            Err(error) => {
                sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
                    .execute(&ddl)
                    .await?;
                ddl.close().await;
                return Err(error);
            }
        };
        let fixture = Self {
            store: Store { pool },
            ddl,
            schema,
        };
        if let Err(error) = fixture.store.migrate().await {
            fixture.cleanup().await?;
            return Err(error.into());
        }
        Ok(Some(fixture))
    }

    async fn cleanup(self) -> Result<()> {
        self.store.pool.close().await;
        let result = sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.ddl)
            .await;
        self.ddl.close().await;
        result?;
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires process APP_POSTGRES_URI; private schema only"]
async fn lifecycle_risk_claim_sum_timeout_and_notification() -> Result<()> {
    use market_arb::store::NewLeg;
    use std::time::Duration;
    let f = Fixture::new().await?.unwrap();
    let exercised: Result<()> = async {
        let s = &f.store;
        let id:i64=sqlx::query_scalar("INSERT INTO arb_orders(event_id,unified_index,status) VALUES($1,0,'completed') RETURNING id").bind(Uuid::new_v4()).fetch_one(&s.pool).await?;
        sqlx::query("INSERT INTO legs(order_id,platform,token_id,label,side,intent,funder_address,status,req_shares,req_price,req_fee,actual_shares,actual_price,actual_fee,submitted_at) VALUES($1,'polymarket','token','yes','BUY','arb_buy','funder','completed',10,0.5,0,10,0.5,0,NOW())").bind(id).execute(&s.pool).await?;
        let before=s.refresh_order_actuals(id).await?.actuals().unwrap();
        let (a,b)=tokio::join!(s.try_claim_lifecycle(id,"rebalance"),s.try_claim_lifecycle(id,"take_profit"));
        let (a,b)=(a?,b?);ensure!(a.is_some()!=b.is_some());
        let (claim,action)=if let Some(a)=a {(a,"rebalance")} else {(b.unwrap(),"take_profit")};
        let new=NewLeg{platform:"polymarket",token_id:"token",label:"yes",side:"SELL",intent:action,funder:Some("funder"),wallet:None,service:None,req_price:dec("0.5"),req_shares:dec("1"),req_fee:Decimal::ZERO,client_order_id:None,fee_estimate:None};
        let legs=s.insert_legs_atomic(&|_,_|None,id,action,claim,&[new]).await?;
        sqlx::query("UPDATE arb_orders SET actual_cost=20,actual_rev=3,actual_profit=-17 WHERE id=$1").bind(id).execute(&s.pool).await?;
        let risk=s.sum_actual_profit_with_timeouts(Duration::from_secs(300),Duration::from_secs(300)).await?;
        ensure!(risk==(before.2,0));
        ensure!(s.sum_actual_profit_with_timeouts(Duration::ZERO,Duration::ZERO).await?==(dec("-17"),1));
        let evidence:Value=sqlx::query_scalar("SELECT actuals_projection FROM arb_orders WHERE id=$1").bind(id).fetch_one(&s.pool).await?;
        ensure!(evidence["status"]=="unknown");
        ensure!(!s.release_lifecycle(id,action,Uuid::new_v4()).await?);
        ensure!(s.sum_actual_profit_with_timeouts(Duration::from_secs(300),Duration::from_secs(300)).await?==risk);
        // Candidate JSON corruption must fail closed without a SQL cast or panic.
        for bad in [json!(null),json!([]),json!({"version":"wrong"}),json!(true)] {
            let mut corrupt=evidence.clone();corrupt["lifecycle_risk"]=bad;
            sqlx::query("UPDATE arb_orders SET actuals_projection=$2 WHERE id=$1").bind(id).bind(corrupt).execute(&s.pool).await?;
            ensure!(s.sum_actual_profit_with_timeouts(Duration::from_secs(300),Duration::from_secs(300)).await?.1==1);
        }
        sqlx::query("UPDATE arb_orders SET actuals_projection=$2 WHERE id=$1").bind(id).bind(&evidence).execute(&s.pool).await?;
        let (released,amounts)=s.release_lifecycle_with_actuals(id,action,claim).await?;ensure!(released && amounts.is_none());
        let stored:Value=sqlx::query_scalar("SELECT actuals_projection FROM arb_orders WHERE id=$1").bind(id).fetch_one(&s.pool).await?;ensure!(stored.get("lifecycle_risk").is_none());
        ensure!(s.sum_actual_profit_with_timeouts(Duration::from_secs(300),Duration::from_secs(300)).await?==(dec("-17"),1));
        let next=s.try_claim_lifecycle(id,action).await?.unwrap();
        ensure!(s.sum_actual_profit_with_timeouts(Duration::from_secs(300),Duration::from_secs(300)).await?.1==1);
        s.abort_unsubmitted_legs(&|_,_|None,&legs,"test cleanup").await?;
        ensure!(s.release_lifecycle(id,action,next).await?);
        // actual_cost has scale 8 while rev/profit retain the full product precision.
        sqlx::query("UPDATE legs SET actual_shares=1.12345678,actual_price=0.12345678 WHERE order_id=$1 AND intent='arb_buy'").bind(id).execute(&s.pool).await?;
        let precise=s.refresh_order_actuals(id).await?.actuals().unwrap();
        ensure!(s.sum_actual_profit().await?==(precise.2,0));
        let claim=s.try_claim_lifecycle(id,action).await?.unwrap();
        ensure!(s.sum_actual_profit().await?==(precise.2,0));
        ensure!(s.release_lifecycle(id,action,claim).await?);
        Ok(())
    }.await;
    f.cleanup().await?;
    exercised
}

async fn insert_order(store: &Store, quantity: Decimal) -> Result<i64> {
    let id: i64 = sqlx::query_scalar("INSERT INTO arb_orders(event_id,unified_index,status) VALUES($1,0,'completed') RETURNING id")
        .bind(Uuid::new_v4()).fetch_one(&store.pool).await?;
    insert_leg(store, id, "BUY", quantity).await?;
    save_results(store, id).await?;
    Ok(id)
}

async fn save_results(store: &Store, id: i64) -> Result<()> {
    use market_arb::settlement::SettlementPayout;
    for (platform, market, tokens) in [
        ("polymarket", "condition-fixture", ["pm-win", "pm-lose"]),
        ("outcome", "10000000", ["#100000001", "#100000000"]),
    ] {
        sqlx::query("INSERT INTO arb_order_market_identities(order_id,platform,market_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
            .bind(id).bind(platform).bind(market).execute(&store.pool).await?;
        let payouts = [
            SettlementPayout {
                token_id: tokens[0].into(),
                payout: Decimal::ONE,
            },
            SettlementPayout {
                token_id: tokens[1].into(),
                payout: Decimal::ZERO,
            },
        ];
        let response = if platform == "polymarket" {
            json!({"condition_id":market,"tokens":[{"token_id":tokens[0],"winner":true,"price":"1"},{"token_id":tokens[1],"winner":false,"price":"0"}]})
        } else {
            json!({"request_outcome":10000000,"settleFraction":"0"})
        };
        store
            .save_platform_settlement_result(id, platform, market, "fixture", &payouts, &response)
            .await?;
    }
    Ok(())
}

async fn insert_leg(store: &Store, order_id: i64, side: &str, quantity: Decimal) -> Result<i64> {
    let row = leg(order_id, 0, side, quantity);
    // Source snapshot fields have no public fixture API; insert only required
    // source columns, and persist an exactly matching ordinary trade fill.
    let id: i64 = sqlx::query_scalar("INSERT INTO legs(order_id,platform,token_id,label,side,intent,wallet_address,status,actual_shares,actual_price,actual_fee,submitted_at) VALUES($1,'outcome',$2,'YES',$3,$4,$5,'completed',$6,$7,0,NOW()) RETURNING id")
        .bind(order_id).bind(key().token).bind(side).bind(&row.leg.intent).bind(key().wallet)
        .bind(quantity).bind(row.actual_price).fetch_one(&store.pool).await?;
    for fill in leg(order_id, id, side, quantity).fills {
        sqlx::query("INSERT INTO fills(leg_id,third_order_id,trade_id,shares,price,fee,raw) VALUES($1,$2,$3,$4,$5,$6,$7)")
            .bind(id).bind(fill.third_order_id).bind(fill.trade_id).bind(fill.shares).bind(fill.price).bind(fill.fee).bind(fill.raw)
            .execute(&store.pool).await?;
    }
    sqlx::query("UPDATE legs SET third_order_id=$2 WHERE id=$1")
        .bind(id)
        .bind(id.to_string())
        .execute(&store.pool)
        .await?;
    Ok(id)
}

#[tokio::test]
#[ignore = "requires process APP_POSTGRES_URI; private schema only"]
async fn platform_evidence_pending_claim_conflict_and_progress_cas() -> Result<()> {
    let Some(f) = Fixture::new().await? else {
        unreachable!()
    };
    let result: Result<()> = async {
        let store = &f.store;
        let id = insert_order(store, dec("9")).await?;
        let first = order_json(store,id).await?;
        save_results(store,id).await?;
        ensure!(first["settlement_pending_since"] == order_json(store,id).await?["settlement_pending_since"]);
        // Recreate the connection pool as after restart; cached evidence needs no HTTP endpoint.
        let uri=std::env::var("APP_POSTGRES_URI")?;
        let restarted=Store{pool:private_pool(&uri,&f.schema,1).await?};
        let loaded = restarted.platform_settlement_result(id,"outcome","10000000","fixture").await?.unwrap();
        restarted.pool.close().await;
        ensure!(store.platform_settlement_result(id,"outcome","wrong","fixture").await.is_err());
        let mut conflict = loaded.clone();
        for p in &mut conflict { p.payout = Decimal::ONE-p.payout; }
        ensure!(store.save_platform_settlement_result(id,"outcome","10000000","fixture",&conflict,&json!({"request_outcome":10000000,"settleFraction":"1"})).await.is_err());
        ensure!(store.platform_settlement_result(id,"outcome","10000000","fixture").await? == Some(loaded));
        let claim = Uuid::new_v4();
        sqlx::query("UPDATE arb_orders SET lifecycle_action='rebalance',lifecycle_claim_id=$2,lifecycle_claimed_at=NOW() WHERE id=$1")
            .bind(id).bind(claim).execute(&store.pool).await?;
        save_results(store,id).await?;
        ensure!(order_json(store,id).await?["lifecycle_claim_id"] == json!(claim));
        ensure!(store.finalize_position_settlement(id,"test",&json!({})).await?.is_none());
        ensure!(store.release_lifecycle(id,"rebalance",claim).await?);
        ensure!(order_json(store,id).await?["position_status"] == "settlement_pending");
        let page1 = json!({"page":1});
        let page2 = json!({"page":2});
        store.save_settlement_fee_progress(&key(),None,&page1).await?;
        store.save_settlement_fee_progress(&key(),Some(&page1),&page2).await?;
        ensure!(store.save_settlement_fee_progress(&key(),Some(&page1),&json!({"page":"stale"})).await.is_err());
        store.settlement_fee_blocked(&key(),Some(&page1),"stale error").await?;
        ensure!(store.settlement_fee_progress(&key()).await? == Some(page2));
        Ok(())
    }.await;
    f.cleanup().await?;
    result
}

async fn order_json(store: &Store, id: i64) -> Result<Value> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(o) FROM arb_orders o WHERE id=$1")
            .bind(id)
            .fetch_one(&store.pool)
            .await?,
    )
}

async fn ledger_json(store: &Store) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object('groups',(SELECT COALESCE(jsonb_agg(to_jsonb(g) ORDER BY id),'[]') FROM outcome_settlement_fee_groups g),'events',(SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY id),'[]') FROM outcome_settlement_fee_events e),'allocations',(SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY order_id),'[]') FROM outcome_settlement_fee_allocations a))")
        .fetch_one(&store.pool).await?)
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; creates and drops only a unique private schema"]
async fn shared_orders_snapshot_prepare_commit_and_concurrent_idempotency() -> Result<()> {
    let Some(f) = Fixture::new().await? else {
        return Ok(());
    };
    let exercised: Result<()> = async {
        let store = &f.store;
        // Acquire all four connections simultaneously to exercise after_connect,
        // not merely whichever connection the pool happened to return first.
        let mut connections = Vec::new();
        for _ in 0..4 {
            let mut connection = store.pool.acquire().await?;
            let path: String = sqlx::query_scalar("SHOW search_path").fetch_one(&mut *connection).await?;
            ensure!(path == f.schema);
            connections.push(connection);
        }
        drop(connections);
        let migrated_schema: String = sqlx::query_scalar("SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass('_sqlx_migrations')")
            .fetch_one(&store.pool).await?;
        ensure!(migrated_schema == f.schema);
        let first = insert_order(store, dec("3")).await?;
        let second = insert_order(store, dec("6")).await?;
        let readonly = store.settlement_fee_group_snapshot(&key()).await?;
        ensure!(readonly.legs.len() == 2 && readonly.allocations.is_empty() && !readonly.sealed);
        ensure!(readonly.legs.iter().map(|l| l.leg.order_id).collect::<Vec<_>>() == vec![first, second]);
        ensure!(ledger_json(store).await?["groups"] == json!([]));
        let prepared = store.prepare_settlement_fee_group(&key()).await?;
        ensure!(serde_json::to_value(&readonly.legs)? == serde_json::to_value(&prepared.legs)?);
        let events = [event(dec("9"), dec("0.012096"))];
        let evidence = proof();
        let (a, b) = tokio::join!(
            store.commit_settlement_fee_group(&prepared, Decimal::ONE, &events, &evidence),
            store.commit_settlement_fee_group(&prepared, Decimal::ONE, &events, &evidence)
        );
        let a = a?;
        ensure!(serde_json::to_value(&a)? == serde_json::to_value(b?)?);
        ensure!(a.len() == 2 && a[0].fee == dec("0.004032") && a[1].fee == dec("0.008064"));
        let sealed = store.prepare_settlement_fee_group(&key()).await?;
        ensure!(sealed.sealed && sealed.allocations.len() == 2);
        let before = ledger_json(store).await?;
        ensure!(before["groups"].as_array().unwrap().len() == 1 && before["events"].as_array().unwrap().len() == 1);
        let sums: (Decimal, Decimal) = sqlx::query_as("SELECT SUM(quantity),SUM(fee) FROM outcome_settlement_fee_allocations").fetch_one(&store.pool).await?;
        ensure!(sums == (dec("9"), dec("0.012096")));
        store.commit_settlement_fee_group(&prepared, Decimal::ONE, &events, &evidence).await?;
        ensure!(before == ledger_json(store).await?);
        let conflict = [event(dec("9"), dec("0.02"))];
        ensure!(store.commit_settlement_fee_group(&prepared, Decimal::ONE, &conflict, &evidence).await.is_err());
        ensure!(before == ledger_json(store).await?);
        Ok(())
    }.await;
    f.cleanup().await?;
    exercised
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; creates and drops only a unique private schema"]
async fn historical_unknown_cas_preserves_cost_timestamp_and_repeat_is_noop() -> Result<()> {
    let Some(f) = Fixture::new().await? else {
        return Ok(());
    };
    let exercised: Result<()> = async {
        let store = &f.store;
        let id = insert_order(store, dec("9")).await?;
        sqlx::query("UPDATE arb_orders SET position_status='settled',settled_at='2025-01-02T03:04:05Z',settlement_source='legacy',settlement_result='{}',actual_cost=4.89448998,actual_rev=9,actual_profit=4.10551002 WHERE id=$1")
            .bind(id).execute(&store.pool).await?;
        let expected = store.historical_settlement_snapshot(id).await?.expect("legacy unknown snapshot");
        let prepared = store.prepare_settlement_fee_group(&key()).await?;
        store.commit_settlement_fee_group(&prepared, Decimal::ONE, &[event(dec("9"), dec("0.012096"))], &proof()).await?;
        let before = order_json(store, id).await?;
        let ledger_before = ledger_json(store).await?;
        let mut stale = expected.clone();
        stale.actual_rev += Decimal::ONE;
        ensure!(!store.apply_historical_settlement_fees(&stale, &payouts()).await?);
        ensure!(before == order_json(store, id).await? && ledger_before == ledger_json(store).await?);
        ensure!(store.apply_historical_settlement_fees(&expected, &payouts()).await?);
        let actual: (Decimal, Decimal, Decimal, chrono::DateTime<chrono::Utc>) = sqlx::query_as("SELECT actual_cost,actual_rev,actual_profit,settled_at FROM arb_orders WHERE id=$1").bind(id).fetch_one(&store.pool).await?;
        ensure!(actual == (dec("4.89448998"), dec("8.987904"), dec("4.09341402"), expected.settled_at));
        let applied = order_json(store, id).await?;
        ensure!(applied["settlement_result"]["settlement_fee_status"] == "verified");
        ensure!(applied["settlement_result"]["profit_basis"] == "net_payout_less_trade_costs");
        let ledger_applied = ledger_json(store).await?;
        ensure!(!ledger_applied["allocations"][0]["applied_at"].is_null());
        ensure!(ledger_applied["allocations"][0]["application_audit"]["mode"] == "historical_cas");
        ensure!(store.historical_settlement_snapshot(id).await?.is_none());
        ensure!(!store.apply_historical_settlement_fees(&expected, &payouts()).await?);
        ensure!(applied == order_json(store, id).await? && ledger_applied == ledger_json(store).await?);
        Ok(())
    }.await;
    f.cleanup().await?;
    exercised
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; creates and drops only a unique private schema"]
async fn finalize_requires_fee_then_records_net_sample_and_is_idempotent() -> Result<()> {
    let Some(f) = Fixture::new().await? else {
        return Ok(());
    };
    let exercised: Result<()> = async {
        let store = &f.store;
        let id = insert_order(store, dec("9")).await?;
        let before = order_json(store, id).await?;
        ensure!(store
            .finalize_position_settlement(id, "test", &json!({}))
            .await
            .is_err());
        ensure!(before == order_json(store, id).await?);
        let prepared = store.prepare_settlement_fee_group(&key()).await?;
        store
            .commit_settlement_fee_group(
                &prepared,
                Decimal::ONE,
                &[event(dec("9"), dec("0.012096"))],
                &proof(),
            )
            .await?;
        let actual = store
            .finalize_position_settlement(id, "test", &json!({"fixture":true}))
            .await?;
        ensure!(actual == Some((dec("4.89448998"), dec("8.987904"), dec("4.09341402"))));
        let applied = order_json(store, id).await?;
        ensure!(applied["position_status"] == "settled" && !applied["settled_at"].is_null());
        ensure!(applied["settlement_result"]["settlement_fee_status"] == "verified");
        ensure!(applied["settlement_result"]["profit_basis"] == "net_payout_less_trade_costs");
        let amounts = &applied["settlement_result"]["platform_amounts"];
        for (part, total) in [
            ("cost", "actual_cost"),
            ("net_revenue", "actual_rev"),
            ("profit", "actual_profit"),
        ] {
            let sum: Decimal = ["polymarket", "outcome"]
                .iter()
                .map(|p| serde_json::from_value::<Decimal>(amounts[p][part].clone()).unwrap())
                .sum();
            ensure!(sum == serde_json::from_value::<Decimal>(applied[total].clone())?);
        }
        let ledger = ledger_json(store).await?;
        ensure!(ledger["allocations"][0]["application_audit"]["mode"] == "finalize");
        ensure!(store
            .finalize_position_settlement(id, "retry", &json!({"different":true}))
            .await?
            .is_none());
        ensure!(applied == order_json(store, id).await? && ledger == ledger_json(store).await?);
        Ok(())
    }
    .await;
    f.cleanup().await?;
    exercised
}

#[tokio::test]
#[ignore = "requires process APP_POSTGRES_URI; private schema only"]
async fn first_reported_order_four_legs_platform_sum_regression() -> Result<()> {
    let Some(f) = Fixture::new().await? else {
        unreachable!()
    };
    let tested:Result<()> = async {
        let store=&f.store;
        let id:i64=sqlx::query_scalar("INSERT INTO arb_orders(event_id,unified_index,status) VALUES($1,0,'completed') RETURNING id").bind(Uuid::new_v4()).fetch_one(&store.pool).await?;
        let buy=insert_leg(store,id,"BUY",dec("30")).await?;
        let sell=insert_leg(store,id,"SELL",dec("15")).await?;
        for (leg_id,price,fee) in [(buy,dec("0.09"),Decimal::ZERO),(sell,dec("0.08951"),dec("0.00180452"))] {
            sqlx::query("UPDATE legs SET actual_price=$2,actual_fee=$3 WHERE id=$1").bind(leg_id).bind(price).bind(fee).execute(&store.pool).await?;
            sqlx::query("UPDATE fills SET price=$2,fee=$3 WHERE leg_id=$1").bind(leg_id).bind(price).bind(fee).execute(&store.pool).await?;
        }
        sqlx::query("INSERT INTO legs(order_id,platform,token_id,label,side,intent,status,actual_shares,actual_price,actual_fee) VALUES($1,'polymarket','pm-win','yes','BUY','arb_buy','completed',30,.9,.135),($1,'polymarket','pm-win','yes','SELL','rebalance_sell','completed',15,.96,.0288)").bind(id).execute(&store.pool).await?;
        // This sample's Outcome side loses; save its explicit zero market payout.
        sqlx::query("INSERT INTO arb_order_market_identities VALUES($1,'polymarket','condition-fixture'),($1,'outcome','10000000')").bind(id).execute(&store.pool).await?;
        use market_arb::settlement::SettlementPayout;
        for (platform,market,vector,response) in [
            ("polymarket","condition-fixture",vec![SettlementPayout{token_id:"pm-win".into(),payout:Decimal::ONE},SettlementPayout{token_id:"pm-lose".into(),payout:Decimal::ZERO}],json!({"condition_id":"condition-fixture","tokens":[{"token_id":"pm-win","winner":true,"price":"1"},{"token_id":"pm-lose","winner":false,"price":"0"}]})),
            ("outcome","10000000",vec![SettlementPayout{token_id:key().token,payout:Decimal::ZERO},SettlementPayout{token_id:"#100000000".into(),payout:Decimal::ONE}],json!({"request_outcome":10000000,"settleFraction":"1"}))
        ] {store.save_platform_settlement_result(id,platform,market,"fixture",&vector,&response).await?;}
        let s=store.prepare_settlement_fee_group(&key()).await?;
        let mut zero=event(dec("15"),Decimal::ZERO); zero.payout=Decimal::ZERO;
        store.commit_settlement_fee_group(&s,Decimal::ZERO,&[zero],&proof()).await?;
        ensure!(store.finalize_position_settlement(id,"sample",&json!({})).await?==Some((dec("29.835"),dec("30.71204548"),dec("0.87704548"))));
        let row=order_json(store,id).await?;
        let amounts=&row["settlement_result"]["platform_amounts"];
        ensure!(serde_json::from_value::<Decimal>(amounts["polymarket"]["profit"].clone())?==dec("2.2362"));
        ensure!(serde_json::from_value::<Decimal>(amounts["outcome"]["profit"].clone())?==dec("-1.35915452"));
        Ok(())
    }.await;
    f.cleanup().await?;
    tested
}

#[tokio::test]
#[ignore = "requires process APP_POSTGRES_URI; private schema only"]
async fn pm_price_upgrade_replay_and_fractional_finalization() -> Result<()> {
    use market_arb::settlement::{parse_polymarket_settlement, SettlementStatus};
    let Some(f) = Fixture::new().await? else {
        unreachable!()
    };
    let tested: Result<()> = async {
        let store = &f.store;
        let id = insert_order(store, Decimal::ZERO).await?;
        sqlx::query("INSERT INTO legs(order_id,platform,token_id,label,side,intent,status,actual_shares,actual_price,actual_fee) VALUES($1,'polymarket','pm-win','yes','BUY','arb_buy','completed',10,.2,0),($1,'polymarket','pm-win','yes','SELL','rebalance_sell','completed',4,.4,0)").bind(id).execute(&store.pool).await?;
        // Model a pre-upgrade row without inventing price from the winner bit.
        sqlx::query("UPDATE order_platform_settlement_results SET source='clob_market_winner',evidence=jsonb_set(evidence,'{source}','\"clob_market_winner\"'::jsonb),observed_at=NOW()-INTERVAL '1 day' WHERE order_id=$1 AND platform='polymarket'").bind(id).execute(&store.pool).await?;
        let old: Value = sqlx::query_scalar("SELECT to_jsonb(r) FROM order_platform_settlement_results r WHERE order_id=$1 AND platform='polymarket'").bind(id).fetch_one(&store.pool).await?;
        let pending = order_json(store,id).await?["settlement_pending_since"].clone();
        ensure!(store.platform_settlement_result(id,"polymarket","condition-fixture","fixture").await?.is_none());
        ensure!(store.finalize_position_settlement(id,"legacy",&json!({})).await.is_err());
        let response = json!({"condition_id":"condition-fixture","tokens":[{"token_id":"pm-win","winner":true,"price":"0.3"},{"token_id":"pm-lose","winner":false,"price":"0.7"}]});
        let SettlementStatus::Settled { payouts: vector } = parse_polymarket_settlement(&response)? else { unreachable!() };
        ensure!(store.save_platform_settlement_result(id,"polymarket","condition-fixture","wrong",&vector,&response).await.is_err());
        let (a,b) = tokio::join!(
            store.save_platform_settlement_result(id,"polymarket","condition-fixture","fixture",&vector,&response),
            store.save_platform_settlement_result(id,"polymarket","condition-fixture","fixture",&vector,&response)
        ); a?; b?;
        let upgraded: Value = sqlx::query_scalar("SELECT to_jsonb(r) FROM order_platform_settlement_results r WHERE order_id=$1 AND platform='polymarket'").bind(id).fetch_one(&store.pool).await?;
        for field in ["source","payouts","evidence"] { ensure!(upgraded["evidence"]["previous_evidence"][field] == old[field]); }
        ensure!(serde_json::from_value::<chrono::DateTime<chrono::Utc>>(upgraded["evidence"]["previous_evidence"]["observed_at"].clone())? == serde_json::from_value::<chrono::DateTime<chrono::Utc>>(old["observed_at"].clone())?);
        ensure!(upgraded["observed_at"] != old["observed_at"]);
        ensure!(order_json(store,id).await?["settlement_pending_since"] == pending);
        let cached = store.platform_settlement_result(id,"polymarket","condition-fixture","fixture").await?.unwrap();
        ensure!(cached.iter().find(|p|p.token_id=="pm-win").unwrap().payout == dec("0.3"));
        // Every durable consumer rejects unknown versions, sources and response tampering.
        for mutation in ["evidence_version=2", "source='unknown'", "evidence=jsonb_set(evidence,'{version}','2'::jsonb)", "evidence=jsonb_set(evidence,'{response,tokens,0,price}','\"0.5\"'::jsonb)"] {
            sqlx::query(&format!("UPDATE order_platform_settlement_results SET {mutation} WHERE order_id=$1 AND platform='polymarket'")).bind(id).execute(&store.pool).await?;
            ensure!(store.platform_settlement_result(id,"polymarket","condition-fixture","fixture").await.is_err());
            ensure!(store.save_platform_settlement_result(id,"polymarket","condition-fixture","fixture",&vector,&response).await.is_err());
            ensure!(store.finalize_position_settlement(id,"invalid",&json!({})).await.is_err());
            sqlx::query("UPDATE order_platform_settlement_results SET evidence_version=1,source='clob_market_price',evidence=$2 WHERE order_id=$1 AND platform='polymarket'").bind(id).bind(&upgraded["evidence"]).execute(&store.pool).await?;
        }
        let mut conflicting = response.clone(); conflicting["tokens"][0]["price"] = json!("0.5"); conflicting["tokens"][1]["price"] = json!("0.5");
        let SettlementStatus::Settled { payouts: other } = parse_polymarket_settlement(&conflicting)? else { unreachable!() };
        ensure!(store.save_platform_settlement_result(id,"polymarket","condition-fixture","fixture",&other,&conflicting).await.is_err());
        ensure!(store.save_platform_settlement_result(id,"polymarket","condition-fixture","fixture",&other,&response).await.is_err());
        ensure!(store.finalize_position_settlement(id,"price",&json!({})).await? == Some((dec("2"),dec("3.4"),dec("1.4"))));
        let settled = order_json(store,id).await?;
        let amounts = &settled["settlement_result"]["platform_amounts"];
        for (field, expected) in [("cost",dec("2")),("net_revenue",dec("3.4")),("profit",dec("1.4"))] {
            ensure!(serde_json::from_value::<Decimal>(amounts["polymarket"][field].clone())? + serde_json::from_value::<Decimal>(amounts["outcome"][field].clone())? == expected);
        }
        ensure!(serde_json::from_value::<Decimal>(amounts["polymarket"]["remaining_gross_payout"].clone())? == dec("1.8"));
        ensure!(store.save_platform_settlement_result(id,"polymarket","condition-fixture","fixture",&vector,&response).await.is_err());
        ensure!(store.finalize_position_settlement(id,"retry",&json!({})).await?.is_none());
        ensure!(settled == order_json(store,id).await?);
        let unchanged: Value = sqlx::query_scalar("SELECT to_jsonb(r) FROM order_platform_settlement_results r WHERE order_id=$1 AND platform='polymarket'").bind(id).fetch_one(&store.pool).await?;
        ensure!(unchanged == upgraded);
        // A historical settled winner row must remain byte-for-byte untouched.
        sqlx::query("UPDATE order_platform_settlement_results SET source='clob_market_winner',evidence=$2 WHERE order_id=$1 AND platform='polymarket'").bind(id).bind(&old["evidence"]).execute(&store.pool).await?;
        let historical: Value = sqlx::query_scalar("SELECT to_jsonb(r) FROM order_platform_settlement_results r WHERE order_id=$1 AND platform='polymarket'").bind(id).fetch_one(&store.pool).await?;
        ensure!(store.save_platform_settlement_result(id,"polymarket","condition-fixture","fixture",&vector,&response).await.is_err());
        ensure!(store.finalize_position_settlement(id,"historical",&json!({})).await?.is_none());
        let after: Value = sqlx::query_scalar("SELECT to_jsonb(r) FROM order_platform_settlement_results r WHERE order_id=$1 AND platform='polymarket'").bind(id).fetch_one(&store.pool).await?;
        ensure!(historical == after && settled == order_json(store,id).await?);
        // A fresh fractional row follows the INSERT path, not just legacy UPDATE.
        let fresh = insert_order(store,Decimal::ZERO).await?;
        sqlx::query("DELETE FROM order_platform_settlement_results WHERE order_id=$1 AND platform='polymarket'").bind(fresh).execute(&store.pool).await?;
        store.save_platform_settlement_result(fresh,"polymarket","condition-fixture","fixture",&other,&conflicting).await?;
        ensure!(store.platform_settlement_result(fresh,"polymarket","condition-fixture","fixture").await?.unwrap().iter().all(|p|p.payout==dec("0.5")));
        ensure!(store.finalize_position_settlement(fresh,"fresh",&json!({})).await?.is_some());
        Ok(())
    }.await;
    f.cleanup().await?;
    tested
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; creates and drops only a unique private schema"]
async fn zero_remaining_finalizes_without_settlement_fee_evidence() -> Result<()> {
    let Some(f) = Fixture::new().await? else {
        return Ok(());
    };
    let exercised: Result<()> = async {
        let store = &f.store;
        let id = insert_order(store, dec("9")).await?;
        insert_leg(store, id, "SELL", dec("9")).await?;
        ensure!(store
            .settlement_fee_keys_for_order(id, "mainnet")
            .await?
            .is_empty());
        ensure!(
            store
                .finalize_position_settlement(id, "test", &json!({}))
                .await?
                == Some((dec("4.89448998"), dec("4.89448998"), Decimal::ZERO))
        );
        ensure!(
            order_json(store, id).await?["settlement_result"]["settlement_fee_status"]
                == "not_applicable"
        );
        ensure!(ledger_json(store).await?["events"] == json!([]));
        Ok(())
    }
    .await;
    f.cleanup().await?;
    exercised
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; creates and drops only a unique private schema"]
async fn claims_nonterminal_legs_and_changed_snapshots_block_group_commit() -> Result<()> {
    let Some(f) = Fixture::new().await? else {
        return Ok(());
    };
    let exercised: Result<()> = async {
        let store = &f.store;
        let first = insert_order(store, dec("3")).await?;
        let second = insert_order(store, dec("6")).await?;
        let prepared = store.prepare_settlement_fee_group(&key()).await?;
        let events = [event(dec("9"), dec("0.012096"))];
        sqlx::query("UPDATE arb_orders SET lifecycle_action='rebalance',lifecycle_claim_id=$2,lifecycle_claimed_at=NOW() WHERE id=$1")
            .bind(second).bind(Uuid::new_v4()).execute(&store.pool).await?;
        ensure!(store.prepare_settlement_fee_group(&key()).await.is_err());
        ensure!(store.commit_settlement_fee_group(&prepared, Decimal::ONE, &events, &proof()).await.is_err());
        ensure!(store.finalize_position_settlement(second, "test", &json!({})).await?.is_none());
        sqlx::query("UPDATE arb_orders SET lifecycle_action=NULL,lifecycle_claim_id=NULL,lifecycle_claimed_at=NULL WHERE id=$1").bind(second).execute(&store.pool).await?;
        // A nonterminal sibling outside the Outcome group must also block it.
        let sibling: i64 = sqlx::query_scalar("INSERT INTO legs(order_id,platform,token_id,label,side,status,actual_shares,actual_price,actual_fee) VALUES($1,'polymarket','other-token','NO','BUY','submitted',0,0,0) RETURNING id")
            .bind(first).fetch_one(&store.pool).await?;
        ensure!(store.prepare_settlement_fee_group(&key()).await.is_err());
        ensure!(store.commit_settlement_fee_group(&prepared, Decimal::ONE, &events, &proof()).await.is_err());
        ensure!(store.finalize_position_settlement(first, "test", &json!({})).await.is_err());
        sqlx::query("UPDATE legs SET status='cancelled' WHERE id=$1").bind(sibling).execute(&store.pool).await?;
        sqlx::query("UPDATE legs SET last_order_info=$2 WHERE order_id=$1 AND platform='outcome'").bind(first).bind(json!({"changed":true})).execute(&store.pool).await?;
        ensure!(store.commit_settlement_fee_group(&prepared, Decimal::ONE, &events, &proof()).await.is_err());
        ensure!(ledger_json(store).await?["events"] == json!([]));
        let fresh = store.prepare_settlement_fee_group(&key()).await?;
        store.commit_settlement_fee_group(&fresh, Decimal::ONE, &events, &proof()).await?;
        Ok(())
    }.await;
    f.cleanup().await?;
    exercised
}

#[test]
fn event_actuals_have_no_periodic_or_wall_clock_expiry_path() {
    let store = include_str!("../src/store.rs");
    let query = store
        .split("pub async fn sum_actual_profit_with_timeouts")
        .nth(1)
        .unwrap()
        .split("pub async fn count_stale_unknown_legs")
        .next()
        .unwrap();
    assert!(!query.contains("fallback_valid_until"));
    assert!(query.contains("actuals_projection->'version'"));
    assert!(query.contains("actuals_projection->'stale'"));
    assert!(!store.contains("refresh_actuals_batch"));
    let lock = store
        .split("async fn lock_leg_parent(")
        .nth(1)
        .unwrap()
        .split("async fn leg_actuals_evidence")
        .next()
        .unwrap();
    assert!(!lock.contains("UPDATE arb_orders"));
    let exec = include_str!("../src/exec.rs");
    assert!(!exec.contains("actuals_scan_cursor"));
    assert!(!exec.contains("refresh_order_actuals"));
}

fn reserve_snapshot(taker: &str, builder: &str) -> Value {
    json!({"fee_estimate":{"version":1,"fee_model":"out_usdc_taker_close_v1","outcome_id":"10000000","token_ids":["#100000000","#100000001"],"taker_rate":taker,"builder_rate":builder,"user_fees_fetched_at":"2000-01-01T00:00:00Z","outcome_meta_fetched_at":"2000-01-01T00:00:00Z"}})
}

#[tokio::test]
#[ignore = "requires process APP_POSTGRES_URI; private schema only"]
async fn actuals_event_estimate_survives_expiry_and_locked_evidence() -> Result<()> {
    use market_arb::store::actuals::LatestFee;
    let f = Fixture::new().await?.unwrap();
    let exercised:Result<()> = async {
        let s=&f.store;let id=insert_order(s,dec("30")).await?;
        let source=reserve_snapshot("0.001344","0.0003")["fee_estimate"].clone();
        let deadline=chrono::Utc::now()+chrono::Duration::seconds(800);
        let resolver=|wallet:&str,token:&str| Some(LatestFee {wallet:wallet.into(),token:token.into(),snapshot:source.clone(),valid_until:deadline});
        let before:Value=sqlx::query_scalar("SELECT jsonb_build_object('legs',(SELECT jsonb_agg(to_jsonb(l)) FROM legs l WHERE order_id=$1),'fills',(SELECT jsonb_agg(to_jsonb(f)) FROM fills f JOIN legs l ON l.id=f.leg_id WHERE l.order_id=$1))").bind(id).fetch_one(&s.pool).await?;
        let projected=s.refresh_order_actuals_with_fallback(id,&resolver).await?;
        ensure!(projected.actuals().unwrap().1 == -dec("0.04932"));
        ensure!(projected.evidence()["source_kind"]=="latest_valid_fallback");
        ensure!(s.sum_actual_profit().await?.1==0);
        let after:Value=sqlx::query_scalar("SELECT jsonb_build_object('legs',(SELECT jsonb_agg(to_jsonb(l)) FROM legs l WHERE order_id=$1),'fills',(SELECT jsonb_agg(to_jsonb(f)) FROM fills f JOIN legs l ON l.id=f.leg_id WHERE l.order_id=$1))").bind(id).fetch_one(&s.pool).await?;
        ensure!(before==after);
        // Persisted source deadlines are audit data, not a live gate or a refresh trigger.
        for expiry in [None,Some(json!(null)),Some(json!("999999999999999999999")),Some(json!({})),Some(json!([])),Some(json!(true)),Some(json!(0)),Some(json!(chrono::Utc::now().timestamp()))] {
            let mut evidence=projected.evidence();
            if let Some(expiry)=expiry {evidence["fallback_valid_until"]=expiry;} else {evidence.as_object_mut().unwrap().remove("fallback_valid_until");}
            sqlx::query("UPDATE arb_orders SET actuals_projection=$2 WHERE id=$1").bind(id).bind(evidence).execute(&s.pool).await?;
            ensure!(s.sum_actual_profit().await?.1==0);
        }
        let saved=order_json(s,id).await?;
        s.complete_orders(&resolver).await?;
        ensure!(order_json(s,id).await?==saved);
        ensure!(s.sum_actual_profit().await?.1==0);
        ensure!(s.refresh_order_actuals(id).await?.actuals().is_none());
        // Hold the parent while new trade evidence is committed; refresh must reread the locked legs.
        let mut tx=s.pool.begin().await?;
        sqlx::query("SELECT id FROM arb_orders WHERE id=$1 FOR UPDATE").bind(id).execute(&mut *tx).await?;
        sqlx::query("UPDATE legs SET actual_shares=15 WHERE order_id=$1").bind(id).execute(&mut *tx).await?;
        let refresh=s.refresh_order_actuals_with_fallback(id,&resolver);
        let (refreshed, committed)=tokio::join!(refresh,tx.commit()); committed?;
        ensure!(serde_json::from_value::<Decimal>(refreshed?.evidence()["estimated_fee"].clone())?==dec("0.02466"));
        // Zero-position finalization and fallback refresh must serialize without overwriting final evidence.
        insert_leg(s,id,"SELL",dec("15")).await?;
        let (closed,refresh)=tokio::join!(s.finalize_closed_position(id),s.refresh_order_actuals_with_fallback(id,&resolver));
        ensure!(closed?.is_some());refresh?;
        let final_row=order_json(s,id).await?;
        ensure!(final_row["actuals_projection"]["status"]=="final");
        s.refresh_order_actuals_with_fallback(id,&resolver).await?;
        ensure!(order_json(s,id).await?==final_row);
        ensure!(s.sum_actual_profit().await?.1==0);
        Ok(())
    }.await;
    f.cleanup().await?;
    exercised
}

#[tokio::test]
#[ignore = "requires process APP_POSTGRES_URI; private schema only"]
async fn actuals_reserve_unknown_completion_retry_and_partial_close() -> Result<()> {
    let f = Fixture::new().await?.unwrap();
    let exercised:Result<()> = async {
        let s=&f.store;let id=insert_order(s,dec("30")).await?;
        sqlx::query("UPDATE arb_orders SET status='actived',actual_cost=3,actual_rev=2,actual_profit=-1 WHERE id=$1").bind(id).execute(&s.pool).await?;
        sqlx::query("UPDATE legs SET status='failed' WHERE order_id=$1").bind(id).execute(&s.pool).await?;
        s.complete_orders(&|_, _| None, ).await?;
        let row=order_json(s,id).await?;
        ensure!(row["status"]=="completed" && row["actuals_projection"]["status"]=="unknown");
        ensure!(serde_json::from_value::<Decimal>(row["actual_profit"].clone())?==dec("-1"));
        let fills:i64=sqlx::query_scalar("SELECT COUNT(*) FROM fills").fetch_one(&s.pool).await?;ensure!(fills==1);
        ensure!(s.sum_actual_profit().await?.1==1);
        let claim=Uuid::new_v4();
        sqlx::query("UPDATE arb_orders SET lifecycle_action='take_profit',lifecycle_claim_id=$2,lifecycle_claimed_at=NOW() WHERE id=$1").bind(id).bind(claim).execute(&s.pool).await?;
        ensure!(s.refresh_order_actuals(id).await?.actuals().is_none());
        ensure!(s.release_lifecycle(id,"take_profit",claim).await?);

        sqlx::query("UPDATE legs SET last_order_info=$2 WHERE order_id=$1").bind(id).bind(reserve_snapshot("0.001344","0.0003")).execute(&s.pool).await?;
        for malformed in [json!({"version":1,"stale":false}),json!({"version":2,"status":"estimated","stale":false}),json!({"status":"not_applicable","stale":false}),json!({"version":1,"status":"estimated","stale":"invalid"})] {
            sqlx::query("UPDATE arb_orders SET actuals_projection=$2 WHERE id=$1").bind(id).bind(malformed).execute(&s.pool).await?;
            ensure!(s.sum_actual_profit().await?.1==1);
        }
        let first=s.refresh_order_actuals(id).await?.actuals().unwrap();
        ensure!(first.1 == -dec("0.04932"));ensure!(s.refresh_order_actuals(id).await?.actuals()==Some(first));
        sqlx::query("UPDATE arb_orders SET status='actived' WHERE id=$1").bind(id).execute(&s.pool).await?;s.complete_orders(&|_, _| None, ).await?;
        let row=order_json(s,id).await?;ensure!(serde_json::from_value::<Decimal>(row["actual_rev"].clone())?==first.1);
        let sell=insert_leg(s,id,"SELL",dec("15")).await?;
        sqlx::query("UPDATE legs SET status='cancelled' WHERE id=$1").bind(sell).execute(&s.pool).await?;
        s.refresh_order_actuals(id).await?;
        ensure!(order_json(s,id).await?["actuals_projection"]["estimated_fee"]==json!(dec("0.02466")));
        sqlx::query("UPDATE arb_orders SET position_status='settlement_pending' WHERE id=$1").bind(id).execute(&s.pool).await?;
        ensure!(s.refresh_order_actuals(id).await?.actuals().is_some());
        sqlx::query("UPDATE legs SET status='unknown' WHERE id=$1").bind(sell).execute(&s.pool).await?;
        ensure!(s.refresh_order_actuals(id).await?.actuals().is_none());
        sqlx::query("UPDATE legs SET status='cancelled' WHERE id=$1").bind(sell).execute(&s.pool).await?;
        s.refresh_order_actuals(id).await?;
        sqlx::query("UPDATE arb_orders SET position_status='watching' WHERE id=$1").bind(id).execute(&s.pool).await?;
        insert_leg(s,id,"SELL",dec("15")).await?;
        let (closed,refreshed)=tokio::join!(s.finalize_closed_position(id),s.refresh_order_actuals(id));
        ensure!(closed?.is_some());refreshed?;
        let final_row=order_json(s,id).await?;ensure!(final_row["actuals_projection"]["status"]=="final");
        ensure!(serde_json::from_value::<Decimal>(final_row["actual_profit"].clone())?==Decimal::ZERO);
        s.refresh_order_actuals(id).await?;ensure!(order_json(s,id).await?==final_row);
        // Legacy terminal rows are included without making new entry globally impossible.
        sqlx::query("UPDATE arb_orders SET actuals_projection='{\"status\":\"unknown\",\"stale\":true}' WHERE id=$1").bind(id).execute(&s.pool).await?;
        ensure!(s.sum_actual_profit().await?.1==0);
        Ok(())
    }.await;
    f.cleanup().await?;
    exercised
}

#[tokio::test]
#[ignore = "requires process APP_POSTGRES_URI; private schema only"]
async fn actuals_reserve_final_fee_replaces_estimate_above_and_below() -> Result<()> {
    for (real_fee, use_fallback) in [
        ("0.001", false),
        ("0.1", false),
        ("0.001", true),
        ("0.1", true),
    ] {
        let f = Fixture::new().await?.unwrap();
        let exercised: Result<()> = async {
            let s = &f.store;
            let id = insert_order(s, dec("9")).await?;
            let source = reserve_snapshot("0.001344", "0.0003");
            let deadline = chrono::Utc::now() + chrono::Duration::seconds(800);
            let resolver = |wallet: &str, token: &str| {
                Some(market_arb::store::actuals::LatestFee {
                    wallet: wallet.into(),
                    token: token.into(),
                    snapshot: source["fee_estimate"].clone(),
                    valid_until: deadline,
                })
            };
            if !use_fallback {
                sqlx::query("UPDATE legs SET last_order_info=$2 WHERE order_id=$1")
                    .bind(id)
                    .bind(&source)
                    .execute(&s.pool)
                    .await?;
            }
            ensure!(
                s.refresh_order_actuals_with_fallback(id, &resolver)
                    .await?
                    .actuals()
                    .unwrap()
                    .1
                    == -dec("0.014796")
            );
            let prepared = s.prepare_settlement_fee_group(&key()).await?;
            s.commit_settlement_fee_group(
                &prepared,
                Decimal::ONE,
                &[event(dec("9"), dec(real_fee))],
                &proof(),
            )
            .await?;
            let final_evidence = json!({});
            let (finalized, refresh) = tokio::join!(
                s.finalize_position_settlement(id, "test", &final_evidence),
                s.refresh_order_actuals_with_fallback(id, &resolver)
            );
            refresh?;
            let actual = finalized?.unwrap();
            ensure!(actual.1 == dec("9") - dec(real_fee));
            ensure!(actual.2 == actual.1 - actual.0);
            let row = order_json(s, id).await?;
            ensure!(row["actuals_projection"]["status"] == "final");
            s.refresh_order_actuals(id).await?;
            ensure!(order_json(s, id).await? == row);
            Ok(())
        }
        .await;
        f.cleanup().await?;
        exercised?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires process APP_POSTGRES_URI; private schema only"]
async fn manual_unknown_recompute_bounded_locked_and_evidence_preserving() -> Result<()> {
    let f = Fixture::new().await?.unwrap();
    let exercised: Result<()> = async {
        let s = &f.store;
        ensure!(s.unknown_actuals_page(0, s.unknown_actuals_upper_id().await?).await?.is_empty());
        let mut ids = Vec::new();
        for _ in 0..23 { ids.push(insert_order(s, dec("9")).await?); }
        let upper = s.unknown_actuals_upper_id().await?;
        let later = insert_order(s, dec("9")).await?;
        let first = s.unknown_actuals_page(0, upper).await?;
        ensure!(first == ids[..20]);
        ensure!(s.unknown_actuals_page(first[19], upper).await? == ids[20..]);
        ensure!(s.unknown_actuals_page(upper, upper).await?.is_empty());
        ensure!(!first.contains(&later));
        let id = ids[0];
        let before: Value = sqlx::query_scalar("SELECT jsonb_build_object('legs',(SELECT jsonb_agg(to_jsonb(l) ORDER BY id) FROM legs l WHERE order_id=$1),'fills',(SELECT jsonb_agg(to_jsonb(f) ORDER BY f.id) FROM fills f JOIN legs l ON l.id=f.leg_id WHERE l.order_id=$1))")
            .bind(id).fetch_one(&s.pool).await?;
        let unknown = s.refresh_unknown_order_actuals_with_fallback(id, &|_, _| None).await?.unwrap();
        ensure!(unknown.actuals().is_none());
        let source = reserve_snapshot("0.001344", "0.0003")["fee_estimate"].clone();
        let resolver = |wallet: &str, token: &str| Some(market_arb::store::actuals::LatestFee {
            wallet: wallet.into(), token: token.into(), snapshot: source.clone(),
            valid_until: chrono::Utc::now() + chrono::Duration::seconds(800),
        });
        // Two simultaneous maintenance attempts: the second must recheck under the lock.
        let (a,b) = tokio::join!(s.refresh_unknown_order_actuals_with_fallback(id, &resolver),
            s.refresh_unknown_order_actuals_with_fallback(id, &resolver));
        let (a,b) = (a?, b?);
        ensure!(a.is_some() != b.is_some());
        ensure!(a.or(b).unwrap().actuals().is_some());
        let priced = order_json(s, id).await?;
        ensure!(s.refresh_unknown_order_actuals_with_fallback(id, &|_,_| panic!("already repaired")).await?.is_none());
        ensure!(order_json(s,id).await? == priced);
        let after: Value = sqlx::query_scalar("SELECT jsonb_build_object('legs',(SELECT jsonb_agg(to_jsonb(l) ORDER BY id) FROM legs l WHERE order_id=$1),'fills',(SELECT jsonb_agg(to_jsonb(f) ORDER BY f.id) FROM fills f JOIN legs l ON l.id=f.leg_id WHERE l.order_id=$1))")
            .bind(id).fetch_one(&s.pool).await?;
        ensure!(after == before);
        for id in &ids[1..4] {
            // Candidate was fetched before another writer finalizes/closes/deletes it.
            if *id == ids[1] {
                sqlx::query("UPDATE arb_orders SET position_status='settled',settled_at=NOW(),settlement_source='test',settlement_result='{}',actuals_projection='{\"status\":\"final\"}' WHERE id=$1").bind(id).execute(&s.pool).await?;
            } else if *id == ids[2] {
                sqlx::query("UPDATE arb_orders SET position_status='closed' WHERE id=$1").bind(id).execute(&s.pool).await?;
            } else {
                // Nonexistent parent follows the same safe skip path without touching a resolver.
                ensure!(s.refresh_unknown_order_actuals_with_fallback(later+100, &|_,_| panic!("deleted")).await?.is_none());
                continue;
            }
            ensure!(s.refresh_unknown_order_actuals_with_fallback(*id, &|_,_| panic!("final")).await?.is_none());
        }
        let failed = ids[4];
        let original = order_json(s, failed).await?;
        sqlx::query("CREATE FUNCTION reject_manual_projection() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test rollback'; END $$").execute(&s.pool).await?;
        sqlx::query("CREATE TRIGGER reject_manual_projection BEFORE UPDATE OF actuals_projection ON arb_orders FOR EACH ROW EXECUTE FUNCTION reject_manual_projection()").execute(&s.pool).await?;
        ensure!(s.refresh_unknown_order_actuals_with_fallback(failed, &resolver).await.is_err());
        ensure!(order_json(s, failed).await? == original);
        sqlx::query("DROP TRIGGER reject_manual_projection ON arb_orders").execute(&s.pool).await?;
        ensure!(s.refresh_unknown_order_actuals_with_fallback(failed, &resolver).await?.unwrap().actuals().is_some());
        Ok(())
    }.await;
    f.cleanup().await?;
    exercised
}

#[tokio::test]
#[ignore = "requires process APP_POSTGRES_URI; private schema only"]
async fn order_events_project_atomically_and_noops_preserve_saved_estimates() -> Result<()> {
    use market_arb::store::{actuals::LatestFee, NewLeg};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let f = Fixture::new().await?.unwrap();
    let exercised: Result<()> = async {
        let s = &f.store;
        let id = insert_order(s, dec("30")).await?;
        let calls = AtomicUsize::new(0);
        let source = reserve_snapshot("0.001344", "0.0003")["fee_estimate"].clone();
        let resolver = |wallet: &str, token: &str| {
            calls.fetch_add(1, Ordering::SeqCst);
            Some(LatestFee { wallet: wallet.into(), token: token.into(), snapshot: source.clone(),
                valid_until: chrono::Utc::now() + chrono::Duration::seconds(800) })
        };
        // No new event means historical unknown remains unknown, even with a valid cache.
        let historical = order_json(s, id).await?;
        s.complete_orders(&resolver).await?;
        ensure!(order_json(s, id).await? == historical);
        ensure!(calls.load(Ordering::SeqCst) == 0);
        let claim = s.try_claim_lifecycle(id, "take_profit").await?.unwrap();
        let pending = NewLeg { platform: "polymarket", token_id: "pm-win", label: "NO",
            side: "SELL", intent: "take_profit", funder: Some("funder"), wallet: None,
            service: None, req_price: dec("0.5"), req_shares: dec("1"), req_fee: Decimal::ZERO,
            client_order_id: None, fee_estimate: None };
        let leg_id = s.insert_leg_for_claim(&resolver, id, "take_profit", claim, &pending).await?;
        let before = order_json(s, id).await?;
        let leg = s.open_legs().await?.into_iter().find(|leg| leg.id == leg_id).unwrap();
        s.record_reconciliation_wait(&leg, "diagnostic only").await?;
        ensure!(order_json(s, id).await? == before);
        sqlx::query("UPDATE arb_orders SET status='actived' WHERE id=$1").bind(id).execute(&s.pool).await?;
        let incomplete = order_json(s, id).await?;
        s.complete_orders(&|_, _| panic!("incomplete scan must not price")).await?;
        ensure!(order_json(s, id).await? == incomplete);
        let poll = market_arb::platforms::OrderPoll {
            found: true, status: "live".into(), order_id: Some("pm-event-order".into()),
            coin: Some("pm-win".into()), ..Default::default()
        };
        let leg = s.open_legs().await?.into_iter().find(|leg| leg.id == leg_id).unwrap();
        let current = s.record_order_poll(&resolver, &leg, &poll).await?.unwrap();
        let polled = order_json(s, id).await?;
        let no_cache = |_: &str, _: &str| -> Option<LatestFee> { panic!("no-op must not resolve fees") };
        let current = s.record_order_poll(&no_cache, &current, &poll).await?.unwrap();
        ensure!(order_json(s, id).await? == polled);
        let evidence = market_arb::reconcile::FillEvidence {
            poll: poll.clone(), page_complete: false, history_complete: false,
            expected_shares: None, pm_scan: None, outcome_scan: None, pm_order_constraints: None,
        };
        s.record_reconciliation(&no_cache, &current, &[], &evidence, &json!({"diagnostic_page":1})).await?;
        ensure!(order_json(s, id).await? == polled);
        // A failed parent projection must roll back the terminal receipt too.
        sqlx::query("CREATE FUNCTION reject_event_projection() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test projection failure'; END $$")
            .execute(&s.pool).await?;
        sqlx::query("CREATE TRIGGER reject_event_projection BEFORE UPDATE OF actuals_projection ON arb_orders FOR EACH ROW EXECUTE FUNCTION reject_event_projection()")
            .execute(&s.pool).await?;
        // Restore the pre-ack fixture state to exercise the submission terminal branch.
        sqlx::query("UPDATE legs SET status='pending' WHERE id=$1").bind(leg_id).execute(&s.pool).await?;
        ensure!(s.record_submission(&resolver, leg_id, "cancelled", None, &json!({"kind":"no_match"}), &json!({})).await.is_err());
        let status: String = sqlx::query_scalar("SELECT status FROM legs WHERE id=$1").bind(leg_id).fetch_one(&s.pool).await?;
        ensure!(status == "pending");
        sqlx::query("DROP TRIGGER reject_event_projection ON arb_orders").execute(&s.pool).await?;
        sqlx::query("DROP FUNCTION reject_event_projection()").execute(&s.pool).await?;
        // Accepting a zero-fill terminal receipt resolves the new leg and prices the old
        // Outcome exposure in the same transaction, without manufacturing a frozen leg.
        s.record_submission(&resolver, leg_id, "cancelled", None, &json!({"kind":"no_match"}), &json!({})).await?;
        let priced = order_json(s, id).await?;
        ensure!(priced["actuals_projection"]["source_kind"] == "latest_valid_fallback");
        ensure!(serde_json::from_value::<Decimal>(priced["actual_rev"].clone())? == -dec("0.04932"));
        let frozen: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM legs WHERE order_id=$1 AND last_order_info ? 'fee_estimate')")
            .bind(id).fetch_one(&s.pool).await?;
        ensure!(!frozen);
        let count = calls.load(Ordering::SeqCst);
        let no_cache = |_: &str, _: &str| -> Option<LatestFee> { panic!("no-op must not resolve fees") };
        let leg_before:Value=sqlx::query_scalar("SELECT to_jsonb(l) FROM legs l WHERE id=$1").bind(leg_id).fetch_one(&s.pool).await?;
        s.record_submission(&no_cache, leg_id, "cancelled", None, &json!({"kind":"no_match","diagnostic":2}), &json!({"replay":true})).await?;
        let leg_after:Value=sqlx::query_scalar("SELECT to_jsonb(l) FROM legs l WHERE id=$1").bind(leg_id).fetch_one(&s.pool).await?;
        let expected=leg_before;
        ensure!(leg_after==expected);
        s.complete_orders(&no_cache).await?;
        ensure!(order_json(s, id).await? == priced);
        ensure!(calls.load(Ordering::SeqCst) == count);
        let (released, amounts) = s.release_lifecycle_with_actuals(id, "take_profit", claim).await?;
        ensure!(released && amounts.is_some());
        ensure!(!s.release_lifecycle_with_actuals(id, "take_profit", claim).await?.0);
        ensure!(s.mark_rebalance(id, "completed").await?.unwrap().actuals() == amounts);
        let completed = order_json(s, id).await?;
        ensure!(s.mark_rebalance(id, "completed").await?.is_none());
        ensure!(order_json(s, id).await? == completed);
        // A new real transition with expired/missing cache cannot reuse the old estimate.
        let claim = s.try_claim_lifecycle(id, "rebalance").await?.unwrap();
        let leg_id = s.insert_leg_for_claim(&resolver, id, "rebalance", claim, &pending).await?;
        s.abort_unsubmitted_legs(&|_, _| None, &[leg_id], "aborted").await?;
        ensure!(order_json(s, id).await?["actuals_projection"]["status"] == "unknown");
        let unknown = order_json(s, id).await?;
        s.abort_unsubmitted_legs(&no_cache, &[leg_id], "replay").await?;
        s.complete_orders(&no_cache).await?;
        ensure!(order_json(s, id).await? == unknown);
        Ok(())
    }.await;
    f.cleanup().await?;
    exercised
}
