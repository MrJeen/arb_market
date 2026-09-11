//! 默认只读，不加载 dotenv、不迁移、不创建交易客户端。
use market_arb::error::{Error, Result};
use market_arb::platforms::outcome::settlement::SettlementScan;
use market_arb::settlement::{parse_outcome_settlement, OutcomeSettlement};
use market_arb::settlement_fees::{advance, prove};
use market_arb::store::settlement_fees::preview_allocations;
use market_arb::store::Store;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run().await.map_err(Into::into)
}

async fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("settlement-audit --order-id ID [--info-url URL] [--apply --confirm-report FINGERPRINT]\n默认只读；APP_POSTGRES_URI 由进程环境注入，不读取 .env。apply 必须先审阅只读结果并明确确认。");
        return Ok(());
    }
    let mut order_id = None;
    let mut endpoint = "https://api.hyperliquid.xyz/info".to_string();
    let mut apply = false;
    let mut confirm_report = None;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--apply" {
            apply = true;
        } else {
            index += 1;
            let value = args
                .get(index)
                .ok_or_else(|| Error::msg("missing argument value"))?;
            match arg.as_str() {
                "--order-id" => order_id = Some(value.parse::<i64>().map_err(Error::msg)?),
                "--info-url" => endpoint = value.clone(),
                "--confirm-report" => confirm_report = Some(value.clone()),
                _ => return Err(Error::msg("unknown audit argument")),
            }
        }
        index += 1;
    }
    let order_id = order_id
        .filter(|id| *id > 0)
        .ok_or_else(|| Error::msg("--order-id must be positive"))?;
    if apply && confirm_report.is_none() {
        return Err(Error::msg(
            "apply requires --confirm-report from the reviewed dry-run",
        ));
    }
    let uri = std::env::var("APP_POSTGRES_URI")
        .map_err(|_| Error::msg("APP_POSTGRES_URI is required; .env is not loaded"))?;
    // 会话默认只读；即使误调用写方法，dry-run 也由数据库拒绝。
    let options: sqlx::postgres::PgConnectOptions = uri
        .parse()
        .map_err(|_| Error::msg("invalid database connection configuration"))?;
    let options = if apply {
        options
    } else {
        options.options([("default_transaction_read_only", "on")])
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await?;
    let store = Store { pool };
    let old = store
        .historical_settlement_snapshot(order_id)
        .await?
        .ok_or_else(|| Error::msg("order is not an eligible historical gross settlement"))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let keys = store
        .settlement_fee_keys_for_order(order_id, &endpoint)
        .await?;
    let mut fee = Decimal::ZERO;
    let mut reports = Vec::new();
    let mut verified = Vec::new();
    let mut payouts = HashMap::new();
    let mut reviewed = Vec::new();
    for key in keys {
        let snapshot = store.settlement_fee_group_snapshot(&key).await?;
        let encoding = key
            .token
            .strip_prefix('#')
            .ok_or_else(|| Error::msg("invalid Outcome token"))?
            .parse::<u64>()
            .map_err(Error::msg)?;
        let raw: Value = client
            .post(&endpoint)
            .json(&json!({"type":"settledOutcome","outcome":encoding/10}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let payout = match parse_outcome_settlement(encoding / 10, &raw)? {
            OutcomeSettlement::Settled { payouts } => {
                payouts
                    .into_iter()
                    .find(|p| p.token_id == key.token)
                    .ok_or_else(|| Error::msg("missing token payout"))?
                    .payout
            }
            _ => return Err(Error::msg("Outcome market not settled")),
        };
        payouts.insert(("outcome".to_string(), key.token.clone()), payout);
        reviewed.push(json!({"key":key,"legs":snapshot.legs}));
        if snapshot.sealed {
            let allocation = snapshot
                .allocations
                .iter()
                .find(|a| a.order_id == order_id)
                .ok_or_else(|| Error::msg("sealed group missing order allocation"))?;
            fee += allocation.fee;
            reports.push(json!({"group":key,"sealed":true,"allocations":snapshot.allocations}));
            continue;
        }
        let mut progress: Option<SettlementScan> = None;
        for _ in 0..32 {
            let previous = progress.as_ref().map(serde_json::to_value).transpose()?;
            let next = advance(
                &client,
                &endpoint,
                &snapshot,
                previous.as_ref(),
                chrono::Utc::now().timestamp_millis().max(0) as u64,
            )
            .await?;
            let complete = next.complete;
            progress = Some(next);
            if complete {
                break;
            }
        }
        let progress = progress.ok_or_else(|| Error::msg("missing scan result"))?;
        let (events, evidence) = prove(&client, &endpoint, &snapshot, &progress, payout).await?;
        let allocations = preview_allocations(&snapshot, payout, &events)?;
        fee += allocations
            .iter()
            .find(|a| a.order_id == order_id)
            .ok_or_else(|| Error::msg("missing order allocation"))?
            .fee;
        reports.push(json!({"group":key,"events":events,"allocations":allocations}));
        verified.push((snapshot, payout, events, evidence));
    }
    let fingerprint = hex::encode(Sha256::digest(serde_json::to_vec(&json!({
        "before":old,"participants":reviewed,"groups":reports,"fee":fee.to_string(),
    }))?));
    if apply && confirm_report.as_deref() != Some(fingerprint.as_str()) {
        return Err(Error::msg(
            "reviewed report changed; run a new dry-run before applying",
        ));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "report_fingerprint":fingerprint,
            "order_id":order_id,"mode":if apply {"apply"} else {"dry_run"},
            "before":old,"settlement_fee":fee.to_string(),
            "after":{"actual_cost":old.actual_cost.to_string(),"actual_rev":(old.actual_rev-fee).to_string(),"actual_profit":(old.actual_profit-fee).to_string()},
            "groups":reports,
        }))?
    );
    if apply {
        for (snapshot, payout, events, evidence) in verified {
            store
                .commit_settlement_fee_group(&snapshot, payout, &events, &evidence)
                .await?;
        }
        let changed = store
            .apply_historical_settlement_fees(&old, &payouts)
            .await?;
        println!("{}", json!({"order_id":order_id,"applied":changed}));
    }
    Ok(())
}
