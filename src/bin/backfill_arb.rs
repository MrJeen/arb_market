//! 把英超历史成交回填成已完成套利订单。默认只打印，`--confirm` 才写库。
use chrono::{DateTime, Utc};
use market_arb::backfill::{
    match_backfill, ComplementaryPair, MatchError, PlannedOrder, TradeFillInput, TradeSide,
    BACKFILL_TAG,
};
use market_arb::config::{Config, OUTCOME, POLYMARKET};
use market_arb::domain::{parse_side_coin, UnifiedOption};
use market_arb::error::{Error, Result};
use market_arb::exec::arb_calc_excluded;
use market_arb::platforms::outcome::OutcomeVenue;
use market_arb::store::{CompletedBackfillLeg, CompletedBackfillOrder, Store};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use uuid::Uuid;

const OUTCOME_ACCOUNT: &str = "0xFc039d81A8B973C6a336A1ecFfC1Cb9BbFdD05Bb";

#[derive(Deserialize)]
struct EventFile {
    id: String,
    title: String,
    end_date: String,
    unified_options: Vec<UnifiedOption>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run().await.map_err(Into::into)
}

async fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") || args.is_empty() {
        println!(
            "backfill-arb --event EVENT.json --polymarket PM.json --outcome OUT.json [--confirm]\n\
             默认只打印将写入的订单。--confirm 才连接数据库写入 completed 订单。\n\
             启动时加载 .env。Outcome 账户必须是 {OUTCOME_ACCOUNT}。"
        );
        return Ok(());
    }
    let mut event_path = None;
    let mut pm_path = None;
    let mut out_path = None;
    let mut confirm = false;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--confirm" {
            confirm = true;
            index += 1;
            continue;
        }
        index += 1;
        let value = args
            .get(index)
            .ok_or_else(|| Error::msg(format!("missing value for {arg}")))?;
        match arg.as_str() {
            "--event" => event_path = Some(value.clone()),
            "--polymarket" => pm_path = Some(value.clone()),
            "--outcome" => out_path = Some(value.clone()),
            _ => return Err(Error::msg(format!("unknown argument {arg}"))),
        }
        index += 1;
    }
    let event_path = event_path.ok_or_else(|| Error::msg("--event is required"))?;
    let pm_path = pm_path.ok_or_else(|| Error::msg("--polymarket is required"))?;
    let out_path = out_path.ok_or_else(|| Error::msg("--outcome is required"))?;

    let cfg = Config::from_env()?;
    let account = cfg
        .outcome_account_address
        .as_deref()
        .ok_or_else(|| Error::msg("OUTCOME_ACCOUNT_ADDRESS is required"))?;
    if !account.eq_ignore_ascii_case(OUTCOME_ACCOUNT) {
        return Err(Error::msg(
            "OUTCOME_ACCOUNT_ADDRESS is not the backfill outcome account",
        ));
    }

    let event = load_event(&event_path)?;
    let event_id = Uuid::parse_str(event.id.trim()).map_err(Error::msg)?;
    if !arb_calc_excluded(event_id) {
        return Err(Error::msg("event file is not the hardcoded backfill event"));
    }
    let end_date = parse_end_date(&event.end_date)?;
    let pairs = complementary_pairs(&event.unified_options)?;
    let pm = load_pm_fills(&pm_path, &pairs)?;
    let outcome = load_outcome_fills(&out_path, &pairs)?;
    let plan = match_backfill(&pairs, &pm, &outcome).map_err(match_err)?;

    let mut missing = BTreeSet::new();
    println!(
        "事件 {} {}，订单 {} 笔，忽略 PM 多头 {} 笔",
        event_id,
        event.title,
        plan.orders.len(),
        plan.ignored_pm.len()
    );
    for order in &plan.orders {
        let known = funder(&cfg, &order.pm_wallet).is_some();
        if !known {
            missing.insert(order.pm_wallet.clone());
        }
        println!(
            "idx={} {} wallet={} funder={} pm {} {} shares={} price={} fee={} | outcome {} {} shares={} price={} fee={}",
            order.unified_index,
            order.market_title,
            order.pm_wallet,
            if known { "yes" } else { "MISSING" },
            order.pm_label,
            order.pm_token,
            order.pm_shares,
            order.pm_price,
            order.pm_fee,
            order.out_label,
            order.out_token,
            order.out_shares,
            order.out_price,
            order.out_fee
        );
    }
    for ignored in &plan.ignored_pm {
        println!(
            "忽略 PM wallet={} token={} shares={}",
            ignored.wallet, ignored.token_id, ignored.shares
        );
    }
    if !confirm {
        println!("未写入数据库。确认后追加 --confirm。");
        return Ok(());
    }
    if !missing.is_empty() {
        return Err(Error::msg(format!(
            "polymarket funder missing for {}",
            missing.into_iter().collect::<Vec<_>>().join(",")
        )));
    }
    let venue = OutcomeVenue::connect(&cfg)?;
    venue.refresh_fees().await?;
    let mut estimates = BTreeMap::new();
    for pair in &pairs {
        let outcome_id = pair
            .option_id
            .parse::<u64>()
            .map_err(|_| Error::msg(format!("invalid outcome option id {}", pair.option_id)))?;
        if estimates.contains_key(&outcome_id) {
            continue;
        }
        estimates.insert(outcome_id, venue.fee_snapshot(outcome_id)?.estimate_json());
    }
    let writes = plan
        .orders
        .iter()
        .map(|order| to_write(&cfg, event_id, &event.title, end_date, order, &estimates))
        .collect::<Result<Vec<_>>>()?;
    let store = Store::connect(&cfg.app_postgres_uri).await?;
    let ids = store.insert_completed_backfill(event_id, &writes).await?;
    println!(
        "已写入 {} 笔，订单号 {}-{}",
        ids.len(),
        ids[0],
        ids[ids.len() - 1]
    );
    Ok(())
}

fn to_write(
    cfg: &Config,
    event_id: Uuid,
    title: &str,
    end_date: DateTime<Utc>,
    order: &PlannedOrder,
    estimates: &BTreeMap<u64, Value>,
) -> Result<CompletedBackfillOrder> {
    let funder = funder(cfg, &order.pm_wallet)
        .ok_or_else(|| Error::msg(format!("missing funder {}", order.pm_wallet)))?;
    let (outcome_id, _) = parse_side_coin(&order.out_token)
        .ok_or_else(|| Error::msg(format!("invalid outcome token {}", order.out_token)))?;
    let estimate = estimates
        .get(&outcome_id)
        .cloned()
        .ok_or_else(|| Error::msg(format!("missing fee estimate for outcome {outcome_id}")))?;
    let submitted_at = DateTime::from_timestamp(order.submitted_at, 0)
        .ok_or_else(|| Error::msg("invalid fill timestamp"))?;
    let pm_notional = order.pm_shares * order.pm_price;
    let out_notional = order.out_shares * order.out_price;
    Ok(CompletedBackfillOrder {
        event_id,
        unified_index: order.unified_index,
        title: title.to_string(),
        market_title: order.market_title.clone(),
        end_date: Some(end_date),
        estimated_cost: (pm_notional + out_notional + order.pm_fee + order.out_fee).round_dp(8),
        fills: json!({
            "backfill": BACKFILL_TAG,
            "pm_sources": order.pm_sources,
            "outcome_sources": order.out_sources,
        }),
        condition_id: order.condition_id.clone(),
        option_id: order.option_id.clone(),
        legs: vec![
            CompletedBackfillLeg {
                platform: POLYMARKET.to_string(),
                token_id: order.pm_token.clone(),
                label: order.pm_label.clone(),
                funder: Some(funder.funder_address.clone()),
                wallet: Some(funder.funder_address.clone()),
                service: funder.service.clone(),
                price: order.pm_price,
                shares: order.pm_shares,
                fee: order.pm_fee,
                client_order_id: client_order_id("pm", &order.pm_sources),
                submitted_at,
                last_order_info: None,
            },
            CompletedBackfillLeg {
                platform: OUTCOME.to_string(),
                token_id: order.out_token.clone(),
                label: order.out_label.clone(),
                funder: None,
                wallet: Some(OUTCOME_ACCOUNT.to_string()),
                service: None,
                price: order.out_price,
                shares: order.out_shares,
                fee: order.out_fee,
                client_order_id: client_order_id("out", &order.out_sources),
                submitted_at,
                last_order_info: Some(json!({ "fee_estimate": estimate })),
            },
        ],
    })
}

fn funder<'a>(
    cfg: &'a Config,
    wallet: &str,
) -> Option<&'a market_arb::config::PolymarketFunderConfig> {
    cfg.polymarket_funders
        .iter()
        .find(|item| item.funder_address.eq_ignore_ascii_case(wallet))
}

fn client_order_id(platform: &str, sources: &[String]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(platform.as_bytes());
    for source in sources {
        hasher.update(source.as_bytes());
        hasher.update([0]);
    }
    format!("bf-{platform}-{}", hex::encode(&hasher.finalize()[..12]))
}

fn load_event(path: &str) -> Result<EventFile> {
    let raw = std::fs::read_to_string(path).map_err(Error::msg)?;
    let value: Value = serde_json::from_str(&raw)?;
    let value = match value {
        Value::Array(items) => items
            .into_iter()
            .next()
            .ok_or_else(|| Error::msg("event file is empty"))?,
        other => other,
    };
    Ok(serde_json::from_value(value)?)
}

fn parse_end_date(raw: &str) -> Result<DateTime<Utc>> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw.trim()) {
        return Ok(parsed.with_timezone(&Utc));
    }
    DateTime::parse_from_str(raw.trim(), "%Y-%m-%d %H:%M:%S%.f %:z")
        .or_else(|_| DateTime::parse_from_str(raw.trim(), "%Y-%m-%d %H:%M:%S %:z"))
        .map(|parsed| parsed.with_timezone(&Utc))
        .map_err(|err| Error::msg(format!("invalid end_date: {err}")))
}

fn complementary_pairs(options: &[UnifiedOption]) -> Result<Vec<ComplementaryPair>> {
    let mut pairs = Vec::new();
    for option in options {
        let pm = option
            .platform_options
            .iter()
            .find(|item| item.platform.eq_ignore_ascii_case(POLYMARKET));
        let outcome = option
            .platform_options
            .iter()
            .find(|item| item.platform.eq_ignore_ascii_case(OUTCOME));
        let (Some(pm), Some(outcome)) = (pm, outcome) else {
            continue;
        };
        let condition_id = pm
            .condition_id
            .clone()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| Error::msg(format!("missing condition id for {}", option.title)))?;
        let pm_yes = token_for(pm, "yes")?;
        let pm_no = token_for(pm, "no")?;
        let out_yes = token_for(outcome, "yes")?;
        let out_no = token_for(outcome, "no")?;
        pairs.push(ComplementaryPair {
            unified_index: option.index,
            market_title: option.title.clone(),
            pm_token: pm_no,
            pm_label: "no".to_string(),
            out_token: out_yes,
            out_label: "yes".to_string(),
            condition_id: condition_id.clone(),
            option_id: outcome.option_id.clone(),
        });
        pairs.push(ComplementaryPair {
            unified_index: option.index,
            market_title: option.title.clone(),
            pm_token: pm_yes,
            pm_label: "yes".to_string(),
            out_token: out_no,
            out_label: "no".to_string(),
            condition_id,
            option_id: outcome.option_id.clone(),
        });
    }
    Ok(pairs)
}

fn token_for(option: &market_arb::domain::PlatformOption, label: &str) -> Result<String> {
    option
        .outcomes
        .iter()
        .find(|item| item.label.eq_ignore_ascii_case(label))
        .map(|item| item.token_id.clone())
        .ok_or_else(|| Error::msg(format!("missing {label} token")))
}

fn load_pm_fills(path: &str, pairs: &[ComplementaryPair]) -> Result<Vec<TradeFillInput>> {
    let tokens: BTreeSet<&str> = pairs.iter().map(|pair| pair.pm_token.as_str()).collect();
    let raw = std::fs::read_to_string(path).map_err(Error::msg)?;
    let rows: Vec<Value> = serde_json::from_str(&raw)?;
    let mut fills = Vec::new();
    for row in rows {
        let token = text(&row, "token_id")?;
        if !tokens.contains(token.as_str()) {
            continue;
        }
        let onchain = row.get("onchain_fill");
        let shares = match onchain.and_then(|item| item.get("shares")) {
            Some(value) => decimal(value)?,
            None => decimal(required(&row, "size")?)?,
        };
        if shares <= Decimal::ZERO {
            continue;
        }
        let fee = onchain
            .and_then(|item| item.get("fee_collateral"))
            .map(decimal)
            .transpose()?
            .unwrap_or(Decimal::ZERO);
        let logs = onchain
            .and_then(|item| item.get("log_indices"))
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_u64)
                    .map(|index| index.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let tx = text(&row, "transaction_hash")?;
        fills.push(TradeFillInput {
            ts: row
                .get("timestamp")
                .and_then(Value::as_i64)
                .ok_or_else(|| Error::msg("polymarket timestamp missing"))?,
            account: text(&row, "proxy_wallet")?.to_ascii_lowercase(),
            token_id: token,
            side: trade_side(&text(&row, "side")?)?,
            shares,
            price: decimal(required(&row, "price")?)?,
            fee,
            source_id: format!("{tx}:{logs}"),
        });
    }
    Ok(fills)
}

fn load_outcome_fills(path: &str, pairs: &[ComplementaryPair]) -> Result<Vec<TradeFillInput>> {
    let tokens: BTreeSet<&str> = pairs.iter().map(|pair| pair.out_token.as_str()).collect();
    let raw = std::fs::read_to_string(path).map_err(Error::msg)?;
    let rows: Vec<Value> = serde_json::from_str(&raw)?;
    let mut fills = Vec::new();
    for row in rows {
        let token = text(&row, "coin")?;
        if !tokens.contains(token.as_str()) {
            continue;
        }
        let shares = decimal(required(&row, "sz")?)?;
        if shares <= Decimal::ZERO {
            continue;
        }
        let time_ms = row
            .get("time")
            .and_then(Value::as_i64)
            .ok_or_else(|| Error::msg("outcome time missing"))?;
        fills.push(TradeFillInput {
            ts: time_ms.div_euclid(1000),
            account: OUTCOME_ACCOUNT.to_ascii_lowercase(),
            token_id: token,
            side: trade_side(&text(&row, "dir")?)?,
            shares,
            price: decimal(required(&row, "px")?)?,
            fee: match row.get("fee") {
                Some(value) => decimal(value)?,
                None => Decimal::ZERO,
            },
            source_id: ident(&row, "tid")?,
        });
    }
    Ok(fills)
}

fn trade_side(raw: &str) -> Result<TradeSide> {
    match raw.to_ascii_lowercase().as_str() {
        "buy" | "b" => Ok(TradeSide::Buy),
        "sell" | "a" => Ok(TradeSide::Sell),
        _ => Err(Error::msg(format!("unknown trade side {raw}"))),
    }
}

fn text(row: &Value, key: &str) -> Result<String> {
    row.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| Error::msg(format!("missing {key}")))
}

fn ident(row: &Value, key: &str) -> Result<String> {
    match row.get(key) {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        _ => Err(Error::msg(format!("missing {key}"))),
    }
}

fn required<'a>(row: &'a Value, key: &str) -> Result<&'a Value> {
    row.get(key)
        .ok_or_else(|| Error::msg(format!("missing {key}")))
}

fn decimal(value: &Value) -> Result<Decimal> {
    let raw = match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => return Err(Error::msg("expected decimal")),
    };
    Decimal::from_str(&raw).map_err(|_| Error::msg(format!("invalid decimal {raw}")))
}

fn match_err(err: MatchError) -> Error {
    Error::msg(err.to_string())
}
