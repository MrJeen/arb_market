//! 本地调试 Polymarket / Outcome 市价下单接口。
//!
//! 不连 Postgres、不订盘口 WS、不轮询成交，只走签名 + `/order` 或 exchange 提交。
//! 默认只签名不发单；真实下单必须加 `--confirm`（或 `PLACE_TEST_CONFIRM=1`）。
//!
//! ```text
//! cargo run --bin place-test -- --platform polymarket --side buy \
//!   --token <pm_token_id> --shares 5 --price 0.40 --confirm
//!
//! cargo run --bin place-test -- --platform outcome --side sell \
//!   --token '#5160' --shares 5 --price 0.40 --confirm
//!
//! cargo run --bin place-test -- --all --pm-token <id> --out-token '#5160' \
//!   --shares 5 --price 0.40 --confirm
//! ```

use market_arb::book::Level;
use market_arb::config::{Config, OUTCOME, POLYMARKET};
use market_arb::domain::{parse_side_coin, side_asset_id};
use market_arb::error::{Error, Result};
use market_arb::platforms::outcome::OutcomeVenue;
use market_arb::platforms::polymarket::PolymarketVenue;
use market_arb::platforms::{MarketOrderRequest, OrderSide, SubmitResult};
use rust_decimal::Decimal;
use std::env;
use std::str::FromStr;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let argv: Vec<String> = env::args().skip(1).collect();
    if argv.is_empty() {
        print_help();
        return Ok(());
    }
    let args = Args::parse(argv)?;
    if args.help {
        print_help();
        return Ok(());
    }
    args.validate()?;
    let cfg = load_trading_cfg()?;
    let jobs = args.jobs()?;
    if jobs.is_empty() {
        return Err(Error::msg("no jobs to run").into());
    }
    print_plan(&args, &jobs);
    if !args.dry_run && !args.confirm {
        return Err(Error::msg("refusing live submit: pass --confirm or --dry-run").into());
    }

    let need_pm = jobs.iter().any(|j| j.platform == POLYMARKET);
    let need_out = jobs.iter().any(|j| j.platform == OUTCOME);
    let pm = if need_pm {
        Some(PolymarketVenue::connect(&cfg).await?)
    } else {
        None
    };
    let outcome = if need_out {
        Some(OutcomeVenue::connect(&cfg)?)
    } else {
        None
    };
    if need_pm && pm.as_ref().is_some_and(|v| v.account_count() == 0) {
        return Err(Error::msg("no polymarket funder configured").into());
    }
    if need_out
        && outcome
            .as_ref()
            .is_some_and(|v| v.account_address().is_none())
    {
        return Err(Error::msg("missing OUTCOME_ACCOUNT_ADDRESS").into());
    }

    let mut failed = 0usize;
    for (i, job) in jobs.iter().enumerate() {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(args.sleep_ms)).await;
        }
        match run_job(&args, job, pm.as_ref(), outcome.as_ref()).await {
            Ok(()) => {}
            Err(err) => {
                failed += 1;
                tracing::error!(
                    platform = job.platform,
                    side = job.side.as_str(),
                    token = %job.token_id,
                    error = %err,
                    "place-test job failed"
                );
            }
        }
    }
    if failed > 0 {
        return Err(Error::msg(format!("{failed}/{} job(s) failed", jobs.len())).into());
    }
    Ok(())
}

fn load_trading_cfg() -> Result<Config> {
    let _ = dotenvy::dotenv();
    if env_blank("COMMON_POSTGRES_URI") {
        env::set_var("COMMON_POSTGRES_URI", "postgres://place-test/unused");
    }
    if env_blank("APP_POSTGRES_URI") {
        env::set_var("APP_POSTGRES_URI", "postgres://place-test/unused");
    }
    Config::from_env()
}

fn env_blank(key: &str) -> bool {
    env::var(key)
        .ok()
        .map(|s| s.trim().is_empty())
        .unwrap_or(true)
}

async fn run_job(
    args: &Args,
    job: &Job,
    pm: Option<&PolymarketVenue>,
    outcome: Option<&OutcomeVenue>,
) -> Result<()> {
    let req = job.request(args)?;
    tracing::info!(
        platform = job.platform,
        side = job.side.as_str(),
        token = %job.token_id,
        shares = %req.shares,
        cap_price = %req.cap_price,
        dry_run = args.dry_run,
        "place-test submit"
    );
    if job.platform == POLYMARKET {
        let venue = pm.ok_or_else(|| Error::msg("polymarket venue not connected"))?;
        let funder = resolve_pm_funder(venue, args.funder.as_deref()).await?;
        print_venue_book(
            venue.rest_book(&job.token_id).await,
            POLYMARKET,
            job,
            args.shares,
            args.price,
        );
        print_pm_balances(venue, &funder, &job.token_id, job.side).await;
        if args.dry_run {
            let prepared = venue.prepare_market_order(&funder, &req).await?;
            println!(
                "DRY-RUN {} {} token={} hash={} funder={}",
                POLYMARKET,
                job.side.as_str(),
                job.token_id,
                prepared.order_hash,
                funder
            );
            return Ok(());
        }
        if job.side == OrderSide::Buy {
            let need = args.shares * args.price;
            match venue.balance(&funder).await {
                Ok(bal) if bal >= need => {}
                Ok(bal) => {
                    println!("SKIP {POLYMARKET} BUY usdc {bal} < {need}");
                    return Ok(());
                }
                Err(err) => return Err(err),
            }
        } else {
            match venue.token_balance(&funder, &job.token_id).await {
                Ok(bal) if bal >= args.shares => {}
                Ok(bal) => {
                    println!(
                        "SKIP {POLYMARKET} SELL token={} balance {bal} < {}",
                        job.token_id, args.shares
                    );
                    return Ok(());
                }
                Err(err) => return Err(err),
            }
        }
        let result = venue.market_order(&funder, &req).await?;
        print_result(POLYMARKET, job, &result);
        return Ok(());
    }
    let venue = outcome.ok_or_else(|| Error::msg("outcome venue not connected"))?;
    print_venue_book(
        venue.rest_book(&job.token_id).await,
        OUTCOME,
        job,
        args.shares,
        args.price,
    );
    print_outcome_balances(venue, &job.token_id, job.side).await;
    if args.dry_run {
        let prepared = venue.prepare_market_order(&req)?;
        println!(
            "DRY-RUN {} {} token={} hash={} asset={:?}",
            OUTCOME,
            job.side.as_str(),
            job.token_id,
            prepared.order_hash,
            req.asset_id
        );
        return Ok(());
    }
    if job.side == OrderSide::Buy {
        let need = args.shares * args.price;
        match venue.user_state().await {
            Ok(bal) if bal >= need => {}
            Ok(bal) => {
                println!("SKIP {OUTCOME} BUY usdc {bal} < {need}");
                return Ok(());
            }
            Err(err) => return Err(err),
        }
    } else {
        match venue.token_balance(&job.token_id).await {
            Ok(bal) if bal >= args.shares => {}
            Ok(bal) => {
                println!(
                    "SKIP {OUTCOME} SELL token={} balance {bal} < {}",
                    job.token_id, args.shares
                );
                return Ok(());
            }
            Err(err) => return Err(err),
        }
    }
    let result = venue.market_order(&req).await?;
    print_result(OUTCOME, job, &result);
    Ok(())
}

async fn resolve_pm_funder(venue: &PolymarketVenue, requested: Option<&str>) -> Result<String> {
    if let Some(funder) = requested {
        venue
            .account(funder)
            .ok_or_else(|| Error::msg(format!("unknown polymarket funder {funder}")))?;
        return Ok(funder.to_string());
    }
    venue
        .next_funder()
        .await
        .ok_or_else(|| Error::msg("no polymarket funder configured"))
}

fn print_venue_book(
    fetched: Result<(Vec<Level>, Vec<Level>, i64)>,
    platform: &str,
    job: &Job,
    shares: Decimal,
    cap: Decimal,
) {
    match fetched {
        Ok((bids, asks, ts)) => print_book(platform, job, shares, cap, &bids, &asks, ts),
        Err(err) => tracing::warn!(
            platform,
            token = %job.token_id,
            error = %err,
            "order book fetch failed"
        ),
    }
}

fn print_book(
    platform: &str,
    job: &Job,
    shares: Decimal,
    cap: Decimal,
    bids: &[Level],
    asks: &[Level],
    ts: i64,
) {
    const DEPTH: usize = 5;
    println!(
        "BOOK {} {} token={} ts={}",
        platform,
        job.side.as_str(),
        job.token_id,
        ts
    );
    print_levels("  ask", asks.iter().take(DEPTH));
    print_levels("  bid", bids.iter().take(DEPTH));
    if asks.is_empty() && bids.is_empty() {
        println!("  empty book");
        return;
    }
    let best_ask = asks.first().map(|l| l.price);
    let best_bid = bids.first().map(|l| l.price);
    let mid = match (best_bid, best_ask) {
        (Some(b), Some(a)) => Some((b + a) / Decimal::from(2)),
        (Some(b), None) => Some(b),
        (None, Some(a)) => Some(a),
        (None, None) => None,
    };
    println!(
        "  best_bid={} best_ask={} mid={} cap={}",
        fmt_opt(best_bid),
        fmt_opt(best_ask),
        fmt_opt(mid),
        cap
    );
    match job.side {
        OrderSide::Buy => {
            if let Some(ask) = best_ask {
                if cap >= ask {
                    println!("  cap >= best_ask, IOC buy can lift");
                } else {
                    println!("  cap < best_ask, IOC buy will not lift the ask");
                }
            }
        }
        OrderSide::Sell => {
            if let Some(bid) = best_bid {
                if cap <= bid {
                    println!("  cap <= best_bid, IOC sell can hit");
                } else {
                    println!("  cap > best_bid, IOC sell will not hit the bid");
                }
            }
        }
    }
    let cap_ntl = shares * cap;
    let mid_ntl = mid.map(|m| shares * m);
    let touch = match job.side {
        OrderSide::Buy => best_ask,
        OrderSide::Sell => best_bid,
    };
    let touch_ntl = touch.map(|p| shares * p);
    println!(
        "  notional shares={shares} cap={cap_ntl} mid={} touch={}",
        fmt_opt(mid_ntl),
        fmt_opt(touch_ntl)
    );
}

fn print_levels<'a>(label: &str, levels: impl Iterator<Item = &'a Level>) {
    let rows: Vec<String> = levels
        .map(|l| format!("{} x {}", l.price, l.size))
        .collect();
    if rows.is_empty() {
        println!("{label}: (none)");
    } else {
        println!("{label}: {}", rows.join(" | "));
    }
}

fn fmt_opt(value: Option<Decimal>) -> String {
    value.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
}

async fn print_pm_balances(venue: &PolymarketVenue, funder: &str, token_id: &str, side: OrderSide) {
    match venue.balance(funder).await {
        Ok(bal) => println!("polymarket usdc funder={funder} balance={bal}"),
        Err(err) => tracing::warn!(funder, error = %err, "polymarket usdc balance unavailable"),
    }
    if side == OrderSide::Sell {
        match venue.token_balance(funder, token_id).await {
            Ok(bal) => println!("polymarket token={token_id} balance={bal}"),
            Err(err) => {
                tracing::warn!(token_id, error = %err, "polymarket token balance unavailable")
            }
        }
    }
}

async fn print_outcome_balances(venue: &OutcomeVenue, token_id: &str, side: OrderSide) {
    match venue.user_state().await {
        Ok(bal) => println!("outcome usdc balance={bal}"),
        Err(err) => tracing::warn!(error = %err, "outcome usdc balance unavailable"),
    }
    if side == OrderSide::Sell {
        match venue.token_balance(token_id).await {
            Ok(bal) => println!("outcome token={token_id} balance={bal}"),
            Err(err) => tracing::warn!(token_id, error = %err, "outcome token balance unavailable"),
        }
    }
}

fn print_result(platform: &str, job: &Job, result: &SubmitResult) {
    match result {
        SubmitResult::Ack {
            order_id,
            order_hash,
            making,
            taking,
            avg_px,
            ..
        } => {
            println!(
                "ACK {} {} token={} order_id={} hash={} making={:?} taking={:?} avg_px={:?}",
                platform,
                job.side.as_str(),
                job.token_id,
                order_id,
                order_hash,
                making,
                taking,
                avg_px
            );
        }
        SubmitResult::NoMatch {
            order_hash,
            message,
            ..
        } => {
            println!(
                "NO_MATCH {} {} token={} hash={} message={}",
                platform,
                job.side.as_str(),
                job.token_id,
                order_hash,
                message
            );
        }
        SubmitResult::Unknown {
            order_id,
            order_hash,
            message,
            ..
        } => {
            println!(
                "UNKNOWN {} {} token={} order_id={:?} hash={} message={}",
                platform,
                job.side.as_str(),
                job.token_id,
                order_id,
                order_hash,
                message
            );
        }
        SubmitResult::Failed {
            order_hash,
            status,
            message,
            ..
        } => {
            println!(
                "FAILED {} {} token={} http={} hash={} message={}",
                platform,
                job.side.as_str(),
                job.token_id,
                status,
                order_hash,
                message
            );
        }
    }
}

fn print_plan(args: &Args, jobs: &[Job]) {
    let mode = if args.dry_run {
        "dry-run (sign only, no HTTP submit)"
    } else {
        "LIVE submit to mainnet APIs"
    };
    println!("place-test mode={mode} jobs={}", jobs.len());
    for job in jobs {
        println!(
            "  {} {} token={} shares={} price={}",
            job.platform,
            job.side.as_str(),
            job.token_id,
            args.shares,
            args.price
        );
    }
}

fn print_help() {
    eprintln!(
        "\
place-test — 本地调试两平台市价下单（买/卖），不读撮合、不写库

用法:
  cargo run --bin place-test -- --platform polymarket --side buy --token TOKEN --shares 5 --price 0.40
  cargo run --bin place-test -- --platform outcome --side sell --token '#5160' --shares 5 --price 0.40 --confirm
  cargo run --bin place-test -- --all --pm-token TOKEN --out-token '#5160' --shares 5 --price 0.40 --confirm

参数:
  --platform polymarket|outcome|both   目标平台；--all 等同 both + 买卖两边
  --side buy|sell|both                 省略则两边都测
  --token TOKEN                        单平台 token_id（Outcome 形如 #5160）
  --pm-token TOKEN                     Polymarket token_id（--all / both 时）
  --out-token TOKEN                    Outcome token_id
  --shares N                           股数（必填，>0）
  --price P                            cap price（必填，>0）
  --funder 0x...                       指定 Polymarket funder；默认轮询第一个
  --neg-risk true|false                Polymarket neg_risk；省略则请求 /neg-risk
  --asset-id N                         覆盖 Outcome assetId
  --sleep-ms N                         多笔间隔，默认 500
  --dry-run                            只签名，不提交（默认）
  --confirm                            真实提交；也可设 PLACE_TEST_CONFIRM=1
  --help                               帮助

读 .env 与 polymarket_funders.json。不使用 Postgres / WS / NATS。
接口通：ACK / NO_MATCH / 交易所拒绝都算链路正常；FAILED / 签名错误才是接口异常。"
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlatformSel {
    Polymarket,
    Outcome,
    Both,
}

#[derive(Debug, Clone)]
struct Job {
    platform: &'static str,
    side: OrderSide,
    token_id: String,
}

impl Job {
    fn request(&self, args: &Args) -> Result<MarketOrderRequest> {
        let asset_id = if self.platform == OUTCOME {
            Some(resolve_outcome_asset(&self.token_id, args.asset_id)?)
        } else {
            None
        };
        Ok(MarketOrderRequest {
            token_id: self.token_id.clone(),
            shares: args.shares,
            cap_price: args.price,
            side: self.side,
            neg_risk: args.neg_risk,
            tick_size: None,
            asset_id,
            funder_address: args.funder.clone(),
        })
    }
}

fn resolve_outcome_asset(token_id: &str, override_id: Option<u64>) -> Result<u64> {
    if let Some(id) = override_id {
        return Ok(id);
    }
    parse_side_coin(token_id)
        .map(|(id, side)| side_asset_id(id, side))
        .ok_or_else(|| {
            Error::msg(format!(
                "cannot derive outcome assetId from token {token_id}; pass --asset-id"
            ))
        })
}

#[derive(Debug)]
struct Args {
    help: bool,
    platform: Option<PlatformSel>,
    all: bool,
    sides: Vec<OrderSide>,
    token: Option<String>,
    pm_token: Option<String>,
    out_token: Option<String>,
    shares: Decimal,
    price: Decimal,
    funder: Option<String>,
    neg_risk: Option<bool>,
    asset_id: Option<u64>,
    sleep_ms: u64,
    dry_run: bool,
    confirm: bool,
}

impl Args {
    fn parse(raw: Vec<String>) -> Result<Self> {
        let mut help = false;
        let mut platform = None;
        let mut all = false;
        let mut sides = Vec::new();
        let mut token = None;
        let mut pm_token = None;
        let mut out_token = None;
        let mut shares = None;
        let mut price = None;
        let mut funder = None;
        let mut neg_risk = None;
        let mut asset_id = None;
        let mut sleep_ms = 500u64;
        let mut dry_run = true;
        let mut confirm = env_flag("PLACE_TEST_CONFIRM");
        let mut i = 0;
        while i < raw.len() {
            let arg = raw[i].as_str();
            match arg {
                "-h" | "--help" => help = true,
                "--all" => all = true,
                "--dry-run" => dry_run = true,
                "--confirm" => {
                    confirm = true;
                    dry_run = false;
                }
                "--platform" => {
                    platform = Some(parse_platform(next(&raw, &mut i, "--platform")?)?);
                }
                "--side" => sides = parse_sides(next(&raw, &mut i, "--side")?)?,
                "--token" => token = Some(next(&raw, &mut i, "--token")?.to_string()),
                "--pm-token" => pm_token = Some(next(&raw, &mut i, "--pm-token")?.to_string()),
                "--out-token" => out_token = Some(next(&raw, &mut i, "--out-token")?.to_string()),
                "--shares" => shares = Some(parse_decimal_arg(next(&raw, &mut i, "--shares")?)?),
                "--price" => price = Some(parse_decimal_arg(next(&raw, &mut i, "--price")?)?),
                "--funder" => funder = Some(next(&raw, &mut i, "--funder")?.to_string()),
                "--neg-risk" => neg_risk = Some(parse_bool_arg(next(&raw, &mut i, "--neg-risk")?)?),
                "--asset-id" => {
                    asset_id = Some(
                        next(&raw, &mut i, "--asset-id")?
                            .parse()
                            .map_err(|_| Error::msg("invalid --asset-id"))?,
                    );
                }
                "--sleep-ms" => {
                    sleep_ms = next(&raw, &mut i, "--sleep-ms")?
                        .parse()
                        .map_err(|_| Error::msg("invalid --sleep-ms"))?;
                }
                other if other.starts_with("--") && other.contains('=') => {
                    let (k, v) = other.split_once('=').unwrap();
                    match k {
                        "--platform" => platform = Some(parse_platform(v)?),
                        "--side" => sides = parse_sides(v)?,
                        "--token" => token = Some(v.to_string()),
                        "--pm-token" => pm_token = Some(v.to_string()),
                        "--out-token" => out_token = Some(v.to_string()),
                        "--shares" => shares = Some(parse_decimal_arg(v)?),
                        "--price" => price = Some(parse_decimal_arg(v)?),
                        "--funder" => funder = Some(v.to_string()),
                        "--neg-risk" => neg_risk = Some(parse_bool_arg(v)?),
                        "--asset-id" => {
                            asset_id =
                                Some(v.parse().map_err(|_| Error::msg("invalid --asset-id"))?)
                        }
                        "--sleep-ms" => {
                            sleep_ms = v.parse().map_err(|_| Error::msg("invalid --sleep-ms"))?
                        }
                        _ => return Err(Error::msg(format!("unknown argument {other}"))),
                    }
                }
                other => return Err(Error::msg(format!("unknown argument {other}"))),
            }
            i += 1;
        }
        if confirm {
            dry_run = false;
        }
        Ok(Self {
            help,
            platform,
            all,
            sides,
            token,
            pm_token,
            out_token,
            shares: shares.unwrap_or(Decimal::ZERO),
            price: price.unwrap_or(Decimal::ZERO),
            funder,
            neg_risk,
            asset_id,
            sleep_ms,
            dry_run,
            confirm,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.help {
            return Ok(());
        }
        if self.shares <= Decimal::ZERO {
            return Err(Error::msg("--shares must be positive"));
        }
        if self.price <= Decimal::ZERO {
            return Err(Error::msg("--price must be positive"));
        }
        if self.price >= Decimal::ONE {
            tracing::warn!(price = %self.price, "price >= 1, usually a prediction-market cap");
        }
        Ok(())
    }

    fn jobs(&self) -> Result<Vec<Job>> {
        let platforms = self.platforms()?;
        let sides = if self.sides.is_empty() {
            vec![OrderSide::Buy, OrderSide::Sell]
        } else {
            self.sides.clone()
        };
        let mut jobs = Vec::new();
        for platform in platforms {
            let token_id = self.token_for(platform)?;
            for side in &sides {
                jobs.push(Job {
                    platform,
                    side: *side,
                    token_id: token_id.clone(),
                });
            }
        }
        Ok(jobs)
    }

    fn platforms(&self) -> Result<Vec<&'static str>> {
        if self.all {
            return Ok(vec![POLYMARKET, OUTCOME]);
        }
        match self.platform {
            Some(PlatformSel::Polymarket) => Ok(vec![POLYMARKET]),
            Some(PlatformSel::Outcome) => Ok(vec![OUTCOME]),
            Some(PlatformSel::Both) => Ok(vec![POLYMARKET, OUTCOME]),
            None => Err(Error::msg("pass --platform or --all")),
        }
    }

    fn token_for(&self, platform: &str) -> Result<String> {
        if platform == POLYMARKET {
            self.pm_token
                .clone()
                .or_else(|| self.token.clone())
                .ok_or_else(|| Error::msg("missing --pm-token or --token for polymarket"))
        } else {
            self.out_token
                .clone()
                .or_else(|| self.token.clone())
                .ok_or_else(|| Error::msg("missing --out-token or --token for outcome"))
        }
    }
}

fn parse_platform(raw: &str) -> Result<PlatformSel> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "polymarket" | "pm" => Ok(PlatformSel::Polymarket),
        "outcome" | "hl" | "hyperliquid" => Ok(PlatformSel::Outcome),
        "both" | "all" => Ok(PlatformSel::Both),
        other => Err(Error::msg(format!("unknown --platform {other}"))),
    }
}

fn parse_sides(raw: &str) -> Result<Vec<OrderSide>> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok(vec![OrderSide::Buy]),
        "sell" => Ok(vec![OrderSide::Sell]),
        "both" | "all" => Ok(vec![OrderSide::Buy, OrderSide::Sell]),
        other => Err(Error::msg(format!("unknown --side {other}"))),
    }
}

fn parse_decimal_arg(raw: &str) -> Result<Decimal> {
    Decimal::from_str(raw.trim()).map_err(|_| Error::msg(format!("invalid decimal {raw}")))
}

fn parse_bool_arg(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(Error::msg(format!("invalid bool {other}"))),
    }
}

fn env_flag(key: &str) -> bool {
    env::var(key)
        .ok()
        .map(|s| {
            matches!(
                s.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn next<'a>(raw: &'a [String], i: &mut usize, flag: &str) -> Result<&'a str> {
    let value = raw
        .get(*i + 1)
        .ok_or_else(|| Error::msg(format!("missing value for {flag}")))?;
    *i += 1;
    Ok(value.as_str())
}
