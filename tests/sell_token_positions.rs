use market_arb::book::Level;
use market_arb::config::{Config, PolymarketFunderConfig, POLYMARKET};
use market_arb::platforms::polymarket::PolymarketVenue;
use market_arb::platforms::{
    MarketOrderRequest, OrderSide, PreparedOrder, SubmissionResponse, SubmitResult,
};
use market_arb::stats::MinuteStats;
use market_arb::store::Store;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;

type Check<T> = Result<T, &'static str>;

#[derive(Debug, Clone, Copy, PartialEq)]
enum SellMode {
    Fak { min_price: Option<Decimal> },
    Gtc { price: Decimal },
}

impl SellMode {
    fn order_type(self) -> &'static str {
        match self {
            Self::Fak { .. } => "FAK",
            Self::Gtc { .. } => "GTC",
        }
    }
}

fn sell_mode(order_type: Option<&str>, price: Option<&str>) -> Check<SellMode> {
    let price = price
        .map(|value| {
            let price = value
                .trim()
                .parse::<Decimal>()
                .map_err(|_| "SELL_PRICE_invalid")?;
            if price <= Decimal::ZERO || price >= Decimal::ONE {
                return Err("SELL_PRICE_invalid");
            }
            Ok(price)
        })
        .transpose()?;
    match order_type.unwrap_or("FAK") {
        "FAK" => Ok(SellMode::Fak { min_price: price }),
        "GTC" => Ok(SellMode::Gtc {
            price: price.ok_or("SELL_PRICE_required_for_GTC")?,
        }),
        _ => Err("SELL_ORDER_TYPE_must_be_FAK_or_GTC"),
    }
}

fn check_fak_price(price: Decimal, min_price: Option<Decimal>) -> Check<()> {
    if min_price.is_some_and(|minimum| price < minimum) {
        return Err("marginal_price_below_SELL_PRICE");
    }
    Ok(())
}

#[derive(sqlx::FromRow)]
struct AccountRow {
    funder_address: Option<String>,
    wallet_address: Option<String>,
    service: Option<String>,
}

#[derive(Debug, PartialEq)]
struct Account {
    address: String,
    db_services: BTreeSet<String>,
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|s| !s.is_empty())
}

fn accounts(rows: Vec<AccountRow>) -> (Vec<Account>, usize) {
    let mut accounts: Vec<Account> = Vec::new();
    let mut indexes = HashMap::new();
    let mut missing = 0;
    // 输入由 SQL 按 leg id 排序，保留首次出现的账号顺序。
    for row in rows {
        let Some(address) = nonempty(row.funder_address.as_deref())
            .or_else(|| nonempty(row.wallet_address.as_deref()))
        else {
            missing += 1;
            continue;
        };
        let key = address.to_ascii_lowercase();
        let index = *indexes.entry(key).or_insert_with(|| {
            accounts.push(Account {
                address: address.to_string(),
                db_services: BTreeSet::new(),
            });
            accounts.len() - 1
        });
        if let Some(service) = nonempty(row.service.as_deref()) {
            accounts[index].db_services.insert(service.to_string());
        }
    }
    (accounts, missing)
}

fn mapped_funder<'a>(
    address: &str,
    funders: &'a [PolymarketFunderConfig],
) -> Check<&'a PolymarketFunderConfig> {
    let mut matching = funders
        .iter()
        .filter(|f| f.funder_address.eq_ignore_ascii_case(address));
    let first = matching.next().ok_or("config_account_missing")?;
    if matching.next().is_some() {
        return Err("config_account_ambiguous");
    }
    Ok(first)
}

fn live_token(token: Option<&str>, confirm: Option<&str>) -> Check<String> {
    if confirm != Some("YES") {
        return Err("SELL_LIVE_CONFIRM_must_be_YES");
    }
    let token = nonempty(token).ok_or("SELL_TOKEN_ID_required")?;
    if !token.bytes().all(|b| b.is_ascii_digit())
        || token.parse::<alloy_primitives::U256>().is_err()
    {
        return Err("SELL_TOKEN_ID_invalid");
    }
    Ok(token.to_string())
}

fn sellable(balance: Decimal) -> Check<Decimal> {
    if balance < Decimal::ZERO {
        return Err("invalid_balance");
    }
    let shares = balance.trunc_with_scale(2);
    if shares == Decimal::ZERO {
        return Err("zero_or_dust_balance");
    }
    Ok(shares)
}

fn valid_price(price: Decimal, tick: Decimal) -> bool {
    tick > Decimal::ZERO
        && tick <= Decimal::new(5, 1)
        && price >= tick
        && price <= Decimal::ONE - tick
        && price.checked_rem(tick) == Some(Decimal::ZERO)
}

fn marginal_price(bids: &[Level], shares: Decimal, tick: Decimal) -> Check<(Decimal, Decimal)> {
    if shares <= Decimal::ZERO {
        return Err("invalid_shares");
    }
    if bids.is_empty() {
        return Err("empty_bids");
    }
    let mut seen = BTreeSet::new();
    for level in bids {
        if !valid_price(level.price, tick)
            || level.size <= Decimal::ZERO
            || !seen.insert(level.price)
        {
            return Err("invalid_bid_or_tick");
        }
    }
    let mut sorted: Vec<_> = bids.iter().collect();
    sorted.sort_by(|a, b| b.price.cmp(&a.price));
    let mut covered = Decimal::ZERO;
    for level in sorted {
        covered = covered.checked_add(level.size).ok_or("depth_overflow")?;
        if covered >= shares {
            // 无额外价格下限或滑点；取最后一档，而非均价或最优买价。
            return Ok((level.price, covered));
        }
    }
    Err("insufficient_depth")
}

fn sell_request(
    token: &str,
    funder: &str,
    shares: Decimal,
    price: Decimal,
    tick: Decimal,
) -> MarketOrderRequest {
    MarketOrderRequest {
        token_id: token.into(),
        shares,
        cap_price: price,
        side: OrderSide::Sell,
        neg_risk: None,
        tick_size: Some(tick),
        asset_id: None,
        funder_address: Some(funder.into()),
    }
}

fn signed_amounts(
    req: &MarketOrderRequest,
    prepared: &PreparedOrder,
    mode: SellMode,
) -> Check<(Decimal, Decimal)> {
    let payload = &prepared.payload;
    if req.side != OrderSide::Sell
        || payload["orderType"] != mode.order_type()
        || matches!(mode, SellMode::Gtc { price } if price != req.cap_price)
        || payload["order"]["side"] != "SELL"
        || payload["order"]["tokenId"].as_str() != Some(req.token_id.as_str())
        || prepared.funder.as_deref() != req.funder_address.as_deref()
        || payload["order"]["maker"]
            .as_str()
            .zip(req.funder_address.as_deref())
            .is_none_or(|(a, b)| !a.eq_ignore_ascii_case(b))
        || !req
            .tick_size
            .is_some_and(|tick| valid_price(req.cap_price, tick))
        || req.shares <= Decimal::ZERO
        || req.shares.trunc_with_scale(2) != req.shares
    {
        return Err("invalid_sell_request");
    }
    let amount = |field: &str| -> Check<Decimal> {
        let raw = payload["order"][field]
            .as_str()
            .ok_or("invalid_signed_amount")?;
        if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
            return Err("invalid_signed_amount");
        }
        raw.parse::<Decimal>()
            .ok()
            .filter(|n| *n > Decimal::ZERO)
            .ok_or("invalid_signed_amount")
    };
    let maker = amount("makerAmount")?;
    let taker = amount("takerAmount")?;
    let scale = Decimal::from(1_000_000);
    let expected_maker = req.shares.checked_mul(scale).ok_or("amount_overflow")?;
    // 核验生产 SELL 精度，不另造签名算法或改写已签名的 payload。
    let proceeds = req
        .shares
        .checked_mul(req.cap_price)
        .ok_or("amount_overflow")?;
    let proceeds = match mode {
        SellMode::Fak { .. } => proceeds.trunc_with_scale(5),
        SellMode::Gtc { .. } => proceeds,
    };
    let expected_taker = proceeds.checked_mul(scale).ok_or("amount_overflow")?;
    if maker != expected_maker || taker != expected_taker {
        return Err("signed_amount_mismatch");
    }
    Ok((maker, taker))
}

fn safe_id(id: &str) -> Option<&str> {
    (!id.is_empty()
        && id.len() <= 256
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
    .then_some(id)
}

fn response_log(result: &SubmitResult, response: &SubmissionResponse) -> Value {
    // 只使用类型化结果的业务白名单；永不透传 body、message、envelope 或认证字段。
    let mut log = match result {
        SubmitResult::Ack {
            order_id,
            order_hash,
            making,
            taking,
            avg_px,
            ..
        } => json!({
            "classification":"ack", "order_id":safe_id(order_id), "order_hash":safe_id(order_hash),
            "making":making, "taking":taking, "avg_px":avg_px,
            "meaning":"accepted_not_proof_of_full_fill"
        }),
        SubmitResult::NoMatch { order_hash, .. } => {
            json!({"classification":"no_match", "order_hash":safe_id(order_hash)})
        }
        SubmitResult::Unknown {
            order_id,
            order_hash,
            ..
        } => json!({
            "classification":"unknown", "order_id":order_id.as_deref().and_then(safe_id),
            "order_hash":safe_id(order_hash), "meaning":"uncertain_do_not_resubmit"
        }),
        SubmitResult::Failed {
            order_hash, status, ..
        } => json!({
            "classification":"failed", "order_hash":safe_id(order_hash), "http_status":status
        }),
    };
    log["response_kind"] = json!(match response {
        SubmissionResponse::Http(_) => "http",
        SubmissionResponse::NoResponse(_) => "no_response",
    });
    let body = match response {
        SubmissionResponse::Http(value) => value.get("body").unwrap_or(value),
        SubmissionResponse::NoResponse(_) => &Value::Null,
    };
    log["status"] = json!(body
        .get("status")
        .and_then(Value::as_str)
        .filter(|status| matches!(
            *status,
            "live" | "matched" | "delayed" | "unmatched" | "canceled" | "cancelled"
        )));
    for field in ["makingAmount", "takingAmount"] {
        log[field] = json!(body.get(field).and_then(|value| {
            let text = match value {
                Value::String(text) => text.clone(),
                Value::Number(number) => number.to_string(),
                _ => return None,
            };
            text.parse::<Decimal>()
                .ok()
                .filter(|amount| *amount >= Decimal::ZERO)
        }));
    }
    // status 是交易所业务状态；成功响应不提供 HTTP status，不能猜测为 200。
    log
}

fn compare_making_amount(log: &mut Value, shares: Decimal, mode: SellMode) {
    log["order_shares"] = json!(shares);
    log["order_type"] = json!(mode.order_type());
    let required = matches!(mode, SellMode::Fak { .. }) && log["status"] == "matched";
    log["amount_comparison_required"] = json!(required);
    // SELL 的 makingAmount 是卖出股数；仅核对已 matched 的 FAK 响应。
    let making = required
        .then(|| {
            log["makingAmount"]
                .as_str()
                .and_then(|value| value.parse::<Decimal>().ok())
        })
        .flatten();
    log["making_amount_matches_order_shares"] = json!(making.map(|amount| amount == shares));
}

fn emit(context: &Value, event: &str, details: Value, started: Instant) {
    let log = json!({"platform":POLYMARKET, "event":event, "context":context,
        "elapsed_ms":started.elapsed().as_millis(), "details":details});
    if matches!(event, "response" | "summary") {
        println!("{log}");
    } else {
        tracing::debug!(target: "sell_token_positions", event, details = %log);
    }
}

async fn sell_account(
    cfg: &Config,
    funder: &PolymarketFunderConfig,
    token: &str,
    mode: SellMode,
    context: &Value,
) -> Check<(&'static str, Value)> {
    let started = Instant::now();
    // connect 会认证首个账号；缩小到唯一匹配账号，禁止默认/轮转账号参与。
    let mut account_cfg = cfg.clone();
    account_cfg.polymarket_funders = vec![funder.clone()];
    let venue = PolymarketVenue::connect(&account_cfg, Arc::new(MinuteStats::new()))
        .await
        .map_err(|_| "account_connect_failed")?;
    let balance = venue
        .token_balance(&funder.funder_address, token)
        .await
        .map_err(|_| "token_balance_failed")?;
    emit(context, "balance", json!({"balance":balance}), started);
    let shares = sellable(balance)?;
    let (price, tick, pricing) = match mode {
        SellMode::Fak { min_price } => {
            // 必须在当前账号余额之后获取新快照，不跨账号共享盘口。
            let book = venue
                .rest_book(token)
                .await
                .map_err(|_| "rest_book_failed")?;
            let tick = match book.tick_size {
                Some(tick) => tick,
                None => venue
                    .fetch_tick_size(token)
                    .await
                    .map_err(|_| "fetch_tick_size_failed")?,
            };
            let (price, covered) = marginal_price(&book.bids, shares, tick)?;
            emit(
                context,
                "price_check",
                json!({"order_type":"FAK",
                "marginal_price":price, "min_price":min_price,
                "accepted":check_fak_price(price, min_price).is_ok()}),
                started,
            );
            // 比较覆盖全部可卖数量的最后一档，不以最优买价或均价替代。
            check_fak_price(price, min_price)?;
            (
                price,
                tick,
                json!({"marginal_price":price, "covered_shares":covered,
                "book_exchange_ts_ms":book.exchange_ts_ms,
                "min_price":min_price,
                "warning":"amount_truncation_may_lower_effective_price"}),
            )
        }
        SellMode::Gtc { price } => {
            // GTC 使用用户指定价格，仅获取 tick 元数据，不查询盘口。
            let tick = venue
                .fetch_tick_size(token)
                .await
                .map_err(|_| "fetch_tick_size_failed")?;
            if !valid_price(price, tick) {
                return Err("SELL_PRICE_incompatible_with_tick");
            }
            (
                price,
                tick,
                json!({"limit_price":price,
                "warning":"unfilled_order_remains_open_until_filled_or_canceled"}),
            )
        }
    };
    let req = sell_request(token, &funder.funder_address, shares, price, tick);
    let prepared = match mode {
        SellMode::Fak { .. } => {
            venue
                .prepare_market_order(&funder.funder_address, &req)
                .await
        }
        SellMode::Gtc { .. } => {
            venue
                .prepare_limit_order(&funder.funder_address, &req)
                .await
        }
    }
    .map_err(|_| "prepare_failed")?;
    let (maker, taker) = signed_amounts(&req, &prepared, mode)?;
    emit(
        context,
        "request",
        json!({
            "endpoint":"POST /order", "side":"SELL", "order_type":mode.order_type(), "balance":balance,
            "shares":shares, "price":price, "pricing":pricing, "tick_size":tick,
            "order_hash":safe_id(&prepared.order_hash), "maker_amount_base_units":maker,
            "taker_amount_base_units":taker, "effective_price":taker / maker
        }),
        started,
    );
    let submit_started = Instant::now();
    // 全文件唯一提交调用点。错误和 Unknown 均不重试，不轮询订单/成交。
    match venue.post_prepared(&prepared).await {
        Ok((result, response)) => {
            let mut log = response_log(&result, &response);
            compare_making_amount(&mut log, shares, mode);
            let classification = match result {
                SubmitResult::Ack { .. } => "ack",
                SubmitResult::NoMatch { .. } => "no_match",
                SubmitResult::Unknown { .. } => "unknown",
                SubmitResult::Failed { .. } => "failed",
            };
            emit(context, "response", log.clone(), submit_started);
            Ok((classification, log))
        }
        Err(_) => {
            let mut log = json!({"classification":"unknown", "reason":"post_prepared_error",
                "order_hash":safe_id(&prepared.order_hash), "meaning":"uncertain_do_not_resubmit",
                "status":null, "makingAmount":null, "takingAmount":null});
            compare_making_amount(&mut log, shares, mode);
            emit(context, "response", log.clone(), submit_started);
            Ok(("unknown", log))
        }
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "真实卖出：须设置 SELL_TOKEN_ID 和 SELL_LIVE_CONFIRM=YES；GTC 须设置 SELL_PRICE"]
async fn sell_token_positions_live() -> Check<()> {
    let token_env = std::env::var("SELL_TOKEN_ID").ok();
    let confirm_env = std::env::var("SELL_LIVE_CONFIRM").ok();
    let token = live_token(token_env.as_deref(), confirm_env.as_deref())?;
    let order_type_env = std::env::var("SELL_ORDER_TYPE").ok();
    let price_env = std::env::var("SELL_PRICE").ok();
    let mode = sell_mode(order_type_env.as_deref(), price_env.as_deref())?;
    // 配置加载（含既有 dotenv/认证流程）仅允许出现在此 ignored 实盘入口。
    let cfg = Config::from_env().map_err(|_| "config_load_failed")?;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("off")),
        )
        .with_writer(std::io::stderr)
        .try_init();
    let store = Store::connect(&cfg.app_postgres_uri)
        .await
        .map_err(|_| "database_connect_failed")?;
    let rows = sqlx::query_as::<_, AccountRow>(
        "SELECT funder_address, wallet_address, service FROM legs \
         WHERE platform = $1 AND token_id = $2 \
         AND (submitted_at IS NOT NULL OR third_order_id IS NOT NULL) ORDER BY id ASC",
    )
    .bind(POLYMARKET)
    .bind(&token)
    .fetch_all(&store.pool)
    .await
    .map_err(|_| "account_select_failed")?;
    let (accounts, missing_address_rows) = accounts(rows);
    let started = Instant::now();
    let root = json!({"token_id":token, "order_type":mode.order_type()});
    emit(
        &root,
        "start",
        json!({"accounts":accounts.len(), "missing_address_rows":missing_address_rows,
        "order_type":mode.order_type(),
        "warning":"no_fill_tracking_or_accounting_updates; no_automatic_cancellation"}),
        started,
    );
    let mut counts = std::collections::BTreeMap::<&str, usize>::new();
    let mut amount_mismatches = Vec::new();
    let mut amount_unavailable = Vec::new();
    let mut skipped = Vec::new();
    for (index, account) in accounts.iter().enumerate() {
        let account_started = Instant::now();
        let mut context = json!({"index":index + 1, "account":account.address, "token_id":token,
            "db_services":account.db_services, "config_service":Value::Null});
        let funder = match mapped_funder(&account.address, &cfg.polymarket_funders) {
            Ok(funder) => funder,
            Err(reason) => {
                emit(&context, "skip", json!({"reason":reason}), account_started);
                *counts.entry("skipped").or_default() += 1;
                skipped.push(json!({"account":account.address, "reason":reason}));
                continue;
            }
        };
        context["config_service"] = json!(funder.service);
        emit(
            &context,
            "account",
            json!({"routing":"local_unique_funder; services_are_labels_only"}),
            account_started,
        );
        match sell_account(&cfg, funder, &token, mode, &context).await {
            Ok((classification, log)) => {
                *counts.entry(classification).or_default() += 1;
                let entry = json!({"account":account.address, "response":log});
                match log["making_amount_matches_order_shares"].as_bool() {
                    Some(false) => amount_mismatches.push(entry),
                    None if log["amount_comparison_required"] == true => {
                        amount_unavailable.push(entry)
                    }
                    _ => {}
                }
            }
            Err(reason) => {
                emit(&context, "skip", json!({"reason":reason}), account_started);
                *counts.entry("skipped").or_default() += 1;
                skipped.push(json!({"account":account.address, "reason":reason}));
            }
        }
    }
    emit(
        &root,
        "summary",
        json!({"counts":counts, "accounts":accounts.len(),
        "missing_address_rows":missing_address_rows,
        "amount_mismatch_count":amount_mismatches.len(), "amount_mismatches":amount_mismatches,
        "amount_unavailable_count":amount_unavailable.len(), "amount_unavailable":amount_unavailable,
        "comparison_note":"FAK matched only: compare SELL makingAmount with order_shares",
        "skipped":skipped}),
        started,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }
    fn row(funder: Option<&str>, wallet: Option<&str>, service: Option<&str>) -> AccountRow {
        AccountRow {
            funder_address: funder.map(Into::into),
            wallet_address: wallet.map(Into::into),
            service: service.map(Into::into),
        }
    }
    fn funder(address: &str) -> PolymarketFunderConfig {
        PolymarketFunderConfig {
            funder_address: address.into(),
            wallet_private_key: String::new(),
            is_wallet_v2: false,
            service: Some("local".into()),
        }
    }
    fn bids(levels: &[(&str, &str)]) -> Vec<Level> {
        levels
            .iter()
            .map(|(p, s)| Level {
                price: d(p),
                size: d(s),
            })
            .collect()
    }
    fn prepared(req: &MarketOrderRequest) -> PreparedOrder {
        PreparedOrder {
            order_hash: "0x123".into(),
            funder: req.funder_address.clone(),
            envelope: json!({"signature":"SENSITIVE"}),
            payload: json!({
                "orderType":"FAK", "owner":"SENSITIVE", "order":{
                    "side":"SELL", "tokenId":req.token_id, "maker":req.funder_address,
                    "makerAmount":"1230000", "takerAmount":"410000", "signature":"SENSITIVE"
                }
            }),
        }
    }

    #[test]
    fn account_dedup_services_and_mapping() {
        let (list, missing) = accounts(vec![
            row(Some(" 0xAb "), Some("ignored"), Some("one")),
            row(None, Some("0xaB"), Some("two")),
            row(Some(" "), Some("0xCD"), None),
            row(Some("0xAB"), None, Some("one")),
            row(None, None, None),
        ]);
        assert_eq!(missing, 1);
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].address, "0xAb");
        assert_eq!(
            list[0].db_services,
            BTreeSet::from(["one".into(), "two".into()])
        );
        assert_eq!(list[1].address, "0xCD");
        let configs = vec![funder("0xab")];
        assert!(mapped_funder("0xAB", &configs).is_ok());
        assert_eq!(
            mapped_funder("missing", &configs).err(),
            Some("config_account_missing")
        );
        assert_eq!(
            mapped_funder("0xAB", &[funder("0xab"), funder("0xAB")]).err(),
            Some("config_account_ambiguous")
        );
    }

    #[test]
    fn marginal_depth_sorted_exact_and_no_floor() {
        let levels = bids(&[("0.1", "3"), ("0.7", "2"), ("0.5", "4")]);
        assert_eq!(
            marginal_price(&levels, d("6"), d("0.1")),
            Ok((d("0.5"), d("6")))
        );
        assert_eq!(
            marginal_price(&levels, d("6.01"), d("0.1")),
            Ok((d("0.1"), d("9")))
        );
        assert_eq!(
            marginal_price(&levels, d("10"), d("0.1")),
            Err("insufficient_depth")
        );
        assert_eq!(
            marginal_price(&bids(&[("0.0001", "1")]), d("1"), d("0.0001")),
            Ok((d("0.0001"), d("1")))
        );
    }

    #[test]
    fn reject_empty_invalid_books_and_ticks() {
        assert_eq!(marginal_price(&[], d("1"), d("0.01")), Err("empty_bids"));
        for levels in [
            bids(&[("0", "1")]),
            bids(&[("1", "1")]),
            bids(&[("0.5", "0")]),
            bids(&[("0.5", "-1")]),
            bids(&[("0.501", "1")]),
            bids(&[("0.5", "1"), ("0.5", "2")]),
        ] {
            assert_eq!(
                marginal_price(&levels, d("1"), d("0.01")),
                Err("invalid_bid_or_tick")
            );
        }
        for tick in ["0", "-0.1", "1", "0.03"] {
            assert!(!valid_price(d("0.5"), d(tick)));
        }
        assert!(valid_price(d("0.99"), d("0.01")));
        assert!(!valid_price(d("0.999"), d("0.01")));
    }

    #[test]
    fn balances_and_explicit_live_gate() {
        assert_eq!(sellable(d("1.239")), Ok(d("1.23")));
        for balance in ["0", "0.009"] {
            assert_eq!(sellable(d(balance)), Err("zero_or_dust_balance"));
        }
        assert_eq!(sellable(d("-1")), Err("invalid_balance"));
        assert_eq!(live_token(Some("123"), Some("YES")), Ok("123".into()));
        assert!(live_token(Some("123"), None).is_err());
        assert!(live_token(Some("123"), Some("yes")).is_err());
        for token in ["", "-1", "1.2", "secret", &"9".repeat(100)] {
            assert!(live_token(Some(token), Some("YES")).is_err());
        }
    }

    #[test]
    fn sell_fak_signed_amount_validation() {
        let req = sell_request("123", "0xabc", d("1.23"), d("0.33334"), d("0.00001"));
        let good = prepared(&req);
        assert_eq!(req.side, OrderSide::Sell);
        assert_eq!(
            signed_amounts(&req, &good, SellMode::Fak { min_price: None }),
            Ok((d("1230000"), d("410000")))
        );
        assert!(d("410000") / d("1230000") < req.cap_price);
        for (pointer, value) in [
            ("/orderType", json!("GTC")),
            ("/order/side", json!("BUY")),
            ("/order/makerAmount", json!("0")),
            ("/order/takerAmount", json!("0")),
            ("/order/makerAmount", json!("1240000")),
            ("/order/takerAmount", json!("410010")),
            ("/order/tokenId", json!("456")),
            ("/order/maker", json!("other")),
        ] {
            let mut invalid = good.clone();
            *invalid.payload.pointer_mut(pointer).unwrap() = value;
            assert!(signed_amounts(&req, &invalid, SellMode::Fak { min_price: None }).is_err());
        }
        let mut buy = req.clone();
        buy.side = OrderSide::Buy;
        assert!(signed_amounts(&buy, &good, SellMode::Fak { min_price: None }).is_err());
    }

    #[test]
    fn order_mode_requires_explicit_gtc_price() {
        assert_eq!(sell_mode(None, None), Ok(SellMode::Fak { min_price: None }));
        assert_eq!(
            sell_mode(Some("FAK"), None),
            Ok(SellMode::Fak { min_price: None })
        );
        assert_eq!(
            sell_mode(Some("GTC"), Some("0.65")),
            Ok(SellMode::Gtc { price: d("0.65") })
        );
        assert!(sell_mode(Some("GTC"), None).is_err());
        for order_type in [None, Some("FAK")] {
            assert_eq!(
                sell_mode(order_type, Some("0.65")),
                Ok(SellMode::Fak {
                    min_price: Some(d("0.65"))
                })
            );
        }
        assert!(sell_mode(Some("FOK"), None).is_err());
        for price in ["", "abc", "0", "-0.1", "1", "1.01"] {
            assert!(sell_mode(Some("GTC"), Some(price)).is_err());
            assert!(sell_mode(Some("FAK"), Some(price)).is_err());
        }
    }

    #[test]
    fn fak_price_floor_checks_marginal_bid() {
        let (price, _) =
            marginal_price(&bids(&[("0.7", "2"), ("0.5", "4")]), d("6"), d("0.01")).unwrap();
        assert_eq!(check_fak_price(price, None), Ok(()));
        assert_eq!(check_fak_price(price, Some(d("0.49"))), Ok(()));
        assert_eq!(check_fak_price(price, Some(d("0.5"))), Ok(()));
        assert_eq!(
            check_fak_price(price, Some(d("0.6"))),
            Err("marginal_price_below_SELL_PRICE")
        );
    }

    #[test]
    fn sell_gtc_signed_amount_validation_preserves_limit_price() {
        let req = sell_request("123", "0xabc", d("1.23"), d("0.3333"), d("0.0001"));
        let mode = SellMode::Gtc {
            price: req.cap_price,
        };
        let mut good = prepared(&req);
        good.payload["orderType"] = json!("GTC");
        good.payload["order"]["takerAmount"] = json!("409959");
        assert_eq!(
            signed_amounts(&req, &good, mode),
            Ok((d("1230000"), d("409959")))
        );
        assert!(signed_amounts(&req, &good, SellMode::Fak { min_price: None }).is_err());
        assert!(signed_amounts(&req, &good, SellMode::Gtc { price: d("0.4") }).is_err());
        // FAK 的五位截断会降低限价，GTC 必须拒绝这种金额。
        good.payload["order"]["takerAmount"] = json!("409950");
        assert_eq!(
            signed_amounts(&req, &good, mode),
            Err("signed_amount_mismatch")
        );
    }

    #[test]
    fn response_status_and_amount_comparison() {
        let result = SubmitResult::NoMatch {
            order_hash: "hash1".into(),
            envelope: json!({}),
            message: String::new(),
        };
        let fak = SellMode::Fak { min_price: None };
        let gtc = SellMode::Gtc { price: d("0.92") };
        for raw in [
            json!({"status":"matched", "makingAmount":"30", "takingAmount":"27.6"}),
            json!({"http_status":400,"body":{"status":"matched","makingAmount":30,"takingAmount":27.6}}),
        ] {
            let mut log = response_log(&result, &SubmissionResponse::Http(raw));
            assert_eq!(log["status"], "matched");
            assert_eq!(log["takingAmount"], "27.6");
            assert_eq!(log["makingAmount"], "30");
            compare_making_amount(&mut log, d("30"), fak);
            assert_eq!(log["amount_comparison_required"], true);
            assert_eq!(log["making_amount_matches_order_shares"], true);
            compare_making_amount(&mut log, d("31"), fak);
            assert_eq!(log["making_amount_matches_order_shares"], false);
            compare_making_amount(&mut log, d("31"), gtc);
            assert_eq!(log["amount_comparison_required"], false);
            assert!(log["making_amount_matches_order_shares"].is_null());
        }
        for status in [
            "live",
            "delayed",
            "unmatched",
            "canceled",
            "cancelled",
            "SENSITIVE",
        ] {
            let mut log = response_log(
                &result,
                &SubmissionResponse::Http(json!({"status":status,"makingAmount":"20"})),
            );
            compare_making_amount(&mut log, d("30"), fak);
            assert_eq!(log["amount_comparison_required"], false);
            assert!(log["making_amount_matches_order_shares"].is_null());
            assert!(!log.to_string().contains("SENSITIVE"));
        }
        for amount in [Value::Null, json!("SENSITIVE"), json!("-1")] {
            let mut log = response_log(
                &result,
                &SubmissionResponse::Http(json!({"status":"matched","makingAmount":amount})),
            );
            compare_making_amount(&mut log, d("30"), fak);
            assert_eq!(log["amount_comparison_required"], true);
            assert!(log["makingAmount"].is_null());
            assert!(log["making_amount_matches_order_shares"].is_null());
            assert!(!log.to_string().contains("SENSITIVE"));
        }
        let mut log = response_log(
            &result,
            &SubmissionResponse::Http(json!({"status":"matched","makingAmount":"0"})),
        );
        compare_making_amount(&mut log, d("30"), fak);
        assert_eq!(log["making_amount_matches_order_shares"], false);
        let mut log = response_log(&result, &SubmissionResponse::NoResponse(json!({})));
        compare_making_amount(&mut log, d("30"), fak);
        assert_eq!(log["amount_comparison_required"], false);
        assert!(log["making_amount_matches_order_shares"].is_null());
    }

    #[test]
    fn response_whitelist_removes_sensitive_fields() {
        let secret = "SENSITIVE";
        let raw = json!({"signature":secret, "owner":secret, "api_key":secret, "error":secret});
        let results = [
            SubmitResult::Ack {
                order_id: "id1".into(),
                order_hash: "hash1".into(),
                envelope: raw.clone(),
                making: Some(d("1")),
                taking: Some(d("0.5")),
                avg_px: Some(d("0.5")),
            },
            SubmitResult::NoMatch {
                order_hash: "hash1".into(),
                envelope: raw.clone(),
                message: secret.into(),
            },
            SubmitResult::Unknown {
                order_id: None,
                order_hash: "hash1".into(),
                envelope: raw.clone(),
                message: secret.into(),
            },
            SubmitResult::Failed {
                order_hash: "hash1".into(),
                envelope: raw.clone(),
                message: secret.into(),
                status: 400,
            },
        ];
        for result in results {
            for response in [
                SubmissionResponse::Http(raw.clone()),
                SubmissionResponse::NoResponse(raw.clone()),
            ] {
                let log = response_log(&result, &response);
                let text = log.to_string();
                for forbidden in [
                    secret,
                    "signature",
                    "owner",
                    "api_key",
                    "envelope",
                    "message",
                ] {
                    assert!(!text.contains(forbidden));
                }
                if !matches!(result, SubmitResult::Failed { .. }) {
                    assert!(log.get("http_status").is_none());
                }
            }
        }
    }
}
