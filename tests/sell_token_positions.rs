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

fn signed_amounts(req: &MarketOrderRequest, prepared: &PreparedOrder) -> Check<(Decimal, Decimal)> {
    let payload = &prepared.payload;
    if req.side != OrderSide::Sell
        || payload["orderType"] != "FAK"
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
        return Err("invalid_sell_fak_request");
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
    let expected_taker = req
        .shares
        .checked_mul(req.cap_price)
        .ok_or("amount_overflow")?
        .trunc_with_scale(5)
        .checked_mul(scale)
        .ok_or("amount_overflow")?;
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
    // 成功响应 API 不提供 HTTP status，不能猜测为 200。
    log
}

fn emit(context: &Value, event: &str, details: Value, started: Instant) {
    println!(
        "{}",
        json!({"platform":POLYMARKET, "event":event, "context":context,
        "elapsed_ms":started.elapsed().as_millis(), "details":details})
    );
}

async fn sell_account(
    cfg: &Config,
    funder: &PolymarketFunderConfig,
    token: &str,
    context: &Value,
) -> Check<&'static str> {
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
    let req = sell_request(token, &funder.funder_address, shares, price, tick);
    let prepared = venue
        .prepare_market_order(&funder.funder_address, &req)
        .await
        .map_err(|_| "prepare_failed")?;
    let (maker, taker) = signed_amounts(&req, &prepared)?;
    emit(
        context,
        "request",
        json!({
            "endpoint":"POST /order", "side":"SELL", "order_type":"FAK", "balance":balance,
            "shares":shares, "marginal_price":price, "covered_shares":covered,
            "tick_size":tick, "book_exchange_ts_ms":book.exchange_ts_ms,
            "order_hash":safe_id(&prepared.order_hash), "maker_amount_base_units":maker,
            "taker_amount_base_units":taker, "effective_price":taker / maker,
            "warning":"no_extra_price_floor; amount_truncation_may_lower_effective_price"
        }),
        started,
    );
    let submit_started = Instant::now();
    // 全文件唯一提交调用点。错误和 Unknown 均不重试，不轮询订单/成交。
    match venue.post_prepared(&prepared).await {
        Ok((result, response)) => {
            let log = response_log(&result, &response);
            let classification = match result {
                SubmitResult::Ack { .. } => "ack",
                SubmitResult::NoMatch { .. } => "no_match",
                SubmitResult::Unknown { .. } => "unknown",
                SubmitResult::Failed { .. } => "failed",
            };
            emit(context, "response", log, submit_started);
            Ok(classification)
        }
        Err(_) => {
            emit(
                context,
                "response",
                json!({"classification":"unknown", "reason":"post_prepared_error",
                "order_hash":safe_id(&prepared.order_hash), "meaning":"uncertain_do_not_resubmit"}),
                submit_started,
            );
            Ok("unknown")
        }
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "真实卖出：须设置 SELL_TOKEN_ID 和 SELL_LIVE_CONFIRM=YES；无额外价格下限"]
async fn sell_token_positions_live() -> Check<()> {
    let token_env = std::env::var("SELL_TOKEN_ID").ok();
    let confirm_env = std::env::var("SELL_LIVE_CONFIRM").ok();
    let token = live_token(token_env.as_deref(), confirm_env.as_deref())?;
    // 配置加载（含既有 dotenv/认证流程）仅允许出现在此 ignored 实盘入口。
    let cfg = Config::from_env().map_err(|_| "config_load_failed")?;
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
    let root = json!({"token_id":token});
    emit(
        &root,
        "start",
        json!({"accounts":accounts.len(), "missing_address_rows":missing_address_rows,
        "warning":"automatic_price_without_extra_floor; no_fill_tracking_or_accounting_updates"}),
        started,
    );
    let mut counts = std::collections::BTreeMap::<&str, usize>::new();
    for (index, account) in accounts.iter().enumerate() {
        let account_started = Instant::now();
        let mut context = json!({"index":index + 1, "account":account.address, "token_id":token,
            "db_services":account.db_services, "config_service":Value::Null});
        let funder = match mapped_funder(&account.address, &cfg.polymarket_funders) {
            Ok(funder) => funder,
            Err(reason) => {
                emit(&context, "skip", json!({"reason":reason}), account_started);
                *counts.entry("skipped").or_default() += 1;
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
        match sell_account(&cfg, funder, &token, &context).await {
            Ok(classification) => *counts.entry(classification).or_default() += 1,
            Err(reason) => {
                emit(&context, "skip", json!({"reason":reason}), account_started);
                *counts.entry("skipped").or_default() += 1;
            }
        }
    }
    emit(
        &root,
        "summary",
        json!({"counts":counts, "accounts":accounts.len(),
        "missing_address_rows":missing_address_rows}),
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
        assert_eq!(signed_amounts(&req, &good), Ok((d("1230000"), d("410000"))));
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
            assert!(signed_amounts(&req, &invalid).is_err());
        }
        let mut buy = req.clone();
        buy.side = OrderSide::Buy;
        assert!(signed_amounts(&buy, &good).is_err());
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
