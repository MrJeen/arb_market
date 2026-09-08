use super::{
    parse_decimal, require_positive, FillFinality, FillPage, MarketOrderRequest, OrderPoll,
    OrderSide, PreparedOrder, SubmitResult, TradeFill,
};
use crate::book::{BookStore, Level};
use crate::config::{Config, OUTCOME};
use crate::domain::{parse_side_coin, side_asset_id, TopicKey};
use crate::error::{Error, Result};
use crate::signing::hyperliquid::{action_hash, order_action, sign_l1_action};
use alloy_signer_local::PrivateKeySigner;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;

const USDC_BALANCE_CACHE_TTL: Duration = Duration::from_secs(10);
const FILL_PAGE_SIZE: usize = 2_000;
const FILL_HISTORY_LIMIT: usize = 10_000;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FillProgress {
    version: u8,
    token_id: String,
    submitted_at_ms: u64,
    end_time: u64,
    cursor: u64,
    // startTime 为包含边界；只保留 cursor 毫秒已见过的账户成交 ID。
    seen_ids: BTreeSet<String>,
    history_checked: bool,
    history_lower_bound: Option<u64>,
    history_complete: bool,
    scanned_count: usize,
    complete: bool,
}

impl FillProgress {
    fn load(token_id: &str, submitted_at_ms: i64, value: &Value) -> Result<Self> {
        let start = u64::try_from(submitted_at_ms)
            .map_err(|_| Error::msg("outcome fill start time must be nonnegative"))?;
        if value.is_null() || value.as_object().is_some_and(|v| v.is_empty()) {
            return Ok(Self {
                version: 1,
                token_id: token_id.to_string(),
                submitted_at_ms: start,
                end_time: unix_millis().max(start),
                cursor: start,
                seen_ids: BTreeSet::new(),
                history_checked: false,
                history_lower_bound: None,
                history_complete: false,
                scanned_count: 0,
                complete: false,
            });
        }
        let mut state: Self = serde_json::from_value(value.clone())?;
        if state.version != 1
            || state.token_id != token_id
            || state.submitted_at_ms != start
            || state.cursor < start
            || state.cursor > state.end_time
            || state.seen_ids.len() > FILL_HISTORY_LIMIT
        {
            return Err(Error::msg("invalid outcome fill progress"));
        }
        if state.complete {
            // 只有完整扫描后才开启下一轮；保留历史下界，但重新探测当前保留范围。
            state.end_time = unix_millis().max(state.end_time).max(start);
            state.cursor = start;
            state.seen_ids.clear();
            state.history_checked = false;
            state.scanned_count = 0;
            state.complete = false;
        }
        Ok(state)
    }
}

#[derive(Clone)]
pub struct OutcomeVenue {
    http: reqwest::Client,
    info_url: String,
    exchange_url: String,
    mainnet: bool,
    signer: Option<PrivateKeySigner>,
    account: Option<String>,
    builder: Option<(String, u32)>,
    nonce: Arc<StdMutex<u64>>,
    usdc_balance_cache: Arc<Mutex<UsdcBalanceCache>>,
    usdc_balance_refresh: Arc<Mutex<()>>,
}

#[derive(Default)]
struct UsdcBalanceCache {
    value: Option<(Decimal, Instant)>,
    generation: u64,
}

impl UsdcBalanceCache {
    fn get_fresh(&self, now: Instant) -> Option<Decimal> {
        self.value.and_then(|(balance, fetched_at)| {
            (now.saturating_duration_since(fetched_at) < USDC_BALANCE_CACHE_TTL).then_some(balance)
        })
    }

    fn invalidate(&mut self) {
        self.value = None;
        self.generation = self.generation.wrapping_add(1);
    }
}

impl OutcomeVenue {
    pub fn connect(cfg: &Config) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let signer = match &cfg.outcome_agent_private_key {
            Some(key) if !key.is_empty() => Some(
                key.parse()
                    .map_err(|e| Error::msg(format!("invalid outcome agent key: {e}")))?,
            ),
            _ => None,
        };
        Ok(Self {
            http,
            info_url: cfg.hyperliquid_info_url.clone(),
            exchange_url: cfg.hyperliquid_exchange_url.clone(),
            mainnet: cfg.hyperliquid_mainnet,
            signer,
            account: cfg.outcome_account_address.clone(),
            builder: cfg
                .outcome_builder_address
                .clone()
                .map(|addr| (addr, cfg.outcome_builder_fee)),
            nonce: Arc::new(StdMutex::new(0)),
            usdc_balance_cache: Arc::new(Mutex::new(UsdcBalanceCache::default())),
            usdc_balance_refresh: Arc::new(Mutex::new(())),
        })
    }

    pub fn account_address(&self) -> Option<&str> {
        self.account.as_deref()
    }

    fn next_nonce(&self) -> u64 {
        let now = unix_millis();
        let mut last = self.nonce.lock().unwrap_or_else(|e| e.into_inner());
        let next = now.max(last.saturating_add(1));
        *last = next;
        next
    }

    pub async fn settlement(
        &self,
        market_id: &str,
    ) -> Result<crate::settlement::OutcomeSettlement> {
        let outcome_id = parse_settlement_market_id(market_id)?;
        let started = Instant::now();
        let value: Value = self
            .http
            .post(&self.info_url)
            .json(&json!({"type": "settledOutcome", "outcome": outcome_id}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let status = crate::settlement::parse_outcome_settlement(outcome_id, &value)?;
        tracing::info!(
            service = "outcome",
            outcome_id,
            settlement_state = status.kind(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "settlement queried"
        );
        Ok(status)
    }

    pub async fn rest_book(&self, coin: &str) -> Result<(Vec<Level>, Vec<Level>, i64)> {
        let body = json!({"type": "l2Book", "coin": coin});
        let value: Value = self
            .http
            .post(&self.info_url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(parse_l2_book(&value))
    }

    pub async fn user_state(&self) -> Result<Decimal> {
        self.cached_usdc_balance(|| self.fetch_usdc_balance()).await
    }

    async fn cached_usdc_balance<F, Fut>(&self, mut fetch: F) -> Result<Decimal>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<Decimal>>,
    {
        if let Some(balance) = self
            .usdc_balance_cache
            .lock()
            .await
            .get_fresh(Instant::now())
        {
            return Ok(balance);
        }

        // Singleflight：缓存过期时仅一个调用刷新，等待者在拿到锁后复用新值。
        let _refresh = self.usdc_balance_refresh.lock().await;
        loop {
            let generation = {
                let cache = self.usdc_balance_cache.lock().await;
                if let Some(balance) = cache.get_fresh(Instant::now()) {
                    return Ok(balance);
                }
                cache.generation
            };
            let balance = fetch().await?;
            let mut cache = self.usdc_balance_cache.lock().await;
            if cache.generation != generation {
                // 请求期间发生过下单，不能把可能已过时的余额重新写入缓存。
                continue;
            }
            cache.value = Some((balance, Instant::now()));
            tracing::debug!(
                ttl_secs = USDC_BALANCE_CACHE_TTL.as_secs(),
                "outcome usdc balance cached"
            );
            return Ok(balance);
        }
    }

    async fn fetch_usdc_balance(&self) -> Result<Decimal> {
        let user = self
            .account
            .clone()
            .ok_or_else(|| Error::msg("missing OUTCOME_ACCOUNT_ADDRESS"))?;
        let value: Value = self
            .http
            .post(&self.info_url)
            .json(&json!({"type": "spotClearinghouseState", "user": user}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(parse_usdc_balance(&value))
    }

    async fn invalidate_usdc_balance(&self) {
        self.usdc_balance_cache.lock().await.invalidate();
        tracing::debug!("outcome usdc balance cache invalidated");
    }

    pub fn prepare_market_order(&self, req: &MarketOrderRequest) -> Result<PreparedOrder> {
        require_positive(req.shares, req.cap_price)?;
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| Error::msg("missing OUTCOME_AGENT_PRIVATE_KEY"))?;
        let asset = req
            .asset_id
            .ok_or_else(|| Error::msg("missing outcome assetId"))?;
        let shares = crate::calc::floor_shares(req.shares);
        let price = crate::calc::align_outcome_price(req.cap_price);
        let cloid = random_cloid();
        let is_buy = req.side == OrderSide::Buy;
        let builder = self
            .builder
            .as_ref()
            .map(|(addr, fee)| (addr.as_str(), *fee));
        let action = order_action(
            asset,
            is_buy,
            &price.to_string(),
            &shares.trunc().to_string(),
            Some(&cloid),
            builder,
        );
        let nonce = self.next_nonce();
        let (r, s, v) = sign_l1_action(signer, &action, nonce, self.mainnet).map_err(Error::msg)?;
        let hash = format!(
            "{:#x}",
            action_hash(&action, None, nonce, None).map_err(Error::msg)?
        );
        let envelope = json!({
            "order_hash": hash,
            "cloid": cloid,
            "nonce": nonce,
            "action": action,
            "asset": asset,
            "token_id": req.token_id,
            "side": req.side.as_str(),
            "shares": shares.to_string(),
            "price": price.to_string()
        });
        let payload = json!({
            "action": action,
            "nonce": nonce,
            "signature": {"r": r, "s": s, "v": v}
        });
        Ok(PreparedOrder {
            order_hash: hash,
            envelope,
            payload,
            funder: None,
        })
    }

    pub async fn post_prepared(&self, prepared: PreparedOrder) -> Result<(SubmitResult, Value)> {
        let hash = prepared.order_hash.clone();
        let envelope = prepared.envelope.clone();
        let cloid = envelope
            .get("cloid")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let response = self
            .http
            .post(&self.exchange_url)
            .json(&prepared.payload)
            .send()
            .await;
        // 即使提交结果不明确，也不能继续复用提交前的余额。
        self.invalidate_usdc_balance().await;
        match response {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body: Value = resp.json().await.unwrap_or(json!({}));
                let stored = if status >= 400 {
                    json!({ "http_status": status, "body": body })
                } else {
                    body.clone()
                };
                Ok((
                    classify_http_submit(status, &body, hash, envelope, &cloid),
                    stored,
                ))
            }
            Err(err) => {
                let response = json!({ "error": err.to_string() });
                Ok((
                    classify_submit_transport_error(err, hash, envelope),
                    response,
                ))
            }
        }
    }

    pub async fn market_order(&self, req: &MarketOrderRequest) -> Result<SubmitResult> {
        let prepared = self.prepare_market_order(req)?;
        Ok(self.post_prepared(prepared).await?.0)
    }

    pub async fn poll_order(&self, oid: &str, coin: &str) -> Result<OrderPoll> {
        let query_id = order_query_id(oid)?;
        let user = self
            .account
            .as_deref()
            .ok_or_else(|| Error::msg("missing OUTCOME_ACCOUNT_ADDRESS"))?;
        let value = self
            .query_info(
                "orderStatus",
                coin,
                &json!({"type": "orderStatus", "user": user, "oid": query_id}),
            )
            .await?;
        Ok(parse_order_status(value, oid))
    }

    // 保留兼容查询入口；可靠回填必须使用带分页和覆盖证据的 poll_fill_page。
    pub async fn poll_fills(&self, coin: Option<&str>) -> Result<Vec<TradeFill>> {
        let user = self
            .account
            .as_deref()
            .ok_or_else(|| Error::msg("missing OUTCOME_ACCOUNT_ADDRESS"))?;
        let value = self
            .query_info(
                "userFills",
                coin.unwrap_or(""),
                &json!({"type": "userFills", "user": user}),
            )
            .await?;
        Ok(parse_user_fills(&value)
            .into_iter()
            .filter(|fill| coin.is_none_or(|want| fill.coin.as_deref() == Some(want)))
            .collect())
    }

    pub async fn poll_fill_page(
        &self,
        token_id: &str,
        submitted_at_ms: i64,
        progress: &Value,
    ) -> Result<FillPage> {
        let mut state = FillProgress::load(token_id, submitted_at_ms, progress)?;
        let user = self
            .account
            .as_deref()
            .ok_or_else(|| Error::msg("missing OUTCOME_ACCOUNT_ADDRESS"))?;
        // 每次只查询一页，让调用方立即持久化成功进度；后页失败不得抹掉此前进展。
        let probing_history = !state.history_checked;
        let start_time = if probing_history { 0 } else { state.cursor };
        let started = Instant::now();
        let raw = self
            .query_info(
                "userFillsByTime",
                token_id,
                &json!({
                    "type": "userFillsByTime", "user": user,
                    "startTime": start_time, "endTime": state.end_time,
                    "aggregateByTime": false
                }),
            )
            .await?;
        let (fills, stalled) = match apply_fill_page(&raw, &mut state, probing_history) {
            Ok(page) => page,
            Err(err) => {
                tracing::error!(
                    service = "outcome", api = "userFillsByTime", token_id,
                    start_time, end_time = state.end_time,
                    elapsed_ms = started.elapsed().as_millis() as u64, error = %err,
                    "outcome fill page validation failed"
                );
                return Err(err);
            }
        };
        if stalled {
            tracing::warn!(
                service = "outcome",
                event = "fill_pagination_stalled",
                api = "userFillsByTime",
                token_id,
                cursor = state.cursor,
                end_time = state.end_time,
                scanned_count = state.scanned_count,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "outcome fill pagination cannot advance safely"
            );
        }
        Ok(FillPage {
            fills,
            complete: state.complete,
            history_complete: state.history_complete,
            progress: serde_json::to_value(state)?,
        })
    }

    async fn query_info(&self, api: &str, token_id: &str, body: &Value) -> Result<Value> {
        let started = Instant::now();
        let mut http_status = None;
        let result: Result<Value> = async {
            let response = self.http.post(&self.info_url).json(body).send().await?;
            http_status = Some(response.status().as_u16());
            Ok(response.error_for_status()?.json().await?)
        }
        .await;
        match &result {
            Ok(_) => tracing::debug!(
                service = "outcome", api, token_id, http_status,
                order_id = ?body.get("oid"), start_time = ?body.get("startTime"),
                end_time = ?body.get("endTime"),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "outcome info query completed"
            ),
            Err(err) => tracing::error!(
                service = "outcome", api, token_id, http_status,
                order_id = ?body.get("oid"), start_time = ?body.get("startTime"),
                end_time = ?body.get("endTime"),
                elapsed_ms = started.elapsed().as_millis() as u64, error = %err,
                "outcome info query failed"
            ),
        }
        result
    }

    pub async fn token_balance(&self, coin: &str) -> Result<Decimal> {
        let user = self
            .account
            .clone()
            .ok_or_else(|| Error::msg("missing OUTCOME_ACCOUNT_ADDRESS"))?;
        let value: Value = self
            .http
            .post(&self.info_url)
            .json(&json!({"type": "spotClearinghouseState", "user": user}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(parse_coin_balance(&value, coin))
    }
}

pub fn parse_l2_book(value: &Value) -> (Vec<Level>, Vec<Level>, i64) {
    let data = value.get("data").unwrap_or(value);
    let ts = data.get("time").and_then(|v| v.as_i64()).unwrap_or(0);
    let levels = data.get("levels").and_then(|v| v.as_array());
    let bids = levels
        .and_then(|arr| arr.first())
        .map(parse_hl_levels)
        .unwrap_or_default();
    let asks = levels
        .and_then(|arr| arr.get(1))
        .map(parse_hl_levels)
        .unwrap_or_default();
    (bids, asks, ts)
}

fn parse_hl_levels(value: &Value) -> Vec<Level> {
    match value {
        Value::Array(items) => items
            .iter()
            .filter_map(|item| {
                let price = item.get("px").and_then(parse_decimal)?;
                let size = item.get("sz").and_then(parse_decimal)?;
                Some(Level { price, size })
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub fn is_explicit_order_reject(message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    m.contains("could not immediately match")
        || m.contains("no liquidity")
        || m.contains("could not fill")
        || m.contains("minimum value")
        || m.contains("divisible by tick")
        || m.contains("tick size")
}

pub fn classify_http_submit(
    status: u16,
    body: &Value,
    hash: String,
    envelope: Value,
    cloid: &str,
) -> SubmitResult {
    if status >= 400 {
        let message: String = body.to_string().chars().take(300).collect();
        // 只有明确拒单才能记零成交；5xx / 408 / 425 / 429 未证明订单没执行，交给 unknown 继续核对。
        if !crate::platforms::http_status_proves_reject(status) {
            return SubmitResult::Unknown {
                order_id: None,
                order_hash: hash,
                envelope,
                message: format!("http {status} does not prove rejection: {message}"),
            };
        }
        return SubmitResult::Failed {
            order_hash: hash,
            envelope,
            status,
            message,
        };
    }
    parse_exchange_submit(body, hash, envelope, cloid)
}

pub fn classify_submit_transport_error(
    err: impl std::fmt::Display,
    hash: String,
    envelope: Value,
) -> SubmitResult {
    SubmitResult::Unknown {
        order_id: None,
        order_hash: hash,
        envelope,
        message: err.to_string(),
    }
}

pub fn parse_exchange_submit(
    body: &Value,
    order_hash: String,
    envelope: Value,
    _cloid: &str,
) -> SubmitResult {
    let statuses = body
        .pointer("/response/data/statuses")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if let Some(first) = statuses.first() {
        if let Some(err) = first.get("error").and_then(|v| v.as_str()) {
            return SubmitResult::NoMatch {
                order_hash,
                envelope,
                message: err.to_string(),
            };
        }
        let Some(oid) = numeric_id(first.pointer("/resting/oid"))
            .or_else(|| numeric_id(first.pointer("/filled/oid")))
        else {
            return SubmitResult::Unknown {
                order_id: None,
                order_hash,
                envelope,
                message: "outcome acknowledgment missing valid numeric oid".into(),
            };
        };
        let taking = first.pointer("/filled/totalSz").and_then(parse_decimal);
        let avg_px = first.pointer("/filled/avgPx").and_then(parse_decimal);
        return SubmitResult::Ack {
            order_id: oid,
            order_hash,
            envelope,
            making: None,
            taking,
            avg_px,
        };
    }
    let status = body.get("status").and_then(|v| v.as_str()).unwrap_or("");
    let message = body.to_string();
    if status != "ok" {
        if is_explicit_order_reject(&message) {
            return SubmitResult::NoMatch {
                order_hash,
                envelope,
                message,
            };
        }
        return SubmitResult::Unknown {
            order_id: None,
            order_hash,
            envelope,
            message,
        };
    }
    SubmitResult::Unknown {
        order_id: None,
        order_hash,
        envelope,
        message,
    }
}

fn numeric_id(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Number(number) => number.as_u64().map(|id| id.to_string()),
        Value::String(text) if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) => {
            text.parse::<u64>().ok().map(|id| id.to_string())
        }
        _ => None,
    }
}

fn order_query_id(id: &str) -> Result<Value> {
    if !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()) {
        let oid = id
            .parse::<u64>()
            .map_err(|_| Error::msg("outcome oid exceeds u64"))?;
        return Ok(json!(oid));
    }
    if id.len() == 34
        && id.starts_with("0x")
        && id.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
    {
        return Ok(json!(id));
    }
    Err(Error::msg(
        "outcome order ID must be u64 or a 0x-prefixed 32-hex cloid",
    ))
}

pub fn parse_order_status(raw: Value, _query_id: &str) -> OrderPoll {
    let status = raw
        .pointer("/order/status")
        .or_else(|| raw.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let order = raw.pointer("/order/order");
    let order_id = numeric_id(order.and_then(|v| v.get("oid")));
    let found = raw.get("status").and_then(Value::as_str) == Some("order") && order_id.is_some();
    OrderPoll {
        found,
        status,
        order_id: if found { order_id } else { None },
        // sz 是未成交量，origSz - sz 也不作为最终成交证明。
        shares: None,
        price: order.and_then(|v| v.get("limitPx")).and_then(parse_decimal),
        original_shares: order.and_then(|v| v.get("origSz")).and_then(parse_decimal),
        remaining_shares: order.and_then(|v| v.get("sz")).and_then(parse_decimal),
        client_order_id: order
            .and_then(|v| v.get("cloid"))
            .and_then(Value::as_str)
            .map(str::to_string),
        coin: order
            .and_then(|v| v.get("coin"))
            .and_then(Value::as_str)
            .map(str::to_string),
        raw,
        ..Default::default()
    }
}

fn parse_user_fill(item: &Value) -> Result<TradeFill> {
    let trade_id =
        numeric_id(item.get("tid")).ok_or_else(|| Error::msg("outcome fill missing valid tid"))?;
    let order_id =
        numeric_id(item.get("oid")).ok_or_else(|| Error::msg("outcome fill missing valid oid"))?;
    let coin = item
        .get("coin")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::msg("outcome fill missing coin"))?;
    let shares = item
        .get("sz")
        .and_then(parse_decimal)
        .filter(|n| *n > Decimal::ZERO)
        .ok_or_else(|| Error::msg("outcome fill has invalid sz"))?;
    let price = item
        .get("px")
        .and_then(parse_decimal)
        .filter(|n| *n > Decimal::ZERO)
        .ok_or_else(|| Error::msg("outcome fill has invalid px"))?;
    let fee = match item.get("fee") {
        None | Some(Value::Null) => None,
        Some(value) => {
            Some(parse_decimal(value).ok_or_else(|| Error::msg("outcome fill has invalid fee"))?)
        }
    };
    let fee_token = match item.get("feeToken") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
        _ => return Err(Error::msg("outcome fill has invalid feeToken")),
    };
    let mut order_ids = vec![order_id.clone()];
    match item.get("cloid") {
        None | Some(Value::Null) => {}
        Some(Value::String(cloid)) if !cloid.is_empty() => {
            if cloid != &order_id {
                order_ids.push(cloid.clone());
            }
        }
        _ => return Err(Error::msg("outcome fill has invalid cloid")),
    }
    Ok(TradeFill {
        trade_id,
        order_id: Some(order_id),
        order_ids,
        coin: Some(coin.to_string()),
        shares,
        price,
        fee,
        fee_rate_bps: None,
        fee_token,
        finality: FillFinality::Confirmed,
        raw: item.clone(),
    })
}

pub fn parse_user_fills(raw: &Value) -> Vec<TradeFill> {
    raw.as_array()
        .or_else(|| raw.get("fills").and_then(Value::as_array))
        .into_iter()
        .flatten()
        .filter_map(|item| parse_user_fill(item).ok())
        .collect()
}

// 返回是否无安全进展。满页按原始账户记录判断，不能按筛选后本币成交数量判断。
fn apply_fill_page(
    raw: &Value,
    state: &mut FillProgress,
    probing_history: bool,
) -> Result<(Vec<TradeFill>, bool)> {
    let items = raw
        .as_array()
        .ok_or_else(|| Error::msg("outcome fill page is not an array"))?;
    if items.len() > FILL_PAGE_SIZE {
        return Err(Error::msg("outcome fill page exceeds documented limit"));
    }
    let start = if probing_history { 0 } else { state.cursor };
    let mut previous_time = start;
    let mut records = Vec::with_capacity(items.len());
    for item in items {
        let time = item
            .get("time")
            .and_then(Value::as_u64)
            .ok_or_else(|| Error::msg("outcome fill missing valid time"))?;
        if time < previous_time || time > state.end_time {
            return Err(Error::msg(
                "outcome fill page has unordered or out-of-window time",
            ));
        }
        previous_time = time;
        let id = numeric_id(item.get("tid"))
            .ok_or_else(|| Error::msg("outcome fill missing valid tid"))?;
        let coin = item
            .get("coin")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::msg("outcome fill missing coin"))?;
        // +/asset-id 是余额接口别名，不将其作为交易接口的 #coin 默默接受或过滤。
        if time >= state.submitted_at_ms
            && coin != state.token_id
            && coin_aliases_match(coin, &state.token_id)
        {
            return Err(Error::msg(
                "outcome fill has unexpected balance-style coin alias",
            ));
        }
        // 没有 coin 时无法排除相关成交；相关坏记录必须报错，不能变成“完整的空结果”。
        let fill = if time >= state.submitted_at_ms && coin == state.token_id {
            Some(parse_user_fill(item)?)
        } else {
            None
        };
        records.push((time, id, fill));
    }
    if probing_history {
        let lower_bound = records.first().map(|record| record.0);
        state.history_checked = true;
        // 最近 10000 条的保留限制不能用“返回了短页/空页”排除更早的成交。
        state.history_complete = lower_bound.is_some_and(|time| time <= state.submitted_at_ms);
        state.history_lower_bound = lower_bound;
        return Ok((Vec::new(), false));
    }

    let old_cursor = state.cursor;
    let mut page_seen = BTreeSet::new();
    let mut fills = Vec::new();
    let mut new_count = 0usize;
    for (time, id, fill) in &records {
        if !page_seen.insert(id.clone()) || (*time == old_cursor && state.seen_ids.contains(id)) {
            continue;
        }
        new_count += 1;
        if let Some(fill) = fill {
            fills.push(fill.clone());
        }
    }
    state.scanned_count = state.scanned_count.saturating_add(new_count);
    if state.scanned_count >= FILL_HISTORY_LIMIT {
        state.history_complete = false;
    }
    if let Some((last_time, _, _)) = records.last() {
        if *last_time > old_cursor {
            state.cursor = *last_time;
            state.seen_ids.clear();
        }
        for (time, id, _) in &records {
            if *time == state.cursor {
                state.seen_ids.insert(id.clone());
            }
        }
    }
    state.complete = items.len() < FILL_PAGE_SIZE;
    // 同一毫秒有 >=2000 条且接口反复返回同一页时，不能 +1 跳过未知成交。
    let stalled = !state.complete
        && ((state.cursor == old_cursor && new_count == 0)
            || state.seen_ids.len() >= FILL_HISTORY_LIMIT);
    Ok((fills, stalled))
}

pub fn apply_ws_book(books: &mut BookStore, payload: &Value, now: Instant) -> Option<String> {
    let data = payload.get("data").unwrap_or(payload);
    let coin = data.get("coin").and_then(|v| v.as_str())?;
    let (bids, asks, ts) = parse_l2_book(payload);
    if books.replace_snapshot(OUTCOME, coin, bids, asks, ts, now) {
        Some(coin.to_string())
    } else {
        None
    }
}

pub async fn run_l2_ws(
    url: String,
    books: Arc<Mutex<BookStore>>,
    calc_tx: mpsc::Sender<TopicKey>,
    mut sub_rx: mpsc::Receiver<Vec<String>>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut coins = Vec::new();
    loop {
        if *shutdown.borrow() {
            break;
        }
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _)) => {
                tracing::info!("hyperliquid l2 ws connected");
                let (mut write, mut read) = ws.split();
                for coin in &coins {
                    let msg =
                        json!({"method":"subscribe","subscription":{"type":"l2Book","coin": coin}});
                    let _ = write.send(Message::Text(msg.to_string().into())).await;
                }
                loop {
                    tokio::select! {
                        msg = sub_rx.recv() => {
                            let Some(next) = msg else { return; };
                            let dropped: Vec<_> = coins
                                .iter()
                                .filter(|coin| !next.contains(*coin))
                                .cloned()
                                .collect();
                            for coin in dropped {
                                let payload = json!({"method":"unsubscribe","subscription":{"type":"l2Book","coin": coin}});
                                if write.send(Message::Text(payload.to_string().into())).await.is_err() {
                                    break;
                                }
                            }
                            coins = next;
                            for coin in &coins {
                                let payload = json!({"method":"subscribe","subscription":{"type":"l2Book","coin": coin}});
                                if write.send(Message::Text(payload.to_string().into())).await.is_err() {
                                    break;
                                }
                            }
                        }
                        incoming = read.next() => {
                            let Some(Ok(msg)) = incoming else { break; };
                            let text = match msg {
                                Message::Text(t) => t.to_string(),
                                Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                                _ => continue,
                            };
                            handle_ws(&text, &books, &calc_tx).await;
                        }
                        _ = wait_shutdown(&shutdown) => return,
                    }
                }
                books.lock().await.mark_platform_stale(OUTCOME);
            }
            Err(err) => tracing::warn!(error = %err, "hyperliquid ws connect failed"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn wait_shutdown(shutdown: &tokio::sync::watch::Receiver<bool>) {
    let mut rx = shutdown.clone();
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            break;
        }
    }
}

async fn handle_ws(text: &str, books: &Arc<Mutex<BookStore>>, calc_tx: &mpsc::Sender<TopicKey>) {
    let parsed: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };
    let channel = parsed.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    if channel != "l2Book" {
        return;
    }
    let now = Instant::now();
    let topics = {
        let mut store = books.lock().await;
        apply_ws_book(&mut store, &parsed, now)
            .map(|coin| store.topics_for(OUTCOME, &coin))
            .unwrap_or_default()
    };
    for topic in topics {
        let _ = calc_tx.send(topic).await;
    }
}

fn random_cloid() -> String {
    format!("0x{}", hex::encode(rand::random::<[u8; 16]>()))
}

fn parse_settlement_market_id(market_id: &str) -> Result<u64> {
    let outcome_id = market_id.trim().parse::<u64>().map_err(|_| {
        Error::msg(format!(
            "invalid outcome settlement market_id: {market_id:?}"
        ))
    })?;
    if outcome_id == 0 {
        return Err(Error::msg("outcome settlement market_id must be positive"));
    }
    Ok(outcome_id)
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn parse_usdc_balance(value: &Value) -> Decimal {
    let usdc = parse_coin_balance(value, "USDC");
    if usdc > Decimal::ZERO {
        return usdc;
    }
    parse_coin_balance(value, "USDH")
}

fn parse_coin_balance(value: &Value, want: &str) -> Decimal {
    let balances = value
        .pointer("/balances")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for item in balances {
        let coin = item.get("coin").and_then(|v| v.as_str()).unwrap_or("");
        if coin_aliases_match(coin, want) {
            return item
                .get("total")
                .or_else(|| item.get("hold"))
                .and_then(parse_decimal)
                .unwrap_or(Decimal::ZERO);
        }
    }
    Decimal::ZERO
}

fn coin_aliases_match(got: &str, want: &str) -> bool {
    if got.eq_ignore_ascii_case(want) {
        return true;
    }
    let Some((want_id, want_side)) = parse_side_coin(want) else {
        return false;
    };
    if parse_side_coin(got) == Some((want_id, want_side)) {
        return true;
    }
    got == side_asset_id(want_id, want_side).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_venue() -> OutcomeVenue {
        OutcomeVenue {
            http: reqwest::Client::new(),
            info_url: "http://127.0.0.1".into(),
            exchange_url: "http://127.0.0.1".into(),
            mainnet: true,
            signer: None,
            account: Some("0xtest".into()),
            builder: None,
            nonce: Arc::new(StdMutex::new(0)),
            usdc_balance_cache: Arc::new(Mutex::new(UsdcBalanceCache::default())),
            usdc_balance_refresh: Arc::new(Mutex::new(())),
        }
    }

    fn fill(tid: u64, time: u64, coin: &str) -> Value {
        json!({
            "tid": tid, "oid": 9007199254740993u64, "time": time,
            "coin": coin, "sz": "3", "px": "0.4", "fee": "0.01", "feeToken": "USDC"
        })
    }

    fn fill_state(start: i64) -> FillProgress {
        let mut state = FillProgress::load("#5160", start, &Value::Null).unwrap();
        state.end_time = 100_000;
        state
    }

    // 只绑定 loopback，直接构造 venue；不初始化配置、签名器或真实网络交易。
    fn info_stub(
        replies: Vec<(u16, Value)>,
    ) -> (OutcomeVenue, std::thread::JoinHandle<Vec<Value>>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for (status, reply) in replies {
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(Instant::now() < deadline, "missing stub HTTP request");
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Err(err) => panic!("stub accept failed: {err}"),
                    }
                };
                // macOS accept 继承 listener 的 O_NONBLOCK；线程内读写需显式改回阻塞。
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut data = Vec::new();
                let (body_start, content_length) = loop {
                    let mut buf = [0; 4096];
                    let len = stream.read(&mut buf).unwrap();
                    assert!(len > 0, "request ended before headers");
                    data.extend_from_slice(&buf[..len]);
                    if let Some(index) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&data[..index]).unwrap();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (index + 4, length);
                    }
                };
                while data.len() < body_start + content_length {
                    let mut buf = [0; 4096];
                    let len = stream.read(&mut buf).unwrap();
                    assert!(len > 0, "request ended before body");
                    data.extend_from_slice(&buf[..len]);
                }
                requests.push(
                    serde_json::from_slice(&data[body_start..body_start + content_length]).unwrap(),
                );
                let body = reply.to_string();
                write!(stream,
                    "HTTP/1.1 {status} Stub\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ).unwrap();
            }
            requests
        });
        let mut venue = test_venue();
        venue.info_url = format!("http://{addr}/info");
        venue.http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        (venue, server)
    }

    #[tokio::test]
    async fn poll_order_serializes_exact_u64_and_cloid() {
        let ids = [
            "0",
            "00042",
            "9007199254740993",
            "18446744073709551615",
            "0x0123456789abcdefABCDEF0123456789",
        ];
        let (venue, server) = info_stub(
            ids.iter()
                .map(|_| (200, json!({"status": "unknownOid"})))
                .collect(),
        );
        for id in ids {
            assert!(!venue.poll_order(id, "#5160").await.unwrap().found);
        }
        let requests = server.join().unwrap();
        for (request, expected) in requests.iter().zip([0, 42, 9007199254740993, u64::MAX]) {
            assert_eq!(request["type"], "orderStatus");
            assert_eq!(request["oid"].as_u64(), Some(expected));
            assert!(!request["oid"].is_string());
        }
        assert_eq!(requests[4]["oid"].as_str(), Some(ids[4]));
    }

    #[tokio::test]
    async fn invalid_order_ids_fail_before_account_or_http() {
        let mut venue = test_venue();
        venue.account = None;
        for id in [
            "",
            " 42",
            "+42",
            "-1",
            "1.0",
            "1e3",
            "18446744073709551616",
            "0xabc",
            "0x0123456789abcdef0123456789abcdeg",
        ] {
            let err = venue.poll_order(id, "#5160").await.unwrap_err();
            assert!(!err.to_string().contains("missing OUTCOME_ACCOUNT_ADDRESS"));
            assert!(order_query_id(id).is_err());
        }
    }

    #[test]
    fn order_status_uses_official_nested_order_and_separate_sizes() {
        let cloid = "0x0123456789abcdef0123456789abcdef";
        let raw = json!({"status": "order", "order": {
            "status": "canceled", "statusTimestamp": 100,
            "order": {"oid": u64::MAX, "cloid": cloid, "coin": "#5160",
                "sz": "7", "origSz": "10", "limitPx": "0.4"}
        }});
        let order = parse_order_status(raw.clone(), cloid);
        assert!(order.found);
        assert_eq!(order.status, "canceled");
        assert_eq!(order.order_id.as_deref(), Some("18446744073709551615"));
        assert_eq!(order.client_order_id.as_deref(), Some(cloid));
        assert_eq!(order.coin.as_deref(), Some("#5160"));
        assert_eq!(order.original_shares, Some(Decimal::from(10)));
        assert_eq!(order.remaining_shares, Some(Decimal::from(7)));
        assert_eq!(order.price, Some("0.4".parse().unwrap()));
        assert_eq!(order.shares, None);
        assert_eq!(order.raw, raw);
    }

    #[test]
    fn unknown_or_invalid_status_never_promotes_query_id_to_oid() {
        for raw in [
            Value::Null,
            json!({"status":"unknownOid"}),
            json!({}),
            json!({"status":"order", "order":{"status":"filled","order":{"oid":"0xabc"}}}),
            json!({"status":"order", "order":{"status":"filled","order":{"oid":1.0}}}),
        ] {
            let result = parse_order_status(raw, "0x0123456789abcdef0123456789abcdef");
            assert!(!result.found);
            assert!(result.order_id.is_none());
            assert!(result.shares.is_none());
        }
        assert_eq!(
            numeric_id(Some(&json!("18446744073709551615"))),
            Some(u64::MAX.to_string())
        );
        assert_eq!(numeric_id(Some(&json!("00042"))), Some("42".into()));
        assert!(numeric_id(Some(&json!("18446744073709551616"))).is_none());
    }

    #[test]
    fn fills_preserve_official_fees_finality_and_optional_cloid() {
        let mut raw = fill(1, 10, "#5160");
        raw["fee"] = json!("-0.002");
        let fills = parse_user_fills(&json!([raw]));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id.as_deref(), Some("9007199254740993"));
        assert_eq!(fills[0].fee, Some("-0.002".parse().unwrap()));
        assert_eq!(fills[0].fee_token.as_deref(), Some("USDC"));
        assert_eq!(fills[0].finality, FillFinality::Confirmed);
        assert_eq!(fills[0].order_ids.len(), 1);
        let mut raw = fill(2, 10, "#5160");
        raw.as_object_mut().unwrap().remove("fee");
        raw.as_object_mut().unwrap().remove("feeToken");
        let parsed = parse_user_fill(&raw).unwrap();
        assert_eq!(parsed.fee, None);
        assert_eq!(parsed.fee_token, None);
        assert_eq!(parsed.fee_rate_bps, None);
    }

    #[test]
    fn history_probe_requires_positive_lower_bound_evidence() {
        for (raw, covered) in [
            (json!([]), false),
            (json!([fill(1, 101, "#other")]), false),
            (json!([fill(1, 100, "#other")]), true),
            (json!([fill(1, 99, "#other")]), true),
        ] {
            let mut state = fill_state(100);
            apply_fill_page(&raw, &mut state, true).unwrap();
            assert_eq!(state.history_complete, covered);
            apply_fill_page(&json!([]), &mut state, false).unwrap();
            assert!(state.complete);
            assert_eq!(state.history_complete, covered);
        }
        let mut state = fill_state(100);
        state.history_complete = true;
        state.scanned_count = FILL_HISTORY_LIMIT - 1;
        apply_fill_page(&json!([fill(1, 100, "#5160")]), &mut state, false).unwrap();
        assert!(state.complete);
        assert!(!state.history_complete);
    }

    #[test]
    fn malformed_relevant_fills_and_page_metadata_fail_closed() {
        for field in ["sz", "px", "fee", "feeToken", "coin", "tid", "oid", "time"] {
            let mut raw = fill(1, 100, "#5160");
            raw[field] = json!({"invalid": true});
            let mut state = fill_state(100);
            assert!(
                apply_fill_page(&json!([raw]), &mut state, false).is_err(),
                "{field}"
            );
            assert!(!state.complete);
        }
        for raw in [
            json!({"error": "unavailable"}),
            json!([fill(1, 99, "#5160")]),
            json!([fill(1, 101, "#5160"), fill(2, 100, "#5160")]),
        ] {
            assert!(apply_fill_page(&raw, &mut fill_state(100), false).is_err());
        }
    }

    #[test]
    fn fill_pages_do_not_accept_balance_coin_aliases() {
        for coin in ["+5160", "100005160"] {
            let mut state = fill_state(100);
            assert!(apply_fill_page(&json!([fill(1, 100, coin)]), &mut state, false).is_err());
            assert!(!state.complete);
        }
    }

    #[tokio::test]
    async fn time_pagination_uses_account_page_size_and_inclusive_boundary() {
        let first: Vec<_> = (1..=2000).map(|id| fill(id, id + 99, "#other")).collect();
        let last = first.last().unwrap().clone();
        let (venue, server) = info_stub(vec![
            (200, json!([fill(0, 99, "#other")])),
            (200, json!(first)),
            (
                200,
                json!([last, fill(2001, 2099, "#5160"), fill(2002, 2100, "#5160")]),
            ),
        ]);
        let probe = venue
            .poll_fill_page("#5160", 100, &Value::Null)
            .await
            .unwrap();
        assert!(!probe.complete);
        assert!(probe.fills.is_empty());
        assert_eq!(probe.progress["historyChecked"], true);
        let first = venue
            .poll_fill_page("#5160", 100, &probe.progress)
            .await
            .unwrap();
        assert!(!first.complete);
        assert!(first.fills.is_empty());
        assert_eq!(first.progress["cursor"], 2099);
        let page = venue
            .poll_fill_page("#5160", 100, &first.progress)
            .await
            .unwrap();
        assert!(page.complete);
        assert!(page.history_complete);
        assert_eq!(
            page.fills
                .iter()
                .map(|f| f.trade_id.as_str())
                .collect::<Vec<_>>(),
            vec!["2001", "2002"]
        );
        let requests = server.join().unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|r| r["startTime"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![0, 100, 2099]
        );
        assert!(requests
            .iter()
            .all(|r| r["endTime"] == requests[0]["endTime"]));
        assert!(requests.iter().all(|r| r["aggregateByTime"] == false));
    }

    #[tokio::test]
    async fn fill_progress_continues_second_call_and_only_restarts_after_completion() {
        let first: Vec<_> = (1..=2000).map(|id| fill(id, id + 99, "#5160")).collect();
        let second: Vec<_> = (2000..4000).map(|id| fill(id, id + 99, "#5160")).collect();
        let (venue, server) = info_stub(vec![
            (200, json!([fill(0, 99, "#other")])),
            (200, json!(first)),
            (200, json!(second)),
            (
                200,
                json!([fill(3999, 4098, "#5160"), fill(4000, 4099, "#5160")]),
            ),
            (200, json!([fill(0, 99, "#other")])),
            (200, json!([])),
        ]);
        let probe = venue
            .poll_fill_page("#5160", 100, &json!({}))
            .await
            .unwrap();
        assert!(!probe.complete);
        let first = venue
            .poll_fill_page("#5160", 100, &probe.progress)
            .await
            .unwrap();
        assert!(!first.complete);
        assert_eq!(first.fills.len(), 2000);
        let saved: Value = serde_json::from_str(&first.progress.to_string()).unwrap();
        let second = venue.poll_fill_page("#5160", 100, &saved).await.unwrap();
        assert!(!second.complete);
        assert_eq!(second.fills.len(), 1999);
        let third = venue
            .poll_fill_page("#5160", 100, &second.progress)
            .await
            .unwrap();
        assert!(third.complete);
        assert!(third.history_complete);
        assert_eq!(third.fills.len(), 1);
        assert_eq!(third.fills[0].trade_id, "4000");
        assert_eq!(third.progress["endTime"], first.progress["endTime"]);
        assert_eq!(third.progress["historyLowerBound"], 99);
        let next_probe = venue
            .poll_fill_page("#5160", 100, &third.progress)
            .await
            .unwrap();
        assert!(!next_probe.complete);
        let next_round = venue
            .poll_fill_page("#5160", 100, &next_probe.progress)
            .await
            .unwrap();
        assert!(next_round.complete);
        let requests = server.join().unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|r| r["startTime"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![0, 100, 2099, 4098, 0, 100]
        );
        assert!(requests[..4]
            .iter()
            .all(|r| r["endTime"] == requests[0]["endTime"]));
    }

    #[tokio::test]
    async fn full_timestamp_page_never_skips_stalled_boundary() {
        let full: Vec<_> = (1..=2000).map(|id| fill(id, 100, "#5160")).collect();
        let (venue, server) = info_stub(vec![
            (200, json!([fill(0, 99, "#other")])),
            (200, json!(full)),
            (200, json!(full)),
            (200, json!(full)),
        ]);
        let probe = venue
            .poll_fill_page("#5160", 100, &Value::Null)
            .await
            .unwrap();
        assert!(!probe.complete);
        let first = venue
            .poll_fill_page("#5160", 100, &probe.progress)
            .await
            .unwrap();
        assert!(!first.complete);
        assert_eq!(first.fills.len(), 2000);
        assert_eq!(first.progress["cursor"], 100);
        let next = venue
            .poll_fill_page("#5160", 100, &first.progress)
            .await
            .unwrap();
        assert!(!next.complete);
        assert!(next.fills.is_empty());
        assert_eq!(next.progress["cursor"], 100);
        let retry = venue
            .poll_fill_page("#5160", 100, &next.progress)
            .await
            .unwrap();
        assert!(!retry.complete);
        assert!(retry.fills.is_empty());
        assert_eq!(retry.progress["cursor"], 100);
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests[1..].iter().all(|r| r["startTime"] == 100));
    }

    #[tokio::test]
    async fn empty_or_truncated_history_never_proves_zero_fills() {
        for probe in [json!([]), json!([fill(1, 101, "#other")])] {
            let (venue, server) = info_stub(vec![(200, probe), (200, json!([]))]);
            let probe = venue
                .poll_fill_page("#5160", 100, &Value::Null)
                .await
                .unwrap();
            assert!(!probe.complete);
            assert!(!probe.history_complete);
            let page = venue
                .poll_fill_page("#5160", 100, &probe.progress)
                .await
                .unwrap();
            assert!(page.complete);
            assert!(!page.history_complete);
            assert!(page.fills.is_empty());
            assert_eq!(server.join().unwrap().len(), 2);
        }
    }

    #[tokio::test]
    async fn http_error_and_malformed_page_do_not_return_complete() {
        let (venue, server) = info_stub(vec![(503, json!({"error": "unavailable"}))]);
        assert!(venue
            .poll_fill_page("#5160", 100, &Value::Null)
            .await
            .is_err());
        assert_eq!(server.join().unwrap().len(), 1);
        let mut bad = fill(1, 100, "#5160");
        bad["fee"] = json!("not-a-fee");
        let (venue, server) = info_stub(vec![
            (200, json!([fill(0, 99, "#other")])),
            (200, json!([bad])),
        ]);
        let probe = venue
            .poll_fill_page("#5160", 100, &Value::Null)
            .await
            .unwrap();
        assert!(!probe.complete);
        assert!(venue
            .poll_fill_page("#5160", 100, &probe.progress)
            .await
            .is_err());
        assert_eq!(server.join().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn later_page_failures_retry_saved_cursor_instead_of_first_page() {
        let first: Vec<_> = (1..=2000).map(|id| fill(id, id + 99, "#5160")).collect();
        let mut bad = fill(2001, 2100, "#5160");
        bad["fee"] = json!("invalid");
        let (venue, server) = info_stub(vec![
            (200, json!([fill(0, 99, "#other")])),
            (200, json!(first)),
            (503, json!({"error": "temporary"})),
            (200, json!([bad])),
            (
                200,
                json!([fill(2000, 2099, "#5160"), fill(2001, 2100, "#5160")]),
            ),
        ]);
        let probe = venue
            .poll_fill_page("#5160", 100, &Value::Null)
            .await
            .unwrap();
        let first = venue
            .poll_fill_page("#5160", 100, &probe.progress)
            .await
            .unwrap();
        assert_eq!(first.fills.len(), 2000);
        assert!(!first.complete);
        let saved: Value = serde_json::from_str(&first.progress.to_string()).unwrap();
        assert_eq!(saved["cursor"], 2099);
        for _ in 0..2 {
            assert!(venue.poll_fill_page("#5160", 100, &saved).await.is_err());
        }
        let recovered = venue.poll_fill_page("#5160", 100, &saved).await.unwrap();
        assert!(recovered.complete);
        assert!(recovered.history_complete);
        assert_eq!(recovered.fills.len(), 1);
        assert_eq!(recovered.fills[0].trade_id, "2001");
        let requests = server.join().unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|r| r["startTime"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![0, 100, 2099, 2099, 2099]
        );
        assert!(requests
            .iter()
            .all(|r| r["endTime"] == requests[0]["endTime"]));
    }

    #[test]
    fn fill_progress_rejects_other_queries_and_invalid_cursors() {
        let state = fill_state(100);
        let value = serde_json::to_value(state).unwrap();
        assert!(FillProgress::load("#5161", 100, &value).is_err());
        assert!(FillProgress::load("#5160", 101, &value).is_err());
        assert!(FillProgress::load("#5160", -1, &Value::Null).is_err());
        let mut bad = value;
        bad["cursor"] = json!(99);
        assert!(FillProgress::load("#5160", 100, &bad).is_err());
    }

    #[test]
    fn submit_ack_without_valid_numeric_oid_stays_unknown() {
        let envelope = json!({"cloid": "0x0123456789abcdef0123456789abcdef"});
        for oid in [
            Value::Null,
            json!("0xabc"),
            json!(1.5),
            json!("18446744073709551616"),
        ] {
            let body = exchange_ok(json!({"filled": {"totalSz": "5", "avgPx": "0.4", "oid": oid}}));
            match parse_exchange_submit(&body, "hash".into(), envelope.clone(), "cloid") {
                SubmitResult::Unknown {
                    order_id,
                    envelope: saved,
                    ..
                } => {
                    assert!(order_id.is_none());
                    assert_eq!(saved, envelope);
                }
                other => panic!("expected Unknown, got {other:?}"),
            }
        }
        let body =
            exchange_ok(json!({"filled": {"totalSz": "5", "avgPx": "0.4", "oid": u64::MAX}}));
        match parse_exchange_submit(&body, "hash".into(), envelope, "cloid") {
            SubmitResult::Ack { order_id, .. } => assert_eq!(order_id, u64::MAX.to_string()),
            other => panic!("expected Ack, got {other:?}"),
        }
    }

    #[test]
    fn usdc_cache_expires_after_ten_seconds() {
        let now = Instant::now();
        let fresh_at = now
            .checked_sub(USDC_BALANCE_CACHE_TTL - Duration::from_millis(1))
            .unwrap();
        let expired_at = now.checked_sub(USDC_BALANCE_CACHE_TTL).unwrap();
        let mut cache = UsdcBalanceCache {
            value: Some((Decimal::from(7), fresh_at)),
            ..Default::default()
        };
        assert_eq!(cache.get_fresh(now), Some(Decimal::from(7)));
        cache.value = Some((Decimal::from(7), expired_at));
        assert_eq!(cache.get_fresh(now), None);
    }

    #[tokio::test]
    async fn usdc_cache_reuses_success_and_invalidation_refreshes() {
        let venue = test_venue();
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = || {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Decimal::from(11))
            }
        };
        assert_eq!(
            venue.cached_usdc_balance(fetch).await.unwrap(),
            Decimal::from(11)
        );
        assert_eq!(
            venue.cached_usdc_balance(fetch).await.unwrap(),
            Decimal::from(11)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        venue.invalidate_usdc_balance().await;
        assert_eq!(
            venue.cached_usdc_balance(fetch).await.unwrap(),
            Decimal::from(11)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn usdc_cache_does_not_store_failures() {
        let venue = test_venue();
        let calls = Arc::new(AtomicUsize::new(0));
        let result = venue
            .cached_usdc_balance(|| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(Error::msg("temporary failure"))
                }
            })
            .await;
        assert!(result.is_err());
        let balance = venue
            .cached_usdc_balance(|| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Decimal::from(9))
                }
            })
            .await
            .unwrap();
        assert_eq!(balance, Decimal::from(9));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn concurrent_usdc_calls_share_one_refresh() {
        let venue = test_venue();
        let calls = Arc::new(AtomicUsize::new(0));
        let futures = (0..8).map(|_| {
            let venue = venue.clone();
            let calls = calls.clone();
            async move {
                venue
                    .cached_usdc_balance(|| {
                        let calls = calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            Ok(Decimal::from(13))
                        }
                    })
                    .await
            }
        });
        let balances = futures_util::future::join_all(futures).await;
        assert!(balances
            .into_iter()
            .all(|balance| balance.unwrap() == Decimal::from(13)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn invalidation_during_refresh_discards_stale_result() {
        let venue = test_venue();
        let calls = Arc::new(AtomicUsize::new(0));
        let balance = venue
            .cached_usdc_balance(|| {
                let venue = venue.clone();
                let calls = calls.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        venue.invalidate_usdc_balance().await;
                        Ok(Decimal::from(20))
                    } else {
                        Ok(Decimal::from(15))
                    }
                }
            })
            .await
            .unwrap();
        assert_eq!(balance, Decimal::from(15));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn parses_l2_snapshot() {
        let raw = json!({
            "channel": "l2Book",
            "data": {
                "coin": "#5160",
                "time": 10,
                "levels": [
                    [{"px": "0.40", "sz": "12", "n": 1}],
                    [{"px": "0.45", "sz": "8", "n": 1}]
                ]
            }
        });
        let (bids, asks, ts) = parse_l2_book(&raw);
        assert_eq!(ts, 10);
        assert_eq!(bids[0].price.to_string(), "0.40");
        assert_eq!(asks[0].size.to_string(), "8");
    }

    #[test]
    fn parse_user_fills_keeps_coin_and_ids() {
        let fills = parse_user_fills(&json!([{
            "tid": 1,
            "oid": 99,
            "cloid": "0xabc",
            "coin": "#5160",
            "sz": "3",
            "px": "0.4",
            "fee": "0.01"
        }, {
            "tid": 2,
            "oid": 100,
            "coin": "#5161",
            "sz": "4",
            "px": "0.5",
            "fee": "0"
        }]));
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].coin.as_deref(), Some("#5160"));
        assert!(fills[0].matches(Some("99"), Some("0xabc")));
        assert_eq!(fills[0].fee, Some("0.01".parse().unwrap()));
        assert_eq!(fills[1].fee, Some(Decimal::ZERO));
        assert!(!fills[1].matches(Some("99"), None));
    }

    #[test]
    fn parse_coin_balance_reads_named_token() {
        let raw = json!({"balances": [
            {"coin": "USDC", "total": "10"},
            {"coin": "+5160", "total": "7"}
        ]});
        assert_eq!(parse_coin_balance(&raw, "#5160").to_string(), "7");
        assert_eq!(parse_coin_balance(&raw, "+5160").to_string(), "7");
        assert_eq!(parse_usdc_balance(&raw).to_string(), "10");
    }

    #[test]
    fn parses_settlement_market_id() {
        assert_eq!(parse_settlement_market_id(" 516 ").unwrap(), 516);
        assert!(parse_settlement_market_id("not-a-number").is_err());
        assert!(parse_settlement_market_id("0").is_err());
    }

    #[test]
    fn parse_coin_balance_matches_asset_id() {
        let raw = json!({"balances": [{"coin": "100012110", "total": "119"}]});
        assert_eq!(parse_coin_balance(&raw, "#12110").to_string(), "119");
    }

    fn exchange_ok(status_item: Value) -> Value {
        json!({
            "status": "ok",
            "response": {"type": "order", "data": {"statuses": [status_item]}}
        })
    }

    #[test]
    fn parse_submit_ioc_unfilled_is_no_match() {
        let body = exchange_ok(json!({
            "error": "Order could not immediately match against any resting orders."
        }));
        match parse_exchange_submit(&body, "0x1".into(), json!({}), "cloid") {
            SubmitResult::NoMatch { message, .. } => {
                assert!(message.contains("immediately match"));
            }
            other => panic!("expected NoMatch, got {other:?}"),
        }
    }

    #[test]
    fn parse_submit_min_notional_is_no_match() {
        let body = exchange_ok(json!({"error": "Order must have minimum value of 1 USDC."}));
        assert!(matches!(
            parse_exchange_submit(&body, "0x1".into(), json!({}), "cloid"),
            SubmitResult::NoMatch { .. }
        ));
    }

    #[test]
    fn parse_submit_no_liquidity_is_no_match() {
        let body = exchange_ok(json!({"error": "No liquidity available for market order."}));
        assert!(matches!(
            parse_exchange_submit(&body, "0x1".into(), json!({}), "cloid"),
            SubmitResult::NoMatch { .. }
        ));
    }

    #[test]
    fn parse_submit_filled_reads_avg_px() {
        let body = exchange_ok(json!({
            "filled": {"totalSz": "5", "avgPx": "0.55", "oid": 777}
        }));
        match parse_exchange_submit(&body, "0x1".into(), json!({}), "cloid") {
            SubmitResult::Ack {
                order_id,
                taking,
                avg_px,
                ..
            } => {
                assert_eq!(order_id, "777");
                assert_eq!(taking.unwrap().to_string(), "5");
                assert_eq!(avg_px.unwrap().to_string(), "0.55");
            }
            other => panic!("expected Ack, got {other:?}"),
        }
    }

    #[test]
    fn parse_submit_top_level_err_and_ok_share_classifier() {
        let err_body = json!({
            "status": "err",
            "response": "Order could not immediately match against any resting orders."
        });
        assert!(matches!(
            parse_exchange_submit(&err_body, "0x1".into(), json!({}), "cloid"),
            SubmitResult::NoMatch { .. }
        ));
        let ok_empty =
            json!({"status": "ok", "response": {"type": "order", "data": {"statuses": []}}});
        assert!(matches!(
            parse_exchange_submit(&ok_empty, "0x1".into(), json!({}), "cloid"),
            SubmitResult::Unknown { .. }
        ));
    }

    #[test]
    fn explicit_reject_is_failed_but_ambiguous_http_statuses_stay_unknown() {
        let body = json!({"error": "unauthorized"});
        match classify_http_submit(400, &body, "0x1".into(), json!({}), "cloid") {
            SubmitResult::Failed { status, .. } => assert_eq!(status, 400),
            other => panic!("expected Failed, got {other:?}"),
        }
        // 5xx / 408 / 425 / 429 未证明订单没被撮合，记零成交会漏记真实持仓。
        for status in [408u16, 425, 429, 500, 502, 503, 504] {
            match classify_http_submit(status, &body, "0x1".into(), json!({}), "cloid") {
                SubmitResult::Unknown { message, .. } => assert!(
                    message.contains(&status.to_string()),
                    "status {status}: {message}"
                ),
                other => panic!("expected Unknown for {status}, got {other:?}"),
            }
        }
    }

    #[test]
    fn transport_error_is_unknown() {
        match classify_submit_transport_error("connection timed out", "0x1".into(), json!({})) {
            SubmitResult::Unknown { message, .. } => {
                assert!(message.contains("timed out"));
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn http_200_ioc_error_stays_no_match() {
        let body = exchange_ok(json!({
            "error": "Order could not immediately match against any resting orders."
        }));
        assert!(matches!(
            classify_http_submit(200, &body, "0x1".into(), json!({}), "cloid"),
            SubmitResult::NoMatch { .. }
        ));
    }
}
