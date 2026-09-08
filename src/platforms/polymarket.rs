use super::{
    parse_decimal, require_positive, FillFinality, FillPage, MarketOrderRequest, OrderPoll,
    OrderSide, PreparedOrder, SubmitResult, TradeFill,
};
use crate::book::{BookStore, Level};
use crate::config::{Config, PolymarketFunderConfig, POLYMARKET};
use crate::domain::TopicKey;
use crate::error::{Error, Result};
use crate::signing::polymarket::{
    clob_auth_headers, l2_hmac_signature, order_hash_hex, sign_order, SignedOrder,
};
use crate::stats::MinuteStats;
use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;

const FUNDER_CURSOR_FILE: &str = "polymarket_funder_cursor";
const API_CREDS_FILE: &str = "polymarket_api_creds.json";
const USDC_BALANCE_CACHE_TTL: Duration = Duration::from_secs(10);
const USDC_BALANCE_MAX_REFRESH_ATTEMPTS: usize = 3;
const FAK_UNFILLED: &str = "no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.";
/// 每个账户在 `POLYMARKET_AUTH_TTL_SECS` 上叠加 10–30 分钟抖动，步长为整分钟。
const AUTH_TTL_JITTER_MIN_MINS: u64 = 10;
const AUTH_TTL_JITTER_MAX_MINS: u64 = 30;
/// 最早一条凭证距过期不足该时长时主动刷新。
const AUTH_REFRESH_LEAD_SECS: u64 = 10 * 60;
/// CLOB FAK 市价单精度：maker 最多 2 位小数，taker 最多 5 位小数。
/// 买单 USDC 向上取到分，避免隐含限价低于盘口；卖单金额仍向下截断。
const MARKET_MAKER_DECIMALS: u32 = 2;
const MARKET_TAKER_DECIMALS: u32 = 5;
// 官方 CLOB 客户端以 base64("0") 起始，以 base64("-1") 表示已读到末尾。
const TRADES_INITIAL_CURSOR: &str = "MA==";
const TRADES_END_CURSOR: &str = "LTE=";

#[derive(Clone)]
pub struct PolymarketAccount {
    pub funder: String,
    pub service: Option<String>,
    pub signature_type: u8,
    signer: PrivateKeySigner,
    api_key: String,
    api_secret: String,
    api_passphrase: String,
    created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredApiCreds {
    api_key: String,
    secret: String,
    passphrase: String,
    created_at: u64,
}

#[derive(Clone)]
pub struct PolymarketVenue {
    http: reqwest::Client,
    base: String,
    funders: Vec<PolymarketFunderConfig>,
    authed: Arc<Mutex<HashMap<String, PolymarketAccount>>>,
    init_lock: Arc<Mutex<()>>,
    cursor_path: PathBuf,
    creds_path: PathBuf,
    auth_ttl: Duration,
    tick_cache: Arc<Mutex<HashMap<String, Decimal>>>,
    neg_risk_cache: Arc<Mutex<HashMap<String, bool>>>,
    rr: Arc<Mutex<usize>>,
    usdc_balance_cache: Arc<Mutex<HashMap<String, FunderBalanceEntry>>>,
    usdc_balance_refreshes: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    stats: Arc<MinuteStats>,
}

#[derive(Clone, Copy, Default)]
struct FunderBalanceEntry {
    value: Option<(Decimal, Instant)>,
    generation: u64,
}

impl FunderBalanceEntry {
    fn get_fresh(self, now: Instant) -> Option<Decimal> {
        self.value.and_then(|(balance, fetched_at)| {
            (now.saturating_duration_since(fetched_at) < USDC_BALANCE_CACHE_TTL).then_some(balance)
        })
    }
}

impl PolymarketVenue {
    pub async fn connect(cfg: &Config, stats: Arc<MinuteStats>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let funders = cfg.polymarket_funders.clone();
        let cursor_path = PathBuf::from(FUNDER_CURSOR_FILE);
        let rr = load_funder_rr(&funders, &cursor_path);
        let venue = Self {
            http,
            base: cfg.polymarket_clob_url.trim_end_matches('/').to_string(),
            funders,
            authed: Arc::new(Mutex::new(HashMap::new())),
            init_lock: Arc::new(Mutex::new(())),
            cursor_path,
            creds_path: PathBuf::from(API_CREDS_FILE),
            auth_ttl: cfg.polymarket_auth_ttl,
            tick_cache: Arc::new(Mutex::new(HashMap::new())),
            neg_risk_cache: Arc::new(Mutex::new(HashMap::new())),
            rr: Arc::new(Mutex::new(rr)),
            usdc_balance_cache: Arc::new(Mutex::new(HashMap::new())),
            usdc_balance_refreshes: Arc::new(Mutex::new(HashMap::new())),
            stats,
        };
        if let Some(first) = venue.funders.get(rr).cloned() {
            venue.ensure_account(&first.funder_address).await?;
            tracing::info!(
                funder = %first.funder_address,
                idx = rr,
                total = venue.funders.len(),
                ttl_secs = venue.auth_ttl.as_secs(),
                jitter_secs = auth_ttl_jitter_secs(&first.funder_address),
                "polymarket first account ready"
            );
        }
        Ok(venue)
    }

    pub fn account_count(&self) -> usize {
        self.funders.len()
    }

    pub fn has_funder(&self, funder: &str) -> bool {
        funder_index(&self.funders, funder).is_some()
    }

    async fn ensure_account(&self, funder: &str) -> Result<PolymarketAccount> {
        let key = funder.to_ascii_lowercase();
        let ttl = effective_auth_ttl(self.auth_ttl, &key);
        let now = unix_secs();
        if let Some(account) = self.authed.lock().await.get(&key) {
            if creds_fresh(account.created_at, ttl, now) {
                return Ok(account.clone());
            }
        }
        let _gate = self.init_lock.lock().await;
        let now = unix_secs();
        if let Some(account) = self.authed.lock().await.get(&key) {
            if creds_fresh(account.created_at, ttl, now) {
                return Ok(account.clone());
            }
        }
        let cfg = self
            .funders
            .iter()
            .find(|item| item.funder_address.eq_ignore_ascii_case(funder))
            .cloned()
            .ok_or_else(|| Error::msg("unknown polymarket funder"))?;
        if let Some(stored) = load_fresh_api_cred(&self.creds_path, &key, ttl, now) {
            match account_from_creds(&cfg, &stored) {
                Ok(account) => {
                    tracing::info!(
                        funder = %cfg.funder_address,
                        age_secs = now.saturating_sub(stored.created_at),
                        "polymarket account loaded"
                    );
                    self.authed.lock().await.insert(key, account.clone());
                    return Ok(account);
                }
                Err(err) => {
                    tracing::warn!(
                        funder = %cfg.funder_address,
                        error = %err,
                        "polymarket stored creds unusable"
                    );
                }
            }
        }
        tracing::info!(funder = %cfg.funder_address, "polymarket account authenticating");
        let account = init_account(&self.http, &self.base, &cfg).await?;
        self.persist_authed(key, account.clone()).await;
        Ok(account)
    }

    /// 每分钟读一遍落盘凭证：只看 `created_at` 最早的地址，剩余不足 10 分钟则刷新。
    pub async fn refresh_oldest_expiring_auth(&self) -> Result<()> {
        if self.auth_ttl.is_zero() {
            return Ok(());
        }
        let creds = load_api_creds(&self.creds_path);
        let Some((funder, created_at)) = oldest_stored_cred(&creds) else {
            return Ok(());
        };
        if !self.has_funder(&funder) {
            tracing::warn!(
                funder = %funder,
                "oldest polymarket api creds funder not in config"
            );
            return Ok(());
        }
        let ttl = effective_auth_ttl(self.auth_ttl, &funder);
        let now = unix_secs();
        let remaining = auth_remaining_secs(created_at, ttl, now);
        if !auth_refresh_due(created_at, ttl, now) {
            return Ok(());
        }
        tracing::info!(
            funder = %funder,
            remaining_secs = remaining,
            ttl_secs = ttl.as_secs(),
            "polymarket auth refresh due"
        );
        let cfg = self
            .funders
            .iter()
            .find(|item| item.funder_address.eq_ignore_ascii_case(&funder))
            .cloned()
            .ok_or_else(|| Error::msg("unknown polymarket funder"))?;
        let _gate = self.init_lock.lock().await;
        let creds = load_api_creds(&self.creds_path);
        let Some((latest, created_at)) = oldest_stored_cred(&creds) else {
            return Ok(());
        };
        if latest != funder || !auth_refresh_due(created_at, ttl, unix_secs()) {
            return Ok(());
        }
        tracing::info!(funder = %cfg.funder_address, "polymarket account authenticating");
        let account = init_account(&self.http, &self.base, &cfg).await?;
        self.persist_authed(funder.to_ascii_lowercase(), account)
            .await;
        Ok(())
    }

    async fn persist_authed(&self, key: String, account: PolymarketAccount) {
        if let Err(err) = save_api_cred(&self.creds_path, &key, &stored_creds(&account)) {
            tracing::warn!(error = %err, "polymarket api creds persist failed");
        }
        self.authed.lock().await.insert(key, account);
    }

    async fn persist_rr(&self, rr: usize) {
        if let Some(funder) = self.funders.get(rr) {
            if let Err(err) = save_funder_rr(&self.cursor_path, &funder.funder_address) {
                tracing::warn!(error = %err, "polymarket funder cursor persist failed");
            }
        }
    }

    pub async fn next_funder(&self) -> Option<String> {
        if self.funders.is_empty() {
            return None;
        }
        let mut rr = self.rr.lock().await;
        let n = self.funders.len();
        let idx = *rr % n;
        *rr = (*rr + 1) % n;
        let next = self.funders[idx].funder_address.clone();
        self.persist_rr(*rr).await;
        Some(next)
    }

    pub async fn rotate_from(&self, current: &str) -> Option<String> {
        if self.funders.len() < 2 {
            return self.next_funder().await;
        }
        let idx = funder_index(&self.funders, current)?;
        let next_idx = (idx + 1) % self.funders.len();
        let mut rr = self.rr.lock().await;
        *rr = (next_idx + 1) % self.funders.len();
        self.persist_rr(*rr).await;
        Some(self.funders[next_idx].funder_address.clone())
    }

    pub async fn balance(&self, funder: &str) -> Result<Decimal> {
        self.stats.pm_balance_call();
        self.cached_usdc_balance(funder, || self.fetch_usdc_balance(funder))
            .await
    }

    async fn cached_usdc_balance<F, Fut>(&self, funder: &str, mut fetch: F) -> Result<Decimal>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<Decimal>>,
    {
        let key = funder.to_ascii_lowercase();
        if let Some(balance) = self
            .usdc_balance_cache
            .lock()
            .await
            .get(&key)
            .copied()
            .and_then(|entry| entry.get_fresh(Instant::now()))
        {
            self.stats.pm_balance_cache_hit();
            return Ok(balance);
        }
        let refresh = {
            let mut refreshes = self.usdc_balance_refreshes.lock().await;
            refreshes
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = refresh.lock().await;
        let started = Instant::now();
        // 持续下单可能让每次刷新都失效；限制重试，但不能回退到已判旧的余额。
        for _ in 0..USDC_BALANCE_MAX_REFRESH_ATTEMPTS {
            let generation = {
                let cache = self.usdc_balance_cache.lock().await;
                if let Some(balance) = cache
                    .get(&key)
                    .copied()
                    .and_then(|entry| entry.get_fresh(Instant::now()))
                {
                    self.stats.pm_balance_cache_hit();
                    return Ok(balance);
                }
                cache.get(&key).map(|entry| entry.generation).unwrap_or(0)
            };
            self.stats.pm_balance_refresh();
            let balance = match fetch().await {
                Ok(balance) => balance,
                Err(err) => {
                    self.stats.pm_balance_refresh_fail();
                    return Err(err);
                }
            };
            let mut cache = self.usdc_balance_cache.lock().await;
            let entry = cache.entry(key.clone()).or_default();
            if entry.generation != generation {
                continue;
            }
            entry.value = Some((balance, Instant::now()));
            tracing::debug!(funder = %key, ttl_secs = USDC_BALANCE_CACHE_TTL.as_secs(), "polymarket usdc balance cached");
            return Ok(balance);
        }
        self.stats.pm_balance_refresh_fail();
        tracing::error!(
            service = "polymarket",
            operation = "balance-allowance",
            funder = %key,
            attempts = USDC_BALANCE_MAX_REFRESH_ATTEMPTS,
            elapsed_ms = started.elapsed().as_millis() as u64,
            reason = "generation_conflict",
            "polymarket usdc balance refresh exhausted"
        );
        Err(Error::msg(format!(
            "polymarket usdc balance invalidated during all {USDC_BALANCE_MAX_REFRESH_ATTEMPTS} refresh attempts"
        )))
    }

    async fn fetch_usdc_balance(&self, funder: &str) -> Result<Decimal> {
        let account = self.ensure_account(funder).await?;
        let value = self
            .l2_json(
                &account,
                reqwest::Method::GET,
                "/balance-allowance",
                &[
                    ("asset_type", "COLLATERAL"),
                    ("signature_type", &account.signature_type.to_string()),
                ],
                None,
            )
            .await?;
        let raw = value
            .get("balance")
            .and_then(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .or_else(|| v.as_u64().map(|n| n.to_string()))
            })
            .unwrap_or_else(|| "0".into());
        let units: Decimal = raw.parse().unwrap_or(Decimal::ZERO);
        Ok(units / Decimal::from(1_000_000))
    }

    async fn invalidate_usdc_balance(&self, funder: &str) {
        let key = funder.to_ascii_lowercase();
        let mut cache = self.usdc_balance_cache.lock().await;
        let entry = cache.entry(key.clone()).or_default();
        entry.value = None;
        entry.generation = entry.generation.wrapping_add(1);
        tracing::debug!(funder = %key, "polymarket usdc balance cache invalidated");
    }

    pub async fn token_balance(&self, funder: &str, token_id: &str) -> Result<Decimal> {
        let account = self.ensure_account(funder).await?;
        let value = self
            .l2_json(
                &account,
                reqwest::Method::GET,
                "/balance-allowance",
                &[
                    ("asset_type", "CONDITIONAL"),
                    ("token_id", token_id),
                    ("signature_type", &account.signature_type.to_string()),
                ],
                None,
            )
            .await?;
        let raw = value
            .get("balance")
            .and_then(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .or_else(|| v.as_u64().map(|n| n.to_string()))
            })
            .unwrap_or_else(|| "0".into());
        let units: Decimal = raw.parse().unwrap_or(Decimal::ZERO);
        Ok(units / Decimal::from(1_000_000))
    }

    pub async fn settlement(
        &self,
        condition_id: &str,
    ) -> Result<crate::settlement::SettlementStatus> {
        if condition_id.is_empty() {
            return Err(Error::msg("missing polymarket condition_id"));
        }
        let started = Instant::now();
        let value: Value = self
            .http
            .get(format!("{}/markets/{condition_id}", self.base))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let status = crate::settlement::parse_polymarket_settlement(&value)?;
        tracing::info!(
            service = "polymarket",
            condition_id,
            settlement_state = status.kind(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "settlement queried"
        );
        Ok(status)
    }

    /// 保存当前市场费率快照，不宣称该费率是历史成交时的实扣费率。
    pub async fn fee_schedule(&self, condition_id: &str) -> Result<Value> {
        if condition_id.trim().is_empty() {
            return Err(Error::msg("missing polymarket condition_id"));
        }
        let mut url = url::Url::parse(&format!("{}/clob-markets", self.base))
            .map_err(|_| Error::msg("invalid polymarket fee schedule URL"))?;
        url.path_segments_mut()
            .map_err(|_| Error::msg("invalid polymarket fee schedule URL"))?
            .push(condition_id);
        let started = Instant::now();
        let mut http_status = None;
        let result = async {
            let response = self.http.get(url).send().await.map_err(|err| {
                Error::msg(if err.is_timeout() {
                    "polymarket fee schedule request timeout"
                } else {
                    "polymarket fee schedule transport error"
                })
            })?;
            let status = response.status();
            http_status = Some(status.as_u16());
            if !status.is_success() {
                return Err(Error::Http {
                    status: status.as_u16(),
                    message: "polymarket fee schedule HTTP error".into(),
                });
            }
            let raw: Value = response
                .json()
                .await
                .map_err(|_| Error::msg("polymarket fee schedule invalid JSON response"))?;
            parse_fee_schedule(&raw, condition_id, unix_millis())
        }
        .await;
        match &result {
            Ok(_) => tracing::debug!(
                service = "polymarket",
                operation = "fee_schedule",
                endpoint = "/clob-markets/{condition_id}",
                condition_id,
                http_status = ?http_status,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "polymarket fee schedule received"
            ),
            Err(err) => tracing::warn!(
                service = "polymarket",
                operation = "fee_schedule",
                endpoint = "/clob-markets/{condition_id}",
                condition_id,
                http_status = ?http_status,
                elapsed_ms = started.elapsed().as_millis() as u64,
                reason = %err,
                "polymarket fee schedule unavailable"
            ),
        }
        result
    }

    pub async fn rest_book(&self, token_id: &str) -> Result<(Vec<Level>, Vec<Level>, i64)> {
        let url = format!("{}/book", self.base);
        let value: Value = self
            .http
            .get(url)
            .query(&[("token_id", token_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(parse_book_json(&value))
    }

    pub async fn rest_books(&self, token_ids: &[String]) -> Result<Vec<Value>> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let started = Instant::now();
        let body: Vec<Value> = token_ids
            .iter()
            .map(|token_id| json!({ "token_id": token_id }))
            .collect();
        let resp = self
            .http
            .post(format!("{}/books", self.base))
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(Error::Http {
                status: status.as_u16(),
                message: redact_http(&text),
            });
        }
        let parsed: Value = serde_json::from_str(&text).unwrap_or(json!([]));
        let items = match parsed {
            Value::Array(items) => items,
            other => vec![other],
        };
        tracing::info!(
            requested = token_ids.len(),
            returned = items.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "polymarket books fetched"
        );
        Ok(items)
    }

    /// 仅在内存未命中时请求 `/tick-size`，结果写入 venue 缓存。
    pub async fn fetch_tick_size(&self, token_id: &str) -> Result<Decimal> {
        if let Some(v) = self.tick_cache.lock().await.get(token_id).copied() {
            return Ok(v);
        }
        let started = Instant::now();
        let value: Value = self
            .http
            .get(format!("{}/tick-size", self.base))
            .query(&[("token_id", token_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let tick = value
            .get("minimum_tick_size")
            .or_else(|| value.get("tickSize"))
            .or_else(|| value.get("tick_size"))
            .and_then(parse_decimal)
            .ok_or_else(|| Error::msg("tick-size response missing minimum_tick_size"))?;
        if tick <= Decimal::ZERO {
            return Err(Error::msg("invalid polymarket tick_size"));
        }
        tracing::info!(
            token_id,
            %tick,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "polymarket tick_size fetched"
        );
        self.tick_cache
            .lock()
            .await
            .insert(token_id.to_string(), tick);
        Ok(tick)
    }

    pub async fn neg_risk(&self, token_id: &str, fallback: Option<bool>) -> Result<bool> {
        if let Some(v) = fallback {
            self.neg_risk_cache
                .lock()
                .await
                .insert(token_id.to_string(), v);
            return Ok(v);
        }
        if let Some(v) = self.neg_risk_cache.lock().await.get(token_id).copied() {
            return Ok(v);
        }
        let value: Value = self
            .http
            .get(format!("{}/neg-risk", self.base))
            .query(&[("token_id", token_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let flag = value
            .get("neg_risk")
            .or_else(|| value.get("negRisk"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        self.neg_risk_cache
            .lock()
            .await
            .insert(token_id.to_string(), flag);
        Ok(flag)
    }

    pub async fn prepare_market_order(
        &self,
        funder: &str,
        req: &MarketOrderRequest,
    ) -> Result<PreparedOrder> {
        require_positive(req.shares, req.cap_price)?;
        let account = self.ensure_account(funder).await?;
        let tick = match req.tick_size {
            Some(v) => v,
            None => self.fetch_tick_size(&req.token_id).await?,
        };
        let neg_risk = self.neg_risk(&req.token_id, req.neg_risk).await?;
        let unsigned = build_unsigned_order(&account, req, tick)?;
        let signed = sign_order(&account.signer, unsigned, neg_risk).map_err(Error::msg)?;
        let order_hash = order_hash_hex(&signed, neg_risk).map_err(Error::msg)?;
        let envelope = signed_envelope(
            &signed,
            &req.token_id,
            req.side,
            req.shares,
            req.cap_price,
            tick,
            neg_risk,
            &order_hash,
        );
        let payload = order_submit_payload(&signed, &account.api_key);
        Ok(PreparedOrder {
            order_hash,
            envelope,
            payload,
            funder: Some(funder.to_string()),
        })
    }

    pub async fn post_prepared(&self, prepared: &PreparedOrder) -> Result<(SubmitResult, Value)> {
        let funder = prepared
            .funder
            .as_deref()
            .ok_or_else(|| Error::msg("missing funder on prepared order"))?;
        let account = self.ensure_account(funder).await?;
        let order_hash = prepared.order_hash.clone();
        let envelope = prepared.envelope.clone();
        let response = self
            .l2_json(
                &account,
                reqwest::Method::POST,
                "/order",
                &[],
                Some(&prepared.payload),
            )
            .await;
        // 提交结果无论明确与否，都不能继续复用提交前余额。
        self.invalidate_usdc_balance(funder).await;
        match response {
            Ok(body) => {
                let result = parse_submit(&body, order_hash, envelope);
                Ok((result, body))
            }
            Err(err) => {
                let response = super::submit_http_error_response(&err);
                Ok((classify_submit_error(&err, order_hash, envelope), response))
            }
        }
    }

    pub async fn market_order(
        &self,
        funder: &str,
        req: &MarketOrderRequest,
    ) -> Result<SubmitResult> {
        let prepared = self.prepare_market_order(funder, req).await?;
        Ok(self.post_prepared(&prepared).await?.0)
    }

    pub async fn poll_order(&self, funder: &str, order_id: &str) -> Result<OrderPoll> {
        let account = self.ensure_account(funder).await?;
        let path = format!("/data/order/{order_id}");
        match self
            .l2_json(&account, reqwest::Method::GET, &path, &[], None)
            .await
        {
            Ok(raw) => Ok(parse_order_poll(raw, order_id)),
            Err(Error::Http { status: 404, .. }) => Ok(OrderPoll {
                status: "not_found".into(),
                order_id: Some(order_id.into()),
                raw: json!({}),
                ..OrderPoll::default()
            }),
            Err(err) => Err(err),
        }
    }

    /// 每次只读一页，让调用方将 fills 与 progress 一起持久化；后页失败不丢前页进度。
    /// 读到 END 只证明本次历史扫描结束。下轮重新扫描，才能刷新未确认成交的状态。
    pub async fn poll_trade_page(
        &self,
        funder: &str,
        token_id: &str,
        progress: &Value,
    ) -> Result<FillPage> {
        let (cursor, mut seen) = trade_page_cursor(progress, funder, token_id)?;
        let account = self.ensure_account(funder).await?;
        let started = Instant::now();
        let raw = self
            .l2_json(
                &account,
                reqwest::Method::GET,
                "/data/trades",
                &[("asset_id", token_id), ("next_cursor", &cursor)],
                None,
            )
            .await?;
        let parsed = (|| {
            let next = json_str(&raw, &["next_cursor"])
                .ok_or_else(|| Error::msg("polymarket trades missing next_cursor"))?;
            if next != TRADES_END_CURSOR && (next == cursor || seen.contains(&next)) {
                return Err(Error::msg("polymarket trades repeated cursor"));
            }
            // 分页协议必须是 data 数组；缺失或畸形不能被解释成空历史。
            if !raw.get("data").is_some_and(Value::is_array) {
                return Err(Error::msg("polymarket trades missing data array"));
            }
            let fills = parse_trades(&raw)?;
            seen.push(cursor);
            let complete = next == TRADES_END_CURSOR;
            Ok(FillPage {
                fills,
                progress: json!({
                    "funder": funder.to_ascii_lowercase(),
                    "asset_id": token_id,
                    "next_cursor": next,
                    "seen_cursors": seen,
                }),
                complete,
                history_complete: complete,
            })
        })();
        match &parsed {
            Ok(page) => tracing::debug!(
                service = "polymarket",
                operation = "trade_page",
                funder,
                token_id,
                fills = page.fills.len(),
                complete = page.complete,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "polymarket trade page parsed"
            ),
            Err(err) => tracing::warn!(
                service = "polymarket",
                operation = "trade_page",
                funder,
                token_id,
                reason = %err,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "polymarket trade page invalid"
            ),
        }
        parsed
    }

    /// 旧接口不能表达进度，因此不允许把未完整的第一页当成完整历史交给调用方。
    pub async fn poll_trades(&self, funder: &str, token_id: &str) -> Result<Vec<TradeFill>> {
        let page = self.poll_trade_page(funder, token_id, &Value::Null).await?;
        if !page.complete {
            return Err(Error::msg(
                "polymarket trades require resumable poll_trade_page",
            ));
        }
        Ok(page.fills)
    }

    async fn l2_json(
        &self,
        account: &PolymarketAccount,
        method: reqwest::Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<&Value>,
    ) -> Result<Value> {
        let bytes = match body {
            Some(v) => serde_json::to_vec(v)?,
            None => Vec::new(),
        };
        let ts = unix_secs();
        let sig = l2_hmac_signature(&account.api_secret, ts, method.as_str(), path, &bytes)
            .map_err(Error::msg)?;
        let mut req = self
            .http
            .request(method.clone(), format!("{}{path}", self.base))
            .header("POLY_ADDRESS", format!("{:#x}", account.signer.address()))
            .header("POLY_API_KEY", &account.api_key)
            .header("POLY_PASSPHRASE", &account.api_passphrase)
            .header("POLY_SIGNATURE", sig)
            .header("POLY_TIMESTAMP", ts.to_string());
        if !query.is_empty() {
            req = req.query(query);
        }
        if body.is_some() {
            req = req.header("Content-Type", "application/json").body(bytes);
        }
        let poll_request = path == "/data/trades" || path.starts_with("/data/order/");
        let started = Instant::now();
        let resp = req.send().await.map_err(|err| {
            if poll_request {
                tracing::warn!(
                    service = "polymarket",
                    operation = %method,
                    endpoint = path,
                    funder = %account.funder,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    reason = if err.is_timeout() { "timeout" } else { "transport" },
                    "polymarket poll request failed"
                );
            }
            err
        })?;
        let status = resp.status();
        let text = match resp.text().await {
            Ok(text) => text,
            Err(err) if poll_request => {
                tracing::warn!(
                    service = "polymarket",
                    operation = %method,
                    endpoint = path,
                    funder = %account.funder,
                    http_status = status.as_u16(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    reason = "response_body",
                    "polymarket poll response failed"
                );
                return Err(err.into());
            }
            Err(_) => String::new(),
        };
        if poll_request {
            if status.is_success() || status.as_u16() == 404 {
                tracing::debug!(
                    service = "polymarket",
                    operation = %method,
                    endpoint = path,
                    funder = %account.funder,
                    http_status = status.as_u16(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "polymarket poll response received"
                );
            } else {
                tracing::warn!(
                    service = "polymarket",
                    operation = %method,
                    endpoint = path,
                    funder = %account.funder,
                    http_status = status.as_u16(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    reason = "http_status",
                    "polymarket poll request rejected"
                );
            }
        }
        if !status.is_success() {
            return Err(Error::Http {
                status: status.as_u16(),
                message: if poll_request {
                    "polymarket poll HTTP error".into()
                } else {
                    redact_http(&text)
                },
            });
        }
        if text.is_empty() || text == "null" {
            return Ok(json!({}));
        }
        Ok(serde_json::from_str(&text).unwrap_or_else(|_| json!({"raw": text})))
    }
}

fn funder_index(funders: &[PolymarketFunderConfig], addr: &str) -> Option<usize> {
    funders
        .iter()
        .position(|item| item.funder_address.eq_ignore_ascii_case(addr))
}

fn load_funder_rr(funders: &[PolymarketFunderConfig], path: &Path) -> usize {
    let Ok(text) = fs::read_to_string(path) else {
        return 0;
    };
    let addr = text.trim();
    if addr.is_empty() {
        return 0;
    }
    funder_index(funders, addr).unwrap_or(0)
}

fn save_funder_rr(path: &Path, funder: &str) -> std::io::Result<()> {
    let tmp = path.with_extension("cursor.tmp");
    fs::write(&tmp, funder)?;
    fs::rename(&tmp, path)
}

async fn init_account(
    http: &reqwest::Client,
    base: &str,
    cfg: &PolymarketFunderConfig,
) -> Result<PolymarketAccount> {
    let signer: PrivateKeySigner = cfg
        .wallet_private_key
        .parse()
        .map_err(|e| Error::msg(format!("invalid polymarket key: {e}")))?;
    let ts = unix_secs();
    let headers = clob_auth_headers(&signer, ts, 0).map_err(Error::msg)?;
    let create = send_auth(http, base, "/auth/api-key", reqwest::Method::POST, &headers).await;
    let body = match create {
        Ok(v) => v,
        Err(Error::Http { status: 400, .. }) => {
            send_auth(
                http,
                base,
                "/auth/derive-api-key",
                reqwest::Method::GET,
                &headers,
            )
            .await?
        }
        Err(err) => return Err(err),
    };
    let api_key =
        json_str(&body, &["apiKey", "key"]).ok_or_else(|| Error::msg("missing apiKey"))?;
    let api_secret = json_str(&body, &["secret"]).ok_or_else(|| Error::msg("missing secret"))?;
    let api_passphrase =
        json_str(&body, &["passphrase"]).ok_or_else(|| Error::msg("missing passphrase"))?;
    Ok(PolymarketAccount {
        funder: cfg.funder_address.clone(),
        service: cfg.service.clone(),
        signature_type: if cfg.is_wallet_v2 { 3 } else { 2 },
        signer,
        api_key,
        api_secret,
        api_passphrase,
        created_at: unix_secs(),
    })
}

fn account_from_creds(
    cfg: &PolymarketFunderConfig,
    creds: &StoredApiCreds,
) -> Result<PolymarketAccount> {
    let signer: PrivateKeySigner = cfg
        .wallet_private_key
        .parse()
        .map_err(|e| Error::msg(format!("invalid polymarket key: {e}")))?;
    Ok(PolymarketAccount {
        funder: cfg.funder_address.clone(),
        service: cfg.service.clone(),
        signature_type: if cfg.is_wallet_v2 { 3 } else { 2 },
        signer,
        api_key: creds.api_key.clone(),
        api_secret: creds.secret.clone(),
        api_passphrase: creds.passphrase.clone(),
        created_at: creds.created_at,
    })
}

fn stored_creds(account: &PolymarketAccount) -> StoredApiCreds {
    StoredApiCreds {
        api_key: account.api_key.clone(),
        secret: account.api_secret.clone(),
        passphrase: account.api_passphrase.clone(),
        created_at: account.created_at,
    }
}

fn oldest_stored_cred(creds: &HashMap<String, StoredApiCreds>) -> Option<(String, u64)> {
    creds
        .iter()
        .min_by(|a, b| {
            a.1.created_at
                .cmp(&b.1.created_at)
                .then_with(|| a.0.cmp(b.0))
        })
        .map(|(funder, cred)| (funder.clone(), cred.created_at))
}

fn auth_remaining_secs(created_at: u64, ttl: Duration, now: u64) -> u64 {
    ttl.as_secs().saturating_sub(now.saturating_sub(created_at))
}

fn auth_refresh_due(created_at: u64, ttl: Duration, now: u64) -> bool {
    !ttl.is_zero() && auth_remaining_secs(created_at, ttl, now) < AUTH_REFRESH_LEAD_SECS
}

fn creds_fresh(created_at: u64, ttl: Duration, now: u64) -> bool {
    ttl.is_zero() || now.saturating_sub(created_at) < ttl.as_secs()
}

fn auth_ttl_jitter_secs(funder: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in funder.to_ascii_lowercase().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let span = AUTH_TTL_JITTER_MAX_MINS - AUTH_TTL_JITTER_MIN_MINS + 1;
    (AUTH_TTL_JITTER_MIN_MINS + (hash % span)) * 60
}

fn effective_auth_ttl(base: Duration, funder: &str) -> Duration {
    if base.is_zero() {
        return base;
    }
    base.saturating_add(Duration::from_secs(auth_ttl_jitter_secs(funder)))
}

fn load_api_creds(path: &Path) -> HashMap<String, StoredApiCreds> {
    fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

fn load_fresh_api_cred(
    path: &Path,
    funder: &str,
    ttl: Duration,
    now: u64,
) -> Option<StoredApiCreds> {
    let creds = load_api_creds(path).remove(&funder.to_ascii_lowercase())?;
    creds_fresh(creds.created_at, ttl, now).then_some(creds)
}

fn save_api_cred(path: &Path, funder: &str, creds: &StoredApiCreds) -> std::io::Result<()> {
    let mut all = load_api_creds(path);
    all.insert(funder.to_ascii_lowercase(), creds.clone());
    let tmp = path.with_extension("json.tmp");
    let data = serde_json::to_vec_pretty(&all)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    fs::write(&tmp, data)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600));
    }
    fs::rename(&tmp, path)
}

async fn send_auth(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    method: reqwest::Method,
    headers: &[(String, String)],
) -> Result<Value> {
    let mut req = http.request(method, format!("{}{path}", base.trim_end_matches('/')));
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let resp = req.send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(Error::Http {
            status: status.as_u16(),
            message: redact_http(&text),
        });
    }
    Ok(serde_json::from_str(&text).unwrap_or(json!({})))
}

fn build_unsigned_order(
    account: &PolymarketAccount,
    req: &MarketOrderRequest,
    tick: Decimal,
) -> Result<SignedOrder> {
    let price = if req.side == OrderSide::Buy {
        crate::calc::align_polymarket_price(req.cap_price, tick)
    } else {
        crate::calc::align_polymarket_sell_price(req.cap_price, tick)
    };
    if price < tick {
        return Err(Error::msg("price below tick"));
    }
    let (maker_amount, taker_amount) = market_order_base_units(req.side, req.shares, price)?;
    let maker: Address = account
        .funder
        .parse()
        .map_err(|_| Error::msg("invalid funder address"))?;
    let signer = if account.signature_type == 3 {
        maker
    } else {
        account.signer.address()
    };
    Ok(SignedOrder {
        builder: B256::ZERO,
        expiration: 0,
        maker,
        maker_amount,
        metadata: B256::ZERO,
        order_type: "FAK".into(),
        salt: rand::random::<u64>() & ((1u64 << 53) - 1),
        side: req.side.as_str().into(),
        signature: "0x".into(),
        signature_type: account.signature_type,
        signer,
        taker_amount,
        timestamp: unix_millis(),
        token_id: req.token_id.clone(),
        post_only: false,
    })
}

/// CLOB V2 要求 `salt` 为 JSON number，其余金额/时间字段为十进制字符串。
fn order_submit_payload(signed: &SignedOrder, owner: &str) -> Value {
    json!({
        "deferExec": false,
        "order": {
            "builder": format!("{:#x}", signed.builder),
            "expiration": signed.expiration.to_string(),
            "maker": format!("{:#x}", signed.maker),
            "makerAmount": signed.maker_amount.to_string(),
            "metadata": format!("{:#x}", signed.metadata),
            "salt": signed.salt,
            "side": signed.side,
            "signature": signed.signature,
            "signatureType": signed.signature_type,
            "signer": format!("{:#x}", signed.signer),
            "takerAmount": signed.taker_amount.to_string(),
            "timestamp": signed.timestamp.to_string(),
            "tokenId": signed.token_id
        },
        "orderType": "FAK",
        "owner": owner
    })
}

fn signed_envelope(
    order: &SignedOrder,
    token_id: &str,
    side: OrderSide,
    shares: Decimal,
    price: Decimal,
    tick: Decimal,
    neg_risk: bool,
    order_hash: &str,
) -> Value {
    json!({
        "order_hash": order_hash,
        "order_version": 2,
        "order_type": "FAK",
        "token_id": token_id,
        "side": side.as_str(),
        "shares": shares.to_string(),
        "price": price.to_string(),
        "tick_size": tick.to_string(),
        "neg_risk": neg_risk,
        "signed_order": {
            "builder": format!("{:#x}", order.builder),
            "expiration": order.expiration,
            "maker": format!("{:#x}", order.maker),
            "maker_amount": order.maker_amount,
            "metadata": format!("{:#x}", order.metadata),
            "order_type": "FAK",
            "salt": order.salt,
            "side": order.side,
            "signature": order.signature,
            "signature_type": order.signature_type,
            "signer": format!("{:#x}", order.signer),
            "taker_amount": order.taker_amount,
            "timestamp": order.timestamp,
            "token_id": order.token_id,
            "post_only": false
        }
    })
}

pub fn classify_submit_error(err: &Error, order_hash: String, envelope: Value) -> SubmitResult {
    match err {
        Error::Http { status, message }
            if *status >= 400 && crate::platforms::http_status_proves_reject(*status) =>
        {
            SubmitResult::Failed {
                order_hash,
                envelope,
                status: *status,
                message: message.clone(),
            }
        }
        _ => SubmitResult::Unknown {
            order_id: None,
            order_hash,
            envelope,
            message: err.to_string(),
        },
    }
}

pub fn parse_submit(body: &Value, order_hash: String, envelope: Value) -> SubmitResult {
    let success = body
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let status = body
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let order_id = body
        .get("orderID")
        .or_else(|| body.get("orderId"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let error = body
        .get("errorMsg")
        .or_else(|| body.get("error"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if error.contains("FAK") || error == FAK_UNFILLED || status == "unmatched" {
        return SubmitResult::NoMatch {
            order_hash,
            envelope,
            message: error,
        };
    }
    if success && !order_id.is_empty() && matches!(status.as_str(), "live" | "matched") {
        return SubmitResult::Ack {
            order_id,
            order_hash,
            envelope,
            making: body.get("makingAmount").and_then(parse_decimal),
            taking: body.get("takingAmount").and_then(parse_decimal),
            avg_px: None,
        };
    }
    SubmitResult::Unknown {
        order_id: if order_id.is_empty() {
            None
        } else {
            Some(order_id)
        },
        order_hash,
        envelope,
        message: error,
    }
}

pub fn parse_book_json(value: &Value) -> (Vec<Level>, Vec<Level>, i64) {
    let ts = value
        .get("timestamp")
        .and_then(|v| {
            v.as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| v.as_i64())
        })
        .unwrap_or(0);
    (
        parse_levels(value.get("bids")),
        parse_levels(value.get("asks")),
        ts,
    )
}

fn apply_book_tick(books: &mut BookStore, token: &str, payload: &Value) {
    if let Some(tick) = parse_tick_size(payload) {
        books.set_tick_size(POLYMARKET, token, tick);
    }
}

fn parse_tick_size(value: &Value) -> Option<Decimal> {
    value
        .get("tick_size")
        .or_else(|| value.get("tickSize"))
        .or_else(|| value.get("minimum_tick_size"))
        .or_else(|| value.get("order_price_min_tick_size"))
        .and_then(parse_decimal)
        .filter(|tick| *tick > Decimal::ZERO)
}

fn parse_levels(value: Option<&Value>) -> Vec<Level> {
    let Some(Value::Array(items)) = value else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let price = item.get("price").and_then(parse_decimal)?;
            let size = item.get("size").and_then(parse_decimal)?;
            Some(Level { price, size })
        })
        .collect()
}

fn parse_fee_schedule(raw: &Value, condition_id: &str, observed_at_ms: u64) -> Result<Value> {
    // 官方精简市场结构使用 c；若同时有长字段，两者都不能与请求身份冲突。
    for field in ["c", "condition_id"] {
        if let Some(value) = raw.get(field) {
            if !value
                .as_str()
                .is_some_and(|id| id.eq_ignore_ascii_case(condition_id))
            {
                return Err(Error::msg("polymarket fee schedule condition mismatch"));
            }
        }
    }
    let rate = raw
        .pointer("/fd/r")
        .and_then(parse_decimal)
        .filter(|rate| *rate >= Decimal::ZERO && *rate <= Decimal::ONE)
        .ok_or_else(|| Error::msg("polymarket fee schedule missing or invalid rate"))?;
    Ok(json!({
        "condition_id": condition_id,
        "rate": rate.normalize().to_string(),
        "observed_at_ms": observed_at_ms,
        "source": "clob-markets",
        "currency": "pUSD",
        "valuation": "1 USD",
        "rounding": "midpoint_away_from_zero_5dp",
    }))
}

pub fn parse_order_poll(raw: Value, order_id: &str) -> OrderPoll {
    let shares = raw.get("size_matched").and_then(parse_decimal);
    let original_shares = raw.get("original_size").and_then(parse_decimal);
    let remaining_shares = original_shares.zip(shares).and_then(|(original, matched)| {
        (matched >= Decimal::ZERO && original >= matched)
            .then(|| original.checked_sub(matched))
            .flatten()
    });
    OrderPoll {
        found: true,
        status: json_str(&raw, &["status"]).unwrap_or_default(),
        order_id: json_str(&raw, &["id"]).or_else(|| Some(order_id.to_string())),
        shares,
        price: raw.get("price").and_then(parse_decimal),
        fee: raw
            .get("fee_amount")
            .or_else(|| raw.get("fee"))
            .and_then(parse_decimal),
        original_shares,
        remaining_shares,
        client_order_id: json_str(&raw, &["client_order_id"]),
        coin: json_str(&raw, &["asset_id"]),
        // 不补写 raw.associate_trades；缺失/畸形与协议明确返回 [] 必须由上层区分。
        associated_trades: raw
            .get("associate_trades")
            .and_then(Value::as_array)
            .and_then(|ids| {
                ids.iter()
                    .map(|id| id.as_str().filter(|id| !id.is_empty()).map(str::to_string))
                    .collect::<Option<Vec<_>>>()
            })
            .unwrap_or_default(),
        raw,
    }
}

fn trade_page_cursor(
    progress: &Value,
    funder: &str,
    token_id: &str,
) -> Result<(String, Vec<String>)> {
    if progress.is_null() || progress.as_object().is_some_and(|obj| obj.is_empty()) {
        return Ok((TRADES_INITIAL_CURSOR.into(), Vec::new()));
    }
    let invalid = || Error::msg("invalid polymarket trade pagination progress");
    if !progress
        .get("funder")
        .and_then(Value::as_str)
        .is_some_and(|previous| previous.eq_ignore_ascii_case(funder))
        || progress.get("asset_id").and_then(Value::as_str) != Some(token_id)
    {
        return Err(invalid());
    }
    let cursor = json_str(progress, &["next_cursor"]).ok_or_else(invalid)?;
    let seen: Vec<String> = progress
        .get("seen_cursors")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?
        .iter()
        .map(|v| {
            v.as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .ok_or_else(invalid)
        })
        .collect::<Result<_>>()?;
    let mut unique = std::collections::HashSet::new();
    if seen.first().map(String::as_str) != Some(TRADES_INITIAL_CURSOR)
        || seen
            .iter()
            .any(|c| c == TRADES_END_CURSOR || !unique.insert(c))
        || seen.contains(&cursor)
    {
        return Err(invalid());
    }
    if cursor == TRADES_END_CURSOR {
        return Ok((TRADES_INITIAL_CURSOR.into(), Vec::new()));
    }
    Ok((cursor, seen))
}

fn required_trade_string(item: &Value, field: &str) -> Result<String> {
    json_str(item, &[field])
        .ok_or_else(|| Error::msg(format!("polymarket trade missing or invalid {field}")))
}

fn optional_trade_decimal(item: &Value, fields: &[&str]) -> Result<Option<Decimal>> {
    for field in fields {
        if let Some(value) = item.get(*field).filter(|value| !value.is_null()) {
            return parse_decimal(value)
                .filter(|value| *value >= Decimal::ZERO)
                .map(Some)
                .ok_or_else(|| Error::msg(format!("polymarket trade invalid {field}")));
        }
    }
    Ok(None)
}

fn parse_trade_fill(item: &Value, maker: Option<&Value>) -> Result<TradeFill> {
    let (record, role, order_field, size_field) = match maker {
        Some(maker) => (maker, "maker", "order_id", "matched_amount"),
        None => (item, "taker", "taker_order_id", "size"),
    };
    let trade_id = required_trade_string(item, "id")?;
    let order_id = required_trade_string(record, order_field)?;
    let coin = required_trade_string(record, "asset_id")?;
    let shares = optional_trade_decimal(record, &[size_field])?
        .filter(|value| *value > Decimal::ZERO)
        .ok_or_else(|| Error::msg(format!("polymarket trade missing or invalid {size_field}")))?;
    let price = optional_trade_decimal(record, &["price"])?
        .filter(|value| *value > Decimal::ZERO && *value <= Decimal::ONE)
        .ok_or_else(|| Error::msg("polymarket trade missing or invalid price"))?;
    let finality = match item.get("status").and_then(Value::as_str) {
        Some("CONFIRMED") => FillFinality::Confirmed,
        Some("FAILED") => FillFinality::Failed,
        _ => FillFinality::Pending,
    };
    // maker 的费率/实收费用必须来自子订单；不得套用 taker 金额或猜测为零。
    let fee = optional_trade_decimal(record, &["fee_amount", "fee"])?;
    let fee_rate_bps = optional_trade_decimal(record, &["fee_rate_bps"])?;
    let mut fee_token = None;
    for field in ["fee_token", "feeToken", "fee_currency", "feeCurrency"] {
        if record.get(field).is_some_and(|value| !value.is_null()) {
            fee_token = Some(required_trade_string(record, field)?);
            break;
        }
    }
    let mut raw = record.clone();
    raw["role"] = json!(role);
    if maker.is_some() {
        // 保留原始整笔 trade 及 maker 子订单，顶层 side/outcome 仍是该 maker 的原值。
        raw["taker_trade"] = item.clone();
        raw["maker_order"] = record.clone();
        raw["id"] = json!(trade_id);
        raw["size"] = json!(shares.to_string());
        if let Some(status) = item.get("status") {
            raw["status"] = status.clone();
        }
    }
    Ok(TradeFill {
        trade_id,
        order_id: Some(order_id.clone()),
        order_ids: vec![order_id],
        coin: Some(coin),
        shares,
        price,
        fee,
        fee_rate_bps,
        fee_token,
        finality,
        raw,
    })
}

pub fn parse_trades(raw: &Value) -> Result<Vec<TradeFill>> {
    let items = if raw.is_array() {
        raw.as_array()
    } else {
        raw.get("data").and_then(Value::as_array)
    }
    .ok_or_else(|| Error::msg("polymarket trades missing data array"))?;
    let mut fills = Vec::new();
    for item in items {
        let taker = parse_trade_fill(item, None)?;
        let makers = item
            .get("maker_orders")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::msg("polymarket trade missing maker_orders array"))?;
        let mut maker_ids = std::collections::HashSet::new();
        fills.push(taker);
        for maker in makers {
            let fill = parse_trade_fill(item, Some(maker))?;
            if !maker_ids.insert(fill.order_id.clone()) {
                return Err(Error::msg("polymarket trade duplicate maker order"));
            }
            fills.push(fill);
        }
    }
    Ok(fills)
}

pub fn apply_ws_message(
    books: &mut BookStore,
    payload: &Value,
    now: Instant,
) -> Vec<(String, bool)> {
    let mut changed = Vec::new();
    let event = payload
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if event == "book" {
        let token = payload
            .get("asset_id")
            .or_else(|| payload.get("assetId"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if token.is_empty() {
            return changed;
        }
        let (bids, asks, ts) = parse_book_json(payload);
        if books.replace_snapshot(POLYMARKET, token, bids, asks, ts, now) {
            apply_book_tick(books, token, payload);
            changed.push((token.to_string(), true));
        }
    } else if event == "tick_size_change" {
        let token = payload
            .get("asset_id")
            .or_else(|| payload.get("assetId"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if token.is_empty() {
            return changed;
        }
        if let Some(tick) = payload
            .get("new_tick_size")
            .or_else(|| payload.get("newTickSize"))
            .and_then(parse_decimal)
        {
            books.set_tick_size(POLYMARKET, token, tick);
            changed.push((token.to_string(), false));
        }
    } else if event == "price_change" {
        let ts = payload
            .get("timestamp")
            .and_then(|v| {
                v.as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| v.as_i64())
            })
            .unwrap_or(unix_millis() as i64);
        if let Some(arr) = payload.get("price_changes").and_then(|v| v.as_array()) {
            for change in arr {
                let token = change
                    .get("asset_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let price = change.get("price").and_then(parse_decimal);
                let size = change.get("size").and_then(parse_decimal);
                let side = change.get("side").and_then(|v| v.as_str()).unwrap_or("");
                if token.is_empty() || price.is_none() || size.is_none() {
                    continue;
                }
                let is_bid = side.eq_ignore_ascii_case("BUY") || side.eq_ignore_ascii_case("BID");
                if books.apply_level(
                    POLYMARKET,
                    token,
                    is_bid,
                    price.unwrap(),
                    size.unwrap(),
                    ts,
                    now,
                ) {
                    changed.push((token.to_string(), true));
                }
            }
        }
    }
    changed
}

pub fn apply_rest_books(
    books: &mut BookStore,
    payloads: &[Value],
    now: Instant,
) -> (Vec<String>, usize) {
    let mut applied = Vec::new();
    let mut skipped_old = 0usize;
    for payload in payloads {
        let token = payload
            .get("asset_id")
            .or_else(|| payload.get("assetId"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if token.is_empty() {
            continue;
        }
        let (bids, asks, ts) = parse_book_json(payload);
        if books.replace_snapshot(POLYMARKET, token, bids, asks, ts, now) {
            apply_book_tick(books, token, payload);
            applied.push(token.to_string());
        } else {
            skipped_old += 1;
        }
    }
    (applied, skipped_old)
}

pub async fn run_market_ws(
    url: String,
    books: Arc<Mutex<BookStore>>,
    calc_tx: mpsc::Sender<TopicKey>,
    mut sub_rx: mpsc::Receiver<Vec<String>>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut subscribed = Vec::new();
    loop {
        if *shutdown.borrow() {
            break;
        }
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _)) => {
                tracing::info!("polymarket market ws connected");
                let (mut write, mut read) = ws.split();
                if !subscribed.is_empty() {
                    let _ = write
                        .send(Message::Text(
                            json!({"operation":"subscribe","assets_ids": subscribed})
                                .to_string()
                                .into(),
                        ))
                        .await;
                }
                let mut ping = tokio::time::interval(Duration::from_secs(10));
                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if write.send(Message::Text("PING".into())).await.is_err() {
                                break;
                            }
                        }
                        msg = sub_rx.recv() => {
                            let Some(tokens) = msg else { return; };
                            let dropped: Vec<_> = subscribed
                                .iter()
                                .filter(|id| !tokens.contains(*id))
                                .cloned()
                                .collect();
                            if !dropped.is_empty() {
                                if write.send(Message::Text(
                                    json!({"operation":"unsubscribe","assets_ids": dropped}).to_string().into()
                                )).await.is_err() {
                                    break;
                                }
                            }
                            subscribed = tokens;
                            if write.send(Message::Text(
                                json!({"operation":"subscribe","assets_ids": subscribed}).to_string().into()
                            )).await.is_err() {
                                break;
                            }
                        }
                        incoming = read.next() => {
                            let Some(Ok(msg)) = incoming else { break; };
                            let text = match msg {
                                Message::Text(t) => t.to_string(),
                                Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                                Message::Ping(p) => {
                                    let _ = write.send(Message::Pong(p)).await;
                                    continue;
                                }
                                _ => continue,
                            };
                            handle_ws_text(&text, &books, &calc_tx).await;
                        }
                        _ = wait_shutdown(&shutdown) => return,
                    }
                }
                books.lock().await.mark_platform_stale(POLYMARKET);
            }
            Err(err) => tracing::warn!(error = %err, "polymarket ws connect failed"),
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

async fn handle_ws_text(
    text: &str,
    books: &Arc<Mutex<BookStore>>,
    calc_tx: &mpsc::Sender<TopicKey>,
) {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "PING" || trimmed == "PONG" || trimmed == "[]" {
        return;
    }
    let parsed: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return,
    };
    let messages = match parsed {
        Value::Array(items) => items,
        other => vec![other],
    };
    let now = Instant::now();
    let mut topics = Vec::new();
    {
        let mut store = books.lock().await;
        for payload in &messages {
            for (token, _) in apply_ws_message(&mut store, payload, now) {
                topics.extend(store.topics_for(POLYMARKET, &token));
            }
        }
    }
    for topic in topics {
        let _ = calc_tx.send(topic).await;
    }
}

fn market_order_base_units(side: OrderSide, size: Decimal, price: Decimal) -> Result<(u128, u128)> {
    let (maker, taker) = match side {
        OrderSide::Buy => {
            let shares = size.trunc_with_scale(MARKET_TAKER_DECIMALS);
            let usdc = (shares * price).round_dp_with_strategy(
                MARKET_MAKER_DECIMALS,
                RoundingStrategy::ToPositiveInfinity,
            );
            (usdc, shares)
        }
        OrderSide::Sell => {
            let shares = size.trunc_with_scale(MARKET_MAKER_DECIMALS);
            let usdc = (shares * price).trunc_with_scale(MARKET_TAKER_DECIMALS);
            (shares, usdc)
        }
    };
    let maker_amount = base_units(maker);
    let taker_amount = base_units(taker);
    if maker_amount == 0 || taker_amount == 0 {
        return Err(Error::msg("order amounts round to zero"));
    }
    Ok((maker_amount, taker_amount))
}

fn base_units(value: Decimal) -> u128 {
    let scaled = (value * Decimal::from(1_000_000)).round();
    scaled.to_u128().unwrap_or(0)
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn json_str(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = value.get(*key).and_then(|v| v.as_str()) {
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn redact_http(text: &str) -> String {
    text.chars().take(300).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn cache_test_venue() -> PolymarketVenue {
        PolymarketVenue {
            http: reqwest::Client::new(),
            base: "http://127.0.0.1".into(),
            funders: Vec::new(),
            authed: Arc::new(Mutex::new(HashMap::new())),
            init_lock: Arc::new(Mutex::new(())),
            cursor_path: PathBuf::new(),
            creds_path: PathBuf::new(),
            auth_ttl: Duration::from_secs(1),
            tick_cache: Arc::new(Mutex::new(HashMap::new())),
            neg_risk_cache: Arc::new(Mutex::new(HashMap::new())),
            rr: Arc::new(Mutex::new(0)),
            usdc_balance_cache: Arc::new(Mutex::new(HashMap::new())),
            usdc_balance_refreshes: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(MinuteStats::new()),
        }
    }

    #[test]
    fn funder_balance_cache_expires_after_ten_seconds() {
        let now = Instant::now();
        let fresh = FunderBalanceEntry {
            value: Some((Decimal::ONE, now - Duration::from_secs(9))),
            generation: 0,
        };
        let expired = FunderBalanceEntry {
            value: Some((Decimal::ONE, now - Duration::from_secs(10))),
            generation: 0,
        };
        assert_eq!(fresh.get_fresh(now), Some(Decimal::ONE));
        assert_eq!(expired.get_fresh(now), None);
    }

    #[tokio::test]
    async fn funder_balance_cache_reuses_and_normalizes_address() {
        let venue = cache_test_venue();
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = || {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Decimal::from(11))
            }
        };
        assert_eq!(
            venue.cached_usdc_balance("0xAbC", fetch).await.unwrap(),
            Decimal::from(11)
        );
        assert_eq!(
            venue.cached_usdc_balance("0xaBc", fetch).await.unwrap(),
            Decimal::from(11)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        venue.invalidate_usdc_balance("0xABC").await;
        assert_eq!(
            venue.cached_usdc_balance("0xabc", fetch).await.unwrap(),
            Decimal::from(11)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn concurrent_same_funder_uses_single_refresh() {
        let venue = cache_test_venue();
        let calls = Arc::new(AtomicUsize::new(0));
        let futures = (0..8).map(|_| {
            let venue = venue.clone();
            let calls = calls.clone();
            async move {
                venue
                    .cached_usdc_balance("0xabc", || async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok(Decimal::from(13))
                    })
                    .await
            }
        });
        let results = futures_util::future::join_all(futures).await;
        assert!(results
            .into_iter()
            .all(|value| value.unwrap() == Decimal::from(13)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn different_funders_refresh_independently() {
        let venue = cache_test_venue();
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let fetch = |value| {
            let active = active.clone();
            let peak = peak.clone();
            async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(Decimal::from(value))
            }
        };
        let (a, b) = tokio::join!(
            venue.cached_usdc_balance("0xa", || fetch(1)),
            venue.cached_usdc_balance("0xb", || fetch(2))
        );
        assert_eq!(a.unwrap(), Decimal::ONE);
        assert_eq!(b.unwrap(), Decimal::from(2));
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn funder_balance_failures_are_not_cached_and_invalidation_wins() {
        let venue = cache_test_venue();
        let failed_calls = AtomicUsize::new(0);
        let error = venue
            .cached_usdc_balance("0xa", || async {
                failed_calls.fetch_add(1, Ordering::SeqCst);
                Err(Error::msg("fail"))
            })
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Msg(message) if message == "fail"));
        assert_eq!(failed_calls.load(Ordering::SeqCst), 1);
        let failed = venue.stats.snapshot_and_reset();
        assert_eq!(failed.pm_balance_refresh, 1);
        assert_eq!(failed.pm_balance_refresh_fail, 1);
        assert_eq!(failed.pm_balance_cache_hit, 0);
        assert!(!venue.usdc_balance_cache.lock().await.contains_key("0xa"));
        let calls = Arc::new(AtomicUsize::new(0));
        let value = venue
            .cached_usdc_balance("0xa", || {
                let venue = venue.clone();
                let calls = calls.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        venue.invalidate_usdc_balance("0xa").await;
                        Ok(Decimal::from(20))
                    } else {
                        Ok(Decimal::from(15))
                    }
                }
            })
            .await
            .unwrap();
        assert_eq!(value, Decimal::from(15));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let recovered = venue.stats.snapshot_and_reset();
        assert_eq!(recovered.pm_balance_refresh, 2);
        assert_eq!(recovered.pm_balance_refresh_fail, 0);
    }

    #[tokio::test]
    async fn funder_balance_generation_conflicts_exhaust_after_three_fetches() {
        let venue = cache_test_venue();
        let calls = AtomicUsize::new(0);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            venue.cached_usdc_balance("0xa", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                venue.invalidate_usdc_balance("0xa").await;
                tokio::task::yield_now().await;
                Ok(Decimal::from(20))
            }),
        )
        .await
        .expect("generation conflicts must terminate without an unbounded retry");
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            Error::Msg(message)
                if message == "polymarket usdc balance invalidated during all 3 refresh attempts"
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let stats = venue.stats.snapshot_and_reset();
        assert_eq!(stats.pm_balance_refresh, 3);
        assert_eq!(stats.pm_balance_refresh_fail, 1);
        assert_eq!(stats.pm_balance_cache_hit, 0);
        assert_eq!(stats.pm_balance_call, 0);
        assert!(venue.usdc_balance_cache.lock().await["0xa"].value.is_none());
        let refresh = venue.usdc_balance_refreshes.lock().await["0xa"].clone();
        assert!(refresh.try_lock().is_ok());
        let recovered = tokio::time::timeout(
            Duration::from_secs(1),
            venue.cached_usdc_balance("0xa", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Decimal::from(15))
            }),
        )
        .await
        .expect("refresh lock must be released after exhausted attempts")
        .unwrap();
        assert_eq!(recovered, Decimal::from(15));
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn funder_balance_generation_conflicts_can_succeed_on_third_fetch() {
        let venue = cache_test_venue();
        let calls = AtomicUsize::new(0);
        let value = tokio::time::timeout(
            Duration::from_secs(1),
            venue.cached_usdc_balance("0xa", || async {
                let attempt = calls.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    venue.invalidate_usdc_balance("0xa").await;
                    tokio::task::yield_now().await;
                    Ok(Decimal::from(20))
                } else {
                    Ok(Decimal::from(15))
                }
            }),
        )
        .await
        .expect("third fetch must complete")
        .unwrap();
        assert_eq!(value, Decimal::from(15));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let stats = venue.stats.snapshot_and_reset();
        assert_eq!(stats.pm_balance_refresh, 3);
        assert_eq!(stats.pm_balance_refresh_fail, 0);
        assert_eq!(stats.pm_balance_cache_hit, 0);
        assert_eq!(stats.pm_balance_call, 0);
        let cached = venue
            .cached_usdc_balance("0xa", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Decimal::from(99))
            })
            .await
            .unwrap();
        assert_eq!(cached, Decimal::from(15));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let cached_stats = venue.stats.snapshot_and_reset();
        assert_eq!(cached_stats.pm_balance_cache_hit, 1);
        assert_eq!(cached_stats.pm_balance_refresh, 0);
        assert_eq!(cached_stats.pm_balance_refresh_fail, 0);
    }

    #[test]
    fn order_submit_payload_matches_clob_v2_wire_types() {
        let signed = SignedOrder {
            builder: B256::ZERO,
            expiration: 0,
            maker: Address::ZERO,
            maker_amount: 132_0000,
            metadata: B256::ZERO,
            order_type: "FAK".into(),
            salt: 479_249_096_354,
            side: "BUY".into(),
            signature: "0xabc".into(),
            signature_type: 2,
            signer: Address::ZERO,
            taker_amount: 3_000_000,
            timestamp: 1_780_000_000_000,
            token_id: "1".into(),
            post_only: false,
        };
        let payload = order_submit_payload(&signed, "api-key");
        let order = payload.get("order").expect("order");
        assert!(order.get("salt").unwrap().is_number());
        assert_eq!(order.get("salt").unwrap().as_u64(), Some(479_249_096_354));
        assert!(order.get("expiration").unwrap().is_string());
        assert!(order.get("makerAmount").unwrap().is_string());
        assert!(order.get("takerAmount").unwrap().is_string());
        assert!(order.get("timestamp").unwrap().is_string());
        assert!(order.get("signatureType").unwrap().is_number());
        assert_eq!(order.get("side").and_then(|v| v.as_str()), Some("BUY"));
        assert_eq!(order.get("signatureType").and_then(|v| v.as_u64()), Some(2));
    }

    #[test]
    fn parses_fak_unfilled() {
        let body = json!({"success": false, "status": "unmatched", "orderID": "", "errorMsg": FAK_UNFILLED});
        match parse_submit(&body, "0x1".into(), json!({})) {
            SubmitResult::NoMatch { .. } => {}
            other => panic!("expected no match, got {other:?}"),
        }
    }

    #[test]
    fn explicit_reject_is_failed_but_ambiguous_http_statuses_stay_unknown() {
        let fak = Error::Http {
            status: 400,
            message: FAK_UNFILLED.into(),
        };
        match classify_submit_error(&fak, "0x1".into(), json!({})) {
            SubmitResult::Failed { status, .. } => assert_eq!(status, 400),
            other => panic!("expected Failed, got {other:?}"),
        }
        // 5xx / 408 / 425 / 429 未证明订单没被撮合，记零成交会漏记真实持仓。
        for status in [408u16, 425, 429, 500, 502, 503, 504] {
            let err = Error::Http {
                status,
                message: "bad gateway".into(),
            };
            match classify_submit_error(&err, "0x1".into(), json!({})) {
                SubmitResult::Unknown { message, .. } => {
                    assert!(
                        message.contains("bad gateway"),
                        "status {status}: {message}"
                    )
                }
                other => panic!("expected Unknown for {status}, got {other:?}"),
            }
        }
    }

    #[test]
    fn transport_error_is_unknown() {
        let err = Error::msg("connection reset");
        match classify_submit_error(&err, "0x1".into(), json!({})) {
            SubmitResult::Unknown { message, .. } => assert!(message.contains("connection reset")),
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn stores_tick_size_from_book_payload() {
        let mut books = BookStore::default();
        let now = Instant::now();
        apply_ws_message(
            &mut books,
            &json!({
                "event_type": "book",
                "asset_id": "t1",
                "timestamp": "100",
                "tick_size": "0.001",
                "bids": [{"price": "0.45", "size": "10"}],
                "asks": [{"price": "0.46", "size": "8"}]
            }),
            now,
        );
        assert_eq!(
            books.get(POLYMARKET, "t1").unwrap().tick_size,
            Some(Decimal::from_str("0.001").unwrap())
        );
        apply_ws_message(
            &mut books,
            &json!({
                "event_type": "tick_size_change",
                "asset_id": "t1",
                "old_tick_size": "0.001",
                "new_tick_size": "0.01",
                "timestamp": "101"
            }),
            now,
        );
        assert_eq!(
            books.get(POLYMARKET, "t1").unwrap().tick_size,
            Some(Decimal::from_str("0.01").unwrap())
        );
    }

    #[test]
    fn leaves_tick_size_empty_when_book_omits_field() {
        let mut books = BookStore::default();
        apply_ws_message(
            &mut books,
            &json!({
                "event_type": "book",
                "asset_id": "t1",
                "timestamp": "100",
                "bids": [{"price": "0.40", "size": "10"}],
                "asks": [{"price": "0.41", "size": "8"}]
            }),
            Instant::now(),
        );
        assert_eq!(books.get(POLYMARKET, "t1").unwrap().tick_size, None);
    }

    #[test]
    fn applies_rest_books_and_skips_older_snapshot() {
        let mut books = BookStore::default();
        let now = Instant::now();
        books.replace_snapshot(
            POLYMARKET,
            "t1",
            vec![],
            vec![Level {
                price: Decimal::from_str("0.40").unwrap(),
                size: Decimal::from_str("10").unwrap(),
            }],
            200,
            now,
        );
        let (applied, skipped_old) = apply_rest_books(
            &mut books,
            &[
                json!({
                    "asset_id": "t1",
                    "timestamp": "100",
                    "tick_size": "0.001",
                    "bids": [],
                    "asks": [{"price": "0.99", "size": "1"}]
                }),
                json!({
                    "asset_id": "t2",
                    "timestamp": "150",
                    "tick_size": "0.01",
                    "bids": [{"price": "0.45", "size": "10"}],
                    "asks": [{"price": "0.46", "size": "8"}]
                }),
            ],
            now,
        );
        assert_eq!(applied, vec!["t2".to_string()]);
        assert_eq!(skipped_old, 1);
        assert_eq!(
            books.get(POLYMARKET, "t1").unwrap().asks[0].price,
            Decimal::from_str("0.40").unwrap()
        );
        assert_eq!(
            books.get(POLYMARKET, "t2").unwrap().tick_size,
            Some(Decimal::from_str("0.01").unwrap())
        );
        assert_eq!(
            books.get(POLYMARKET, "t2").unwrap().asks[0].price,
            Decimal::from_str("0.46").unwrap()
        );
    }

    #[test]
    fn parse_order_poll_keeps_identity_and_matched_quantity_separate() {
        let raw = json!({
            "id": "remote-oid", "status": "MATCHED", "asset_id": "yes",
            "original_size": "20", "size_matched": "3", "price": "0.4",
            "associate_trades": ["trade-1", "trade-2"]
        });
        let poll = parse_order_poll(raw.clone(), "requested-oid");
        assert_eq!(poll.order_id.as_deref(), Some("remote-oid"));
        assert_eq!(poll.coin.as_deref(), Some("yes"));
        assert_eq!(poll.shares, Some(d("3")));
        assert_eq!(poll.original_shares, Some(d("20")));
        assert_eq!(poll.remaining_shares, Some(d("17")));
        assert_eq!(poll.associated_trades, ["trade-1", "trade-2"]);
        assert!(poll.client_order_id.is_none());
        assert_eq!(poll.raw, raw);
        let empty = parse_order_poll(
            json!({"status": "MATCHED", "original_size": "20", "price": "0.4"}),
            "oid-1",
        );
        assert_eq!(empty.order_id.as_deref(), Some("oid-1"));
        assert!(empty.shares.is_none());
        assert!(empty.remaining_shares.is_none());
        assert!(empty.associated_trades.is_empty());
        assert!(empty.raw.get("associate_trades").is_none());
        let explicit_empty = parse_order_poll(json!({"associate_trades": []}), "oid-1");
        assert_eq!(explicit_empty.raw["associate_trades"], json!([]));
        let invalid = parse_order_poll(
            json!({
                "original_size": "2", "size_matched": "3", "associate_trades": ["t1", null]
            }),
            "oid-1",
        );
        assert!(invalid.remaining_shares.is_none());
        assert!(invalid.associated_trades.is_empty());
        assert_eq!(invalid.raw["associate_trades"], json!(["t1", null]));
    }

    fn trade_fixture(id: &str) -> Value {
        json!({
            "id": id, "taker_order_id": "taker-1", "asset_id": "yes",
            "size": "5", "price": "0.4", "side": "BUY", "outcome": "Yes",
            "status": "CONFIRMED", "fee_rate_bps": "700", "maker_orders": []
        })
    }

    #[test]
    fn trade_status_matrix_requires_explicit_confirmation() {
        for (status, expected) in [
            (json!("CONFIRMED"), FillFinality::Confirmed),
            (json!("FAILED"), FillFinality::Failed),
            (json!("MATCHED"), FillFinality::Pending),
            (json!("MINED"), FillFinality::Pending),
            (json!("RETRYING"), FillFinality::Pending),
            (json!("UNKNOWN"), FillFinality::Pending),
            (json!("confirmed"), FillFinality::Pending),
            (Value::Null, FillFinality::Pending),
            (json!(true), FillFinality::Pending),
        ] {
            let mut trade = trade_fixture("t1");
            trade["status"] = status;
            let fills = parse_trades(&json!([trade])).unwrap();
            assert_eq!(fills.len(), 1);
            assert_eq!(fills[0].finality, expected);
            assert_eq!(fills[0].fee, None);
            assert_eq!(fills[0].fee_token, None);
        }
        let mut missing = trade_fixture("missing-status");
        missing.as_object_mut().unwrap().remove("status");
        assert_eq!(
            parse_trades(&json!([missing])).unwrap()[0].finality,
            FillFinality::Pending
        );
    }

    #[test]
    fn parse_trades_assigns_each_maker_only_its_own_size_asset_and_fee() {
        let mut trade = trade_fixture("t1");
        trade["fee_amount"] = json!("0.01");
        trade["fee_token"] = json!("pUSD");
        trade["maker_orders"] = json!([
            {"order_id": "maker-1", "owner": "same-user", "matched_amount": "2",
             "price": "0.4", "asset_id": "yes", "outcome": "Yes", "side": "SELL",
             "fee_rate_bps": "0", "fee_amount": "0", "feeToken": "pUSD"},
            {"order_id": "maker-2", "owner": "same-user", "matched_amount": "3",
             "price": "0.6", "asset_id": "no", "outcome": "No", "fee_rate_bps": "700"}
        ]);
        let fills = parse_trades(&json!({"data": [trade.clone()]})).unwrap();
        assert_eq!(fills.len(), 3);
        assert_eq!(fills[0].shares, d("5"));
        assert_eq!(fills[0].raw["role"], "taker");
        assert!(fills[0].matches(Some("taker-1"), None));
        assert!(!fills[0].matches(Some("maker-1"), None));
        assert_eq!(fills[0].fee, Some(d("0.01")));
        assert_eq!(fills[0].fee_token.as_deref(), Some("pUSD"));
        assert_eq!(fills[1].shares, d("2"));
        assert_eq!(fills[1].coin.as_deref(), Some("yes"));
        assert_eq!(fills[1].raw["side"], "SELL");
        assert_eq!(fills[1].raw["role"], "maker");
        assert_eq!(fills[1].fee, Some(Decimal::ZERO));
        assert_eq!(fills[1].fee_token.as_deref(), Some("pUSD"));
        assert!(fills[1].matches(Some("maker-1"), None));
        assert!(!fills[1].matches(Some("maker-2"), None));
        assert!(!fills[1].matches(Some("taker-1"), None));
        assert_eq!(fills[2].shares, d("3"));
        assert_eq!(fills[2].price, d("0.6"));
        assert_eq!(fills[2].coin.as_deref(), Some("no"));
        assert_eq!(fills[2].raw["outcome"], "No");
        assert!(fills[2].raw.get("side").is_none());
        assert_eq!(fills[2].fee, None);
        assert_eq!(fills[2].fee_rate_bps, Some(d("700")));
        assert_eq!(fills[2].fee_token, None);
        assert_eq!(fills[2].raw["taker_trade"], trade);
        assert_eq!(fills[2].raw["maker_order"], trade["maker_orders"][1]);
        for fill in fills {
            assert_eq!(fill.trade_id, "t1");
            assert_eq!(fill.order_ids.len(), 1);
            assert_eq!(fill.finality, FillFinality::Confirmed);
        }
    }

    #[test]
    fn parse_trades_rejects_malformed_records_instead_of_skipping_them() {
        for field in [
            "id",
            "taker_order_id",
            "asset_id",
            "size",
            "price",
            "maker_orders",
        ] {
            let mut bad = trade_fixture("bad");
            bad.as_object_mut().unwrap().remove(field);
            assert!(
                parse_trades(&json!([trade_fixture("good"), bad])).is_err(),
                "{field}"
            );
        }
        for (field, value) in [
            ("size", json!("invalid")),
            ("size", json!("0")),
            ("price", json!("1.1")),
            ("price", json!("-1")),
            ("fee_rate_bps", json!("broken")),
            ("fee_amount", json!("broken")),
            ("fee_token", json!(5)),
            ("maker_orders", Value::Null),
        ] {
            let mut bad = trade_fixture("bad");
            bad[field] = value;
            assert!(parse_trades(&json!([bad])).is_err(), "{field}");
        }
        let mut bad_maker = trade_fixture("maker-error");
        bad_maker["maker_orders"] = json!([{"order_id": "maker-1"}]);
        assert!(parse_trades(&json!([bad_maker])).is_err());
        for bad_page in [Value::Null, json!({}), json!({"data": {}}), json!([null])] {
            assert!(parse_trades(&bad_page).is_err());
        }
        assert!(parse_trades(&json!({"data": []})).unwrap().is_empty());
    }

    async fn poll_stub(
        responses: Vec<(u16, Value)>,
    ) -> (PolymarketVenue, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut socket, _) =
                    tokio::time::timeout(Duration::from_secs(3), listener.accept())
                        .await
                        .expect("local stub request timed out")
                        .unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut buffer = [0u8; 1024];
                    let count =
                        tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                            .await
                            .unwrap()
                            .unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buffer[..count]);
                    if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                    assert!(bytes.len() < 16_384);
                }
                // 不保留/输出认证头；这里只核对方法、路径和查询参数。
                requests.push(
                    String::from_utf8(bytes)
                        .unwrap()
                        .lines()
                        .next()
                        .unwrap()
                        .to_string(),
                );
                let body = body.to_string();
                let response = format!(
                    "HTTP/1.1 {status} Stub\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        let mut venue = cache_test_venue();
        venue.base = format!("http://{address}");
        venue.http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        venue.auth_ttl = Duration::ZERO;
        venue.authed.lock().await.insert(
            "test-funder".into(),
            PolymarketAccount {
                funder: "test-funder".into(),
                service: None,
                signature_type: 2,
                signer: PrivateKeySigner::random(),
                api_key: "stub-key".into(),
                api_secret: "dGVzdA==".into(),
                api_passphrase: "stub-passphrase".into(),
                created_at: unix_secs(),
            },
        );
        (venue, server)
    }

    #[tokio::test]
    async fn fee_schedule_fetches_public_market_and_preserves_valuation_policy() {
        let (venue, server) =
            poll_stub(vec![(200, json!({"c": "0xABC", "fd": {"r": 0.07}}))]).await;
        // 此公开查询不需要账户初始化、凭证文件或 L2 认证。
        venue.authed.lock().await.clear();
        let before = unix_millis();
        let snapshot = venue.fee_schedule("0xabc").await.unwrap();
        let after = unix_millis();
        assert_eq!(snapshot["condition_id"], "0xabc");
        assert_eq!(snapshot["rate"], "0.07");
        assert_eq!(snapshot.as_object().unwrap().len(), 7);
        assert_eq!(snapshot["source"], "clob-markets");
        assert_eq!(snapshot["currency"], "pUSD");
        assert_eq!(snapshot["valuation"], "1 USD");
        assert_eq!(snapshot["rounding"], "midpoint_away_from_zero_5dp");
        let observed_at = snapshot["observed_at_ms"].as_u64().unwrap();
        assert!((before..=after).contains(&observed_at));
        assert_eq!(server.await.unwrap(), ["GET /clob-markets/0xabc HTTP/1.1"]);
    }

    #[test]
    fn fee_schedule_accepts_rate_boundaries_and_ignores_unrelated_fields() {
        for rate in ["0", "1", "0.07"] {
            let snapshot = parse_fee_schedule(
                &json!({
                    "condition_id": "condition", "fd": {"r": rate}
                }),
                "condition",
                123,
            )
            .unwrap();
            assert_eq!(snapshot["rate"], rate);
            assert_eq!(snapshot["observed_at_ms"], 123);
            assert_eq!(snapshot.as_object().unwrap().len(), 7);
        }
        // 只消费费率；响应中其他参数缺失或畸形均不能阻挡已验证的 rate。
        let rate_only =
            parse_fee_schedule(&json!({"fd": {"r": "0.05"}}), "condition", 123).unwrap();
        for unrelated in [Value::Null, json!(-1), json!("invalid"), json!({})] {
            assert_eq!(
                parse_fee_schedule(
                    &json!({"fd": {"r": "0.05", "e": unrelated}}),
                    "condition",
                    123
                )
                .unwrap(),
                rate_only
            );
        }
    }

    #[tokio::test]
    async fn fee_schedule_rejects_invalid_values_identity_and_http_error() {
        let invalid = vec![
            json!({}),
            json!({"fd": {}}),
            json!({"fd": null}),
            json!({"fd": {"r": "-0.01"}}),
            json!({"fd": {"r": "1.01"}}),
            json!({"fd": {"r": "NaN"}}),
            json!({"fd": {"r": true}}),
            json!({"fd": {"r": null}}),
            json!({"c": "different", "fd": {"r": "0.07"}}),
            json!({"c": null, "fd": {"r": "0.07"}}),
            json!({"c": "condition", "condition_id": "different", "fd": {"r": "0.07"}}),
        ];
        let count = invalid.len();
        let mut responses: Vec<_> = invalid.into_iter().map(|raw| (200, raw)).collect();
        responses.push((503, json!({"error": "do not expose payload"})));
        let (venue, server) = poll_stub(responses).await;
        for _ in 0..count {
            assert!(venue.fee_schedule("condition").await.is_err());
        }
        let error = venue.fee_schedule("condition").await.unwrap_err();
        assert!(matches!(error, Error::Http { status: 503, .. }));
        assert!(!error.to_string().contains("do not expose payload"));
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), count + 1);
        assert!(requests
            .iter()
            .all(|request| request == "GET /clob-markets/condition HTTP/1.1"));
    }

    #[tokio::test]
    async fn fee_schedule_rejects_empty_condition_without_network() {
        assert!(cache_test_venue().fee_schedule(" ").await.is_err());
    }

    fn assert_trade_request(request: &str, cursor: &str) {
        assert!(request.starts_with("GET /data/trades?"));
        let target = request.split_whitespace().nth(1).unwrap();
        let url = url::Url::parse(&format!("http://localhost{target}")).unwrap();
        let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query.get("asset_id").map(String::as_str), Some("yes"));
        assert_eq!(query.get("next_cursor").map(String::as_str), Some(cursor));
    }

    #[tokio::test]
    async fn trade_pagination_resumes_second_page_and_restarts_after_end() {
        let (venue, server) = poll_stub(vec![
            (200, json!({"data": [trade_fixture("first")], "next_cursor": "MQ=="})),
            // 跨页重复 trade 不在解析层去重，上层按 trade + order ID 持久化。
            (200, json!({"data": [trade_fixture("first"), trade_fixture("second")], "next_cursor": "LTE="})),
            (200, json!({"data": [], "next_cursor": "LTE="})),
        ]).await;
        let first = venue
            .poll_trade_page("test-funder", "yes", &Value::Null)
            .await
            .unwrap();
        assert!(!first.complete && !first.history_complete);
        assert_eq!(first.fills[0].trade_id, "first");
        let saved = serde_json::to_string(&first.progress).unwrap();
        let restored: Value = serde_json::from_str(&saved).unwrap();
        let second = venue
            .poll_trade_page("test-funder", "yes", &restored)
            .await
            .unwrap();
        assert!(second.complete && second.history_complete);
        assert_eq!(second.fills.len(), 2);
        assert_eq!(second.fills[1].trade_id, "second");
        assert_eq!(second.progress["seen_cursors"], json!(["MA==", "MQ=="]));
        let next = venue
            .poll_trade_page("test-funder", "yes", &second.progress)
            .await
            .unwrap();
        assert!(next.complete && next.history_complete);
        let requests = server.await.unwrap();
        assert_trade_request(&requests[0], "MA==");
        assert_trade_request(&requests[1], "MQ==");
        assert_trade_request(&requests[2], "MA==");
    }

    #[tokio::test]
    async fn trade_pagination_rejects_repeated_cursor_and_preserves_saved_progress() {
        let (venue, server) = poll_stub(vec![
            (200, json!({"data": [], "next_cursor": "MQ=="})),
            (200, json!({"data": [], "next_cursor": "MA=="})),
            (200, json!({"data": [], "next_cursor": "MQ=="})),
        ])
        .await;
        let first = venue
            .poll_trade_page("test-funder", "yes", &json!({}))
            .await
            .unwrap();
        let saved = first.progress.clone();
        for _ in 0..2 {
            let err = venue
                .poll_trade_page("test-funder", "yes", &first.progress)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("repeated cursor"));
            assert_eq!(first.progress, saved);
        }
        let requests = server.await.unwrap();
        assert_trade_request(&requests[1], "MQ==");
        assert_trade_request(&requests[2], "MQ==");
    }

    #[tokio::test]
    async fn trade_pagination_rejects_bad_page_and_http_failure_without_advancing() {
        let (venue, server) = poll_stub(vec![
            (
                200,
                json!({"data": [trade_fixture("saved")], "next_cursor": "MQ=="}),
            ),
            (200, json!({"data": {}, "next_cursor": "LTE="})),
            (200, json!({"data": [null], "next_cursor": "LTE="})),
            (200, json!({"data": []})),
            (503, json!({"error": "unavailable"})),
            (
                200,
                json!({"data": [trade_fixture("recovered")], "next_cursor": "LTE="}),
            ),
        ])
        .await;
        let first = venue
            .poll_trade_page("test-funder", "yes", &Value::Null)
            .await
            .unwrap();
        for _ in 0..4 {
            assert!(venue
                .poll_trade_page("test-funder", "yes", &first.progress)
                .await
                .is_err());
        }
        let recovered = venue
            .poll_trade_page("test-funder", "yes", &first.progress)
            .await
            .unwrap();
        assert!(recovered.complete && recovered.history_complete);
        assert_eq!(recovered.fills[0].trade_id, "recovered");
        let requests = server.await.unwrap();
        for request in &requests[1..] {
            assert_trade_request(request, "MQ==");
        }
    }

    #[test]
    fn trade_pagination_rejects_unknown_history_and_wrong_query_progress() {
        for progress in [
            json!({"next_cursor": "MQ=="}),
            json!({"funder": "other", "asset_id": "yes", "next_cursor": "MQ==", "seen_cursors": ["MA=="]}),
            json!({"funder": "test-funder", "asset_id": "no", "next_cursor": "MQ==", "seen_cursors": ["MA=="]}),
            json!({"funder": "test-funder", "asset_id": "yes", "next_cursor": "LTE=", "seen_cursors": []}),
            json!({"funder": "test-funder", "asset_id": "yes", "next_cursor": "MQ==", "seen_cursors": ["MA==", "MQ=="]}),
        ] {
            assert!(trade_page_cursor(&progress, "test-funder", "yes").is_err());
        }
    }

    #[tokio::test]
    async fn poll_order_404_is_not_found_without_fill_or_association_evidence() {
        let (venue, server) = poll_stub(vec![(404, json!({"error": "not found"}))]).await;
        let order = venue.poll_order("test-funder", "order-id").await.unwrap();
        assert!(!order.found);
        assert_eq!(order.status, "not_found");
        assert_eq!(order.order_id.as_deref(), Some("order-id"));
        assert!(
            order.shares.is_none()
                && order.original_shares.is_none()
                && order.remaining_shares.is_none()
        );
        assert!(order.associated_trades.is_empty());
        assert!(order.raw.get("associate_trades").is_none());
        assert_eq!(server.await.unwrap(), ["GET /data/order/order-id HTTP/1.1"]);
    }

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn market_buy_ceils_maker_usdc_to_2_decimals() {
        let (maker, taker) = market_order_base_units(OrderSide::Buy, d("7"), d("0.333")).unwrap();
        assert_eq!(maker, 2_340_000);
        assert_eq!(taker, 7_000_000);
    }

    #[test]
    fn market_buy_floors_taker_shares_to_5_decimals() {
        let (maker, taker) =
            market_order_base_units(OrderSide::Buy, d("1.234567"), d("0.50")).unwrap();
        assert_eq!(taker, 1_234_560);
        assert_eq!(maker, 620_000);
    }

    #[test]
    fn market_buy_keeps_cent_usdc_unchanged() {
        let (maker, taker) = market_order_base_units(OrderSide::Buy, d("10"), d("0.45")).unwrap();
        assert_eq!(maker, 4_500_000);
        assert_eq!(taker, 10_000_000);
    }

    #[test]
    fn market_sell_floors_maker_shares_to_2_decimals() {
        let (maker, taker) =
            market_order_base_units(OrderSide::Sell, d("5.129"), d("0.40")).unwrap();
        assert_eq!(maker, 5_120_000);
        assert_eq!(taker, 2_048_000);
    }

    #[test]
    fn market_buy_rejects_when_shares_trunc_to_zero() {
        let err = market_order_base_units(OrderSide::Buy, d("0.000001"), d("0.50")).unwrap_err();
        assert!(err.to_string().contains("round to zero"));
    }

    fn dummy_funder(addr: &str) -> PolymarketFunderConfig {
        PolymarketFunderConfig {
            funder_address: addr.into(),
            wallet_private_key: "0x".into(),
            is_wallet_v2: false,
            service: None,
        }
    }

    #[test]
    fn funder_cursor_resumes_saved_address() {
        let path = std::env::temp_dir().join(format!(
            "pm-funder-cursor-{}-{}",
            std::process::id(),
            "resume"
        ));
        let funders = vec![
            dummy_funder("0xaaa"),
            dummy_funder("0xbbb"),
            dummy_funder("0xccc"),
        ];
        assert_eq!(load_funder_rr(&funders, &path), 0);
        save_funder_rr(&path, "0xBBB").unwrap();
        assert_eq!(load_funder_rr(&funders, &path), 1);
        save_funder_rr(&path, "0xmissing").unwrap();
        assert_eq!(load_funder_rr(&funders, &path), 0);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("cursor.tmp"));
    }

    #[test]
    fn api_creds_roundtrip_and_ttl() {
        let path = std::env::temp_dir().join(format!(
            "pm-api-creds-{}-{}.json",
            std::process::id(),
            "ttl"
        ));
        let _ = fs::remove_file(&path);
        let creds = StoredApiCreds {
            api_key: "k".into(),
            secret: "s".into(),
            passphrase: "p".into(),
            created_at: 1_000,
        };
        save_api_cred(&path, "0xAbC", &creds).unwrap();
        assert!(creds_fresh(1_000, Duration::from_secs(100), 1_050));
        assert!(!creds_fresh(1_000, Duration::from_secs(100), 1_100));
        assert!(creds_fresh(1_000, Duration::ZERO, 9_999));
        let loaded = load_fresh_api_cred(&path, "0xabc", Duration::from_secs(100), 1_050).unwrap();
        assert_eq!(loaded.api_key, "k");
        assert!(load_fresh_api_cred(&path, "0xabc", Duration::from_secs(100), 1_100).is_none());
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("json.tmp"));
    }

    #[test]
    fn auth_ttl_jitter_is_stable_and_in_range() {
        let a = auth_ttl_jitter_secs("0xAbc");
        let b = auth_ttl_jitter_secs("0xabc");
        assert_eq!(a, b);
        assert_eq!(a % 60, 0);
        assert!((AUTH_TTL_JITTER_MIN_MINS * 60..=AUTH_TTL_JITTER_MAX_MINS * 60).contains(&a));
        assert_eq!(effective_auth_ttl(Duration::ZERO, "0xabc"), Duration::ZERO);
        let ttl = effective_auth_ttl(Duration::from_secs(86_400), "0xabc");
        assert_eq!(ttl, Duration::from_secs(86_400 + a));
        let other = auth_ttl_jitter_secs("0xdef");
        assert_ne!(a, other);
    }

    fn sample_cred(created_at: u64) -> StoredApiCreds {
        StoredApiCreds {
            api_key: "k".into(),
            secret: "s".into(),
            passphrase: "p".into(),
            created_at,
        }
    }

    #[test]
    fn oldest_stored_cred_picks_earliest_created_at() {
        let mut creds = HashMap::new();
        creds.insert("0xbbb".into(), sample_cred(2_000));
        creds.insert("0xaaa".into(), sample_cred(3_000));
        creds.insert("0xccc".into(), sample_cred(1_000));
        assert_eq!(oldest_stored_cred(&creds), Some(("0xccc".into(), 1_000)));
    }

    #[test]
    fn auth_refresh_due_only_within_ten_minutes() {
        let ttl = Duration::from_secs(86_400);
        assert!(!auth_refresh_due(1_000, ttl, 1_000 + 86_400 - 600));
        assert!(auth_refresh_due(1_000, ttl, 1_000 + 86_400 - 599));
        assert!(auth_refresh_due(1_000, ttl, 1_000 + 86_400));
        assert!(!auth_refresh_due(1_000, Duration::ZERO, 9_999));
    }
}
