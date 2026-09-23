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
use num_bigint::BigInt;
use num_traits::Zero;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
/// 买单只接受原股数与 cap 可无损表达的金额；卖单金额仍向下截断。
const MARKET_MAKER_DECIMALS: u32 = 2;
const MARKET_TAKER_DECIMALS: u32 = 5;
// 官方 CLOB 客户端以 base64("0") 起始，以 base64("-1") 表示已读到末尾。
const TRADES_INITIAL_CURSOR: &str = "MA==";
const TRADES_END_CURSOR: &str = "LTE=";
// 按操作限频而非按订单/成交缓存，避免两秒轮询和 maker 展开反复告警。
static ORDER_STATUS_LAST_WARN_SECS: AtomicU64 = AtomicU64::new(0);
static TRADE_STATUS_LAST_WARN_SECS: AtomicU64 = AtomicU64::new(0);

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

    pub fn settlement_endpoint(&self) -> &str {
        &self.base
    }

    pub async fn settlement(
        &self,
        condition_id: &str,
    ) -> Result<crate::settlement::SettlementStatus> {
        Ok(self.settlement_with_evidence(condition_id).await?.0)
    }

    pub async fn settlement_with_evidence(
        &self,
        condition_id: &str,
    ) -> Result<(crate::settlement::SettlementStatus, Value)> {
        if condition_id.is_empty() {
            return Err(Error::msg("missing polymarket condition_id"));
        }
        let started = Instant::now();
        let transport_error = |error: reqwest::Error| {
            tracing::warn!(service="polymarket",api="markets",condition_id,http_status=?error.status().map(|s|s.as_u16()),elapsed_ms=started.elapsed().as_millis() as u64,error=%error.without_url(),"settlement query failed");
            Error::msg("polymarket settlement HTTP/response failure")
        };
        let value: Value = self
            .http
            .get(format!("{}/markets/{condition_id}", self.base))
            .send()
            .await
            .map_err(&transport_error)?
            .error_for_status()
            .map_err(&transport_error)?
            .json()
            .await
            .map_err(&transport_error)?;
        if value.get("condition_id").and_then(Value::as_str) != Some(condition_id) {
            return Err(Error::msg(
                "polymarket settlement condition identity mismatch or missing",
            ));
        }
        let status = crate::settlement::parse_polymarket_settlement(&value).map_err(|error| {
            tracing::warn!(service="polymarket",api="markets",condition_id,elapsed_ms=started.elapsed().as_millis() as u64,error=%error,"invalid settlement response"); error
        })?;
        tracing::debug!(
            service = "polymarket",
            condition_id,
            settlement_state = status.kind(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "settlement queried"
        );
        let tokens: Vec<Value> = value["tokens"].as_array().unwrap().iter().map(|t| json!({"token_id":t.get("token_id"),"winner":t.get("winner"),"price":t.get("price"),"outcome":t.get("outcome")})).collect();
        Ok((
            status,
            json!({"condition_id":value.get("condition_id"),"tokens":tokens,"closed":value.get("closed"),"accepting_orders":value.get("accepting_orders"),"enable_order_book":value.get("enable_order_book")}),
        ))
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

    pub async fn rest_book(&self, token_id: &str) -> Result<BookSnapshot> {
        let started = Instant::now();
        let mut status = None;
        let result = async {
            let response = self
                .http
                .get(format!("{}/book", self.base))
                .query(&[("token_id", token_id)])
                .send()
                .await?;
            status = Some(response.status().as_u16());
            let value: Value = response.error_for_status()?.json().await?;
            validate_book_identity(&value, token_id)?;
            parse_book_json(&value)
        }
        .await;
        match &result {
            Ok(_) => tracing::debug!(
                platform = POLYMARKET,
                token = token_id,
                interface = "/book",
                ?status,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "polymarket book fetched"
            ),
            Err(error) => tracing::warn!(platform = POLYMARKET, token = token_id,
                interface = "/book", ?status, %error, elapsed_ms = started.elapsed().as_millis() as u64,
                "polymarket book request failed"),
        }
        result
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
        let mut phase = "send";
        let mut http_status = None;
        let result = async {
            let resp = self
                .http
                .post(format!("{}/books", self.base))
                .json(&body)
                .send()
                .await
                .map_err(books_http_error)?;
            let status = resp.status();
            http_status = Some(status.as_u16());
            phase = "read_body";
            let text = resp.text().await.map_err(books_http_error)?;
            if !status.is_success() {
                return Err(Error::Http {
                    status: status.as_u16(),
                    message: "polymarket books kind=status".into(),
                });
            }
            phase = "decode";
            let parsed: Value = serde_json::from_str(&text).map_err(|error| {
                // JSON 错误文本可能包含响应值，只保留类别及位置。
                Error::msg(format!(
                    "polymarket books kind=decode category={:?} line={} column={}",
                    error.classify(),
                    error.line(),
                    error.column()
                ))
            })?;
            parsed
                .as_array()
                .cloned()
                .ok_or_else(|| Error::msg("polymarket books kind=decode reason=missing_array"))
        }
        .await;
        let items = result.map_err(|error| {
            tracing::warn!(
                service = "polymarket",
                interface = "/books",
                phase,
                ?http_status,
                requested = token_ids.len(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                %error,
                "polymarket books request failed"
            );
            error
        })?;
        tracing::debug!(
            requested = token_ids.len(),
            returned = items.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "polymarket books fetched"
        );
        Ok(items)
    }

    /// 真正请求无时间戳 tick；正常交易路径由 BookStore 持票初始化并缓存。
    pub async fn fetch_tick_size(&self, token_id: &str) -> Result<Decimal> {
        let started = Instant::now();
        let mut status = None;
        let result = async {
            let response = self
                .http
                .get(format!("{}/tick-size", self.base))
                .query(&[("token_id", token_id)])
                .send()
                .await?;
            status = Some(response.status().as_u16());
            let value: Value = response.error_for_status()?.json().await?;
            value
                .get("minimum_tick_size")
                .or_else(|| value.get("tickSize"))
                .or_else(|| value.get("tick_size"))
                .and_then(parse_decimal)
                .filter(|tick| *tick > Decimal::ZERO && *tick <= Decimal::ONE)
                .ok_or_else(|| Error::msg("invalid polymarket tick_size"))
        }
        .await;
        match &result {
            Ok(tick) => tracing::debug!(platform = POLYMARKET, token = token_id,
                interface = "/tick-size", ?status, %tick,
                elapsed_ms = started.elapsed().as_millis() as u64, "polymarket tick fetched"),
            Err(error) => tracing::warn!(platform = POLYMARKET, token = token_id,
                interface = "/tick-size", ?status, %error,
                elapsed_ms = started.elapsed().as_millis() as u64, "polymarket tick request failed"),
        }
        result
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
        self.prepare_order(funder, req, "FAK").await
    }

    pub async fn prepare_limit_order(
        &self,
        funder: &str,
        req: &MarketOrderRequest,
    ) -> Result<PreparedOrder> {
        self.prepare_order(funder, req, "GTC").await
    }

    async fn prepare_order(
        &self,
        funder: &str,
        req: &MarketOrderRequest,
        order_type: &str,
    ) -> Result<PreparedOrder> {
        require_positive(req.shares, req.cap_price)?;
        let account = self.ensure_account(funder).await?;
        let tick = match req.tick_size {
            Some(v) => v,
            None => self.fetch_tick_size(&req.token_id).await?,
        };
        let neg_risk = self.neg_risk(&req.token_id, req.neg_risk).await?;
        if order_type == "GTC"
            && (tick <= Decimal::ZERO
                || req.cap_price < tick
                || req.cap_price > Decimal::ONE - tick
                || req.cap_price.checked_rem(tick) != Some(Decimal::ZERO))
        {
            return Err(Error::msg("polymarket limit price incompatible with tick"));
        }
        let unsigned = build_unsigned_order(&account, req, tick, order_type)?;
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

    #[tracing::instrument(name = "polymarket_submit", skip_all, fields(order_hash = %prepared.order_hash))]
    pub async fn post_prepared(
        &self,
        prepared: &PreparedOrder,
    ) -> Result<(SubmitResult, super::SubmissionResponse)> {
        let started = Instant::now();
        let funder = prepared
            .funder
            .as_deref()
            .ok_or_else(|| Error::msg("missing funder on prepared order"))?;
        let account = self.ensure_account(funder).await?;
        let order_hash = prepared.order_hash.clone();
        let envelope = prepared.envelope.clone();
        let mut audit = None;
        let response = self
            .l2_json_with_submit_response(
                &account,
                reqwest::Method::POST,
                "/order",
                &[],
                Some(&prepared.payload),
                Some(&mut audit),
            )
            .await;
        // 提交结果无论明确与否，都不能继续复用提交前余额。
        self.invalidate_usdc_balance(funder).await;
        match response {
            Ok(body) => {
                let result = parse_submit(&body, order_hash, envelope);
                log_submit_result(&result, &body, &prepared.order_hash, None, started);
                Ok((
                    result,
                    audit.ok_or_else(|| Error::msg("missing submit response evidence"))?,
                ))
            }
            Err(err) => {
                let response = audit.unwrap_or_else(|| {
                    super::SubmissionResponse::NoResponse(
                        json!({"kind":"local_error","error":err.to_string()}),
                    )
                });
                let result = classify_submit_error(&err, order_hash, envelope);
                log_submit_result(
                    &result,
                    &Value::Null,
                    &prepared.order_hash,
                    Some(&err),
                    started,
                );
                Ok((result, response))
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
        let started = Instant::now();
        let poll = match self
            .l2_json(&account, reqwest::Method::GET, &path, &[], None)
            .await
        {
            Ok(raw) => {
                // 只有明确的 null 才表示 missing；畸形成功响应不能触发历史查询兜底。
                if !raw.is_null()
                    && (!raw.is_object()
                        || !raw
                            .get("status")
                            .and_then(Value::as_str)
                            .is_some_and(|status| !status.trim().is_empty()))
                {
                    if status_warning_due(&ORDER_STATUS_LAST_WARN_SECS, unix_secs()) {
                        tracing::warn!(
                            service = "polymarket",
                            operation = "order_poll",
                            endpoint = %path,
                            funder,
                            order_id,
                            raw_status = ?status_log_value(raw.get("status")),
                            http_status = 200,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            reason = "invalid_order_body",
                            "polymarket order response invalid"
                        );
                    }
                    return Err(Error::msg("polymarket order missing or invalid status"));
                }
                parse_order_poll(raw, order_id)
            }
            Err(Error::Http { status: 404, .. }) => OrderPoll {
                status: "not_found".into(),
                order_id: Some(order_id.into()),
                raw: json!({"lookup_missing": "http_404"}),
                ..OrderPoll::default()
            },
            Err(err) => return Err(err),
        };
        if !poll.found {
            tracing::debug!(
                service = "polymarket",
                operation = "order_poll",
                endpoint = %path,
                funder,
                order_id,
                lookup_missing = poll.raw["lookup_missing"].as_str().unwrap_or_default(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "polymarket order not found"
            );
        }
        Ok(poll)
    }

    /// 每次只读一页，让调用方将 fills 与 progress 一起持久化；后页失败不丢前页进度。
    /// Unix 秒窗口固定为 5 分钟；读到 END 后重扫同一窗口，刷新未确认成交的状态。
    pub async fn poll_trade_page(
        &self,
        funder: &str,
        token_id: &str,
        order_id: &str,
        after: i64,
        before: i64,
        progress: &Value,
    ) -> Result<FillPage> {
        let (cursor, mut seen, mut trade_ids) =
            trade_page_cursor(progress, funder, token_id, order_id, after, before)?;
        // after 为排他下界，请求回看 10 秒以覆盖同秒成交；不改变持久化窗口和分页身份。
        let request_after = (after - 10).max(0);
        let account = self.ensure_account(funder).await?;
        let started = Instant::now();
        let raw = self
            .l2_json(
                &account,
                reqwest::Method::GET,
                "/data/trades",
                &[
                    ("asset_id", token_id),
                    ("next_cursor", &cursor),
                    ("after", &request_after.to_string()),
                    ("before", &before.to_string()),
                ],
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
            let mut known: std::collections::HashSet<_> = trade_ids.iter().cloned().collect();
            // fills 保留全页；扫描证据只收集本单，不能把同 token 的其他订单算入覆盖。
            for fill in &fills {
                if fill.order_id.as_deref() != Some(order_id) {
                    continue;
                }
                if fill.coin.as_deref() != Some(token_id) {
                    return Err(Error::msg("polymarket trade order asset mismatch"));
                }
                if known.insert(fill.trade_id.clone()) {
                    trade_ids.push(fill.trade_id.clone());
                }
            }
            seen.push(cursor);
            let complete = next == TRADES_END_CURSOR;
            Ok(FillPage {
                fills,
                progress: json!({
                    "version": 2,
                    "funder": funder.to_ascii_lowercase(),
                    "asset_id": token_id,
                    "order_id": order_id,
                    "after": after,
                    "before": before,
                    "next_cursor": next,
                    "seen_cursors": seen,
                    "trade_ids": trade_ids,
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
                order_id,
                after,
                request_after,
                before,
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
                order_id,
                after,
                request_after,
                before,
                reason = %err,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "polymarket trade page invalid"
            ),
        }
        parsed
    }

    /// 旧接口仍须指定订单和固定窗口，且不能把未完整的第一页当成完整历史。
    pub async fn poll_trades(
        &self,
        funder: &str,
        token_id: &str,
        order_id: &str,
        after: i64,
        before: i64,
    ) -> Result<Vec<TradeFill>> {
        let page = self
            .poll_trade_page(funder, token_id, order_id, after, before, &Value::Null)
            .await?;
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
        self.l2_json_with_submit_response(account, method, path, query, body, None)
            .await
    }

    // 仅 POST /order 可使用完整审计通道；其它请求及错误摘要保持原约定。
    async fn l2_json_with_submit_response(
        &self,
        account: &PolymarketAccount,
        method: reqwest::Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<&Value>,
        mut audit: Option<&mut Option<super::SubmissionResponse>>,
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
        let submit_request = method == reqwest::Method::POST && path == "/order";
        let started = Instant::now();
        let resp = req.send().await.map_err(|err| {
            if submit_request {
                if let Some(slot) = audit.as_deref_mut() {
                    *slot = Some(super::SubmissionResponse::transport(&err));
                }
                log_submit_http(
                    None,
                    started,
                    if err.is_timeout() {
                        "timeout"
                    } else {
                        "transport"
                    },
                );
            }
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
        let mut submit_body_failure = None;
        let text = match resp.text().await {
            Ok(text) => {
                if submit_request {
                    if let Some(slot) = audit.as_deref_mut() {
                        *slot = Some(super::SubmissionResponse::http(
                            status.as_u16(),
                            Ok(text.clone()),
                        ));
                    }
                }
                text
            }
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
            Err(err) => {
                if submit_request {
                    submit_body_failure = Some(if err.is_timeout() {
                        "response_body_timeout"
                    } else {
                        "response_body"
                    });
                    if let Some(slot) = audit.as_deref_mut() {
                        *slot = Some(super::SubmissionResponse::http(status.as_u16(), Err(err)));
                    }
                }
                // POST /order 沿用读取失败时空响应的分类语义，日志不改变返回值。
                String::new()
            }
        };
        if submit_request {
            let parsed = serde_json::from_str::<Value>(&text);
            let reason = submit_body_failure.unwrap_or_else(|| {
                if !status.is_success() {
                    "http_status"
                } else if text.is_empty() {
                    "empty_body"
                } else {
                    match &parsed {
                        Err(_) => "invalid_json",
                        Ok(value) => submit_body_log_reason(value),
                    }
                }
            });
            log_submit_http(Some(status.as_u16()), started, reason);
            if !status.is_success() {
                return Err(Error::Http {
                    status: status.as_u16(),
                    message: redact_http(&text),
                });
            }
            // 精确保留既有 empty/null/raw 约定，包括带空白 JSON null 的差异。
            return Ok(if text.is_empty() || text == "null" {
                json!({})
            } else {
                parsed.unwrap_or_else(|_| json!({"raw": text}))
            });
        }
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
        if path.starts_with("/data/order/") {
            // 保留 JSON null 的 missing 语义；空响应或非法 JSON 不能伪装成订单不存在。
            return serde_json::from_str(&text).map_err(|_| {
                tracing::warn!(
                    service = "polymarket",
                    operation = %method,
                    endpoint = path,
                    funder = %account.funder,
                    http_status = status.as_u16(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    reason = "invalid_json",
                    "polymarket order response invalid"
                );
                Error::msg("polymarket order invalid JSON response")
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
    order_type: &str,
) -> Result<SignedOrder> {
    let price = if req.side == OrderSide::Buy {
        crate::calc::align_polymarket_price(req.cap_price, tick)
    } else {
        crate::calc::align_polymarket_sell_price(req.cap_price, tick)
    };
    if price < tick {
        return Err(Error::msg("price below tick"));
    }
    if req.side == OrderSide::Buy && price != req.cap_price {
        return Err(Error::msg("polymarket buy cap incompatible with tick"));
    }
    let (maker_amount, taker_amount) = if order_type == "GTC" && req.side == OrderSide::Sell {
        // 限价卖单不能沿用 FAK 的金额截断，否则实际卖价可能低于指定价格。
        let shares = req.shares.trunc_with_scale(MARKET_MAKER_DECIMALS);
        let taker = shares
            .checked_mul(price)
            .and_then(|amount| amount.checked_mul(Decimal::from(1_000_000)))
            .filter(|amount| amount.fract().is_zero())
            .and_then(|amount| amount.to_u128())
            .filter(|amount| *amount > 0)
            .ok_or_else(|| Error::msg("polymarket limit amount incompatible with precision"))?;
        (base_units(shares), taker)
    } else {
        market_order_base_units(req.side, req.shares, price)?
    };
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
        order_type: order_type.into(),
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
        "orderType": signed.order_type,
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
        "order_type": order.order_type,
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
            "order_type": order.order_type,
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

// 日志只接受交易所订单哈希格式，不回显远端任意字符串；不参与业务分类。
fn safe_submit_order_id(body: &Value) -> Option<&str> {
    body.get("orderID")
        .or_else(|| body.get("orderId"))
        .and_then(Value::as_str)
        .filter(|id| {
            id.len() == 66
                && id.starts_with("0x")
                && id.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
        })
}

fn submit_body_log_reason(body: &Value) -> &'static str {
    if body.is_null() {
        return "null_body";
    }
    let Some(object) = body.as_object() else {
        return "malformed_body";
    };
    let has_result = object.get("success").is_some_and(Value::is_boolean)
        || ["error", "errorMsg", "status"]
            .iter()
            .any(|key| object.get(*key).is_some_and(Value::is_string));
    let invalid_success = object.get("success").is_some_and(|v| !v.is_boolean());
    let invalid_string = ["status", "orderID", "orderId", "error", "errorMsg"]
        .iter()
        .any(|key| object.get(*key).is_some_and(|v| !v.is_string()));
    let invalid_amount = ["makingAmount", "takingAmount"].iter().any(|key| {
        object
            .get(*key)
            .is_some_and(|v| !v.is_null() && parse_decimal(v).is_none())
    });
    if !has_result || invalid_success || invalid_string || invalid_amount {
        "malformed_body"
    } else {
        "received"
    }
}

fn log_submit_http(http_status: Option<u16>, started: Instant, reason: &'static str) {
    if reason == "received" {
        tracing::info!(
            service = "polymarket",
            event = "submit_http",
            operation = "POST",
            endpoint = "/order",
            http_status,
            elapsed_ms = started.elapsed().as_millis() as u64,
            reason,
            "polymarket submit response received"
        );
    } else {
        tracing::warn!(
            service = "polymarket",
            event = "submit_http",
            operation = "POST",
            endpoint = "/order",
            http_status,
            elapsed_ms = started.elapsed().as_millis() as u64,
            reason,
            "polymarket submit request anomaly"
        );
    }
}

fn log_submit_result(
    result: &SubmitResult,
    body: &Value,
    order_hash: &str,
    error: Option<&Error>,
    started: Instant,
) {
    let (classification, reason) = match result {
        SubmitResult::Ack { .. } => ("ack", "accepted"),
        SubmitResult::NoMatch { .. } => ("no_match", "fak_no_match"),
        SubmitResult::Failed { .. } => ("failed", "http_rejected"),
        SubmitResult::Unknown { .. } => (
            "unknown",
            match error {
                Some(Error::Http { .. }) => "http_ambiguous",
                Some(Error::Reqwest(err)) if err.is_timeout() => "timeout",
                Some(Error::Reqwest(_)) => "transport",
                Some(_) => "request_error",
                None => "unrecognized_response",
            },
        ),
    };
    let order_id = safe_submit_order_id(body);
    let success = body.get("success").and_then(Value::as_bool);
    let status = body
        .get("status")
        .and_then(Value::as_str)
        .and_then(normalized_order_status);
    let making = body.get("makingAmount").and_then(parse_decimal);
    let taking = body.get("takingAmount").and_then(parse_decimal);
    // 不记录 SubmitResult/envelope/raw/error 正文；数值先解析，状态与原因只用白名单。
    if matches!(
        result,
        SubmitResult::Ack { .. } | SubmitResult::NoMatch { .. }
    ) {
        tracing::info!(service = "polymarket", event = "submit_classified", order_hash,
            order_id, success, status, making = ?making, taking = ?taking, classification, reason,
            elapsed_ms = started.elapsed().as_millis() as u64, "polymarket submit classified");
    } else {
        tracing::warn!(service = "polymarket", event = "submit_classified", order_hash,
            order_id, success, status, making = ?making, taking = ?taking, classification, reason,
            elapsed_ms = started.elapsed().as_millis() as u64, "polymarket submit classified");
    }
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

fn book_token(value: &Value) -> Option<&str> {
    let token = value
        .get("asset_id")
        .or_else(|| value.get("assetId"))?
        .as_str()?;
    if token.is_empty() {
        return None;
    }
    if value
        .get("assetId")
        .is_some_and(|v| v.as_str() != Some(token))
    {
        return None;
    }
    Some(token)
}

fn validate_book_identity(value: &Value, token: &str) -> Result<()> {
    if book_token(value) != Some(token) {
        return Err(Error::msg("polymarket book asset mismatch"));
    }
    Ok(())
}

fn book_timestamp(value: &Value) -> Result<i64> {
    value
        .get("timestamp")
        .and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok()))
        .filter(|ts| *ts > 0)
        .ok_or_else(|| Error::msg("polymarket book invalid timestamp"))
}

#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub exchange_ts_ms: i64,
    pub tick_size: Option<Decimal>,
}

pub fn parse_book_json(value: &Value) -> Result<BookSnapshot> {
    if book_token(value).is_none() {
        return Err(Error::msg("polymarket book missing asset"));
    }
    let ts = book_timestamp(value)?;
    // 可选 tick 一旦出现也必须有效，不能以缺失语义吞掉坏字段。
    for field in [
        "tick_size",
        "tickSize",
        "minimum_tick_size",
        "order_price_min_tick_size",
    ] {
        if value.get(field).is_some_and(|v| {
            parse_decimal(v).is_none_or(|tick| tick <= Decimal::ZERO || tick > Decimal::ONE)
        }) {
            return Err(Error::msg("polymarket book invalid tick"));
        }
    }
    Ok(BookSnapshot {
        bids: parse_levels(value.get("bids"))?,
        asks: parse_levels(value.get("asks"))?,
        exchange_ts_ms: ts,
        tick_size: parse_tick_size(value),
    })
}

fn parse_tick_size(value: &Value) -> Option<Decimal> {
    value
        .get("tick_size")
        .or_else(|| value.get("tickSize"))
        .or_else(|| value.get("minimum_tick_size"))
        .or_else(|| value.get("order_price_min_tick_size"))
        .and_then(parse_decimal)
        .filter(|tick| *tick > Decimal::ZERO && *tick <= Decimal::ONE)
}

fn parse_levels(value: Option<&Value>) -> Result<Vec<Level>> {
    let items = value
        .and_then(Value::as_array)
        .ok_or_else(|| Error::msg("polymarket book missing levels array"))?;
    let mut prices = std::collections::HashSet::new();
    items
        .iter()
        .map(|item| {
            let price = item
                .get("price")
                .and_then(parse_decimal)
                .filter(|v| *v > Decimal::ZERO && *v <= Decimal::ONE)
                .ok_or_else(|| Error::msg("polymarket book invalid price"))?;
            let size = item
                .get("size")
                .and_then(parse_decimal)
                .filter(|v| *v > Decimal::ZERO)
                .ok_or_else(|| Error::msg("polymarket book invalid size"))?;
            if !prices.insert(price) {
                return Err(Error::msg("polymarket book duplicate price"));
            }
            Ok(Level { price, size })
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

/// 兼容 REST 的 ORDER_STATUS_ 枚举与既有裸值，只认完整白名单。
/// SDK 8898914 的 OpenOrderSchema 原样透传状态，不负责业务终态判断。
pub(crate) fn normalized_order_status(status: &str) -> Option<&'static str> {
    match status.to_ascii_lowercase().as_str() {
        "matched" | "order_status_matched" => Some("matched"),
        "live" | "order_status_live" => Some("live"),
        "invalid" | "order_status_invalid" => Some("invalid"),
        "canceled"
        | "order_status_canceled"
        | "canceled_market_resolved"
        | "order_status_canceled_market_resolved"
        | "cancelled"
        | "expired"
        | "unmatched"
        | "rejected" => Some("cancelled"),
        _ => None,
    }
}

fn status_warning_due(last_warn_secs: &AtomicU64, now: u64) -> bool {
    let previous = last_warn_secs.load(Ordering::Relaxed);
    if previous != 0 && now.saturating_sub(previous) < 60 {
        return false;
    }
    // CAS 只争夺本轮告警资格，无业务状态发布；并发页最多一个调用者记录。
    last_warn_secs
        .compare_exchange(previous, now.max(1), Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

fn status_log_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(status)) => status.chars().take(80).collect(),
        None => "<missing>".into(),
        Some(_) => "<invalid>".into(),
    }
}

pub fn parse_order_poll(raw: Value, order_id: &str) -> OrderPoll {
    if raw.is_null() {
        return OrderPoll {
            status: "not_found".into(),
            order_id: Some(order_id.into()),
            raw: json!({"lookup_missing": "null_body"}),
            ..OrderPoll::default()
        };
    }
    let raw_status = raw
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let status = match normalized_order_status(raw_status) {
        Some(status) => status.to_string(),
        None => {
            if status_warning_due(&ORDER_STATUS_LAST_WARN_SECS, unix_secs()) {
                tracing::warn!(
                    service = "polymarket",
                    operation = "order_poll",
                    order_id,
                    raw_status = ?status_log_value(raw.get("status")),
                    reason = "unknown_order_status",
                    "polymarket order status unknown"
                );
            }
            raw_status.to_string()
        }
    };
    let shares = raw.get("size_matched").and_then(parse_decimal);
    let original_shares = raw.get("original_size").and_then(parse_decimal);
    let remaining_shares = original_shares.zip(shares).and_then(|(original, matched)| {
        (matched >= Decimal::ZERO && original >= matched)
            .then(|| original.checked_sub(matched))
            .flatten()
    });
    OrderPoll {
        found: true,
        status,
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
    order_id: &str,
    after: i64,
    before: i64,
) -> Result<(String, Vec<String>, Vec<String>)> {
    if after < 0 || before.checked_sub(after) != Some(300) {
        return Err(Error::msg(
            "polymarket trades require a fixed 300-second window",
        ));
    }
    if order_id.trim().is_empty() {
        return Err(Error::msg("polymarket trades require an order_id"));
    }
    if progress.is_null() || progress.as_object().is_some_and(|obj| obj.is_empty()) {
        return Ok((TRADES_INITIAL_CURSOR.into(), Vec::new(), Vec::new()));
    }
    let invalid = || Error::msg("invalid polymarket trade pagination progress");
    let legacy = match progress.get("version") {
        None if ["order_id", "after", "before", "trade_ids"]
            .iter()
            .all(|field| progress.get(*field).is_none()) =>
        {
            true
        }
        Some(version) if version.as_u64() == Some(2) => false,
        _ => return Err(invalid()),
    };
    if !progress
        .get("funder")
        .and_then(Value::as_str)
        .is_some_and(|previous| previous.eq_ignore_ascii_case(funder))
        || progress.get("asset_id").and_then(Value::as_str) != Some(token_id)
        || (!legacy
            && (progress.get("order_id").and_then(Value::as_str) != Some(order_id)
                || progress.get("after").and_then(Value::as_i64) != Some(after)
                || progress.get("before").and_then(Value::as_i64) != Some(before)))
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
    // 旧进度没有固定窗口证据，即使已到 END 也必须从第一页迁移，不能继承完成状态。
    if legacy {
        return Ok((TRADES_INITIAL_CURSOR.into(), Vec::new(), Vec::new()));
    }
    let mut unique = std::collections::HashSet::new();
    let trade_ids = progress
        .get("trade_ids")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?
        .iter()
        .map(|v| {
            v.as_str()
                .filter(|id| !id.is_empty() && unique.insert(*id))
                .map(str::to_string)
                .ok_or_else(invalid)
        })
        .collect::<Result<Vec<_>>>()?;
    if cursor == TRADES_END_CURSOR {
        return Ok((TRADES_INITIAL_CURSOR.into(), Vec::new(), Vec::new()));
    }
    Ok((cursor, seen, trade_ids))
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
        Some("CONFIRMED" | "TRADE_STATUS_CONFIRMED") => FillFinality::Confirmed,
        Some("FAILED" | "TRADE_STATUS_FAILED") => FillFinality::Failed,
        Some(
            "MATCHED"
            | "TRADE_STATUS_MATCHED"
            | "MATCHED_NOT_BROADCASTED"
            | "TRADE_STATUS_MATCHED_NOT_BROADCASTED"
            | "MINED"
            | "TRADE_STATUS_MINED"
            | "RETRYING"
            | "TRADE_STATUS_RETRYING",
        ) => FillFinality::Pending,
        _ => {
            if status_warning_due(&TRADE_STATUS_LAST_WARN_SECS, unix_secs()) {
                tracing::warn!(
                    service = "polymarket",
                    operation = "trade_page",
                    trade_id,
                    order_id,
                    raw_status = ?status_log_value(item.get("status")),
                    reason = "unknown_trade_status",
                    "polymarket trade status unknown"
                );
            }
            FillFinality::Pending
        }
    };
    // maker 的实收费用必须来自子订单；不得套用 taker 金额或猜测为零。
    let fee = optional_trade_decimal(record, &["fee_amount", "fee"])?;
    // 接口 fee_rate_bps 语义不可靠，仅保留在 raw；规范化费率由已验证快照恢复。
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
        fee_rate_bps: None,
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
    use crate::book::BookReject;
    let mut changed = Vec::new();
    match payload
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or("")
    {
        "book" => {
            let Some(token) = payload
                .get("asset_id")
                .or_else(|| payload.get("assetId"))
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            else {
                return changed;
            };
            match parse_book_json(payload) {
                Ok(BookSnapshot {
                    bids,
                    asks,
                    exchange_ts_ms: ts,
                    tick_size,
                }) => {
                    if books
                        .replace_snapshot_with_tick(
                            POLYMARKET, token, bids, asks, ts, now, tick_size,
                        )
                        .is_applied()
                    {
                        changed.push((token.into(), true));
                    }
                }
                Err(err) => {
                    books.invalidate_ws(POLYMARKET, token, BookReject::InvalidPayload);
                    tracing::warn!(platform = POLYMARKET, token, error = %err, "invalid WS book");
                }
            }
        }
        "tick_size_change" => {
            let Some(token) = book_token(payload) else {
                return changed;
            };
            let tick = payload
                .get("new_tick_size")
                .or_else(|| payload.get("newTickSize"))
                .and_then(parse_decimal)
                .filter(|v| *v > Decimal::ZERO && *v <= Decimal::ONE);
            let Ok(ts) = book_timestamp(payload) else {
                books.invalidate_ws(POLYMARKET, token, BookReject::InvalidPayload);
                return changed;
            };
            if let Some(tick) = tick {
                if books
                    .set_tick_size_at(POLYMARKET, token, tick, ts)
                    .is_applied()
                {
                    changed.push((token.into(), false));
                }
            } else {
                books.invalidate_ws(POLYMARKET, token, BookReject::InvalidPayload);
            }
        }
        "price_change" => {
            let ts = book_timestamp(payload);
            let Some(arr) = payload.get("price_changes").and_then(Value::as_array) else {
                if let Some(token) = book_token(payload) {
                    books.invalidate_ws(POLYMARKET, token, BookReject::InvalidPayload);
                }
                return changed;
            };
            let mut by_token = std::collections::BTreeMap::<&str, Vec<_>>::new();
            let mut invalid = std::collections::HashSet::new();
            for change in arr {
                let Some(token) = change
                    .get("asset_id")
                    .or_else(|| change.get("assetId"))
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                else {
                    continue;
                };
                let price = change
                    .get("price")
                    .and_then(parse_decimal)
                    .filter(|v| *v > Decimal::ZERO && *v <= Decimal::ONE);
                let size = change
                    .get("size")
                    .and_then(parse_decimal)
                    .filter(|v| *v >= Decimal::ZERO);
                let side = change.get("side").and_then(Value::as_str).and_then(|s| {
                    if s.eq_ignore_ascii_case("BUY") || s.eq_ignore_ascii_case("BID") {
                        Some(true)
                    } else if s.eq_ignore_ascii_case("SELL") || s.eq_ignore_ascii_case("ASK") {
                        Some(false)
                    } else {
                        None
                    }
                });
                if let (Some(price), Some(size), Some(is_bid), Ok(_)) = (price, size, side, &ts) {
                    by_token
                        .entry(token)
                        .or_default()
                        .push((is_bid, price, size));
                } else {
                    invalid.insert(token);
                }
            }
            for token in &invalid {
                books.invalidate_ws(POLYMARKET, token, BookReject::InvalidPayload);
            }
            if let Ok(ts) = ts {
                for (token, updates) in by_token {
                    if !invalid.contains(token)
                        && books
                            .apply_levels(POLYMARKET, token, &updates, ts, now)
                            .is_applied()
                    {
                        changed.push((token.into(), true));
                    }
                }
            }
        }
        _ => {}
    }
    changed
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestBookSkipCounts {
    pub missing_token: u64,
    pub no_ticket: u64,
    pub parse_error: u64,
    pub invalid_payload: u64,
    pub older_timestamp: u64,
    pub timestamp_conflict: u64,
    pub epoch_changed: u64,
    pub revision_changed: u64,
}

impl RestBookSkipCounts {
    pub fn total(&self) -> u64 {
        self.missing_token
            + self.no_ticket
            + self.parse_error
            + self.invalid_payload
            + self.older_timestamp
            + self.timestamp_conflict
            + self.epoch_changed
            + self.revision_changed
    }

    fn record_rejection(&mut self, reason: crate::book::BookReject) {
        use crate::book::BookReject;
        match reason {
            BookReject::InvalidPayload => self.invalid_payload += 1,
            BookReject::OlderTimestamp => self.older_timestamp += 1,
            BookReject::TimestampConflict => self.timestamp_conflict += 1,
            BookReject::EpochChanged => self.epoch_changed += 1,
            BookReject::RevisionChanged => self.revision_changed += 1,
        }
    }
}

pub fn apply_rest_books(
    books: &mut BookStore,
    payloads: &[Value],
    tickets: &[crate::book::RestTicket],
    now: Instant,
) -> (Vec<String>, RestBookSkipCounts) {
    let mut applied = Vec::new();
    let mut rejected = RestBookSkipCounts::default();
    for payload in payloads {
        let Some(token) = book_token(payload) else {
            rejected.missing_token += 1;
            continue;
        };
        let Some(ticket) = tickets
            .iter()
            .find(|t| t.key.platform == POLYMARKET && t.key.token_id == token)
        else {
            rejected.no_ticket += 1;
            continue;
        };
        match parse_book_json(payload) {
            Ok(BookSnapshot {
                bids,
                asks,
                exchange_ts_ms: ts,
                tick_size,
            }) => match books.accept_rest(ticket, bids, asks, ts, now, tick_size) {
                Ok(_) => applied.push(token.into()),
                Err(reason) => rejected.record_rejection(reason),
            },
            Err(err) => {
                rejected.parse_error += 1;
                tracing::warn!(platform = POLYMARKET, token, error = %err, "invalid REST book");
            }
        }
    }
    (applied, rejected)
}

// 错误可能携带 URL、HTTP 内容或帧；仅允许静态类别进入连接日志。
fn market_ws_error_kind(error: &tokio_tungstenite::tungstenite::Error) -> &'static str {
    use tokio_tungstenite::tungstenite::Error;
    match error {
        Error::ConnectionClosed => "connection_closed",
        Error::AlreadyClosed => "already_closed",
        Error::Io(_) => "io",
        Error::Tls(_) => "tls",
        Error::Capacity(_) => "capacity",
        Error::Protocol(_) => "protocol",
        Error::WriteBufferFull(_) => "write_buffer_full",
        Error::Utf8 => "utf8",
        Error::AttackAttempt => "attack_attempt",
        Error::Url(_) => "url",
        Error::Http(_) => "http",
        Error::HttpFormat(_) => "http_format",
    }
}

fn log_market_ws_failure(
    event: &'static str,
    reason: &'static str,
    started: Instant,
    subscription_count: usize,
    error: &tokio_tungstenite::tungstenite::Error,
) {
    let io_kind = match error {
        tokio_tungstenite::tungstenite::Error::Io(error) => Some(error.kind()),
        _ => None,
    };
    tracing::warn!(
        service = "polymarket",
        event,
        reason,
        ?io_kind,
        elapsed_ms = started.elapsed().as_millis() as u64,
        subscription_count,
        error_kind = market_ws_error_kind(error),
        "polymarket market ws failure"
    );
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
        let started = Instant::now();
        if *shutdown.borrow() {
            tracing::info!(
                service = "polymarket",
                event = "ws_stopped",
                reason = "shutdown",
                elapsed_ms = started.elapsed().as_millis() as u64,
                subscription_count = subscribed.len(),
                "polymarket market ws stopped"
            );
            break;
        }
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _)) => {
                books.lock().await.begin_platform_connection(POLYMARKET);
                tracing::info!(
                    service = "polymarket",
                    event = "ws_connected",
                    reason = "connected",
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    subscription_count = subscribed.len(),
                    "polymarket market ws connected"
                );
                let (mut write, mut read) = ws.split();
                if !subscribed.is_empty() {
                    if let Err(err) = write
                        .send(Message::Text(
                            json!({"operation":"subscribe","assets_ids": subscribed})
                                .to_string()
                                .into(),
                        ))
                        .await
                    {
                        log_market_ws_failure(
                            "ws_send_failed",
                            "initial_subscribe_failed",
                            started,
                            subscribed.len(),
                            &err,
                        );
                    }
                }
                let mut ping = tokio::time::interval(Duration::from_secs(10));
                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if let Err(err) = write.send(Message::Text("PING".into())).await {
                                log_market_ws_failure("ws_disconnected", "ping_send_failed",
                                    started, subscribed.len(), &err);
                                break;
                            }
                        }
                        msg = sub_rx.recv() => {
                            let Some(tokens) = msg else {
                                tracing::info!(service = "polymarket", event = "ws_stopped",
                                    reason = "subscription_channel_closed",
                                    elapsed_ms = started.elapsed().as_millis() as u64,
                                    subscription_count = subscribed.len(), "polymarket market ws stopped");
                                return;
                            };
                            let dropped: Vec<_> = subscribed
                                .iter()
                                .filter(|id| !tokens.contains(*id))
                                .cloned()
                                .collect();
                            if !dropped.is_empty() {
                                if let Err(err) = write.send(Message::Text(
                                    json!({"operation":"unsubscribe","assets_ids": dropped}).to_string().into()
                                )).await {
                                    log_market_ws_failure("ws_disconnected", "unsubscribe_failed",
                                        started, subscribed.len(), &err);
                                    break;
                                }
                            }
                            subscribed = tokens;
                            if let Err(err) = write.send(Message::Text(
                                json!({"operation":"subscribe","assets_ids": subscribed}).to_string().into()
                            )).await {
                                log_market_ws_failure("ws_disconnected", "subscribe_failed",
                                    started, subscribed.len(), &err);
                                break;
                            }
                        }
                        incoming = read.next() => {
                            let msg = match incoming {
                                Some(Ok(msg)) => msg,
                                Some(Err(err)) => {
                                    log_market_ws_failure("ws_disconnected", "read_error",
                                        started, subscribed.len(), &err);
                                    break;
                                }
                                None => {
                                    tracing::warn!(service = "polymarket", event = "ws_disconnected",
                                        reason = "eof", elapsed_ms = started.elapsed().as_millis() as u64,
                                        subscription_count = subscribed.len(), "polymarket market ws ended");
                                    break;
                                }
                            };
                            let text = match msg {
                                Message::Text(t) => t.to_string(),
                                Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                                Message::Ping(p) => {
                                    if let Err(err) = write.send(Message::Pong(p)).await {
                                        log_market_ws_failure("ws_send_failed", "pong_send_failed",
                                            started, subscribed.len(), &err);
                                    }
                                    continue;
                                }
                                Message::Close(frame) => {
                                    tracing::info!(service = "polymarket", event = "ws_close_received",
                                        reason = "close_frame", elapsed_ms = started.elapsed().as_millis() as u64,
                                        subscription_count = subscribed.len(),
                                        close_code = ?frame.map(|f| u16::from(f.code)),
                                        "polymarket market ws close received");
                                    continue;
                                }
                                _ => continue,
                            };
                            handle_ws_text(&text, &books, &calc_tx).await;
                        }
                        _ = wait_shutdown(&shutdown) => {
                            let reason = if *shutdown.borrow() { "shutdown" } else { "shutdown_channel_closed" };
                            tracing::info!(service = "polymarket", event = "ws_stopped", reason,
                                elapsed_ms = started.elapsed().as_millis() as u64,
                                subscription_count = subscribed.len(), "polymarket market ws stopped");
                            return;
                        },
                    }
                }
                books.lock().await.mark_platform_stale(POLYMARKET);
            }
            Err(err) => log_market_ws_failure(
                "ws_connect_failed",
                "connect_failed",
                started,
                subscribed.len(),
                &err,
            ),
        }
        tracing::info!(
            service = "polymarket",
            event = "ws_reconnect_wait",
            reason = "retry",
            elapsed_ms = started.elapsed().as_millis() as u64,
            subscription_count = subscribed.len(),
            delay_ms = 2000,
            "polymarket market ws reconnect scheduled"
        );
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

/// 前置准入与签名共用最终金额；不可表示时跳过，不靠舍入改变数量或限价。
pub(crate) fn market_buy_base_units(size: Decimal, cap: Decimal) -> Result<(u128, u128)> {
    require_positive(size, cap)?;
    if cap >= Decimal::ONE {
        return Err(Error::msg("invalid polymarket buy cap"));
    }
    let ten = BigInt::from(10u8);
    let size_scale = ten.pow(size.scale());
    let price_scale = ten.pow(cap.scale());
    let size_n = BigInt::from(size.mantissa());
    let price_n = BigInt::from(cap.mantissa());
    let taker_scaled = &size_n * ten.pow(MARKET_TAKER_DECIMALS);
    if (&taker_scaled % &size_scale) != BigInt::zero() {
        return Err(Error::msg(
            "polymarket buy shares exceed supported precision",
        ));
    }
    let taker = (taker_scaled / &size_scale) * ten.pow(6 - MARKET_TAKER_DECIMALS);
    let denominator = &size_scale * &price_scale;
    let cents_n = &size_n * &price_n * ten.pow(MARKET_MAKER_DECIMALS);
    let cents = (&cents_n + &denominator - 1u8) / &denominator;
    let maker = cents * ten.pow(6 - MARKET_MAKER_DECIMALS);
    // 不做除法投影；即使只超出一个基础单位也必须拒绝。
    if &maker * &price_scale > &taker * &price_n {
        return Err(Error::msg("polymarket buy amounts would exceed cap"));
    }
    let maker = maker.to_u128().filter(|n| *n > 0);
    let taker = taker.to_u128().filter(|n| *n > 0);
    match (maker, taker) {
        (Some(maker), Some(taker)) => Ok((maker, taker)),
        _ => Err(Error::msg("polymarket buy amounts outside supported range")),
    }
}

fn market_order_base_units(side: OrderSide, size: Decimal, price: Decimal) -> Result<(u128, u128)> {
    let (maker, taker) = match side {
        OrderSide::Buy => return market_buy_base_units(size, price),
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

// 与 WS 日志相同，只输出类型化原因；source 的 Display/Debug 可能含 URL、头或载荷。
fn books_http_error(error: reqwest::Error) -> Error {
    let kind = if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_body() {
        "body"
    } else if error.is_decode() {
        "decode"
    } else if error.is_request() {
        "request"
    } else if error.is_builder() {
        "builder"
    } else if error.is_redirect() {
        "redirect"
    } else if error.is_status() {
        "status"
    } else {
        "transport"
    };
    let mut io_kind = None;
    let mut os_error = None;
    let mut source = std::error::Error::source(&error);
    // 有界遍历，只提取标准 I/O 枚举和数字；不格式化任何不可信 source 文本。
    for _ in 0..16 {
        let Some(cause) = source else { break };
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            io_kind = Some(io.kind());
            os_error = io.raw_os_error();
        }
        source = cause.source();
    }
    Error::msg(format!(
        "polymarket books kind={kind} timeout={} connect={} request={} body={} decode={} io_kind={io_kind:?} os_error={os_error:?}",
        error.is_timeout(), error.is_connect(), error.is_request(), error.is_body(), error.is_decode()
    ))
}

fn redact_http(text: &str) -> String {
    text.chars().take(300).collect()
}

#[cfg(test)]
#[path = "../../tests/unit/platforms/polymarket.rs"]
pub(crate) mod tests;
