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
        let parsed: Value = serde_json::from_str(&text)?;
        let items = parsed
            .as_array()
            .cloned()
            .ok_or_else(|| Error::msg("polymarket books missing array"))?;
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

fn redact_http(text: &str) -> String {
    text.chars().take(300).collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncWriteExt;

    #[derive(Debug)]
    struct WsLogEvent {
        level: tracing::Level,
        fields: HashMap<String, String>,
    }

    impl tracing::field::Visit for WsLogEvent {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.fields
                .insert(field.name().into(), format!("{value:?}"));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.fields.insert(field.name().into(), value.into());
        }
    }

    struct WsLogCapture(mpsc::UnboundedSender<WsLogEvent>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WsLogCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut captured = WsLogEvent {
                level: *event.metadata().level(),
                fields: HashMap::new(),
            };
            event.record(&mut captured);
            let _ = self.0.send(captured);
        }
    }

    struct MarketWsFixture {
        listener: tokio::net::TcpListener,
        server: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        books: Arc<Mutex<BookStore>>,
        subscriptions: mpsc::Sender<Vec<String>>,
        shutdown: tokio::sync::watch::Sender<bool>,
        logs: mpsc::UnboundedReceiver<WsLogEvent>,
        client: tokio::task::JoinHandle<()>,
    }

    impl MarketWsFixture {
        async fn new() -> Self {
            use tracing::instrument::WithSubscriber;
            use tracing_subscriber::prelude::*;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}/private-url-marker", listener.local_addr().unwrap());
            let books = Arc::new(Mutex::new(BookStore::default()));
            let (calc_tx, _calc_rx) = mpsc::channel(10);
            let (subscriptions, sub_rx) = mpsc::channel(10);
            let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
            let (log_tx, logs) = mpsc::unbounded_channel();
            let subscriber = tracing_subscriber::registry().with(WsLogCapture(log_tx));
            let client = tokio::spawn(
                run_market_ws(url, books.clone(), calc_tx, sub_rx, shutdown_rx)
                    .with_subscriber(subscriber),
            );
            let (socket, _) = listener.accept().await.unwrap();
            let server = tokio_tungstenite::accept_async(socket).await.unwrap();
            let mut fixture = Self {
                listener,
                server,
                books,
                subscriptions,
                shutdown,
                logs,
                client,
            };
            fixture.log("ws_connected").await;
            assert_eq!(
                fixture.server.next().await.unwrap().unwrap(),
                Message::Text("PING".into())
            );
            fixture
        }

        async fn log(&mut self, event: &str) -> WsLogEvent {
            loop {
                let entry = self.logs.recv().await.unwrap();
                if entry.fields.get("event").map(String::as_str) == Some(event) {
                    assert_eq!(entry.fields["service"], "polymarket");
                    assert!(entry.fields.contains_key("elapsed_ms"));
                    assert!(entry.fields.contains_key("subscription_count"));
                    let rendered = format!("{:?}", entry.fields);
                    for secret in [
                        "private-url-marker",
                        "private-frame-marker",
                        "private-close-marker",
                    ] {
                        assert!(!rendered.contains(secret));
                    }
                    return entry;
                }
            }
        }

        async fn seed_book(&mut self) {
            self.server
                .send(Message::Binary(
                    json!({"event_type":"book","asset_id":"t",
                "timestamp":"100","bids":[],"asks":[]})
                    .to_string()
                    .into_bytes()
                    .into(),
                ))
                .await
                .unwrap();
            // Pong 是已处理前一帧的屏障，不依赖 sleep 或调度次数。
            self.server
                .send(Message::Ping(
                    "private-frame-marker".as_bytes().to_vec().into(),
                ))
                .await
                .unwrap();
            assert_eq!(
                self.server.next().await.unwrap().unwrap(),
                Message::Pong("private-frame-marker".as_bytes().to_vec().into())
            );
            assert!(!self.books.lock().await.get(POLYMARKET, "t").unwrap().stale);
        }

        async fn subscribe(&mut self, tokens: &[&str]) {
            self.subscriptions
                .send(tokens.iter().map(|token| (*token).into()).collect())
                .await
                .unwrap();
            let message = self
                .server
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&message).unwrap(),
                json!({"operation":"subscribe","assets_ids":tokens})
            );
        }

        async fn close_frame(&mut self) {
            use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
            self.server
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: "private-close-marker".into(),
                })))
                .await
                .unwrap();
            let entry = self.log("ws_close_received").await;
            assert_eq!(entry.level, tracing::Level::INFO);
            assert_eq!(entry.fields["close_code"], "Some(1000)");
            assert!(!self.client.is_finished());
            assert!(!self.books.lock().await.get(POLYMARKET, "t").unwrap().stale);
        }
    }

    #[tokio::test]
    async fn market_ws_normal_stops_and_close_keep_books_current() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for stop in [
                "shutdown",
                "shutdown_channel_closed",
                "subscription_channel_closed",
            ] {
                let mut fixture = MarketWsFixture::new().await;
                fixture.seed_book().await;
                fixture.close_frame().await;
                // 服务端保持 TCP 打开，Close 本身不会触发 stale 或重连。
                let replacement_sub = mpsc::channel(1).0;
                let replacement_shutdown = tokio::sync::watch::channel(false).0;
                match stop {
                    "shutdown" => fixture.shutdown.send(true).unwrap(),
                    "shutdown_channel_closed" => drop(std::mem::replace(
                        &mut fixture.shutdown,
                        replacement_shutdown,
                    )),
                    "subscription_channel_closed" => drop(std::mem::replace(
                        &mut fixture.subscriptions,
                        replacement_sub,
                    )),
                    _ => unreachable!(),
                }
                let entry = fixture.log("ws_stopped").await;
                assert_eq!(entry.level, tracing::Level::INFO);
                assert_eq!(entry.fields["reason"], stop);
                fixture.client.await.unwrap();
                assert!(
                    !fixture
                        .books
                        .lock()
                        .await
                        .get(POLYMARKET, "t")
                        .unwrap()
                        .stale
                );
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn market_ws_close_then_subscription_failure_marks_stale() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for (initial, next, reason) in [
                (vec![], vec!["t"], "subscribe_failed"),
                (vec!["t"], vec![], "unsubscribe_failed"),
            ] {
                let mut fixture = MarketWsFixture::new().await;
                fixture.seed_book().await;
                if !initial.is_empty() {
                    fixture.subscribe(&initial).await;
                }
                fixture.close_frame().await;
                fixture
                    .subscriptions
                    .send(next.iter().map(|t| (*t).into()).collect())
                    .await
                    .unwrap();
                let entry = fixture.log("ws_disconnected").await;
                assert_eq!(entry.level, tracing::Level::WARN);
                assert_eq!(entry.fields["reason"], reason);
                assert_eq!(entry.fields["error_kind"], "protocol");
                fixture.log("ws_reconnect_wait").await;
                assert!(
                    fixture
                        .books
                        .lock()
                        .await
                        .get(POLYMARKET, "t")
                        .unwrap()
                        .stale
                );
                fixture.client.abort();
                assert!(fixture.client.await.unwrap_err().is_cancelled());
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn market_ws_read_error_reconnects_after_two_seconds_and_resubscribes() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut fixture = MarketWsFixture::new().await;
            fixture.seed_book().await;
            fixture.subscribe(&["t"]).await;
            let before = fixture.books.lock().await.begin_rest(POLYMARKET, "t");
            // 不发送 Close 的 TCP EOF 在 tungstenite 中是 read_error。
            fixture.server.get_mut().shutdown().await.unwrap();
            let entry = fixture.log("ws_disconnected").await;
            assert_eq!(entry.fields["reason"], "read_error");
            fixture.log("ws_reconnect_wait").await;
            assert!(
                fixture
                    .books
                    .lock()
                    .await
                    .get(POLYMARKET, "t")
                    .unwrap()
                    .stale
            );
            // 网络事件完成后再冻结时间，避免 I/O 等待触发虚拟时钟自动推进。
            tokio::time::pause();
            tokio::time::advance(Duration::from_millis(1999)).await;
            assert!(fixture.logs.try_recv().is_err());
            tokio::time::advance(Duration::from_millis(1)).await;
            tokio::time::resume();
            let (socket, _) = fixture.listener.accept().await.unwrap();
            let mut reconnected = tokio_tungstenite::accept_async(socket).await.unwrap();
            let entry = fixture.log("ws_connected").await;
            assert_eq!(entry.fields["subscription_count"], "1");
            let message = reconnected
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&message).unwrap(),
                json!({"operation":"subscribe","assets_ids":["t"]})
            );
            assert!(fixture.books.lock().await.begin_rest(POLYMARKET, "t").epoch > before.epoch);
            fixture.shutdown.send(true).unwrap();
            fixture.client.await.unwrap();
        })
        .await
        .unwrap();
    }

    #[test]
    fn market_ws_failure_logs_only_safe_error_categories() {
        use tokio_tungstenite::tungstenite::Error as WsError;
        use tracing_subscriber::prelude::*;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let subscriber = tracing_subscriber::registry().with(WsLogCapture(tx));
        tracing::subscriber::with_default(subscriber, || {
            for (error, expected) in [
                (
                    WsError::Io(std::io::Error::other("private-url-marker")),
                    "io",
                ),
                (
                    WsError::WriteBufferFull(Message::Text("private-frame-marker".into())),
                    "write_buffer_full",
                ),
                (
                    WsError::Http(tokio_tungstenite::tungstenite::http::Response::new(Some(
                        b"private-frame-marker".to_vec(),
                    ))),
                    "http",
                ),
            ] {
                for reason in [
                    "initial_subscribe_failed",
                    "pong_send_failed",
                    "ping_send_failed",
                    "connect_failed",
                ] {
                    log_market_ws_failure("ws_send_failed", reason, Instant::now(), 3, &error);
                    let entry = rx.try_recv().unwrap();
                    assert_eq!(entry.level, tracing::Level::WARN);
                    assert_eq!(entry.fields["error_kind"], expected);
                    assert_eq!(
                        entry.fields["io_kind"],
                        if expected == "io" {
                            "Some(Other)"
                        } else {
                            "None"
                        }
                    );
                    assert_eq!(entry.fields["reason"], reason);
                    assert_eq!(entry.fields["subscription_count"], "3");
                    assert!(!format!("{:?}", entry.fields).contains("private-"));
                }
            }
        });
    }

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
    fn price_change_applies_whole_token_batch_once() {
        let mut books = BookStore::default();
        let now = Instant::now();
        for token in ["a", "b"] {
            apply_ws_message(
                &mut books,
                &json!({
                    "event_type":"book", "asset_id":token, "timestamp":"100",
                    "tick_size":"0.01",
                    "bids":[{"price":"0.40","size":"10"}],
                    "asks":[{"price":"0.50","size":"10"}]
                }),
                now,
            );
        }
        let payload = json!({
            "event_type":"price_change", "timestamp":"101", "price_changes":[
                {"asset_id":"a","side":"BUY","price":"0.40","size":"0"},
                {"asset_id":"b","side":"SELL","price":"0.50","size":"3"},
                {"asset_id":"a","side":"SELL","price":"0.50","size":"0"},
                {"asset_id":"a","side":"BUY","price":"0.41","size":"1"},
                {"asset_id":"a","side":"BUY","price":"0.42","size":"2"},
                {"asset_id":"a","side":"BUY","price":"0.41","size":"4"},
                {"asset_id":"a","side":"SELL","price":"0.53","size":"6"},
                {"asset_id":"a","side":"SELL","price":"0.52","size":"5"}
            ]
        });
        let mut changed = apply_ws_message(&mut books, &payload, now);
        changed.sort();
        assert_eq!(changed, vec![("a".into(), true), ("b".into(), true)]);
        let book = books.get(POLYMARKET, "a").unwrap();
        assert_eq!(
            book.bids
                .iter()
                .map(|l| (l.price, l.size))
                .collect::<Vec<_>>(),
            vec![
                ("0.42".parse().unwrap(), Decimal::from(2)),
                ("0.41".parse().unwrap(), Decimal::from(4))
            ]
        );
        assert_eq!(
            book.asks
                .iter()
                .map(|l| (l.price, l.size))
                .collect::<Vec<_>>(),
            vec![
                ("0.52".parse().unwrap(), Decimal::from(5)),
                ("0.53".parse().unwrap(), Decimal::from(6))
            ]
        );
        assert_eq!(book.tick_size, Some("0.01".parse().unwrap()));
        assert_eq!(
            books.get(POLYMARKET, "b").unwrap().asks[0].size,
            Decimal::from(3)
        );
        let later = now + Duration::from_secs(1);
        for ts in ["99"] {
            let mut replay = payload.clone();
            replay["timestamp"] = json!(ts);
            assert!(apply_ws_message(&mut books, &replay, later).is_empty());
            assert_eq!(books.get(POLYMARKET, "a").unwrap().received_at, now);
        }
        // 一个 token 的无变化批次不能阻止同消息中另一个 token 的有效更新。
        books.replace_snapshot(POLYMARKET, "b", vec![], vec![], 102, later);
        let mut mixed = payload;
        mixed["timestamp"] = json!("102");
        assert_eq!(
            apply_ws_message(&mut books, &mixed, later),
            vec![("b".into(), true)]
        );
    }

    #[test]
    fn disconnected_book_waits_for_full_snapshot_through_ws_and_rest() {
        for use_rest in [false, true] {
            let now = Instant::now();
            let mut books = BookStore::default();
            let mut snapshot = json!({
                "event_type":"book", "asset_id":"t", "timestamp":"100", "tick_size":"0.01",
                "bids":[{"price":"0.40","size":"10"}],
                "asks":[{"price":"0.50","size":"10"}]
            });
            apply_ws_message(&mut books, &snapshot, now);
            books.mark_platform_stale(POLYMARKET);
            let later = now + Duration::from_secs(1);
            apply_ws_message(
                &mut books,
                &json!({
                    "event_type":"price_change", "timestamp":"102", "price_changes":[
                        {"asset_id":"t","side":"BUY","price":"0.40","size":"8"}
                    ]
                }),
                later,
            );
            assert!(!books
                .get(POLYMARKET, "t")
                .unwrap()
                .is_fresh(Duration::from_secs(5), later));
            snapshot["timestamp"] = json!("101");
            assert!(apply_ws_message(&mut books, &snapshot, later).is_empty());
            assert!(books.get(POLYMARKET, "t").unwrap().stale);
            snapshot["timestamp"] = json!("103");
            snapshot["asks"] = json!([]);
            snapshot["bids"][0]["size"] = json!("8");
            snapshot.as_object_mut().unwrap().remove("tick_size");
            if use_rest {
                let tickets = vec![books.begin_rest(POLYMARKET, "t")];
                assert_eq!(
                    apply_rest_books(&mut books, &[snapshot.clone()], &tickets, later),
                    (vec!["t".into()], RestBookSkipCounts::default())
                );
            } else {
                assert_eq!(
                    apply_ws_message(&mut books, &snapshot, later),
                    vec![("t".into(), true)]
                );
            }
            let book = books.get(POLYMARKET, "t").unwrap();
            assert!(book.is_fresh(Duration::from_secs(5), later));
            assert!(book.asks.is_empty());
            assert_eq!(book.bids[0].size, Decimal::from(8));
            assert_eq!(book.tick_size, Some("0.01".parse().unwrap()));
            let initialized = apply_ws_message(&mut books, &snapshot, later);
            assert_eq!(initialized.is_empty(), !use_rest);
        }
    }

    #[test]
    fn malformed_ws_payload_invalidates_identified_token_atomically() {
        let now = Instant::now();
        let valid = json!({"event_type":"book", "asset_id":"t", "timestamp":"100",
            "bids":[], "asks":[{"price":"0.5","size":"3"}]});
        for bad in [
            json!({"event_type":"book", "asset_id":"t", "timestamp":"bad", "bids":[], "asks":[]}),
            json!({"event_type":"book", "asset_id":"t", "timestamp":"101", "bids":[]}),
            json!({"event_type":"price_change", "timestamp":"bad", "price_changes":[{"asset_id":"t","side":"BUY","price":"0.4","size":"1"}]}),
            json!({"event_type":"price_change", "timestamp":"101", "price_changes":[
                {"asset_id":"t","side":"SELL","price":"0.5","size":"0"},
                {"asset_id":"t","side":"unexpected","price":"0.4","size":"1"}]}),
            json!({"event_type":"price_change", "timestamp":"101", "price_changes":[{"asset_id":"t","side":"BUY","price":"0.4","size":"-1"}]}),
        ] {
            let mut books = BookStore::default();
            apply_ws_message(&mut books, &valid, now);
            assert!(apply_ws_message(&mut books, &bad, now).is_empty());
            let book = books.get_at(POLYMARKET, "t", now).unwrap();
            assert!(book.stale);
            assert_eq!(book.exchange_ts_ms, 100);
            assert_eq!(book.asks[0].size, Decimal::from(3));
        }
    }

    #[test]
    fn strict_book_parser_rejects_incomplete_identity_timestamp_and_levels() {
        let valid = json!({"asset_id":"t", "timestamp":"100", "bids":[], "asks":[{"price":"0.5","size":"3"}]});
        for (field, bad) in [
            ("timestamp", json!(null)),
            ("timestamp", json!("invalid")),
            ("timestamp", json!(0)),
            ("timestamp", json!(-1)),
            ("timestamp", json!(1.5)),
            ("asset_id", json!("")),
            ("asks", json!(null)),
            ("asks", json!([{"price":"0.5"}])),
            ("asks", json!([{"price":"0.5","size":"-1"}])),
            ("asks", json!([{"price":"1.1","size":"1"}])),
            (
                "asks",
                json!([{"price":"0.5","size":"1"},{"price":"0.5","size":"2"}]),
            ),
            ("tick_size", json!("oops")),
        ] {
            let mut payload = valid.clone();
            payload[field] = bad;
            assert!(parse_book_json(&payload).is_err(), "field={field}");
        }
        assert!(validate_book_identity(&valid, "other").is_err());
        assert!(parse_book_json(&valid).is_ok());
    }

    #[test]
    fn batch_rest_checks_requested_identity_and_each_ticket_independently() {
        let now = Instant::now();
        let mut books = BookStore::default();
        let tickets = ["a", "b", "missing"].map(|t| books.begin_rest(POLYMARKET, t));
        books.set_tick_size(POLYMARKET, "a", Decimal::new(1, 2));
        let payloads: Vec<_> = ["a", "b", "unsolicited"]
            .map(|t| json!({"asset_id":t,"timestamp":"101","bids":[],"asks":[]}))
            .into();
        let (accepted, rejected) = apply_rest_books(&mut books, &payloads, &tickets, now);
        assert_eq!(accepted, vec!["b"]);
        assert_eq!(
            rejected,
            RestBookSkipCounts {
                no_ticket: 1,
                revision_changed: 1,
                ..Default::default()
            }
        );
        assert!(books.get(POLYMARKET, "unsolicited").is_none());
        assert!(books.get(POLYMARKET, "missing").is_none());
        assert_eq!(
            apply_rest_books(&mut books, &payloads, &tickets, now).1,
            RestBookSkipCounts {
                no_ticket: 1,
                revision_changed: 2,
                ..Default::default()
            }
        );
    }

    #[test]
    fn rest_skip_counts_map_every_book_rejection_and_total() {
        use crate::book::BookReject;
        for (reason, expected) in [
            (
                BookReject::InvalidPayload,
                RestBookSkipCounts {
                    invalid_payload: 1,
                    ..Default::default()
                },
            ),
            (
                BookReject::OlderTimestamp,
                RestBookSkipCounts {
                    older_timestamp: 1,
                    ..Default::default()
                },
            ),
            (
                BookReject::TimestampConflict,
                RestBookSkipCounts {
                    timestamp_conflict: 1,
                    ..Default::default()
                },
            ),
            (
                BookReject::EpochChanged,
                RestBookSkipCounts {
                    epoch_changed: 1,
                    ..Default::default()
                },
            ),
            (
                BookReject::RevisionChanged,
                RestBookSkipCounts {
                    revision_changed: 1,
                    ..Default::default()
                },
            ),
        ] {
            let mut counts = RestBookSkipCounts::default();
            counts.record_rejection(reason);
            assert_eq!(counts, expected);
            assert_eq!(counts.total(), 1);
        }
        assert_eq!(RestBookSkipCounts::default().total(), 0);
        assert_eq!(
            RestBookSkipCounts {
                missing_token: 1,
                no_ticket: 2,
                parse_error: 3,
                invalid_payload: 4,
                older_timestamp: 5,
                timestamp_conflict: 6,
                epoch_changed: 7,
                revision_changed: 8,
            }
            .total(),
            36
        );
    }

    #[test]
    fn rest_skip_counts_preserve_reachable_branch_precedence() {
        let now = Instant::now();
        let mut books = BookStore::default();
        for token in ["old", "conflict"] {
            books.replace_snapshot(POLYMARKET, token, vec![], vec![], 200, now);
        }
        let tickets = ["bad", "old", "conflict", "epoch", "revision", "ok"]
            .map(|token| books.begin_rest(POLYMARKET, token));
        let mut tickets = tickets.to_vec();
        tickets
            .iter_mut()
            .find(|t| t.key.token_id == "epoch")
            .unwrap()
            .epoch += 1;
        books.set_tick_size(POLYMARKET, "revision", Decimal::new(1, 2));
        let mut foreign = books.begin_rest("other_platform", "foreign");
        foreign.revision += 1;
        tickets.push(foreign);
        // 身份、票据、解析先于 accept_rest；无效载荷已由严格解析器拦截。
        let payloads = vec![
            json!({"timestamp":"bad"}),
            json!({"asset_id":"unsolicited","timestamp":"bad"}),
            json!({"asset_id":"foreign","timestamp":"bad"}),
            json!({"asset_id":"bad","timestamp":"bad"}),
            json!({"asset_id":"epoch","timestamp":"bad"}),
            json!({"asset_id":"old","timestamp":"100","bids":[],"asks":[]}),
            json!({"asset_id":"conflict","timestamp":"200","bids":[],"asks":[{"price":"0.5","size":"1"}]}),
            json!({"asset_id":"epoch","timestamp":"100","bids":[],"asks":[]}),
            json!({"asset_id":"revision","timestamp":"100","bids":[],"asks":[]}),
            json!({"asset_id":"ok","timestamp":"100","bids":[],"asks":[]}),
        ];
        let (applied, skipped) = apply_rest_books(&mut books, &payloads, &tickets, now);
        assert_eq!(applied, vec!["ok"]);
        assert_eq!(
            skipped,
            RestBookSkipCounts {
                missing_token: 1,
                no_ticket: 2,
                parse_error: 2,
                older_timestamp: 1,
                timestamp_conflict: 1,
                epoch_changed: 1,
                revision_changed: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            applied.len() as u64 + skipped.total(),
            payloads.len() as u64
        );
        assert!(books.get(POLYMARKET, "conflict").unwrap().asks.is_empty());
        assert!(books.get(POLYMARKET, "epoch").is_none());
    }

    #[test]
    fn rest_skip_counts_count_returned_payloads_not_missing_responses() {
        let now = Instant::now();
        let mut books = BookStore::default();
        let tickets = ["a", "not_returned"].map(|token| books.begin_rest(POLYMARKET, token));
        assert_eq!(
            apply_rest_books(&mut books, &[], &tickets, now),
            (vec![], RestBookSkipCounts::default())
        );
        assert_eq!(
            apply_rest_books(&mut books, &[], &[], now),
            (vec![], RestBookSkipCounts::default())
        );
        let payload = json!({"asset_id":"a","timestamp":"100","bids":[],"asks":[]});
        assert_eq!(
            apply_rest_books(&mut books, &[payload.clone()], &[], now),
            (
                vec![],
                RestBookSkipCounts {
                    no_ticket: 1,
                    ..Default::default()
                }
            )
        );
        assert_eq!(
            apply_rest_books(&mut books, &[payload.clone(), payload], &tickets, now),
            (
                vec!["a".into()],
                RestBookSkipCounts {
                    revision_changed: 1,
                    ..Default::default()
                }
            )
        );
        assert!(books.get(POLYMARKET, "not_returned").is_none());
    }

    #[tokio::test]
    async fn frame_array_preserves_same_millisecond_set_delete_order() {
        let books = Arc::new(Mutex::new(BookStore::default()));
        let (tx, _rx) = mpsc::channel(10);
        let frame = json!([
            {"event_type":"book","asset_id":"t","timestamp":"100","bids":[],"asks":[{"price":"0.5","size":"3"}]},
            {"event_type":"price_change","timestamp":"100","price_changes":[{"asset_id":"t","side":"SELL","price":"0.5","size":"2"}]},
            {"event_type":"price_change","timestamp":"100","price_changes":[{"asset_id":"t","side":"SELL","price":"0.5","size":"0"}]},
            {"event_type":"book","asset_id":"t","timestamp":"100","bids":[],"asks":[{"price":"0.5","size":"3"}]}
        ]);
        handle_ws_text(&frame.to_string(), &books, &tx).await;
        let books = books.lock().await;
        let book = books.get(POLYMARKET, "t").unwrap();
        assert!(book.asks.is_empty());
        assert!(book.stale);
    }

    #[test]
    fn tick_200_blocks_new_ticket_rest_150_with_or_without_tick() {
        for tick in [None, Some("0.01")] {
            let now = Instant::now();
            let mut books = BookStore::default();
            apply_ws_message(
                &mut books,
                &json!({
                    "event_type":"book", "asset_id":"t", "timestamp":"100",
                    "bids":[], "asks":[], "tick_size":"0.01"
                }),
                now,
            );
            apply_ws_message(
                &mut books,
                &json!({
                    "event_type":"tick_size_change", "asset_id":"t", "timestamp":"200",
                    "new_tick_size":"0.001"
                }),
                now,
            );
            let ticket = books.begin_rest(POLYMARKET, "t");
            assert_eq!(
                books
                    .accept_rest(
                        &ticket,
                        vec![],
                        vec![],
                        150,
                        now,
                        tick.map(|v| v.parse().unwrap())
                    )
                    .unwrap_err(),
                crate::book::BookReject::OlderTimestamp
            );
            assert_eq!(
                books.get(POLYMARKET, "t").unwrap().tick_size,
                Some("0.001".parse().unwrap())
            );
        }
    }

    #[test]
    fn ws_and_batch_rest_share_tick_ordering_and_atomic_conflict_rules() {
        let now = Instant::now();
        let mut books = BookStore::default();
        apply_ws_message(
            &mut books,
            &json!({"event_type":"book","asset_id":"t","timestamp":"100","bids":[],"asks":[],"tick_size":"0.01"}),
            now,
        );
        apply_ws_message(
            &mut books,
            &json!({"event_type":"tick_size_change","asset_id":"t","timestamp":"200","new_tick_size":"0.001"}),
            now,
        );
        for tick in [None, Some("0.01")] {
            let mut old =
                json!({"event_type":"book","asset_id":"t","timestamp":"150","bids":[],"asks":[]});
            if let Some(tick) = tick {
                old["tick_size"] = json!(tick);
            }
            assert!(apply_ws_message(&mut books, &old, now).is_empty());
            let ticket = books.begin_rest(POLYMARKET, "t");
            assert_eq!(
                apply_rest_books(&mut books, &[old], &[ticket], now),
                (
                    vec![],
                    RestBookSkipCounts {
                        older_timestamp: 1,
                        ..Default::default()
                    }
                )
            );
            assert_eq!(books.tick_size(POLYMARKET, "t"), Some(Decimal::new(1, 3)));
            assert_eq!(books.get(POLYMARKET, "t").unwrap().exchange_ts_ms, 100);
        }
        apply_ws_message(
            &mut books,
            &json!({"event_type":"tick_size_change","asset_id":"t","timestamp":"150","new_tick_size":"0.01"}),
            now,
        );
        assert!(!books.get(POLYMARKET, "t").unwrap().stale);
        let conflict = json!({"event_type":"book","asset_id":"t","timestamp":"200","bids":[],"asks":[{"price":"0.5","size":"3"}],"tick_size":"0.01"});
        assert!(apply_ws_message(&mut books, &conflict, now).is_empty());
        let book = books.get(POLYMARKET, "t").unwrap();
        assert!(book.asks.is_empty());
        assert!(!book.stale);
        assert_eq!(book.tick_size, None);
        let ticket = books.begin_rest(POLYMARKET, "t");
        let restored =
            json!({"asset_id":"t","timestamp":"201","bids":[],"asks":[],"tick_size":"0.001"});
        assert_eq!(
            apply_rest_books(&mut books, &[restored], &[ticket], now),
            (vec!["t".into()], RestBookSkipCounts::default())
        );
        assert_eq!(books.tick_size(POLYMARKET, "t"), Some(Decimal::new(1, 3)));
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
        let tickets = vec![
            books.begin_rest(POLYMARKET, "t1"),
            books.begin_rest(POLYMARKET, "t2"),
        ];
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
            &tickets,
            now,
        );
        assert_eq!(applied, vec!["t2".to_string()]);
        assert_eq!(
            skipped_old,
            RestBookSkipCounts {
                older_timestamp: 1,
                ..Default::default()
            }
        );
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
    fn status_warnings_are_rate_limited_per_operation() {
        let order = AtomicU64::new(0);
        let trade = AtomicU64::new(0);
        assert!(status_warning_due(&order, 100));
        assert!(status_warning_due(&trade, 100));
        for now in [100, 102, 130, 159, 99] {
            assert!(!status_warning_due(&order, now), "now={now}");
            assert!(!status_warning_due(&trade, now), "now={now}");
        }
        assert!(status_warning_due(&order, 160));
        assert!(!status_warning_due(&order, 160));
        assert!(status_warning_due(&trade, 160));
    }

    #[test]
    fn concurrent_status_warnings_have_one_winner() {
        let last_warn_secs = AtomicU64::new(0);
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        status_warning_due(&last_warn_secs, 100)
                    })
                })
                .collect();
            let winners = threads
                .into_iter()
                .map(|thread| usize::from(thread.join().unwrap()))
                .sum::<usize>();
            assert_eq!(winners, 1);
        });
    }

    #[test]
    fn status_log_value_is_bounded_and_never_serializes_a_body() {
        let long_status = "状".repeat(100);
        assert_eq!(status_log_value(Some(&json!(long_status))), "状".repeat(80));
        assert_eq!(status_log_value(None), "<missing>");
        for invalid in [
            Value::Null,
            json!(true),
            json!({"status": "body"}),
            json!(["body"]),
        ] {
            assert_eq!(status_log_value(Some(&invalid)), "<invalid>");
        }
    }

    #[test]
    fn order_status_matrix_normalizes_only_known_full_values_and_preserves_raw() {
        for (bare, prefixed, expected) in [
            ("MATCHED", "ORDER_STATUS_MATCHED", "matched"),
            ("LIVE", "ORDER_STATUS_LIVE", "live"),
            ("INVALID", "ORDER_STATUS_INVALID", "invalid"),
            ("CANCELED", "ORDER_STATUS_CANCELED", "cancelled"),
            (
                "CANCELED_MARKET_RESOLVED",
                "ORDER_STATUS_CANCELED_MARKET_RESOLVED",
                "cancelled",
            ),
        ] {
            for status in [bare, prefixed] {
                for status in [status.to_string(), status.to_ascii_lowercase()] {
                    let raw = json!({"id": "remote-oid", "status": status});
                    let poll = parse_order_poll(raw.clone(), "requested-oid");
                    assert!(poll.found);
                    assert_eq!(poll.status, expected, "status={status}");
                    assert_eq!(normalized_order_status(&status), Some(expected));
                    assert_eq!(poll.raw, raw);
                }
            }
        }
        for status in ["cancelled", "canceled", "expired", "unmatched", "rejected"] {
            for status in [status.to_string(), status.to_ascii_uppercase()] {
                let raw = json!({"status": status});
                let poll = parse_order_poll(raw.clone(), "oid-1");
                assert_eq!(poll.status, "cancelled", "status={status}");
                assert_eq!(normalized_order_status(&status), Some("cancelled"));
                assert_eq!(poll.raw, raw);
            }
        }
    }

    #[test]
    fn order_status_matrix_preserves_unknown_and_invalid_values() {
        for status in [
            json!("UNKNOWN"),
            json!("ORDER_STATUS_UNKNOWN"),
            json!("FUTURE_CANCELED"),
            json!("NOT_CANCELLED"),
            json!("ORDER_STATUS_REJECTED_FUTURE"),
            json!("ORDER_STATUS_REJECTED"),
            json!("ORDER_STATUS_EXPIRED"),
            json!("ORDER_STATUS_UNMATCHED"),
            json!("ORDER_STATUS_CANCELLED"),
            json!("ORDER_STATUS_ORDER_STATUS_MATCHED"),
            json!("TRADE_STATUS_CONFIRMED"),
            json!(" MATCHED "),
            json!(""),
            Value::Null,
            json!(true),
            json!(17),
            json!({"status": "MATCHED"}),
            json!(["MATCHED"]),
        ] {
            let raw = json!({"status": status});
            let poll = parse_order_poll(raw.clone(), "oid-1");
            assert!(poll.found);
            assert_eq!(poll.status, status.as_str().unwrap_or_default());
            assert_eq!(normalized_order_status(&poll.status), None);
            assert_eq!(poll.raw, raw);
        }
        let raw = json!({"id": "oid-1"});
        let poll = parse_order_poll(raw.clone(), "oid-1");
        assert!(poll.status.is_empty());
        assert_eq!(poll.raw, raw);
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
            (json!("TRADE_STATUS_CONFIRMED"), FillFinality::Confirmed),
            (json!("FAILED"), FillFinality::Failed),
            (json!("TRADE_STATUS_FAILED"), FillFinality::Failed),
            (json!("MATCHED"), FillFinality::Pending),
            (json!("TRADE_STATUS_MATCHED"), FillFinality::Pending),
            (json!("MATCHED_NOT_BROADCASTED"), FillFinality::Pending),
            (
                json!("TRADE_STATUS_MATCHED_NOT_BROADCASTED"),
                FillFinality::Pending,
            ),
            (json!("MINED"), FillFinality::Pending),
            (json!("TRADE_STATUS_MINED"), FillFinality::Pending),
            (json!("RETRYING"), FillFinality::Pending),
            (json!("TRADE_STATUS_RETRYING"), FillFinality::Pending),
            (json!("UNKNOWN"), FillFinality::Pending),
            (json!("TRADE_STATUS_UNKNOWN"), FillFinality::Pending),
            (json!("FUTURE_CONFIRMED"), FillFinality::Pending),
            (json!("NOT_FAILED"), FillFinality::Pending),
            (
                json!("TRADE_STATUS_TRADE_STATUS_CONFIRMED"),
                FillFinality::Pending,
            ),
            (json!("ORDER_STATUS_CONFIRMED"), FillFinality::Pending),
            (json!("confirmed"), FillFinality::Pending),
            (json!("trade_status_confirmed"), FillFinality::Pending),
            (json!(" CONFIRMED "), FillFinality::Pending),
            (json!(""), FillFinality::Pending),
            (Value::Null, FillFinality::Pending),
            (json!(true), FillFinality::Pending),
            (json!(17), FillFinality::Pending),
            (json!({"status": "CONFIRMED"}), FillFinality::Pending),
            (json!(["CONFIRMED"]), FillFinality::Pending),
        ] {
            let mut trade = trade_fixture("t1");
            trade["status"] = status.clone();
            trade["maker_orders"] = json!([{
                "order_id": "maker-1", "asset_id": "yes",
                "matched_amount": "5", "price": "0.4"
            }]);
            let fills = parse_trades(&json!([trade])).unwrap();
            assert_eq!(fills.len(), 2);
            for fill in fills {
                assert_eq!(fill.finality, expected, "status={status}");
                assert_eq!(fill.raw["status"], status);
                assert_eq!(fill.fee, None);
                assert_eq!(fill.fee_token, None);
            }
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
        assert!(fills.iter().all(|fill| fill.fee_rate_bps.is_none()));
        assert_eq!(fills[2].raw["fee_rate_bps"], "700");
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
    fn parse_trades_ignores_untrusted_fee_rates_but_preserves_raw() {
        for value in [
            json!("broken"),
            json!("-1"),
            json!("700"),
            json!(true),
            json!({"rate": 700}),
            json!([700]),
            Value::Null,
        ] {
            let mut trade = trade_fixture("untrusted-rate");
            trade["fee_rate_bps"] = value.clone();
            trade["maker_orders"] = json!([{
                "order_id": "maker-1", "asset_id": "yes",
                "matched_amount": "5", "price": "0.4", "fee_rate_bps": value
            }]);
            let fills = parse_trades(&json!([trade])).unwrap();
            assert_eq!(fills.len(), 2);
            for fill in fills {
                assert_eq!(fill.fee_rate_bps, None);
                assert_eq!(fill.fee, None);
                assert_eq!(fill.raw["fee_rate_bps"], value);
                if fill.raw["role"] == "maker" {
                    assert_eq!(fill.raw["maker_order"]["fee_rate_bps"], value);
                }
            }
        }
        for (field, value) in [
            ("order_id", json!("")),
            ("asset_id", json!(null)),
            ("matched_amount", json!("broken")),
            ("price", json!("1.1")),
            ("fee_amount", json!("broken")),
            ("fee", json!("-1")),
            ("fee_token", json!(false)),
        ] {
            let mut trade = trade_fixture("invalid-maker");
            trade["maker_orders"] = json!([{
                "order_id": "maker-1", "asset_id": "yes", "matched_amount": "5",
                "price": "0.4", "fee_rate_bps": "broken"
            }]);
            trade["maker_orders"][0][field] = value;
            assert!(parse_trades(&json!([trade])).is_err(), "{field}");
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
            ("fee_amount", json!("broken")),
            ("fee_amount", json!("-1")),
            ("fee", json!("broken")),
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

    pub(crate) async fn poll_stub(
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

    pub(crate) async fn execution_test_venue(base: String) -> (PolymarketVenue, String) {
        let mut venue = cache_test_venue();
        venue.base = base;
        venue.http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        venue.auth_ttl = Duration::ZERO;
        let signer = PrivateKeySigner::random();
        let funder = format!("{:#x}", signer.address());
        venue.authed.lock().await.insert(
            funder.clone(),
            PolymarketAccount {
                funder: funder.clone(),
                service: None,
                signature_type: 0,
                signer,
                api_key: "stub-key".into(),
                api_secret: "dGVzdA==".into(),
                api_passphrase: "stub-passphrase".into(),
                created_at: unix_secs(),
            },
        );
        (venue, funder)
    }

    #[tokio::test]
    async fn tick_bootstrap_in_flight_obeys_ws_rest_conflict_and_epoch() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for mutation in ["ws", "rest", "disconnect", "conflict", "tickless"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (arrived_tx, arrived_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = [0u8; 2048];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(std::str::from_utf8(&buffer[..count])
                    .unwrap()
                    .starts_with("GET /tick-size?token_id=t "));
                arrived_tx.send(()).unwrap();
                release_rx.await.unwrap();
                let body = json!({"minimum_tick_size":"0.1"}).to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
            });
            let mut venue = cache_test_venue();
            venue.base = format!("http://{address}");
            venue.http = reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap();
            let mut books = BookStore::default();
            let ticket = books.begin_rest(POLYMARKET, "t");
            let request = tokio::spawn(async move { venue.fetch_tick_size("t").await });
            tokio::time::timeout(Duration::from_secs(3), arrived_rx)
                .await
                .unwrap()
                .unwrap();
            let expected = match mutation {
                "ws" => {
                    apply_ws_message(
                        &mut books,
                        &json!({"event_type":"tick_size_change","asset_id":"t","timestamp":"200","new_tick_size":"0.001"}),
                        Instant::now(),
                    );
                    Some(Decimal::new(1, 3))
                }
                "rest" => {
                    let current = books.begin_rest(POLYMARKET, "t");
                    books
                        .accept_rest(
                            &current,
                            vec![],
                            vec![],
                            200,
                            Instant::now(),
                            Some(Decimal::new(1, 2)),
                        )
                        .unwrap();
                    Some(Decimal::new(1, 2))
                }
                "conflict" => {
                    books.set_tick_size_at(POLYMARKET, "t", Decimal::new(1, 2), 200);
                    books.set_tick_size_at(POLYMARKET, "t", Decimal::new(1, 3), 200);
                    None
                }
                "tickless" => {
                    books.replace_snapshot(POLYMARKET, "t", vec![], vec![], 200, Instant::now());
                    None
                }
                _ => {
                    books.mark_platform_stale(POLYMARKET);
                    None
                }
            };
            release_tx.send(()).unwrap();
            let fetched = request.await.unwrap().unwrap();
            assert_eq!(
                books.seed_tick_size(&ticket, fetched),
                expected,
                "mutation={mutation}"
            );
            assert_eq!(books.tick_size(POLYMARKET, "t"), expected);
            if mutation == "conflict" {
                let current = books.begin_rest(POLYMARKET, "t");
                assert_eq!(books.seed_tick_size(&current, fetched), None);
            }
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn tick_fetch_has_no_permanent_venue_cache_and_rejects_bad_values() {
        let (venue, server) = poll_stub(vec![
            (200, json!({"minimum_tick_size":"0.01"})),
            (200, json!({"minimum_tick_size":"0.001"})),
            (200, json!({"minimum_tick_size":"1.1"})),
            (200, json!({"minimum_tick_size":"0"})),
            (500, json!({})),
        ])
        .await;
        assert_eq!(
            venue.fetch_tick_size("t").await.unwrap(),
            Decimal::new(1, 2)
        );
        assert_eq!(
            venue.fetch_tick_size("t").await.unwrap(),
            Decimal::new(1, 3)
        );
        for _ in 0..3 {
            assert!(venue.fetch_tick_size("t").await.is_err());
        }
        assert_eq!(server.await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn rest_book_http_rejects_wrong_identity_and_malformed_success() {
        let valid =
            json!({"asset_id":"t", "timestamp":"100", "bids":[], "asks":[], "tick_size":"0.001"});
        let (venue, server) = poll_stub(vec![
            (
                200,
                json!({"asset_id":"other", "timestamp":"100", "bids":[], "asks":[]}),
            ),
            (
                200,
                json!({"asset_id":"t", "timestamp":"invalid", "bids":[], "asks":[]}),
            ),
            (200, valid),
        ])
        .await;
        assert!(venue.rest_book("t").await.is_err());
        assert!(venue.rest_book("t").await.is_err());
        let snapshot = venue.rest_book("t").await.unwrap();
        assert_eq!(snapshot.exchange_ts_ms, 100);
        assert_eq!(snapshot.tick_size, Some(Decimal::new(1, 3)));
        assert_eq!(server.await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn rest_book_http_in_flight_delete_rejects_newer_response() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (arrived_tx, arrived_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 2048];
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            arrived_tx.send(()).unwrap();
            release_rx.await.unwrap();
            let body = json!({"asset_id":"t", "timestamp":"200", "bids":[], "asks":[{"price":"0.5","size":"3"}]}).to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
        });
        let mut venue = cache_test_venue();
        venue.base = format!("http://{address}");
        venue.http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let mut books = BookStore::default();
        let now = Instant::now();
        apply_ws_message(
            &mut books,
            &json!({"event_type":"book", "asset_id":"t", "timestamp":"100", "bids":[], "asks":[{"price":"0.5","size":"3"}]}),
            now,
        );
        let ticket = books.begin_rest(POLYMARKET, "t");
        let request = tokio::spawn(async move { venue.rest_book("t").await });
        tokio::time::timeout(Duration::from_secs(3), arrived_rx)
            .await
            .unwrap()
            .unwrap();
        apply_ws_message(
            &mut books,
            &json!({"event_type":"price_change", "timestamp":"100", "price_changes":[{"asset_id":"t","side":"SELL","price":"0.5","size":"0"}]}),
            now,
        );
        release_tx.send(()).unwrap();
        let BookSnapshot {
            bids,
            asks,
            exchange_ts_ms: ts,
            ..
        } = request.await.unwrap().unwrap();
        assert_eq!(
            books
                .accept_rest(&ticket, bids, asks, ts, Instant::now(), None)
                .unwrap_err(),
            crate::book::BookReject::RevisionChanged
        );
        assert!(books.get(POLYMARKET, "t").unwrap().asks.is_empty());
        server.await.unwrap();
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

    const TRADES_TEST_AFTER: i64 = 1_700_000_000;
    const TRADES_TEST_BEFORE: i64 = TRADES_TEST_AFTER + 300;

    fn assert_trade_request(request: &str, cursor: &str) {
        assert!(request.starts_with("GET /data/trades?"));
        let target = request.split_whitespace().nth(1).unwrap();
        let url = url::Url::parse(&format!("http://localhost{target}")).unwrap();
        assert_eq!(url.query_pairs().count(), 4);
        let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query.get("asset_id").map(String::as_str), Some("yes"));
        assert_eq!(query.get("next_cursor").map(String::as_str), Some(cursor));
        assert_eq!(query["after"], (TRADES_TEST_AFTER - 10).to_string());
        assert_eq!(query["before"], TRADES_TEST_BEFORE.to_string());
    }

    fn trade_progress_fixture(cursor: &str, seen: &[&str], trade_ids: &[&str]) -> Value {
        json!({
            "version": 2,
            "funder": "test-funder",
            "asset_id": "yes",
            "order_id": "taker-1",
            "after": TRADES_TEST_AFTER,
            "before": TRADES_TEST_BEFORE,
            "next_cursor": cursor,
            "seen_cursors": seen,
            "trade_ids": trade_ids,
        })
    }

    #[tokio::test]
    async fn trade_pagination_resumes_second_page_and_restarts_after_end() {
        let mut unrelated = trade_fixture("unrelated");
        unrelated["taker_order_id"] = json!("other-order");
        let mut case_mismatch = trade_fixture("case-mismatch");
        case_mismatch["taker_order_id"] = json!("TAKER-1");
        let (venue, server) = poll_stub(vec![
            (200, json!({"data": [trade_fixture("first"), unrelated, case_mismatch], "next_cursor": "MQ=="})),
            // fills 不去重，上层按 trade + order ID 持久化；progress 则收集本单去重证据。
            (200, json!({"data": [trade_fixture("first"), trade_fixture("second")], "next_cursor": "LTE="})),
            (200, json!({"data": [trade_fixture("rescan-only")], "next_cursor": "LTE="})),
        ]).await;
        let first = venue
            .poll_trade_page(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &Value::Null,
            )
            .await
            .unwrap();
        assert!(!first.complete && !first.history_complete);
        assert_eq!(first.fills.len(), 3);
        assert_eq!(first.fills[0].trade_id, "first");
        assert_eq!(
            first.progress,
            trade_progress_fixture("MQ==", &["MA=="], &["first"])
        );
        let saved = serde_json::to_string(&first.progress).unwrap();
        let restored: Value = serde_json::from_str(&saved).unwrap();
        let second = venue
            .poll_trade_page(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &restored,
            )
            .await
            .unwrap();
        assert!(second.complete && second.history_complete);
        assert_eq!(second.fills.len(), 2);
        assert_eq!(second.fills[1].trade_id, "second");
        assert_eq!(
            second.progress,
            trade_progress_fixture("LTE=", &["MA==", "MQ=="], &["first", "second"])
        );
        let next = venue
            .poll_trade_page(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &second.progress,
            )
            .await
            .unwrap();
        assert!(next.complete && next.history_complete);
        assert_eq!(
            next.progress,
            trade_progress_fixture("LTE=", &["MA=="], &["rescan-only"])
        );
        let requests = server.await.unwrap();
        assert_trade_request(&requests[0], "MA==");
        assert_trade_request(&requests[1], "MQ==");
        assert_trade_request(&requests[2], "MA==");
    }

    #[tokio::test]
    async fn trade_pagination_collects_only_matching_maker_ids() {
        let mut trade = trade_fixture("maker-trade");
        trade["asset_id"] = json!("no");
        trade["maker_orders"] = json!([
            {"order_id": "maker-1", "matched_amount": "2", "price": "0.4", "asset_id": "yes"},
            {"order_id": "maker-2", "matched_amount": "3", "price": "0.6", "asset_id": "no"}
        ]);
        let (venue, server) = poll_stub(vec![
            (200, json!({"data": [trade.clone(), trade, trade_fixture("unrelated")], "next_cursor": "LTE="})),
        ]).await;
        let page = venue
            .poll_trade_page(
                "test-funder",
                "yes",
                "maker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &Value::Null,
            )
            .await
            .unwrap();
        assert_eq!(page.fills.len(), 7);
        let mut expected = trade_progress_fixture("LTE=", &["MA=="], &["maker-trade"]);
        expected["order_id"] = json!("maker-1");
        assert_eq!(page.progress, expected);
        assert_trade_request(&server.await.unwrap()[0], "MA==");
    }

    #[tokio::test]
    async fn trade_pagination_rejects_matching_order_with_wrong_asset() {
        let mut wrong_taker = trade_fixture("wrong-taker");
        wrong_taker["asset_id"] = json!("no");
        let mut wrong_maker = trade_fixture("wrong-maker");
        wrong_maker["maker_orders"] = json!([
            {"order_id": "maker-1", "matched_amount": "2", "price": "0.4", "asset_id": "no"}
        ]);
        let (venue, server) = poll_stub(vec![
            (
                200,
                json!({"data": [trade_fixture("staged"), wrong_taker], "next_cursor": "LTE="}),
            ),
            (200, json!({"data": [wrong_maker], "next_cursor": "LTE="})),
        ])
        .await;
        for order_id in ["taker-1", "maker-1"] {
            let mut progress = trade_progress_fixture("MQ==", &["MA=="], &["saved"]);
            progress["order_id"] = json!(order_id);
            let saved = progress.clone();
            let err = venue
                .poll_trade_page(
                    "test-funder",
                    "yes",
                    order_id,
                    TRADES_TEST_AFTER,
                    TRADES_TEST_BEFORE,
                    &progress,
                )
                .await
                .unwrap_err();
            assert!(err.to_string().contains("order asset mismatch"));
            assert_eq!(progress, saved);
        }
        for request in server.await.unwrap() {
            assert_trade_request(&request, "MQ==");
        }
    }

    #[tokio::test]
    async fn trade_pagination_rejects_repeated_cursor_and_preserves_saved_progress() {
        let (venue, server) = poll_stub(vec![
            (
                200,
                json!({"data": [trade_fixture("saved")], "next_cursor": "MQ=="}),
            ),
            (200, json!({"data": [], "next_cursor": "MA=="})),
            (200, json!({"data": [], "next_cursor": "MQ=="})),
        ])
        .await;
        let first = venue
            .poll_trade_page(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &json!({}),
            )
            .await
            .unwrap();
        let saved = first.progress.clone();
        for _ in 0..2 {
            let err = venue
                .poll_trade_page(
                    "test-funder",
                    "yes",
                    "taker-1",
                    TRADES_TEST_AFTER,
                    TRADES_TEST_BEFORE,
                    &first.progress,
                )
                .await
                .unwrap_err();
            assert!(err.to_string().contains("repeated cursor"));
            assert_eq!(first.progress, saved);
        }
        let requests = server.await.unwrap();
        assert_trade_request(&requests[0], "MA==");
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
            .poll_trade_page(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &Value::Null,
            )
            .await
            .unwrap();
        let saved = first.progress.clone();
        for _ in 0..4 {
            assert!(venue
                .poll_trade_page(
                    "test-funder",
                    "yes",
                    "taker-1",
                    TRADES_TEST_AFTER,
                    TRADES_TEST_BEFORE,
                    &first.progress
                )
                .await
                .is_err());
            assert_eq!(first.progress, saved);
        }
        let recovered = venue
            .poll_trade_page(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &first.progress,
            )
            .await
            .unwrap();
        assert!(recovered.complete && recovered.history_complete);
        assert_eq!(recovered.fills[0].trade_id, "recovered");
        assert_eq!(
            recovered.progress,
            trade_progress_fixture("LTE=", &["MA==", "MQ=="], &["saved", "recovered"])
        );
        let requests = server.await.unwrap();
        assert_trade_request(&requests[0], "MA==");
        for request in &requests[1..] {
            assert_trade_request(request, "MQ==");
        }
    }

    #[tokio::test]
    async fn trade_pagination_migrates_legacy_progress_by_restarting_same_window() {
        let (venue, server) = poll_stub(vec![
            (
                200,
                json!({"data": [trade_fixture("fresh")], "next_cursor": "MQ=="}),
            ),
            (
                200,
                json!({"data": [trade_fixture("fresh")], "next_cursor": "MQ=="}),
            ),
        ])
        .await;
        for cursor in ["Mg==", "LTE="] {
            let legacy = json!({
                "funder": "TEST-FUNDER", "asset_id": "yes", "next_cursor": cursor,
                "seen_cursors": ["MA==", "MQ=="]
            });
            let page = venue
                .poll_trade_page(
                    "test-funder",
                    "yes",
                    "taker-1",
                    TRADES_TEST_AFTER,
                    TRADES_TEST_BEFORE,
                    &legacy,
                )
                .await
                .unwrap();
            assert!(!page.complete && !page.history_complete);
            assert_eq!(
                page.progress,
                trade_progress_fixture("MQ==", &["MA=="], &["fresh"])
            );
        }
        for request in server.await.unwrap() {
            assert_trade_request(&request, "MA==");
        }
    }

    #[test]
    fn trade_pagination_rejects_unknown_history_and_wrong_query_progress() {
        for progress in [
            json!(false),
            json!([]),
            json!({"next_cursor": "MQ=="}),
            json!({"funder": "other", "asset_id": "yes", "next_cursor": "MQ==", "seen_cursors": ["MA=="]}),
            json!({"funder": "test-funder", "asset_id": "no", "next_cursor": "MQ==", "seen_cursors": ["MA=="]}),
            json!({"funder": "test-funder", "asset_id": "yes", "next_cursor": "LTE=", "seen_cursors": []}),
            json!({"funder": "test-funder", "asset_id": "yes", "next_cursor": "MQ==", "seen_cursors": ["MA==", "MQ=="]}),
            json!({"funder": "test-funder", "asset_id": "yes", "next_cursor": "MQ==", "seen_cursors": ["MA=="], "after": TRADES_TEST_AFTER}),
        ] {
            assert!(trade_page_cursor(
                &progress,
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE
            )
            .is_err());
        }
        let valid = trade_progress_fixture("MQ==", &["MA=="], &["saved"]);
        for (field, value) in [
            ("version", json!(1)),
            ("version", json!(3)),
            ("version", json!("2")),
            ("version", Value::Null),
            ("funder", json!("other")),
            ("asset_id", json!("no")),
            ("order_id", json!("TAKER-1")),
            ("after", json!(TRADES_TEST_AFTER + 1)),
            ("before", json!(TRADES_TEST_BEFORE + 1)),
            ("after", json!(TRADES_TEST_AFTER.to_string())),
            ("before", Value::Null),
            ("next_cursor", json!("")),
            ("next_cursor", json!("MA==")),
            ("seen_cursors", json!([])),
            ("seen_cursors", json!(["MQ=="])),
            ("seen_cursors", json!(["MA==", "MA=="])),
            ("seen_cursors", json!(["MA==", "LTE="])),
            ("seen_cursors", json!(["MA==", null])),
            ("seen_cursors", json!(["MA==", ""])),
            ("trade_ids", Value::Null),
            ("trade_ids", json!(["saved", "saved"])),
            ("trade_ids", json!([""])),
            ("trade_ids", json!([5])),
        ] {
            let mut progress = valid.clone();
            progress[field] = value;
            assert!(
                trade_page_cursor(
                    &progress,
                    "test-funder",
                    "yes",
                    "taker-1",
                    TRADES_TEST_AFTER,
                    TRADES_TEST_BEFORE
                )
                .is_err(),
                "{field}"
            );
        }
        for field in valid.as_object().unwrap().keys() {
            let mut progress = valid.clone();
            progress.as_object_mut().unwrap().remove(field);
            assert!(
                trade_page_cursor(
                    &progress,
                    "test-funder",
                    "yes",
                    "taker-1",
                    TRADES_TEST_AFTER,
                    TRADES_TEST_BEFORE
                )
                .is_err(),
                "missing {field}"
            );
        }
        // END 不能绕过损坏 progress 的校验。
        let corrupt_end = trade_progress_fixture("LTE=", &["MA=="], &["duplicate", "duplicate"]);
        assert!(trade_page_cursor(
            &corrupt_end,
            "test-funder",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE
        )
        .is_err());
    }

    #[tokio::test]
    async fn trade_pagination_rejects_changed_order_or_window_before_request() {
        let (venue, server) =
            poll_stub(vec![(200, json!({"data": [], "next_cursor": "LTE="}))]).await;
        for cursor in ["MQ==", "LTE="] {
            let progress = trade_progress_fixture(cursor, &["MA=="], &["saved"]);
            for (order_id, after, before) in [
                ("other-order", TRADES_TEST_AFTER, TRADES_TEST_BEFORE),
                ("taker-1", TRADES_TEST_AFTER + 1, TRADES_TEST_BEFORE + 1),
            ] {
                let err = venue
                    .poll_trade_page("test-funder", "yes", order_id, after, before, &progress)
                    .await
                    .unwrap_err();
                assert!(err.to_string().contains("pagination progress"));
            }
        }
        let page = venue
            .poll_trade_page(
                "TEST-FUNDER",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &Value::Null,
            )
            .await
            .unwrap();
        assert_eq!(
            page.progress,
            trade_progress_fixture("LTE=", &["MA=="], &[])
        );
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_trade_request(&requests[0], "MA==");
    }

    #[tokio::test]
    async fn trade_queries_require_order_and_exact_nonnegative_300_second_window() {
        let venue = cache_test_venue();
        for (after, before) in [
            (-1, 299),
            (0, 299),
            (0, 301),
            (300, 0),
            (i64::MAX, i64::MIN),
        ] {
            let err = venue
                .poll_trade_page("test-funder", "yes", "taker-1", after, before, &Value::Null)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("300-second window"));
            let err = venue
                .poll_trades("test-funder", "yes", "taker-1", after, before)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("300-second window"));
        }
        for order_id in ["", " "] {
            let err = venue
                .poll_trade_page("test-funder", "yes", order_id, 0, 300, &Value::Null)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("require an order_id"));
        }
        for (after, before) in [(0, 300), (i64::MAX - 300, i64::MAX)] {
            assert_eq!(
                trade_page_cursor(&Value::Null, "test-funder", "yes", "taker-1", after, before)
                    .unwrap(),
                (TRADES_INITIAL_CURSOR.into(), Vec::new(), Vec::new())
            );
        }
    }

    #[tokio::test]
    async fn poll_trades_keeps_bounded_query_and_rejects_incomplete_first_page() {
        let (venue, server) = poll_stub(vec![
            (
                200,
                json!({"data": [trade_fixture("incomplete")], "next_cursor": "MQ=="}),
            ),
            (
                200,
                json!({"data": [trade_fixture("complete")], "next_cursor": "LTE="}),
            ),
        ])
        .await;
        let err = venue
            .poll_trades(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
            )
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("require resumable poll_trade_page"));
        let fills = venue
            .poll_trades(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
            )
            .await
            .unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].trade_id, "complete");
        for request in server.await.unwrap() {
            assert_trade_request(&request, "MA==");
        }
    }

    #[tokio::test]
    async fn trade_request_lookback_preserves_window_and_clamps_at_epoch() {
        for after in [0, 5, TRADES_TEST_AFTER] {
            let mut trade = trade_fixture("same-second");
            trade["match_time"] = json!(after.to_string());
            let (venue, server) = poll_stub(vec![
                (200, json!({"data": [trade], "next_cursor": "LTE="})),
            ])
            .await;
            let page = venue
                .poll_trade_page("test-funder", "yes", "taker-1", after, after + 300, &Value::Null)
                .await
                .unwrap();
            assert_eq!(page.fills[0].trade_id, "same-second");
            assert_eq!(page.progress["after"], json!(after));
            assert_eq!(page.progress["before"], json!(after + 300));
            let requests = server.await.unwrap();
            let target = requests[0].split_whitespace().nth(1).unwrap();
            let url = url::Url::parse(&format!("http://localhost{target}")).unwrap();
            let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
            assert_eq!(query["after"], (after - 10).max(0).to_string());
            assert_eq!(query["before"], (after + 300).to_string());
        }
    }

    #[tokio::test]
    async fn trade_window_is_sent_to_server_without_local_last_update_filter() {
        let mut old_update = trade_fixture("old-update");
        old_update["match_time"] = json!((TRADES_TEST_AFTER + 1).to_string());
        old_update["last_update"] = json!((TRADES_TEST_AFTER - 1).to_string());
        let mut later_update = trade_fixture("later-update");
        later_update["match_time"] = json!((TRADES_TEST_AFTER + 2).to_string());
        later_update["last_update"] = json!((TRADES_TEST_BEFORE + 600).to_string());
        let (venue, server) = poll_stub(vec![
            (200, json!({"data": [old_update.clone(), later_update.clone(), trade_fixture("no-update")], "next_cursor": "LTE="})),
        ]).await;
        let page = venue
            .poll_trade_page(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &Value::Null,
            )
            .await
            .unwrap();
        // 只验证请求结构和本地不丢状态更新；mock 不证明真实服务的时间过滤语义。
        assert_eq!(page.fills.len(), 3);
        assert_eq!(page.fills[0].raw["last_update"], old_update["last_update"]);
        assert_eq!(
            page.fills[1].raw["last_update"],
            later_update["last_update"]
        );
        assert_eq!(
            page.progress,
            trade_progress_fixture(
                "LTE=",
                &["MA=="],
                &["old-update", "later-update", "no-update"]
            )
        );
        assert_trade_request(&server.await.unwrap()[0], "MA==");
    }

    fn assert_missing_order(order: &OrderPoll, source: &str) {
        assert!(!order.found);
        assert_eq!(order.status, "not_found");
        assert_eq!(order.order_id.as_deref(), Some("order-id"));
        assert!(
            order.shares.is_none()
                && order.original_shares.is_none()
                && order.remaining_shares.is_none()
        );
        assert!(order.price.is_none() && order.fee.is_none() && order.coin.is_none());
        assert!(order.associated_trades.is_empty());
        assert_eq!(order.raw, json!({"lookup_missing": source}));
    }

    #[test]
    fn parse_order_poll_null_is_not_found() {
        assert_missing_order(&parse_order_poll(Value::Null, "order-id"), "null_body");
    }

    #[tokio::test]
    async fn poll_order_404_and_null_are_missing_with_distinct_evidence() {
        let (venue, server) = poll_stub(vec![
            (404, json!({"error": "not found"})),
            (200, Value::Null),
        ])
        .await;
        for source in ["http_404", "null_body"] {
            let order = venue.poll_order("test-funder", "order-id").await.unwrap();
            assert_missing_order(&order, source);
        }
        assert_eq!(
            server.await.unwrap(),
            ["GET /data/order/order-id HTTP/1.1"; 2]
        );
    }

    #[tokio::test]
    async fn poll_order_rejects_malformed_success_and_other_http_errors() {
        let invalid = vec![
            json!([]),
            json!(false),
            json!(17),
            json!("null"),
            json!({}),
            json!({"id": "order-id"}),
            json!({"status": null}),
            json!({"status": true}),
            json!({"status": ""}),
            json!({"status": " "}),
        ];
        let count = invalid.len();
        let mut responses: Vec<_> = invalid.into_iter().map(|raw| (200, raw)).collect();
        responses.push((503, json!({"error": "do not expose payload"})));
        // 合法对象仍沿用原 parser 的可选字段行为，不要求完整订单字段。
        responses.push((200, json!({"status": "MATCHED"})));
        let (venue, server) = poll_stub(responses).await;
        for _ in 0..count {
            let err = venue
                .poll_order("test-funder", "order-id")
                .await
                .unwrap_err();
            assert!(err.to_string().contains("missing or invalid status"));
        }
        let err = venue
            .poll_order("test-funder", "order-id")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Http { status: 503, .. }));
        assert!(!err.to_string().contains("do not expose payload"));
        let found = venue.poll_order("test-funder", "order-id").await.unwrap();
        assert!(found.found);
        assert_eq!(found.status, "matched");
        assert_eq!(found.raw["status"], "MATCHED");
        assert_eq!(found.order_id.as_deref(), Some("order-id"));
        assert!(found.shares.is_none() && found.associated_trades.is_empty());
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), count + 2);
        assert!(requests
            .iter()
            .all(|request| request == "GET /data/order/order-id HTTP/1.1"));
    }

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn market_buy_rejects_cent_rounding_above_cap() {
        let err = market_order_base_units(OrderSide::Buy, d("7"), d("0.333")).unwrap_err();
        assert!(err.to_string().contains("exceed cap"));
        assert_eq!(
            market_buy_base_units(d("10"), d("0.333")).unwrap(),
            (3_330_000, 10_000_000)
        );
    }

    #[test]
    fn market_buy_rejects_lossy_shares_and_accepts_trailing_zeros() {
        let err = market_order_base_units(OrderSide::Buy, d("1.234567"), d("0.50")).unwrap_err();
        assert!(err.to_string().contains("shares exceed"));
        assert_eq!(
            market_buy_base_units(d("1.25000"), d("0.4")).unwrap(),
            (500_000, 1_250_000)
        );
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
        assert!(err.to_string().contains("shares exceed"));
    }

    #[test]
    fn market_buy_amounts_preserve_exact_original_constraints() {
        for cap in [
            "0.01",
            "0.333",
            "0.0001",
            "0.9999",
            "0.1234567890123456789012345678",
        ] {
            let cap = d(cap);
            for qty in 1..=200 {
                if let Ok((maker, taker)) = market_buy_base_units(Decimal::from(qty), cap) {
                    assert_eq!(taker, qty as u128 * 1_000_000);
                    assert_eq!(maker % 10_000, 0);
                    assert!(
                        BigInt::from(maker) * BigInt::from(10u8).pow(cap.scale())
                            <= BigInt::from(taker) * BigInt::from(cap.mantissa())
                    );
                }
            }
        }
        for (qty, cap) in [
            (Decimal::ZERO, d("0.5")),
            (d("-1"), d("0.5")),
            (d("1"), Decimal::ZERO),
            (d("1"), Decimal::ONE),
            (d("1"), d("-0.1")),
        ] {
            assert!(market_buy_base_units(qty, cap).is_err());
        }
        // 不通过 Decimal 乘法构造金额，最大尾数仍可安全转换为 u128 基础单位。
        assert!(market_buy_base_units(Decimal::MAX, d("0.5")).is_ok());
    }

    #[test]
    fn unsigned_buy_rejects_realignment_and_preserves_payload_amounts() {
        let account = PolymarketAccount {
            funder: "0x0000000000000000000000000000000000000001".into(),
            service: None,
            signature_type: 0,
            signer: PrivateKeySigner::random(),
            api_key: String::new(),
            api_secret: String::new(),
            api_passphrase: String::new(),
            created_at: 0,
        };
        let mut req = MarketOrderRequest {
            token_id: "1".into(),
            shares: d("10"),
            cap_price: d("0.333"),
            side: OrderSide::Buy,
            neg_risk: Some(false),
            tick_size: Some(d("0.001")),
            asset_id: None,
            funder_address: None,
        };
        assert!(build_unsigned_order(&account, &req, d("0.01")).is_err());
        let order = build_unsigned_order(&account, &req, d("0.001")).unwrap();
        let payload = order_submit_payload(&order, "test-owner");
        assert_eq!(payload["order"]["makerAmount"], "3330000");
        assert_eq!(payload["order"]["takerAmount"], "10000000");
        req.shares = d("7");
        assert!(build_unsigned_order(&account, &req, d("0.001")).is_err());
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
