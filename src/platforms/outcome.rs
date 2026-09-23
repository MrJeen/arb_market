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

pub mod fees;
pub mod settlement;
pub use fees::OutcomeFeeSnapshot;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock as StdRwLock;

const FILL_PAGE_SIZE: usize = 2_000;
const FILL_HISTORY_LIMIT: usize = 10_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum FillPhase {
    #[default]
    InitialProbe,
    Scan,
    FinalProbe,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FillProgress {
    pub(crate) version: u8,
    pub(crate) token_id: String,
    pub(crate) submitted_at_ms: u64,
    pub(crate) end_time: u64,
    pub(crate) cursor: u64,
    // startTime 为包含边界；只保留 cursor 毫秒已见过的账户成交 ID。
    seen_ids: BTreeSet<String>,
    history_checked: bool,
    history_lower_bound: Option<u64>,
    pub(crate) history_complete: bool,
    scanned_count: usize,
    pub(crate) complete: bool,
    #[serde(default)]
    pub(crate) phase: FillPhase,
    #[serde(default)]
    pub(crate) account: String,
    #[serde(default)]
    pub(crate) terminal_observed_at_ms: Option<u64>,
    #[serde(default = "scan_valid_default")]
    scan_valid: bool,
}

fn scan_valid_default() -> bool {
    true
}

impl FillProgress {
    fn load(token_id: &str, submitted_at_ms: i64, value: &Value) -> Result<Self> {
        let start = u64::try_from(submitted_at_ms)
            .map_err(|_| Error::msg("outcome fill start time must be nonnegative"))?;
        if value.is_null() || value.as_object().is_some_and(|v| v.is_empty()) {
            return Ok(Self {
                version: 2,
                phase: FillPhase::InitialProbe,
                account: String::new(),
                terminal_observed_at_ms: None,
                scan_valid: true,
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
        if !matches!(state.version, 1 | 2)
            || state.token_id != token_id
            || state.submitted_at_ms != start
            || state.cursor < start
            || state.cursor > state.end_time
            || state.seen_ids.len() > FILL_HISTORY_LIMIT
        {
            return Err(Error::msg("invalid outcome fill progress"));
        }
        if state.version == 1 {
            // v1 游标仍有效，但旧 historyComplete 不能成为当前覆盖授权。
            state.version = 2;
            state.phase = if state.complete {
                FillPhase::FinalProbe
            } else if state.history_checked {
                FillPhase::Scan
            } else {
                FillPhase::InitialProbe
            };
            state.scan_valid = state.scanned_count < FILL_HISTORY_LIMIT;
            state.complete = false;
        } else if state.phase == FillPhase::Complete {
            state.restart();
        }
        state.history_complete = false;
        Ok(state)
    }

    fn restart(&mut self) {
        self.end_time = unix_millis().max(self.end_time).max(self.submitted_at_ms);
        self.cursor = self.submitted_at_ms;
        self.seen_ids.clear();
        self.history_checked = false;
        self.history_lower_bound = None;
        self.history_complete = false;
        self.scanned_count = 0;
        self.complete = false;
        self.scan_valid = true;
        self.phase = FillPhase::InitialProbe;
    }

    pub(crate) fn follows_final_probe(&self, previous: &Value) -> bool {
        let Ok(mut previous) = serde_json::from_value::<Self>(previous.clone()) else {
            return false;
        };
        if previous.version != 2
            || previous.phase != FillPhase::FinalProbe
            || previous.complete
            || previous.history_complete
        {
            return false;
        }
        previous.phase = self.phase;
        previous.complete = self.complete;
        previous.history_complete = self.history_complete;
        previous.history_lower_bound = self.history_lower_bound;
        previous.history_checked = self.history_checked;
        previous == *self
    }

    pub(crate) fn has_coverage(&self) -> bool {
        self.version == 2
            && self.phase == FillPhase::Complete
            && self.complete
            && self.history_complete
            && self.scan_valid
            && self.scanned_count < FILL_HISTORY_LIMIT
            && self
                .history_lower_bound
                .is_some_and(|time| time < self.submitted_at_ms)
            && self
                .terminal_observed_at_ms
                .is_some_and(|time| time <= self.end_time)
            && !self.account.is_empty()
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
    fee_cache: Arc<StdRwLock<fees::FeeCache>>,
    fee_refreshing: Arc<AtomicBool>,
    fee_last_attempt: Arc<StdMutex<Option<Instant>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeRefreshOutcome {
    Refreshed,
    InFlight,
    Throttled,
}

// 只防重复刷新，不持快照锁跨 HTTP；取消任务也必须释放刷新标记。
struct FeeRefreshGuard<'a>(&'a AtomicBool);
impl Drop for FeeRefreshGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl OutcomeVenue {
    /// Only info queries are configured; no private key parsing or exchange endpoint.
    pub fn connect_read_only(cfg: &crate::config::RecomputeActualsConfig) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()?,
            info_url: cfg.hyperliquid_info_url.clone(),
            exchange_url: String::new(),
            mainnet: true,
            signer: None,
            account: cfg.outcome_account_address.clone(),
            builder: cfg
                .outcome_builder_address
                .clone()
                .map(|addr| (addr, cfg.outcome_builder_fee)),
            nonce: Arc::new(StdMutex::new(0)),
            fee_cache: Arc::new(StdRwLock::new(fees::FeeCache::default())),
            fee_refreshing: Arc::new(AtomicBool::new(false)),
            fee_last_attempt: Arc::new(StdMutex::new(None)),
        })
    }

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
            fee_cache: Arc::new(StdRwLock::new(fees::FeeCache::default())),
            fee_refreshing: Arc::new(AtomicBool::new(false)),
            fee_last_attempt: Arc::new(StdMutex::new(None)),
        })
    }

    pub fn fee_snapshot(&self, outcome_id: u64) -> Result<OutcomeFeeSnapshot> {
        self.lookup_fees(outcome_id)
            .map_err(|err| Error::msg(err.reason()))
    }

    pub fn lookup_fees(
        &self,
        outcome_id: u64,
    ) -> std::result::Result<OutcomeFeeSnapshot, fees::FeeLookupError> {
        self.fee_cache
            .read()
            .map_err(|_| fees::FeeLookupError::CacheLock)?
            .lookup(outcome_id)
    }

    /// Local cache only: historical wallets must not borrow the configured account's rates.
    pub fn latest_actuals_fee(
        &self,
        wallet: &str,
        token: &str,
    ) -> Option<crate::store::actuals::LatestFee> {
        if !self
            .account_address()?
            .trim()
            .eq_ignore_ascii_case(wallet.trim())
        {
            return None;
        }
        let (outcome_id, side) = parse_side_coin(token)?;
        if crate::domain::side_coin(outcome_id, side) != token {
            return None;
        }
        let snapshot = self.fee_snapshot(outcome_id).ok()?;
        Some(crate::store::actuals::LatestFee {
            wallet: wallet.trim().to_ascii_lowercase(),
            token: token.to_owned(),
            valid_until: snapshot.valid_until()?,
            snapshot: snapshot.estimate_json(),
        })
    }

    pub async fn refresh_fees(&self) -> Result<()> {
        self.refresh_fees_coordinated().await.map(|_| ())
    }

    pub async fn refresh_fees_coordinated(&self) -> Result<FeeRefreshOutcome> {
        if self
            .fee_refreshing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(FeeRefreshOutcome::InFlight);
        }
        let _guard = FeeRefreshGuard(&self.fee_refreshing);
        let started = Instant::now();
        {
            let mut last = self
                .fee_last_attempt
                .lock()
                .map_err(|_| Error::msg("fee refresh attempt lock poisoned"))?;
            if last.is_some_and(|at| {
                started.saturating_duration_since(at) < fees::FEE_REFRESH_INTERVAL
            }) {
                return Ok(FeeRefreshOutcome::Throttled);
            }
            // 失败和取消也保留本次尝试时间，避免逐 tick 重试。
            *last = Some(started);
        }
        let result = async {
            let account = self
                .account
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| Error::msg("missing outcome account for userFees"))?;
            let user_fees = self
                .query_info("userFees", "", &json!({"type":"userFees", "user":account}))
                .await?;
            let account = fees::parse_account(&user_fees)?;
            let metadata = self
                .query_info("outcomeMeta", "", &json!({"type":"outcomeMeta"}))
                .await?;
            let markets = fees::parse_markets(&metadata, &account)?;
            // 与 order_action 实际携带的 builder 完全一致，关闭归因就不估 builder 费。
            let builder_rate = self.builder.as_ref().map_or(Decimal::ZERO, |(_, fee)| {
                Decimal::from(*fee) / Decimal::from(100_000)
            });
            let mut next = self
                .fee_cache
                .read()
                .map_err(|_| Error::msg("outcome fee cache lock poisoned"))?
                .clone();
            // Arc 只读旧代，版本计算和整张 map 构造均在锁外；发布只交换指针。
            next.publish(account, markets, builder_rate)?;
            let previous = std::mem::replace(
                &mut *self
                    .fee_cache
                    .write()
                    .map_err(|_| Error::msg("outcome fee cache lock poisoned"))?,
                next,
            );
            drop(previous);
            Ok(())
        }
        .await;
        if let Err(err) = &result {
            if let Ok(mut cache) = self.fee_cache.write() {
                cache.report_availability();
            }
            tracing::warn!(service = "outcome", api = "fee_snapshot", elapsed_ms = started.elapsed().as_millis() as u64,
                error = %err, "outcome fee refresh failed; retaining original source ages");
        }
        result.map(|()| FeeRefreshOutcome::Refreshed)
    }

    #[cfg(test)]
    pub(crate) fn install_test_fee_snapshot(
        &self,
        outcome_id: u64,
        taker_rate: Decimal,
        builder_rate: Decimal,
    ) {
        self.fee_cache
            .write()
            .unwrap()
            .install_test(outcome_id, taker_rate, builder_rate);
    }

    #[cfg(test)]
    pub(crate) fn expire_test_fee_snapshot(&self, _outcome_id: u64) {
        self.fee_cache.write().unwrap().expire_test();
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
        Ok(self.settlement_with_evidence(market_id).await?.0)
    }

    pub async fn settlement_with_evidence(
        &self,
        market_id: &str,
    ) -> Result<(crate::settlement::OutcomeSettlement, Value)> {
        let outcome_id = parse_settlement_market_id(market_id)?;
        let started = Instant::now();
        let value: Value = self
            .query_info(
                "settledOutcome",
                market_id,
                &json!({"type":"settledOutcome","outcome":outcome_id}),
            )
            .await?;
        let status = crate::settlement::parse_outcome_settlement(outcome_id, &value).map_err(|error| {
            tracing::warn!(service="outcome",api="settledOutcome",outcome_id,elapsed_ms=started.elapsed().as_millis() as u64,error=%error,"invalid settlement response"); error
        })?;
        tracing::debug!(
            service = "outcome",
            outcome_id,
            settlement_state = status.kind(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "settlement queried"
        );
        Ok((
            status,
            json!({"request_outcome":outcome_id,"outcome":value.get("outcome"),"settleFraction":value.get("settleFraction").or_else(|| value.get("settle_fraction"))}),
        ))
    }

    pub async fn rest_book(&self, coin: &str) -> Result<(Vec<Level>, Vec<Level>, i64)> {
        let body = json!({"type": "l2Book", "coin": coin});
        let value = self.query_info("l2Book", coin, &body).await?;
        let result = (|| {
            if value.get("coin").and_then(Value::as_str) != Some(coin) {
                return Err(Error::msg("outcome book response coin mismatch"));
            }
            parse_l2_book(&value)
        })();
        if let Err(err) = &result {
            tracing::warn!(service = "outcome", api = "l2Book", token_id = coin,
                error = %err, "outcome book validation failed");
        }
        result
    }

    pub async fn user_state(&self) -> Result<Decimal> {
        self.fetch_usdc_balance().await
    }

    async fn fetch_usdc_balance(&self) -> Result<Decimal> {
        let started = Instant::now();
        let mut http_status = None;
        let result: Result<Decimal> = async {
            let user = self
                .account
                .as_deref()
                .ok_or_else(|| Error::msg("missing OUTCOME_ACCOUNT_ADDRESS"))?;
            let response = self
                .http
                .post(&self.info_url)
                .json(&json!({"type": "spotClearinghouseState", "user": user}))
                .send()
                .await
                .map_err(reqwest::Error::without_url)?;
            http_status = Some(response.status().as_u16());
            let value: Value = response
                .error_for_status()
                .map_err(reqwest::Error::without_url)?
                .json()
                .await
                .map_err(reqwest::Error::without_url)?;
            parse_usdc_balance(&value)
        }
        .await;
        if let Err(err) = &result {
            tracing::error!(
                service = "outcome", api = "spotClearinghouseState",
                operation = "fetch_usdc_balance", coin = "USDC", http_status,
                elapsed_ms = started.elapsed().as_millis() as u64, error = %err,
                "outcome balance query failed"
            );
        } else {
            tracing::debug!(
                service = "outcome",
                api = "spotClearinghouseState",
                operation = "fetch_usdc_balance",
                coin = "USDC",
                http_status,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "outcome balance queried"
            );
        }
        result
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

    pub async fn post_prepared(
        &self,
        prepared: PreparedOrder,
    ) -> Result<(SubmitResult, super::SubmissionResponse)> {
        let started = Instant::now();
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
        match response {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let text = resp.text().await;
                // 分类沿用无法解码时的空对象，审计独立保存真实正文/读取诊断。
                let body = text
                    .as_ref()
                    .ok()
                    .and_then(|text| serde_json::from_str::<Value>(text).ok())
                    .unwrap_or(json!({}));
                let reason = match &text {
                    Err(err) => {
                        if err.is_timeout() {
                            "response_body_timeout"
                        } else {
                            "response_body"
                        }
                    }
                    Ok(text) if text.is_empty() => "empty_body",
                    Ok(text) if serde_json::from_str::<Value>(text).is_err() => "invalid_json",
                    _ if status >= 400 => "http_status",
                    _ => "received",
                };
                if reason == "received" {
                    tracing::debug!(service="outcome", operation="submit", endpoint="/exchange", order_hash=%hash, http_status=status, elapsed_ms=started.elapsed().as_millis() as u64, reason, "outcome submit response received");
                } else {
                    tracing::warn!(service="outcome", operation="submit", endpoint="/exchange", order_hash=%hash, http_status=status, elapsed_ms=started.elapsed().as_millis() as u64, reason, "outcome submit response diagnostic");
                }
                let stored = super::SubmissionResponse::http(status, text);
                Ok((
                    classify_http_submit(status, &body, hash, envelope, &cloid),
                    stored,
                ))
            }
            Err(err) => {
                tracing::warn!(service="outcome", operation="submit", endpoint="/exchange", order_hash=%hash, elapsed_ms=started.elapsed().as_millis() as u64, reason=if err.is_timeout() {"timeout"} else {"transport"}, "outcome submit request failed");
                let response = super::SubmissionResponse::transport(&err);
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

    /// Public, unsigned info endpoint identity for read-only settlement audits.
    pub fn info_endpoint(&self) -> &str {
        &self.info_url
    }

    pub fn http_client(&self) -> &reqwest::Client {
        &self.http
    }

    pub async fn poll_fill_page(
        &self,
        token_id: &str,
        submitted_at_ms: i64,
        progress: &Value,
    ) -> Result<FillPage> {
        self.poll_fill_page_after_terminal(token_id, submitted_at_ms, progress, None)
            .await
    }

    pub(crate) async fn poll_fill_page_after_terminal(
        &self,
        token_id: &str,
        submitted_at_ms: i64,
        progress: &Value,
        terminal_observed_at_ms: Option<u64>,
    ) -> Result<FillPage> {
        let mut state = FillProgress::load(token_id, submitted_at_ms, progress)?;
        let user = self
            .account
            .as_deref()
            .ok_or_else(|| Error::msg("missing OUTCOME_ACCOUNT_ADDRESS"))?;
        if !state.account.is_empty() && !state.account.eq_ignore_ascii_case(user) {
            return Err(Error::msg("outcome fill progress account mismatch"));
        }
        state.account = user.to_ascii_lowercase();
        if let Some(observed) = terminal_observed_at_ms {
            if state.terminal_observed_at_ms != Some(observed) {
                // 首次终态之后重新整轮扫描，后续 poll 不反复重置已保存游标。
                state.restart();
                state.end_time = state.end_time.max(observed);
                state.terminal_observed_at_ms = Some(observed);
            }
        }
        // 每次只查询一页，让调用方立即持久化成功进度；后页失败不得抹掉此前进展。
        let probing_history =
            matches!(state.phase, FillPhase::InitialProbe | FillPhase::FinalProbe);
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
            let response = self
                .http
                .post(&self.info_url)
                .json(body)
                .send()
                .await
                .map_err(reqwest::Error::without_url)?;
            http_status = Some(response.status().as_u16());
            Ok(response
                .error_for_status()
                .map_err(reqwest::Error::without_url)?
                .json()
                .await
                .map_err(reqwest::Error::without_url)?)
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
        let started = Instant::now();
        let mut http_status = None;
        let result: Result<Decimal> = async {
            let user = self
                .account
                .as_deref()
                .ok_or_else(|| Error::msg("missing OUTCOME_ACCOUNT_ADDRESS"))?;
            let response = self
                .http
                .post(&self.info_url)
                .json(&json!({"type": "spotClearinghouseState", "user": user}))
                .send()
                .await
                .map_err(reqwest::Error::without_url)?;
            http_status = Some(response.status().as_u16());
            let value: Value = response
                .error_for_status()
                .map_err(reqwest::Error::without_url)?
                .json()
                .await
                .map_err(reqwest::Error::without_url)?;
            parse_coin_balance(&value, coin)
        }
        .await;
        if let Err(err) = &result {
            tracing::error!(
                service = "outcome", api = "spotClearinghouseState",
                operation = "token_balance", coin, http_status,
                elapsed_ms = started.elapsed().as_millis() as u64, error = %err,
                "outcome balance query failed"
            );
        }
        result
    }
}

pub fn parse_l2_book(value: &Value) -> Result<(Vec<Level>, Vec<Level>, i64)> {
    let data = value.get("data").unwrap_or(value);
    let coin = data
        .get("coin")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::msg("outcome book missing coin"))?;
    parse_side_coin(coin).ok_or_else(|| Error::msg("outcome book invalid coin"))?;
    let ts = data
        .get("time")
        .and_then(Value::as_i64)
        .filter(|time| *time > 0)
        .ok_or_else(|| Error::msg("outcome book missing valid timestamp"))?;
    let levels = data
        .get("levels")
        .and_then(Value::as_array)
        .filter(|levels| levels.len() == 2)
        .ok_or_else(|| Error::msg("outcome book requires two complete sides"))?;
    Ok((
        parse_hl_levels(&levels[0])?,
        parse_hl_levels(&levels[1])?,
        ts,
    ))
}

fn parse_hl_levels(value: &Value) -> Result<Vec<Level>> {
    let items = value
        .as_array()
        .ok_or_else(|| Error::msg("outcome book side is not an array"))?;
    let mut prices = BTreeSet::new();
    items
        .iter()
        .map(|item| {
            let price = item
                .get("px")
                .and_then(parse_decimal)
                .filter(|price| *price > Decimal::ZERO && *price <= Decimal::ONE)
                .ok_or_else(|| Error::msg("outcome book invalid price"))?;
            let size = item
                .get("sz")
                .and_then(parse_decimal)
                .filter(|size| *size > Decimal::ZERO)
                .ok_or_else(|| Error::msg("outcome book invalid size"))?;
            if !prices.insert(price) {
                return Err(Error::msg("outcome book duplicate price"));
            }
            Ok(Level { price, size })
        })
        .collect()
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
    if item.get("dir").and_then(Value::as_str) == Some("Settlement") {
        return Err(Error::msg("settlement event is not an order trade"));
    }
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
        let fill = if time >= state.submitted_at_ms
            && coin == state.token_id
            && item.get("dir").and_then(Value::as_str) != Some("Settlement")
        {
            Some(parse_user_fill(item)?)
        } else {
            None
        };
        records.push((time, id, fill));
    }
    if probing_history {
        let lower_bound = records.first().map(|record| record.0);
        state.history_checked = true;
        state.history_lower_bound = lower_bound;
        state.history_complete = false;
        if state.phase == FillPhase::FinalProbe {
            // 下界必须严格早于查询起点：同毫秒的更早记录也可能已被保留上限截断。
            // 覆盖授权只来自本轮尾页之后的新请求；满页同毫秒下界同样不作证明。
            let ambiguous_boundary = items.len() == FILL_PAGE_SIZE
                && records.first().map(|r| r.0) == records.last().map(|r| r.0);
            state.history_complete = state.scan_valid
                && state.scanned_count < FILL_HISTORY_LIMIT
                && !ambiguous_boundary
                && lower_bound.is_some_and(|time| time < state.submitted_at_ms);
            state.complete = true;
            state.phase = FillPhase::Complete;
        } else {
            state.phase = FillPhase::Scan;
        }
        // probe 也是实际成交观测；跨请求去重交由既有事务按真实 tid 合并。
        let fills = records
            .into_iter()
            .filter_map(|(_, _, fill)| fill)
            .collect();
        return Ok((fills, false));
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
        state.scan_valid = false;
    }
    state.history_complete = false;
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
    let tail = items.len() < FILL_PAGE_SIZE;
    state.complete = false;
    if tail {
        state.phase = FillPhase::FinalProbe;
    }
    // 同一毫秒有 >=2000 条且接口反复返回同一页时，不能 +1 跳过未知成交。
    let stalled = !tail
        && ((state.cursor == old_cursor && new_count == 0)
            || state.seen_ids.len() >= FILL_HISTORY_LIMIT);
    if stalled {
        state.scan_valid = false;
    }
    Ok((fills, stalled))
}

pub fn apply_ws_book(books: &mut BookStore, payload: &Value, now: Instant) -> Option<String> {
    let data = payload.get("data").unwrap_or(payload);
    let coin = data.get("coin").and_then(|v| v.as_str())?;
    let (bids, asks, ts) = match parse_l2_book(payload) {
        Ok(book) => book,
        Err(err) => {
            books.invalidate_ws(OUTCOME, coin, crate::book::BookReject::InvalidPayload);
            tracing::warn!(service = "outcome", event = "invalid_ws_book", token_id = coin,
                error = %err, "outcome websocket book invalidated");
            return Some(coin.to_string());
        }
    };
    if books
        .replace_snapshot(OUTCOME, coin, bids, asks, ts, now)
        .is_applied()
    {
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
                books.lock().await.begin_platform_connection(OUTCOME);
                tracing::info!(
                    service = "outcome",
                    event = "ws_connected",
                    "hyperliquid l2 ws connected"
                );
                let (mut write, mut read) = ws.split();
                for coin in &coins {
                    let msg =
                        json!({"method":"subscribe","subscription":{"type":"l2Book","coin": coin}});
                    let _ = write.send(Message::Text(msg.to_string().into())).await;
                }
                loop {
                    tokio::select! {
                        msg = sub_rx.recv() => {
                            let Some(next) = msg else {
                                books.lock().await.mark_platform_stale(OUTCOME);
                                tracing::info!(service = "outcome", event = "ws_stopped", "outcome subscription channel closed");
                                return;
                            };
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
                        _ = wait_shutdown(&shutdown) => {
                            books.lock().await.mark_platform_stale(OUTCOME);
                            tracing::info!(service = "outcome", event = "ws_stopped", "outcome websocket shutdown");
                            return;
                        },
                    }
                }
                books.lock().await.mark_platform_stale(OUTCOME);
                tracing::warn!(
                    service = "outcome",
                    event = "ws_disconnected",
                    "hyperliquid l2 ws disconnected; reconnecting"
                );
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

fn parse_usdc_balance(value: &Value) -> Result<Decimal> {
    parse_coin_balance(value, "USDC")
}

fn parse_coin_balance(value: &Value, want: &str) -> Result<Decimal> {
    Ok(parse_coin_balance_fields(value, want)?
        .map(|(total, hold)| {
            if total > hold {
                total - hold
            } else {
                Decimal::ZERO
            }
        })
        .unwrap_or(Decimal::ZERO))
}

fn parse_coin_balance_fields(value: &Value, want: &str) -> Result<Option<(Decimal, Decimal)>> {
    let balances = value
        .get("balances")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::msg("outcome balance missing or invalid balances array"))?;
    for item in balances {
        let coin = item.get("coin").and_then(Value::as_str).unwrap_or("");
        if coin_aliases_match(coin, want) {
            let field = |name: &str| {
                item.get(name)
                    .and_then(parse_decimal)
                    .filter(|amount| *amount >= Decimal::ZERO)
                    .ok_or_else(|| {
                        Error::msg(format!("outcome balance {want} missing or invalid {name}"))
                    })
            };
            return Ok(Some((field("total")?, field("hold")?)));
        }
    }
    Ok(None)
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
#[path = "../../tests/unit/platforms/outcome.rs"]
mod tests;
