use super::{
    parse_decimal, require_positive, MarketOrderRequest, OrderPoll, OrderSide, PreparedOrder,
    SubmitResult, TradeFill,
};
use crate::book::{BookStore, Level};
use crate::config::{Config, PolymarketFunderConfig, POLYMARKET};
use crate::domain::TopicKey;
use crate::error::{Error, Result};
use crate::signing::polymarket::{
    clob_auth_headers, l2_hmac_signature, order_hash_hex, sign_order, SignedOrder,
};
use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;

const FAK_UNFILLED: &str = "no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.";
/// CLOB FAK 市价单精度：maker 最多 2 位小数，taker 最多 5 位小数。向下截断，避免超付。
const MARKET_MAKER_DECIMALS: u32 = 2;
const MARKET_TAKER_DECIMALS: u32 = 5;

#[derive(Clone)]
pub struct PolymarketAccount {
    pub funder: String,
    pub service: Option<String>,
    pub signature_type: u8,
    signer: PrivateKeySigner,
    api_key: String,
    api_secret: String,
    api_passphrase: String,
}

#[derive(Clone)]
pub struct PolymarketVenue {
    http: reqwest::Client,
    base: String,
    accounts: Vec<PolymarketAccount>,
    tick_cache: Arc<Mutex<HashMap<String, Decimal>>>,
    neg_risk_cache: Arc<Mutex<HashMap<String, bool>>>,
    rr: Arc<Mutex<usize>>,
}

impl PolymarketVenue {
    pub async fn connect(cfg: &Config) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let mut accounts = Vec::new();
        for funder in &cfg.polymarket_funders {
            accounts.push(init_account(&http, &cfg.polymarket_clob_url, funder).await?);
        }
        Ok(Self {
            http,
            base: cfg.polymarket_clob_url.trim_end_matches('/').to_string(),
            accounts,
            tick_cache: Arc::new(Mutex::new(HashMap::new())),
            neg_risk_cache: Arc::new(Mutex::new(HashMap::new())),
            rr: Arc::new(Mutex::new(0)),
        })
    }

    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    pub fn account(&self, funder: &str) -> Option<&PolymarketAccount> {
        self.accounts
            .iter()
            .find(|a| a.funder.eq_ignore_ascii_case(funder))
    }

    pub async fn next_funder(&self) -> Option<String> {
        if self.accounts.is_empty() {
            return None;
        }
        let mut rr = self.rr.lock().await;
        let idx = *rr % self.accounts.len();
        *rr += 1;
        Some(self.accounts[idx].funder.clone())
    }

    pub async fn rotate_from(&self, current: &str) -> Option<String> {
        if self.accounts.len() < 2 {
            return self.next_funder().await;
        }
        let idx = self
            .accounts
            .iter()
            .position(|a| a.funder.eq_ignore_ascii_case(current))?;
        let next = (idx + 1) % self.accounts.len();
        Some(self.accounts[next].funder.clone())
    }

    pub async fn balance(&self, funder: &str) -> Result<Decimal> {
        let account = self
            .account(funder)
            .ok_or_else(|| Error::msg("unknown polymarket funder"))?;
        let value = self
            .l2_json(
                account,
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

    pub async fn token_balance(&self, funder: &str, token_id: &str) -> Result<Decimal> {
        let account = self
            .account(funder)
            .ok_or_else(|| Error::msg("unknown polymarket funder"))?;
        let value = self
            .l2_json(
                account,
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
        let account = self
            .account(funder)
            .ok_or_else(|| Error::msg("unknown polymarket funder"))?
            .clone();
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
        let payload = json!({
            "deferExec": false,
            "order": {
                "builder": format!("{:#x}", signed.builder),
                "expiration": signed.expiration.to_string(),
                "maker": format!("{:#x}", signed.maker),
                "makerAmount": signed.maker_amount.to_string(),
                "metadata": format!("{:#x}", signed.metadata),
                "salt": signed.salt.to_string(),
                "side": signed.side,
                "signature": signed.signature,
                "signatureType": signed.signature_type,
                "signer": format!("{:#x}", signed.signer),
                "takerAmount": signed.taker_amount.to_string(),
                "timestamp": signed.timestamp.to_string(),
                "tokenId": signed.token_id
            },
            "orderType": "FAK",
            "owner": account.api_key
        });
        Ok(PreparedOrder {
            order_hash,
            envelope,
            payload,
            funder: Some(funder.to_string()),
        })
    }

    pub async fn post_prepared(&self, prepared: &PreparedOrder) -> Result<SubmitResult> {
        let funder = prepared
            .funder
            .as_deref()
            .ok_or_else(|| Error::msg("missing funder on prepared order"))?;
        let account = self
            .account(funder)
            .ok_or_else(|| Error::msg("unknown polymarket funder"))?
            .clone();
        let order_hash = prepared.order_hash.clone();
        let envelope = prepared.envelope.clone();
        match self
            .l2_json(
                &account,
                reqwest::Method::POST,
                "/order",
                &[],
                Some(&prepared.payload),
            )
            .await
        {
            Ok(body) => Ok(parse_submit(&body, order_hash, envelope)),
            Err(Error::Http { status, message }) if status >= 400 && status < 500 => {
                if message.contains("FAK") || message.contains("no orders found") {
                    Ok(SubmitResult::NoMatch {
                        order_hash,
                        envelope,
                        message,
                    })
                } else {
                    Ok(SubmitResult::Unknown {
                        order_id: None,
                        order_hash,
                        envelope,
                        message,
                    })
                }
            }
            Err(err) => Ok(SubmitResult::Unknown {
                order_id: None,
                order_hash,
                envelope,
                message: err.to_string(),
            }),
        }
    }

    pub async fn market_order(
        &self,
        funder: &str,
        req: &MarketOrderRequest,
    ) -> Result<SubmitResult> {
        let prepared = self.prepare_market_order(funder, req).await?;
        self.post_prepared(&prepared).await
    }

    pub async fn poll_order(&self, funder: &str, order_id: &str) -> Result<OrderPoll> {
        let account = self
            .account(funder)
            .ok_or_else(|| Error::msg("unknown polymarket funder"))?;
        let path = format!("/data/order/{order_id}");
        match self
            .l2_json(account, reqwest::Method::GET, &path, &[], None)
            .await
        {
            Ok(raw) => Ok(parse_order_poll(raw, order_id)),
            Err(Error::Http { status, .. }) if status == 404 => Ok(OrderPoll {
                found: false,
                status: "not_found".into(),
                order_id: Some(order_id.into()),
                shares: None,
                price: None,
                fee: None,
                raw: json!({}),
            }),
            Err(err) => Err(err),
        }
    }

    pub async fn poll_trades(&self, funder: &str, token_id: &str) -> Result<Vec<TradeFill>> {
        let account = self
            .account(funder)
            .ok_or_else(|| Error::msg("unknown polymarket funder"))?;
        let raw = self
            .l2_json(
                account,
                reqwest::Method::GET,
                "/data/trades",
                &[("asset_id", token_id)],
                None,
            )
            .await?;
        Ok(parse_trades(&raw))
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
        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(Error::Http {
                status: status.as_u16(),
                message: redact_http(&text),
            });
        }
        if text.is_empty() || text == "null" {
            return Ok(json!({}));
        }
        Ok(serde_json::from_str(&text).unwrap_or_else(|_| json!({"raw": text})))
    }
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
    })
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

pub fn parse_order_poll(raw: Value, order_id: &str) -> OrderPoll {
    let status = raw
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    OrderPoll {
        found: true,
        status,
        order_id: Some(order_id.to_string()),
        shares: raw.get("size_matched").and_then(parse_decimal),
        price: raw.get("price").and_then(parse_decimal),
        fee: raw.get("fee").and_then(parse_decimal),
        raw,
    }
}

pub fn parse_trades(raw: &Value) -> Vec<TradeFill> {
    let items = raw
        .get("data")
        .and_then(|v| v.as_array())
        .cloned()
        .or_else(|| raw.as_array().cloned())
        .unwrap_or_default();
    items
        .into_iter()
        .filter_map(|item| {
            let mut order_ids = Vec::new();
            if let Some(id) = item
                .get("taker_order_id")
                .or_else(|| item.get("order_id"))
                .and_then(|v| v.as_str())
            {
                order_ids.push(id.to_string());
            }
            if let Some(makers) = item.get("maker_orders").and_then(|v| v.as_array()) {
                for maker in makers {
                    if let Some(id) = maker.get("order_id").and_then(|v| v.as_str()) {
                        if !order_ids.iter().any(|existing| existing == id) {
                            order_ids.push(id.to_string());
                        }
                    }
                }
            }
            let order_id = order_ids.first().cloned();
            Some(TradeFill {
                trade_id: item
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                order_id,
                order_ids,
                coin: item
                    .get("asset_id")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
                shares: item.get("size").and_then(parse_decimal)?,
                price: item.get("price").and_then(parse_decimal)?,
                fee: item
                    .get("fee_amount")
                    .or_else(|| item.get("fee"))
                    .and_then(parse_decimal)
                    .unwrap_or(Decimal::ZERO),
                fee_rate_bps: item.get("fee_rate_bps").and_then(parse_decimal),
                raw: item,
            })
        })
        .collect()
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

fn market_order_base_units(
    side: OrderSide,
    size: Decimal,
    price: Decimal,
) -> Result<(u128, u128)> {
    let (maker, taker) = match side {
        OrderSide::Buy => {
            let shares = size.trunc_with_scale(MARKET_TAKER_DECIMALS);
            let usdc = (shares * price).trunc_with_scale(MARKET_MAKER_DECIMALS);
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

    #[test]
    fn parses_fak_unfilled() {
        let body = json!({"success": false, "status": "unmatched", "orderID": "", "errorMsg": FAK_UNFILLED});
        match parse_submit(&body, "0x1".into(), json!({})) {
            SubmitResult::NoMatch { .. } => {}
            other => panic!("expected no match, got {other:?}"),
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
    fn parse_order_poll_ignores_original_size() {
        let poll = parse_order_poll(
            json!({"status": "MATCHED", "original_size": "20", "size_matched": "3", "price": "0.4"}),
            "oid-1",
        );
        assert_eq!(poll.shares.unwrap().to_string(), "3");
        let empty = parse_order_poll(
            json!({"status": "MATCHED", "original_size": "20", "price": "0.4"}),
            "oid-1",
        );
        assert!(empty.shares.is_none());
    }

    #[test]
    fn parse_trades_includes_maker_order_ids() {
        let trades = parse_trades(&json!({"data": [{
            "id": "t1",
            "taker_order_id": "taker-1",
            "size": "5",
            "price": "0.4",
            "fee": "0.01",
            "maker_orders": [{"order_id": "maker-9", "owner": "0xabc"}]
        }]}));
        assert_eq!(trades[0].order_id.as_deref(), Some("taker-1"));
        assert!(trades[0].matches(Some("maker-9"), None));
        assert!(trades[0].matches(Some("taker-1"), None));
        assert!(!trades[0].matches(Some("other"), None));
    }

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn market_buy_floors_maker_usdc_to_2_decimals() {
        let (maker, taker) = market_order_base_units(OrderSide::Buy, d("7"), d("0.333")).unwrap();
        assert_eq!(maker, 2_330_000);
        assert_eq!(taker, 7_000_000);
    }

    #[test]
    fn market_buy_floors_taker_shares_to_5_decimals() {
        let (maker, taker) =
            market_order_base_units(OrderSide::Buy, d("1.234567"), d("0.50")).unwrap();
        assert_eq!(taker, 1_234_560);
        assert_eq!(maker, 610_000);
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
    fn market_buy_rejects_when_usdc_floors_to_zero() {
        let err = market_order_base_units(OrderSide::Buy, d("5"), d("0.001")).unwrap_err();
        assert!(err.to_string().contains("round to zero"));
    }
}
