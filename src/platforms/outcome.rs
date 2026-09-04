use super::{
    json_id, parse_decimal, require_positive, MarketOrderRequest, OrderPoll, OrderSide,
    PreparedOrder, SubmitResult, TradeFill,
};
use crate::book::{BookStore, Level};
use crate::config::{Config, OUTCOME};
use crate::domain::TopicKey;
use crate::error::{Error, Result};
use crate::signing::hyperliquid::{action_hash, order_action, sign_l1_action};
use alloy_signer_local::PrivateKeySigner;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone)]
pub struct OutcomeVenue {
    http: reqwest::Client,
    info_url: String,
    exchange_url: String,
    mainnet: bool,
    signer: Option<PrivateKeySigner>,
    account: Option<String>,
    nonce: Arc<StdMutex<u64>>,
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
            nonce: Arc::new(StdMutex::new(0)),
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
        let action = order_action(
            asset,
            is_buy,
            &price.to_string(),
            &shares.trunc().to_string(),
            Some(&cloid),
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

    pub async fn post_prepared(&self, prepared: PreparedOrder) -> Result<SubmitResult> {
        let hash = prepared.order_hash.clone();
        let envelope = prepared.envelope.clone();
        let cloid = envelope
            .get("cloid")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        match self
            .http
            .post(&self.exchange_url)
            .json(&prepared.payload)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                let body: Value = resp.json().await.unwrap_or(json!({}));
                if !status.is_success() {
                    return Ok(SubmitResult::Unknown {
                        order_id: None,
                        order_hash: hash,
                        envelope,
                        message: body.to_string().chars().take(300).collect(),
                    });
                }
                Ok(parse_exchange_submit(&body, hash, envelope, &cloid))
            }
            Err(err) => Ok(SubmitResult::Unknown {
                order_id: None,
                order_hash: hash,
                envelope,
                message: err.to_string(),
            }),
        }
    }

    pub async fn market_order(&self, req: &MarketOrderRequest) -> Result<SubmitResult> {
        let prepared = self.prepare_market_order(req)?;
        self.post_prepared(prepared).await
    }

    pub async fn poll_order(&self, oid: &str, coin: &str) -> Result<OrderPoll> {
        let user = self
            .account
            .clone()
            .ok_or_else(|| Error::msg("missing OUTCOME_ACCOUNT_ADDRESS"))?;
        let value: Value = self
            .http
            .post(&self.info_url)
            .json(&json!({"type": "orderStatus", "user": user, "oid": oid}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let _ = coin;
        Ok(parse_order_status(value, oid))
    }

    pub async fn poll_fills(&self, coin: Option<&str>) -> Result<Vec<TradeFill>> {
        let user = self
            .account
            .clone()
            .ok_or_else(|| Error::msg("missing OUTCOME_ACCOUNT_ADDRESS"))?;
        let value: Value = self
            .http
            .post(&self.info_url)
            .json(&json!({"type": "userFills", "user": user}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let fills = parse_user_fills(&value);
        Ok(match coin {
            Some(want) => fills
                .into_iter()
                .filter(|fill| fill.coin.as_deref() == Some(want))
                .collect(),
            None => fills,
        })
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

pub fn parse_exchange_submit(
    body: &Value,
    order_hash: String,
    envelope: Value,
    cloid: &str,
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
        let oid = json_id(first.pointer("/resting/oid"))
            .or_else(|| json_id(first.pointer("/filled/oid")))
            .unwrap_or_else(|| cloid.to_string());
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

pub fn parse_order_status(raw: Value, oid: &str) -> OrderPoll {
    let status = raw
        .pointer("/order/status")
        .or_else(|| raw.get("status"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    OrderPoll {
        found: !raw.is_null(),
        status,
        order_id: Some(oid.to_string()),
        shares: raw.pointer("/order/sz").and_then(parse_decimal),
        price: raw.pointer("/order/limitPx").and_then(parse_decimal),
        fee: None,
        raw,
    }
}

pub fn parse_user_fills(raw: &Value) -> Vec<TradeFill> {
    let items = raw.as_array().cloned().unwrap_or_else(|| {
        raw.get("fills")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
    });
    items
        .into_iter()
        .filter_map(|item| {
            let mut order_ids = Vec::new();
            if let Some(oid) = json_id(item.get("oid")) {
                order_ids.push(oid);
            }
            if let Some(cloid) = item.get("cloid").and_then(|v| v.as_str()) {
                if !order_ids.iter().any(|id| id == cloid) {
                    order_ids.push(cloid.to_string());
                }
            }
            Some(TradeFill {
                trade_id: json_id(item.get("tid")).unwrap_or_default(),
                order_id: order_ids.first().cloned(),
                order_ids,
                coin: item
                    .get("coin")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
                shares: item.get("sz").and_then(parse_decimal)?,
                price: item.get("px").and_then(parse_decimal)?,
                fee: item
                    .get("fee")
                    .and_then(parse_decimal)
                    .unwrap_or(Decimal::ZERO),
                fee_rate_bps: None,
                raw: item,
            })
        })
        .collect()
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
        if coin.eq_ignore_ascii_case(want) {
            return item
                .get("total")
                .or_else(|| item.get("hold"))
                .and_then(parse_decimal)
                .unwrap_or(Decimal::ZERO);
        }
    }
    Decimal::ZERO
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!fills[1].matches(Some("99"), None));
    }

    #[test]
    fn parse_coin_balance_reads_named_token() {
        let raw = json!({"balances": [
            {"coin": "USDC", "total": "10"},
            {"coin": "#5160", "total": "7"}
        ]});
        assert_eq!(parse_coin_balance(&raw, "#5160").to_string(), "7");
        assert_eq!(parse_usdc_balance(&raw).to_string(), "10");
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
        let body = exchange_ok(json!({"error": "Order must have minimum value of $10."}));
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
}
