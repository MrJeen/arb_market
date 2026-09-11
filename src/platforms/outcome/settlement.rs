//! Read-only settlement evidence, deliberately separate from ordinary trade backfill.
use crate::error::{Error, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, str::FromStr, time::Instant};

const PAGE_SIZE: usize = 2_000;
const HISTORY_LIMIT: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementEvent {
    pub coin: String,
    pub tid: String,
    pub hash: String,
    pub time: u64,
    pub px: Decimal,
    pub sz: Decimal,
    pub fee: Decimal,
    pub fee_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipTrade {
    pub coin: String,
    pub oid: String,
    pub tid: String,
    /// Exchange side: B (buy) or A (sell).
    pub side: String,
    pub sz: Decimal,
    pub px: Decimal,
    #[serde(rename = "startPosition")]
    pub start_position: Decimal,
    pub time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementScan {
    pub wallet: String,
    pub token: String,
    pub start_time: u64,
    pub end_time: u64,
    pub complete: bool,
    pub history_complete: bool,
    pub events: Vec<SettlementEvent>,
    pub trades: Vec<OwnershipTrade>,
    version: u8,
    cursor: u64,
    final_probe: bool,
    // Full raw identity makes changed duplicates a hard error, including other coins.
    seen: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementPage {
    pub progress: SettlementScan,
    pub events: Vec<SettlementEvent>,
    pub trades: Vec<OwnershipTrade>,
}

pub fn validate_wallet(wallet: &str) -> Result<()> {
    if wallet.len() != 42
        || !wallet.starts_with("0x")
        || !wallet[2..].bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(Error::msg(
            "settlement requires an explicit valid wallet address",
        ));
    }
    Ok(())
}

impl SettlementScan {
    /// Start strictly before the earliest local participating submission; end is fixed per scan.
    pub fn new(wallet: &str, token: &str, start_time: u64, end_time: u64) -> Result<Self> {
        validate_wallet(wallet)?;
        crate::domain::parse_side_coin(token)
            .ok_or_else(|| Error::msg("invalid outcome settlement token"))?;
        if start_time == 0 || start_time > end_time {
            return Err(Error::msg("invalid settlement scan window"));
        }
        Ok(Self {
            wallet: wallet.to_ascii_lowercase(),
            token: token.into(),
            start_time,
            end_time,
            complete: false,
            history_complete: false,
            events: vec![],
            trades: vec![],
            version: 1,
            cursor: start_time,
            final_probe: false,
            seen: BTreeMap::new(),
        })
    }

    fn validate(&self) -> Result<()> {
        Self::new(&self.wallet, &self.token, self.start_time, self.end_time)?;
        if self.version != 1
            || self.cursor < self.start_time
            || self.cursor > self.end_time
            || self.seen.len() >= HISTORY_LIMIT
            || (self.history_complete && !self.complete)
        {
            return Err(Error::msg("invalid settlement scan progress"));
        }
        Ok(())
    }
}

fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::msg(format!("settlement evidence missing {key}")))
}
fn id(v: &Value, key: &str) -> Result<String> {
    if let Some(n) = v.get(key).and_then(Value::as_u64) {
        return Ok(n.to_string());
    }
    let s = text(v, key)?;
    s.parse::<u64>()
        .map(|n| n.to_string())
        .map_err(|_| Error::msg(format!("settlement evidence invalid {key}")))
}
fn decimal(v: &Value, key: &str) -> Result<Decimal> {
    Decimal::from_str(text(v, key)?)
        .map_err(|_| Error::msg(format!("settlement evidence invalid {key}")))
}
fn time(v: &Value) -> Result<u64> {
    v.get("time")
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::msg("settlement evidence missing valid time"))
}

pub fn parse_settlement_event(v: &Value, token: &str) -> Result<SettlementEvent> {
    if text(v, "dir")? != "Settlement"
        || text(v, "coin")? != token
        || text(v, "feeToken")? != "USDC"
    {
        return Err(Error::msg(
            "invalid settlement direction, coin or fee token",
        ));
    }
    let event = SettlementEvent {
        coin: token.into(),
        tid: id(v, "tid")?,
        hash: text(v, "hash")?.into(),
        time: time(v)?,
        px: decimal(v, "px")?,
        sz: decimal(v, "sz")?,
        fee: decimal(v, "fee")?,
        fee_token: "USDC".into(),
    };
    if event.px < Decimal::ZERO
        || event.px > Decimal::ONE
        || event.sz <= Decimal::ZERO
        || event.fee < Decimal::ZERO
    {
        return Err(Error::msg("invalid settlement amounts"));
    }
    Ok(event)
}

pub fn parse_ownership_trade(v: &Value, token: &str) -> Result<OwnershipTrade> {
    if text(v, "coin")? != token || !matches!(text(v, "dir")?, "Buy" | "Sell") {
        return Err(Error::msg("unsupported ordinary outcome fill direction"));
    }
    let side = text(v, "side")?;
    if !matches!((text(v, "dir")?, side), ("Buy", "B") | ("Sell", "A")) {
        return Err(Error::msg("ordinary outcome fill side mismatch"));
    }
    let trade = OwnershipTrade {
        coin: token.into(),
        oid: id(v, "oid")?,
        tid: id(v, "tid")?,
        side: side.into(),
        sz: decimal(v, "sz")?,
        px: decimal(v, "px")?,
        start_position: decimal(v, "startPosition")?,
        time: time(v)?,
    };
    if trade.sz <= Decimal::ZERO
        || trade.px <= Decimal::ZERO
        || trade.start_position < Decimal::ZERO
    {
        return Err(Error::msg("invalid ownership trade quantity"));
    }
    Ok(trade)
}

pub fn ownership_trade(raw: &Value) -> Result<OwnershipTrade> {
    parse_ownership_trade(raw, text(raw, "coin")?)
}

/// Pure and atomic: an invalid page never mutates caller progress.
pub fn apply_page(raw: &Value, progress: &SettlementScan) -> Result<SettlementPage> {
    progress.validate()?;
    if progress.complete {
        return Err(Error::msg("settlement scan already complete"));
    }
    let rows = raw
        .as_array()
        .ok_or_else(|| Error::msg("settlement page is not an array"))?;
    if rows.len() > PAGE_SIZE {
        return Err(Error::msg("settlement page exceeds limit"));
    }
    let mut next = progress.clone();
    let mut previous = if next.final_probe { 0 } else { next.cursor };
    let mut events = vec![];
    let mut trades = vec![];
    let before = next.seen.len();
    for row in rows {
        let ts = time(row)?;
        if ts < previous || ts > next.end_time {
            return Err(Error::msg("settlement page unordered or outside window"));
        }
        previous = ts;
        let tid = id(row, "tid")?;
        let coin = text(row, "coin")?;
        if coin != next.token && super::coin_aliases_match(coin, &next.token) {
            return Err(Error::msg("unexpected outcome fill coin alias"));
        }
        if let Some(old) = next.seen.get(&tid) {
            if old != row {
                return Err(Error::msg("conflicting settlement scan tid"));
            }
            continue;
        }
        if next.final_probe {
            if ts >= next.start_time {
                return Err(Error::msg(
                    "settlement history changed after scan; restart required",
                ));
            }
            continue;
        }
        next.seen.insert(tid, row.clone());
        if coin == next.token {
            if text(row, "dir")? == "Settlement" {
                events.push(parse_settlement_event(row, &next.token)?);
            } else {
                trades.push(parse_ownership_trade(row, &next.token)?);
            }
        }
    }
    if next.final_probe {
        // A strict pre-window lower bound detects retention clipping, including millisecond ties.
        let first = rows.first().map(time).transpose()?;
        let last = rows.last().map(time).transpose()?;
        next.history_complete = first.is_some_and(|t| t < next.start_time)
            && !(rows.len() == PAGE_SIZE && first == last);
        next.complete = true;
    } else {
        if next.seen.len() >= HISTORY_LIMIT {
            return Err(Error::msg("settlement history retention limit reached"));
        }
        next.cursor = previous;
        if rows.len() < PAGE_SIZE {
            next.final_probe = true;
        } else if next.cursor == progress.cursor && next.seen.len() == before {
            return Err(Error::msg(
                "settlement pagination stalled at millisecond boundary",
            ));
        }
        next.events.extend(events.iter().cloned());
        next.trades.extend(trades.iter().cloned());
    }
    Ok(SettlementPage {
        progress: next,
        events,
        trades,
    })
}

async fn query(client: &reqwest::Client, url: &str, token: &str, body: Value) -> Result<Value> {
    let started = Instant::now();
    let mut status = None;
    let result: Result<Value> = async {
        let response = client
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        status = Some(response.status().as_u16());
        Ok(response
            .error_for_status()
            .map_err(reqwest::Error::without_url)?
            .json()
            .await
            .map_err(reqwest::Error::without_url)?)
    }
    .await;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    match &result {
        Ok(_) => tracing::debug!(service="outcome", api=?body.get("type"), token_id=token,
            http_status=?status, elapsed_ms, start_time=?body.get("startTime"),
            end_time=?body.get("endTime"), "settlement read-only query completed"),
        Err(err) => tracing::error!(service="outcome", api=?body.get("type"), token_id=token,
            http_status=?status, elapsed_ms, error=%err, "settlement read-only query failed"),
    }
    result
}

pub async fn scan_page(
    client: &reqwest::Client,
    info_url: &str,
    wallet: &str,
    token: &str,
    progress: &SettlementScan,
) -> Result<SettlementPage> {
    validate_wallet(wallet)?;
    progress.validate()?;
    if !wallet.eq_ignore_ascii_case(&progress.wallet) || token != progress.token {
        return Err(Error::msg("settlement scan wallet or token mismatch"));
    }
    if progress.complete {
        return Err(Error::msg("settlement scan already complete"));
    }
    let start = if progress.final_probe {
        0
    } else {
        progress.cursor
    };
    let raw = query(
        client,
        info_url,
        token,
        json!({"type":"userFillsByTime", "user":wallet,
        "startTime":start, "endTime":progress.end_time, "aggregateByTime":false}),
    )
    .await?;
    let result = match apply_page(&raw, progress) {
        Err(err)
            if err.to_string() == "settlement history changed after scan; restart required" =>
        {
            // 已扫窗口中出现迟到记录：丢弃未封存进度重扫，不能永久卡在旧 final probe。
            Ok(SettlementPage {
                progress: SettlementScan::new(
                    wallet,
                    token,
                    progress.start_time,
                    progress.end_time,
                )?,
                events: Vec::new(),
                trades: Vec::new(),
            })
        }
        result => result,
    };
    if let Err(err) = &result {
        tracing::error!(service="outcome", api="userFillsByTime", token_id=token,
            start_time=start, end_time=progress.end_time, error=%err, "settlement evidence rejected");
    }
    result
}

/// Requires exact local fill evidence, not merely an order-id allowlist.
/// The caller must additionally require check_transfer_ledger to succeed.
pub fn validate_ownership(progress: &SettlementScan, local_fills: &[OwnershipTrade]) -> Result<()> {
    progress.validate()?;
    if !progress.complete || !progress.history_complete {
        return Err(Error::msg("settlement history coverage is incomplete"));
    }
    let mut local = BTreeMap::new();
    for fill in local_fills {
        if local.insert(fill.tid.clone(), fill).is_some() {
            return Err(Error::msg("duplicate local ownership fill"));
        }
    }
    if local.len() != progress.trades.len() || progress.trades.is_empty() {
        return Err(Error::msg(
            "local and remote ownership fills do not cover lifecycle",
        ));
    }
    for trade in &progress.trades {
        if local.get(&trade.tid).copied() != Some(trade) {
            return Err(Error::msg("unmapped ordinary outcome fill"));
        }
    }
    let mut groups: BTreeMap<u64, (Vec<&OwnershipTrade>, Vec<&SettlementEvent>)> = BTreeMap::new();
    for t in &progress.trades {
        groups.entry(t.time).or_default().0.push(t);
    }
    for e in &progress.events {
        groups.entry(e.time).or_default().1.push(e);
    }
    let mut position = Decimal::ZERO;
    for (_, (mut trades, events)) in groups {
        // tid is an identity, not a matching-engine sequence. Mixed trade/settlement
        // milliseconds have no reliable ordering evidence and must be audited.
        if !trades.is_empty() && !events.is_empty() {
            return Err(Error::msg(
                "ambiguous same-millisecond trade and settlement",
            ));
        }
        while !trades.is_empty() {
            let matches: Vec<_> = trades
                .iter()
                .enumerate()
                .filter(|(_, t)| t.start_position == position)
                .map(|(i, _)| i)
                .collect();
            if matches.len() != 1 {
                return Err(Error::msg("ownership position chain missing or ambiguous"));
            }
            let t = trades.remove(matches[0]);
            position = if t.side == "B" {
                position.checked_add(t.sz)
            } else {
                position.checked_sub(t.sz)
            }
            .ok_or_else(|| Error::msg("ownership position overflow"))?;
            if position < Decimal::ZERO {
                return Err(Error::msg("negative ownership position"));
            }
        }
        for e in events {
            position = position
                .checked_sub(e.sz)
                .ok_or_else(|| Error::msg("ownership position overflow"))?;
            if position < Decimal::ZERO {
                return Err(Error::msg("settlement exceeds owned position"));
            }
        }
    }
    if progress.events.is_empty() || position != Decimal::ZERO {
        return Err(Error::msg(
            "settlement lifecycle has no settlement or residual position",
        ));
    }
    Ok(())
}

/// Unknown ledger variants cannot prove absence of token transfers. Only explicitly
/// USDC-only known variants are ignored; target-token activity requires manual audit.
pub fn validate_transfer_ledger(raw: &Value, token: &str, start: u64, end: u64) -> Result<()> {
    let rows = raw
        .as_array()
        .ok_or_else(|| Error::msg("invalid transfer ledger page"))?;
    if rows.len() >= 500 {
        return Err(Error::msg("transfer ledger coverage may be truncated"));
    }
    let mut previous = start;
    for row in rows {
        let ts = time(row)?;
        if ts < previous || ts > end {
            return Err(Error::msg("transfer ledger outside window or unordered"));
        }
        previous = ts;
        // A single transaction hash can carry multiple distinct ledger deltas.
        text(row, "hash")?;
        let delta = row
            .get("delta")
            .ok_or_else(|| Error::msg("missing ledger delta"))?;
        let kind = text(delta, "type")?;
        match kind {
            "deposit"
            | "withdraw"
            | "internalTransfer"
            | "subAccountTransfer"
            | "accountClassTransfer"
                if delta.get("token").is_none()
                    && delta.get("coin").is_none()
                    && delta.get("usdc").is_some() => {}
            "spotTransfer" | "send" | "accountActivationGas"
                if delta.get("token").and_then(Value::as_str) == Some("USDC")
                    && delta.get("coin").is_none_or(|v| v.as_str() == Some("USDC")) => {}
            _ => {
                return Err(Error::msg(format!(
                    "unresolved transfer ledger activity for {token}"
                )))
            }
        }
    }
    Ok(())
}

pub async fn check_transfer_ledger(
    client: &reqwest::Client,
    info_url: &str,
    wallet: &str,
    token: &str,
    start_time: u64,
    end_time: u64,
) -> Result<()> {
    SettlementScan::new(wallet, token, start_time, end_time)?;
    let raw = query(
        client,
        info_url,
        token,
        json!({"type":"userNonFundingLedgerUpdates",
        "user":wallet, "startTime":start_time, "endTime":end_time}),
    )
    .await?;
    let result = validate_transfer_ledger(&raw, token, start_time, end_time);
    if let Err(err) = &result {
        tracing::error!(service="outcome", api="userNonFundingLedgerUpdates", token_id=token,
            start_time, end_time, error=%err, "settlement transfer ledger rejected");
    }
    result?;
    // A short requested-window page alone cannot rule out retained-history clipping.
    let probe = query(
        client,
        info_url,
        token,
        json!({"type":"userNonFundingLedgerUpdates",
        "user":wallet, "startTime":0, "endTime":end_time}),
    )
    .await?;
    let rows = probe
        .as_array()
        .ok_or_else(|| Error::msg("invalid ledger history probe"))?;
    let first = rows.first().map(time).transpose()?;
    let last = rows.last().map(time).transpose()?;
    // Both queries empty means no observed transfers, not a retention guarantee.
    if rows.is_empty() && raw.as_array().is_some_and(Vec::is_empty) {
        return Ok(());
    }
    if rows.len() > 500
        || !first.is_some_and(|t| t < start_time)
        || (rows.len() == 500 && first == last)
    {
        tracing::warn!(
            service = "outcome",
            api = "userNonFundingLedgerUpdates",
            token_id = token,
            start_time,
            end_time,
            "settlement ledger history coverage unavailable"
        );
        return Err(Error::msg("transfer ledger history coverage is incomplete"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    const WALLET: &str = "0x1111111111111111111111111111111111111111";
    fn event(tid: u64, time: u64) -> Value {
        json!({"dir":"Settlement","coin":"#5160","tid":tid,"hash":"0xabc","time":time,
            "px":"0","sz":"2","fee":"0","feeToken":"USDC"})
    }
    #[test]
    fn same_millisecond_position_chain_and_partial_settlements() {
        let buy = |tid, start: &str| {
            json!({"coin":"#5160","dir":"Buy","side":"B",
            "oid":tid,"tid":tid,"sz":"2","px":"0.4","startPosition":start,"time":100})
        };
        let state = SettlementScan::new(WALLET, "#5160", 99, 200).unwrap();
        let page = apply_page(
            &json!([buy(1, "2"), buy(90, "0"), event(100, 101), event(101, 101)]),
            &state,
        )
        .unwrap();
        let done = apply_page(&json!([event(200, 98)]), &page.progress)
            .unwrap()
            .progress;
        assert!(validate_ownership(&done, &done.trades).is_ok());
        let mut residual = done.clone();
        residual.events.pop();
        assert!(validate_ownership(&residual, &residual.trades).is_err());
        let mut ambiguous = done.clone();
        ambiguous.trades[0].start_position = Decimal::ZERO;
        assert!(validate_ownership(&ambiguous, &ambiguous.trades).is_err());
    }
    #[test]
    fn distinct_usdc_deltas_share_transaction_hash() {
        let rows = json!([
            {"time":100,"hash":"same","delta":{"type":"accountActivationGas","amount":"1.0","token":"USDC"}},
            {"time":100,"hash":"same","delta":{"type":"send","amount":"2","token":"USDC"}}
        ]);
        assert!(validate_transfer_ledger(&rows, "#5160", 99, 200).is_ok());
        let rows: Vec<_> = (0..500).map(|_| rows[0].clone()).collect();
        assert!(validate_transfer_ledger(&json!(rows), "#5160", 99, 200).is_err());
    }
    #[test]
    fn strict_settlement_allows_zero_price() {
        assert_eq!(
            parse_settlement_event(&event(1, 100), "#5160").unwrap().px,
            Decimal::ZERO
        );
        for key in [
            "dir", "coin", "tid", "hash", "time", "px", "sz", "fee", "feeToken",
        ] {
            let mut bad = event(1, 100);
            bad.as_object_mut().unwrap().remove(key);
            assert!(parse_settlement_event(&bad, "#5160").is_err(), "{key}");
        }
        let mut bad = event(1, 100);
        bad["fee"] = json!("-1");
        assert!(parse_settlement_event(&bad, "#5160").is_err());
    }
    #[test]
    fn dedup_conflict_is_atomic() {
        let state = SettlementScan::new(WALLET, "#5160", 100, 200).unwrap();
        let mut bad = event(1, 100);
        bad["sz"] = json!("3");
        assert!(apply_page(&json!([event(1, 100), bad]), &state).is_err());
        assert!(state.events.is_empty());
        let page = apply_page(&json!([event(1, 100), event(1, 100)]), &state).unwrap();
        assert_eq!(page.events.len(), 1);
        assert!(!page.progress.complete);
        let done = apply_page(&json!([event(2, 99)]), &page.progress).unwrap();
        assert!(done.progress.history_complete);
        assert!(
            apply_page(&json!([]), &page.progress)
                .unwrap()
                .progress
                .complete
        );
        assert!(
            !apply_page(&json!([]), &page.progress)
                .unwrap()
                .progress
                .history_complete
        );
    }
    #[test]
    fn full_other_coin_page_and_same_millisecond_stall() {
        let state = SettlementScan::new(WALLET, "#5160", 100, 3000).unwrap();
        let rows: Vec<_> = (0..PAGE_SIZE)
            .map(|i| json!({"tid":i,"time":100,"coin":"BTC"}))
            .collect();
        let page = apply_page(&json!(rows), &state).unwrap();
        assert!(page.events.is_empty());
        assert!(!page.progress.complete);
        assert!(apply_page(&json!(rows), &page.progress).is_err());
    }
    #[test]
    fn ownership_and_ledger_fail_closed() {
        assert!(validate_wallet("not-a-wallet").is_err());
        assert!(validate_transfer_ledger(
            &json!([{"time":100,"hash":"x","delta":{"type":"spotTransfer","token":"#5160"}}]),
            "#5160",
            100,
            200
        )
        .is_err());
        assert!(validate_transfer_ledger(
            &json!([{"time":100,"hash":"x","delta":{"type":"unknown","usdc":"2"}}]),
            "#5160",
            100,
            200
        )
        .is_err());
        let buy = json!({"coin":"#5160","dir":"Buy","side":"B","oid":1,"tid":1,"sz":"2","px":"0.4","startPosition":"0","time":100});
        let state = SettlementScan::new(WALLET, "#5160", 99, 200).unwrap();
        let page = apply_page(&json!([buy, event(2, 101)]), &state).unwrap();
        let done = apply_page(&json!([event(3, 98)]), &page.progress)
            .unwrap()
            .progress;
        assert!(validate_ownership(&done, &done.trades).is_ok());
        assert!(validate_ownership(&done, &[]).is_err());
        let mut bad = done.clone();
        bad.trades[0].start_position = Decimal::ONE;
        assert!(validate_ownership(&bad, &bad.trades).is_err());
    }
}
