//! Outcome 账户结算取证；在线续扫和历史只读对账共用同一核对口径。
use crate::error::{Error, Result};
use crate::platforms::outcome::settlement::{
    check_transfer_ledger, ownership_trade, scan_page, validate_ownership, SettlementScan,
};
use crate::store::settlement_fees::{FeeEvent, GroupKey, GroupSnapshot};
use crate::store::Store;
use rust_decimal::Decimal;
use serde_json::{json, Value};

/// 每次在线清扫只取一页，历史审计由调用方逐页推进。
pub async fn advance(
    client: &reqwest::Client,
    info_url: &str,
    snapshot: &GroupSnapshot,
    previous: Option<&Value>,
    end_time: u64,
) -> Result<SettlementScan> {
    let start_time = snapshot
        .legs
        .iter()
        .filter_map(|row| row.leg.submitted_at)
        .map(|time| time.timestamp_millis().max(0) as u64)
        .min()
        .ok_or_else(|| Error::msg("missing settlement ownership start time"))?
        .saturating_sub(1_000);
    let progress = match previous {
        Some(value) if !value.is_null() => serde_json::from_value::<SettlementScan>(value.clone())?,
        _ => SettlementScan::new(
            &snapshot.key.wallet,
            &snapshot.key.token,
            start_time,
            end_time,
        )?,
    };
    let progress = if progress.start_time != start_time
        || progress.wallet != snapshot.key.wallet
        || progress.token != snapshot.key.token
    {
        SettlementScan::new(
            &snapshot.key.wallet,
            &snapshot.key.token,
            start_time,
            end_time,
        )?
    } else {
        progress
    };
    let progress = if progress.complete {
        SettlementScan::new(
            &snapshot.key.wallet,
            &snapshot.key.token,
            start_time,
            end_time,
        )?
    } else {
        progress
    };
    let page = scan_page(
        client,
        info_url,
        &snapshot.key.wallet,
        &snapshot.key.token,
        &progress,
    )
    .await?;
    Ok(page.progress)
}

pub async fn prove(
    client: &reqwest::Client,
    info_url: &str,
    snapshot: &GroupSnapshot,
    progress: &SettlementScan,
    payout: Decimal,
) -> Result<(Vec<FeeEvent>, Value)> {
    if progress.wallet != snapshot.key.wallet || progress.token != snapshot.key.token {
        return Err(Error::msg("settlement proof wallet/token mismatch"));
    }
    if !progress.complete || !progress.history_complete {
        return Err(Error::msg("settlement history coverage incomplete"));
    }
    let mut local = Vec::new();
    for row in &snapshot.legs {
        for fill in &row.fills {
            let raw = fill
                .raw
                .get("reconciliation_v1")
                .and_then(|value| value.get("raw"))
                .ok_or_else(|| Error::msg("missing original outcome fill evidence"))?;
            let trade = ownership_trade(raw)?;
            if trade.sz != fill.shares
                || trade.px != fill.price
                || trade.tid.to_string() != fill.trade_id
                || trade.oid.to_string() != fill.third_order_id
                || trade.coin != snapshot.key.token
                || trade.side
                    != if row.leg.side.eq_ignore_ascii_case("BUY") {
                        "B"
                    } else {
                        "A"
                    }
            {
                return Err(Error::msg(
                    "local outcome fill differs from original evidence",
                ));
            }
            local.push(trade);
        }
    }
    validate_ownership(progress, &local)?;
    check_transfer_ledger(
        client,
        info_url,
        &snapshot.key.wallet,
        &snapshot.key.token,
        progress.start_time,
        progress.end_time,
    )
    .await?;
    let mut events = Vec::new();
    for event in &progress.events {
        if event.px != payout {
            return Err(Error::msg(
                "settlement fill payout differs from market payout",
            ));
        }
        events.push(FeeEvent {
            tid: event.tid.to_string(),
            quantity: event.sz,
            payout: event.px,
            fee: event.fee,
            fee_token: event.fee_token.clone(),
            evidence: serde_json::to_value(event)?,
        });
    }
    if events.is_empty() {
        return Err(Error::msg("settlement event not yet available"));
    }
    Ok((
        events,
        json!({
            "version": 1,
            "source": "userFillsByTime",
            "ownership_verified": true,
            "transfers_checked": true,
            "scan": progress,
        }),
    ))
}

/// 封存组直接复用；未封存组一轮一页，网络失败保留此前成功进度。
pub async fn prepare_online(
    store: &Store,
    client: &reqwest::Client,
    info_url: &str,
    key: &GroupKey,
    payout: Decimal,
) -> Result<bool> {
    let mut expected = store.settlement_fee_progress(key).await?;
    let result = prepare_online_inner(store, client, info_url, key, payout, &mut expected).await;
    if let Err(err) = &result {
        if let Err(save_error) = store
            .settlement_fee_blocked(key, expected.as_ref(), &err.to_string())
            .await
        {
            tracing::error!(service="outcome", token_id=%key.token, error=%save_error, "cannot persist settlement blocking reason");
        }
    }
    result
}

async fn prepare_online_inner(
    store: &Store,
    client: &reqwest::Client,
    info_url: &str,
    key: &GroupKey,
    payout: Decimal,
    expected: &mut Option<Value>,
) -> Result<bool> {
    let snapshot = store.settlement_fee_group_snapshot(key).await?;
    if snapshot.sealed {
        if snapshot.allocations.iter().any(|a| a.payout != payout) {
            return Err(Error::msg("sealed settlement payout changed"));
        }
        return Ok(true);
    }
    let previous = expected.clone();
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let progress = advance(client, info_url, &snapshot, previous.as_ref(), now).await?;
    store
        .save_settlement_fee_progress(key, previous.as_ref(), &serde_json::to_value(&progress)?)
        .await?;
    *expected = Some(serde_json::to_value(&progress)?);
    if !progress.complete {
        return Ok(false);
    }
    let stable = store.prepare_settlement_fee_group(key).await?;
    if serde_json::to_value(&stable.legs)? != serde_json::to_value(&snapshot.legs)? {
        return Err(Error::msg(
            "settlement scan snapshot changed; retry required",
        ));
    }
    let (events, evidence) = prove(client, info_url, &stable, &progress, payout).await?;
    let allocations = store
        .commit_settlement_fee_group(&snapshot, payout, &events, &evidence)
        .await?;
    tracing::info!(service="outcome", token_id=%key.token, event_count=events.len(), order_count=allocations.len(), "settlement fees verified and allocated");
    Ok(true)
}
