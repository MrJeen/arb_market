use crate::config::{OUTCOME, POLYMARKET};
use crate::error::{Error, Result};
use crate::platforms::{FillFinality, OrderPoll, TradeFill};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillEvidence {
    pub poll: OrderPoll,
    pub page_complete: bool,
    pub history_complete: bool,
    pub expected_shares: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegResolution {
    Pending(&'static str),
    Terminal {
        status: &'static str,
        shares: Decimal,
        price: Decimal,
        fee: Decimal,
        fee_sources: Vec<&'static str>,
    },
}

/// 同一交易的最终证据不可被较旧观察降级；矛盾终态交给人工核对，不按到达顺序改账。
pub fn merge_observation(previous: &TradeFill, incoming: &TradeFill) -> Result<TradeFill> {
    if previous.trade_id != incoming.trade_id || previous.order_id != incoming.order_id {
        return Err(Error::msg("fill identity changed during reconciliation"));
    }
    match (previous.finality, incoming.finality) {
        (FillFinality::Confirmed, FillFinality::Failed)
        | (FillFinality::Failed, FillFinality::Confirmed) => {
            Err(Error::msg("conflicting terminal trade evidence"))
        }
        (FillFinality::Confirmed | FillFinality::Failed, FillFinality::Pending) => {
            Ok(previous.clone())
        }
        (FillFinality::Confirmed, FillFinality::Confirmed) => {
            if previous.shares != incoming.shares || previous.price != incoming.price {
                return Err(Error::msg("confirmed trade quantity or price changed"));
            }
            if previous.fee.is_some() && incoming.fee.is_some() && previous.fee != incoming.fee {
                return Err(Error::msg("confirmed trade fee changed"));
            }
            let mut merged = incoming.clone();
            if merged.fee.is_none() {
                merged.fee = previous.fee;
                merged.fee_token = previous.fee_token.clone();
            }
            // 同一成交一旦选定估算快照，重试不得按变化后的市场费率悄悄改账。
            if let Some(snapshot) = previous.raw.get("fee_calculation") {
                merged.raw["fee_calculation"] = snapshot.clone();
            }
            Ok(merged)
        }
        _ => Ok(incoming.clone()),
    }
}

pub fn resolve_leg(
    platform: &str,
    observations: &[TradeFill],
    evidence: &FillEvidence,
) -> Result<LegResolution> {
    let poll = &evidence.poll;
    if !poll.found || poll.order_id.is_none() {
        return Ok(LegResolution::Pending("order_not_found"));
    }
    let mut trades = BTreeMap::<String, TradeFill>::new();
    for fill in observations {
        if fill.trade_id.is_empty() || fill.trade_id.starts_with("ack:") {
            continue;
        }
        if fill.order_id != poll.order_id {
            return Err(Error::msg("trade belongs to a different order"));
        }
        if fill.shares <= Decimal::ZERO || fill.price <= Decimal::ZERO || fill.price > Decimal::ONE
        {
            return Err(Error::msg("invalid trade quantity or price"));
        }
        let merged = match trades.get(&fill.trade_id) {
            Some(previous) => merge_observation(previous, fill)?,
            None => fill.clone(),
        };
        trades.insert(fill.trade_id.clone(), merged);
    }
    let state = poll.status.to_ascii_lowercase();
    let cancelled = cancellation_status(&state);
    let successful: Vec<_> = trades
        .values()
        .filter(|fill| fill.finality == FillFinality::Confirmed)
        .collect();
    let shares: Decimal = successful.iter().map(|fill| fill.shares).sum();
    if trades
        .values()
        .any(|fill| fill.finality == FillFinality::Pending)
    {
        return Ok(LegResolution::Pending("trade_confirmation_pending"));
    }
    if platform == POLYMARKET {
        if state != "matched" && !cancelled {
            return Ok(LegResolution::Pending("order_still_open"));
        }
        if !evidence.page_complete {
            return Ok(LegResolution::Pending("trade_pages_incomplete"));
        }
        // 缺失关联字段和空关联集合不是“所有成交已失败”的证明。
        if !poll
            .raw
            .get("associate_trades")
            .and_then(|v| v.as_array())
            .is_some_and(|ids| {
                ids.iter()
                    .all(|id| id.as_str().is_some_and(|id| !id.is_empty()))
                    && ids.len() == poll.associated_trades.len()
            })
        {
            return Ok(LegResolution::Pending("associated_trades_missing"));
        }
        let associated: HashSet<_> = poll.associated_trades.iter().map(String::as_str).collect();
        if associated.iter().any(|id| !trades.contains_key(*id)) {
            return Ok(LegResolution::Pending("associated_trade_missing"));
        }
        if trades.keys().any(|id| !associated.contains(id.as_str())) {
            return Ok(LegResolution::Pending("order_trade_snapshot_mismatch"));
        }
        let observed: Decimal = trades.values().map(|fill| fill.shares).sum();
        if poll.shares != Some(observed) {
            return Ok(LegResolution::Pending("matched_quantity_incomplete"));
        }
        if observed.is_zero() && !cancelled {
            return Ok(LegResolution::Pending("zero_fill_not_proven"));
        }
    } else if platform == OUTCOME {
        if state != "filled" && !cancelled {
            return Ok(LegResolution::Pending("order_still_open"));
        }
        let expected = evidence.expected_shares.or_else(|| {
            (state == "filled")
                .then_some(poll.original_shares)
                .flatten()
        });
        if let Some(expected) = expected {
            if expected < Decimal::ZERO || shares != expected {
                return Ok(LegResolution::Pending("executed_quantity_incomplete"));
            }
        } else if !evidence.page_complete || !evidence.history_complete {
            return Ok(LegResolution::Pending("fill_history_incomplete"));
        }
        if poll
            .original_shares
            .is_some_and(|original| shares > original)
        {
            return Err(Error::msg("fills exceed original order quantity"));
        }
    } else {
        return Err(Error::msg("unsupported reconciliation platform"));
    }
    if shares.is_zero() {
        return Ok(LegResolution::Terminal {
            status: if trades
                .values()
                .any(|fill| fill.finality == FillFinality::Failed)
            {
                "failed"
            } else {
                "cancelled"
            },
            shares,
            price: Decimal::ZERO,
            fee: Decimal::ZERO,
            fee_sources: Vec::new(),
        });
    }
    let mut fee = Decimal::ZERO;
    let mut fee_sources = Vec::new();
    for fill in &successful {
        let Some((amount, source)) = accounting_fee(platform, fill)? else {
            return Ok(LegResolution::Pending("fee_evidence_missing"));
        };
        fee += amount;
        if !fee_sources.contains(&source) {
            fee_sources.push(source);
        }
    }
    let notional: Decimal = successful.iter().map(|fill| fill.shares * fill.price).sum();
    Ok(LegResolution::Terminal {
        status: "matched",
        shares,
        price: notional / shares,
        fee,
        fee_sources,
    })
}

pub fn accounting_fee(platform: &str, fill: &TradeFill) -> Result<Option<(Decimal, &'static str)>> {
    if let Some(fee) = fill.fee {
        if fill.fee_token.as_deref().is_some_and(|token| {
            token.eq_ignore_ascii_case("USDC")
                || (platform == POLYMARKET && token.eq_ignore_ascii_case("pUSD"))
        }) {
            return Ok(Some((fee, "actual")));
        }
        return Ok(None);
    }
    if platform == POLYMARKET && fill.raw.get("role").and_then(|v| v.as_str()) == Some("maker") {
        // 官方 maker 零费规则不依赖 taker 费率系数或扣费币种。
        return Ok(Some((Decimal::ZERO, "calculated_maker_zero")));
    }
    if platform == POLYMARKET {
        if let Some(snapshot) = fill.raw.get("fee_calculation") {
            let Some(rate) = snapshot
                .get("rate")
                .and_then(crate::platforms::parse_decimal)
            else {
                return Err(Error::msg("fee calculation snapshot missing rate"));
            };
            if rate < Decimal::ZERO
                || rate > Decimal::ONE
                || snapshot.get("source").and_then(|v| v.as_str()) != Some("clob-markets")
            {
                return Err(Error::msg("invalid fee calculation snapshot"));
            }
            // 官方 trading/fees：C × feeRate × p × (1-p)，不采用 SDK 预算辅助函数的指数。
            let amount = fill
                .shares
                .checked_mul(rate)
                .and_then(|amount| amount.checked_mul(fill.price))
                .and_then(|amount| amount.checked_mul(Decimal::ONE - fill.price))
                .ok_or_else(|| Error::msg("fee calculation outside decimal range"))?;
            // 用户批准的估算政策：当前费率适用该成交、pUSD=1USD、逐笔五位四舍五入。
            return Ok(Some((
                amount.round_dp_with_strategy(5, RoundingStrategy::MidpointAwayFromZero),
                "calculated",
            )));
        }
    }
    // 仅有未验证语义的 REST fee_rate_bps 时仍不能冒充可靠费用快照。
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn d(value: &str) -> Decimal {
        value.parse().unwrap()
    }

    fn fill(id: &str, shares: &str, finality: FillFinality) -> TradeFill {
        TradeFill {
            trade_id: id.into(),
            order_id: Some("777".into()),
            order_ids: vec!["777".into()],
            coin: Some("token".into()),
            shares: d(shares),
            price: d("0.5"),
            fee: Some(d("0.01")),
            fee_token: Some("USDC".into()),
            fee_rate_bps: None,
            finality,
            raw: json!({}),
        }
    }

    fn pm_evidence() -> FillEvidence {
        FillEvidence {
            poll: OrderPoll {
                found: true,
                status: "matched".into(),
                order_id: Some("777".into()),
                shares: Some(d("10")),
                associated_trades: vec!["one".into(), "two".into()],
                raw: json!({"associate_trades":["one","two"]}),
                ..Default::default()
            },
            page_complete: true,
            history_complete: true,
            expected_shares: Some(d("10")),
        }
    }

    #[test]
    fn pm_requires_complete_final_trade_set_not_ack_or_order_quantity() {
        let evidence = pm_evidence();
        assert!(matches!(
            resolve_leg(POLYMARKET, &[], &evidence).unwrap(),
            LegResolution::Pending(_)
        ));
        let one = fill("one", "6", FillFinality::Confirmed);
        let mut two = fill("two", "4", FillFinality::Pending);
        assert_eq!(
            resolve_leg(POLYMARKET, &[one.clone(), two.clone()], &evidence).unwrap(),
            LegResolution::Pending("trade_confirmation_pending")
        );
        two.finality = FillFinality::Failed;
        let resolution =
            resolve_leg(POLYMARKET, &[one.clone(), two.clone(), one], &evidence).unwrap();
        assert!(
            matches!(resolution, LegResolution::Terminal {status:"matched", shares, fee, ..}
            if shares == d("6") && fee == d("0.01"))
        );
        let mut incomplete = evidence.clone();
        incomplete.page_complete = false;
        assert_eq!(
            resolve_leg(POLYMARKET, &[two], &incomplete).unwrap(),
            LegResolution::Pending("trade_pages_incomplete")
        );
    }

    #[test]
    fn pm_all_failed_zero_requires_nonempty_complete_evidence() {
        let evidence = pm_evidence();
        assert!(matches!(resolve_leg(POLYMARKET, &[
            fill("one","6",FillFinality::Failed),fill("two","4",FillFinality::Failed)
        ], &evidence).unwrap(), LegResolution::Terminal{status:"failed",shares,..} if shares.is_zero()));
        let mut empty = evidence;
        empty.poll.associated_trades.clear();
        empty.poll.raw = json!({"associate_trades":[]});
        empty.poll.shares = Some(Decimal::ZERO);
        assert_eq!(
            resolve_leg(POLYMARKET, &[], &empty).unwrap(),
            LegResolution::Pending("zero_fill_not_proven")
        );
        empty.poll.status = "cancelled".into();
        assert!(matches!(
            resolve_leg(POLYMARKET, &[], &empty).unwrap(),
            LegResolution::Terminal {
                status: "cancelled",
                ..
            }
        ));
        empty.poll.raw = json!({"associate_trades":[null]});
        assert_eq!(
            resolve_leg(POLYMARKET, &[], &empty).unwrap(),
            LegResolution::Pending("associated_trades_missing")
        );
    }

    #[test]
    fn outcome_does_not_use_remaining_size_or_incomplete_history_as_zero_fill() {
        let mut evidence = FillEvidence {
            poll: OrderPoll {
                found: true,
                status: "canceled".into(),
                order_id: Some("777".into()),
                original_shares: Some(d("10")),
                remaining_shares: Some(d("4")),
                ..Default::default()
            },
            page_complete: true,
            history_complete: false,
            expected_shares: None,
        };
        assert_eq!(
            resolve_leg(OUTCOME, &[], &evidence).unwrap(),
            LegResolution::Pending("fill_history_incomplete")
        );
        let one = fill("one", "6", FillFinality::Confirmed);
        assert_eq!(
            resolve_leg(OUTCOME, &[one.clone()], &evidence).unwrap(),
            LegResolution::Pending("fill_history_incomplete")
        );
        evidence.history_complete = true;
        assert!(
            matches!(resolve_leg(OUTCOME,&[one.clone()],&evidence).unwrap(),LegResolution::Terminal{shares,..} if shares==d("6"))
        );
        evidence.history_complete = false;
        evidence.expected_shares = Some(d("6"));
        assert!(
            matches!(resolve_leg(OUTCOME,&[one],&evidence).unwrap(),LegResolution::Terminal{shares,..} if shares==d("6"))
        );
        evidence.poll.found = false;
        assert_eq!(
            resolve_leg(OUTCOME, &[], &evidence).unwrap(),
            LegResolution::Pending("order_not_found")
        );
    }

    #[test]
    fn actual_zero_is_not_missing_fee_and_unknown_currency_waits() {
        let mut trade = fill("one", "100", FillFinality::Confirmed);
        trade.fee = Some(Decimal::ZERO);
        assert_eq!(
            accounting_fee(OUTCOME, &trade).unwrap(),
            Some((Decimal::ZERO, "actual"))
        );
        trade.fee = None;
        assert_eq!(accounting_fee(OUTCOME, &trade).unwrap(), None);
        trade.fee = Some(d("0.01"));
        trade.fee_token = Some("OTHER".into());
        assert_eq!(accounting_fee(OUTCOME, &trade).unwrap(), None);
    }

    #[test]
    fn pm_unverified_taker_rate_does_not_become_actual_fee() {
        let mut trade = fill("one", "100", FillFinality::Confirmed);
        trade.fee = None;
        trade.fee_token = None;
        trade.fee_rate_bps = Some(d("700"));
        trade.raw = json!({"role":"taker"});
        assert_eq!(accounting_fee(POLYMARKET, &trade).unwrap(), None);
        trade.raw = json!({"role":"maker"});
        assert_eq!(
            accounting_fee(POLYMARKET, &trade).unwrap(),
            Some((Decimal::ZERO, "calculated_maker_zero"))
        );
    }

    #[test]
    fn approved_market_snapshot_calculates_without_strategy_multiplier() {
        let mut trade = fill("one", "100", FillFinality::Confirmed);
        trade.fee = None;
        trade.fee_token = None;
        trade.raw = json!({"role":"taker","fee_calculation":{
            "source":"clob-markets","rate":"0.07",
            "condition_id":"condition","observed_at_ms":1,"currency":"pUSD",
            "valuation":"1 USD","rounding":"midpoint_away_from_zero_5dp"
        }});
        assert_eq!(
            accounting_fee(POLYMARKET, &trade).unwrap(),
            Some((d("1.75"), "calculated"))
        );
        trade.raw["fee_calculation"]["rate"] = json!("0.05");
        assert_eq!(
            accounting_fee(POLYMARKET, &trade).unwrap(),
            Some((d("1.25"), "calculated"))
        );
        trade.raw["fee_calculation"]["rate"] = json!("0.0000006");
        assert_eq!(
            accounting_fee(POLYMARKET, &trade).unwrap(),
            Some((d("0.00002"), "calculated")),
            "round half away from zero at five places"
        );
        let previous = trade.clone();
        trade.raw["fee_calculation"]["rate"] = json!("0.01");
        assert_eq!(
            merge_observation(&previous, &trade).unwrap().raw["fee_calculation"],
            previous.raw["fee_calculation"]
        );
        trade.fee = Some(d("0.3"));
        trade.fee_token = Some("pUSD".into());
        assert_eq!(
            accounting_fee(POLYMARKET, &trade).unwrap(),
            Some((d("0.3"), "actual"))
        );
        trade.fee = None;
        trade.raw["fee_calculation"]["rate"] = json!("-1");
        assert!(accounting_fee(POLYMARKET, &trade).is_err());
    }

    #[test]
    fn confirmed_observation_does_not_regress_or_silently_change() {
        let confirmed = fill("one", "5", FillFinality::Confirmed);
        let mut incoming = confirmed.clone();
        incoming.finality = FillFinality::Pending;
        assert_eq!(
            merge_observation(&confirmed, &incoming).unwrap().finality,
            FillFinality::Confirmed
        );
        incoming.finality = FillFinality::Failed;
        assert!(merge_observation(&confirmed, &incoming).is_err());
        incoming.finality = FillFinality::Confirmed;
        incoming.shares = d("4");
        assert!(merge_observation(&confirmed, &incoming).is_err());
    }
}

pub fn cancellation_status(status: &str) -> bool {
    let normalized = status.to_ascii_lowercase().replace(['_', '-', ' '], "");
    matches!(
        normalized.as_str(),
        "cancelled" | "canceled" | "expired" | "unmatched" | "rejected"
    ) || normalized.ends_with("canceled")
        || normalized.ends_with("cancelled")
        || normalized.ends_with("rejected")
}
