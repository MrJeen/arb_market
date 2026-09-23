use crate::config::{OUTCOME, POLYMARKET};
use crate::error::{Error, Result};
use crate::platforms::{FillFinality, OrderPoll, TradeFill};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillEvidence {
    pub poll: OrderPoll,
    pub page_complete: bool,
    pub history_complete: bool,
    pub expected_shares: Option<Decimal>,
    // 覆盖授权只存在于刚完成 HTTP 验证的调用链，持久化重载不能恢复它。
    #[serde(skip)]
    pub outcome_scan: Option<crate::platforms::outcome::FillProgress>,
    #[serde(default)]
    pub pm_scan: Option<PmTradeScan>,
    #[serde(default)]
    pub pm_order_constraints: Option<PmOrderConstraints>,
}

pub(crate) fn validate_outcome_evidence(
    leg: &crate::store::LegRow,
    evidence: &mut FillEvidence,
    progress: &Value,
) -> Result<()> {
    let info = leg.last_order_info.as_ref();
    evidence.expected_shares = info
        .and_then(|info| info.pointer("/submission/expected_shares"))
        .and_then(crate::platforms::parse_decimal);
    let Some(scan) = evidence.outcome_scan.as_ref() else {
        // v1 / 老调用者仍可提交真实成交并走数量捷径，但持久 bool 不授予覆盖证明。
        evidence.history_complete = false;
        return Ok(());
    };
    let persisted: crate::platforms::outcome::FillProgress =
        serde_json::from_value(progress.clone())?;
    let submitted = leg
        .submitted_at
        .map(|time| time.timestamp_millis().saturating_sub(30_000).max(0) as u64);
    let observed = info
        .and_then(|info| info.get("outcome_terminal_observed_at_ms"))
        .and_then(Value::as_u64);
    if &persisted != scan
        || scan.version != 2
        || scan.token_id != leg.token_id
        || submitted != Some(scan.submitted_at_ms)
        || !leg
            .wallet_address
            .as_deref()
            .is_some_and(|account| account.eq_ignore_ascii_case(&scan.account))
        || evidence.page_complete != scan.complete
        || evidence.history_complete != scan.history_complete
    {
        return Err(Error::msg(
            "outcome scan does not match persisted leg or page",
        ));
    }
    if evidence.history_complete {
        let follows_probe = info
            .and_then(|info| info.get("fill_progress"))
            .is_some_and(|previous| scan.follows_final_probe(previous));
        if !scan.has_coverage() || scan.terminal_observed_at_ms != observed || !follows_probe {
            evidence.history_complete = false;
            evidence.outcome_scan = None;
        }
    }
    Ok(())
}

/// 已查到的成交事实只能增加；缺单响应不能抹掉尚未取齐的关联成交。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PmOrderConstraints {
    pub version: u8,
    pub order_id: String,
    pub asset_id: String,
    pub funder: String,
    pub associated_trade_ids: BTreeSet<String>,
    pub matched_shares_lower_bound: Option<Decimal>,
}

impl PmOrderConstraints {
    pub fn has_execution(&self) -> bool {
        !self.associated_trade_ids.is_empty()
            || self
                .matched_shares_lower_bound
                .is_some_and(|qty| qty > Decimal::ZERO)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.order_id.trim().is_empty()
            || self.asset_id.trim().is_empty()
            || self.funder.trim().is_empty()
            || self
                .associated_trade_ids
                .iter()
                .any(|id| id.trim().is_empty() || id.starts_with("ack:"))
            || self
                .matched_shares_lower_bound
                .is_some_and(|qty| qty < Decimal::ZERO)
        {
            return Err(Error::msg("invalid polymarket order constraints"));
        }
        Ok(())
    }

    fn absorb_poll(&mut self, poll: &OrderPoll) -> Result<()> {
        if !poll.found || poll.order_id.as_deref() != Some(self.order_id.as_str()) {
            return Ok(());
        }
        if poll
            .coin
            .as_deref()
            .is_some_and(|coin| coin != self.asset_id)
        {
            return Err(Error::msg("polymarket order constraint asset mismatch"));
        }
        if let Some(ids) = poll.raw.get("associate_trades").and_then(Value::as_array) {
            // 两类字段分别吸收；畸形关联列表不能削弱此前的合法约束。
            if ids.iter().all(|id| {
                id.as_str()
                    .is_some_and(|id| !id.trim().is_empty() && !id.starts_with("ack:"))
            }) {
                self.associated_trade_ids
                    .extend(ids.iter().filter_map(Value::as_str).map(str::to_owned));
            }
        }
        if let Some(qty) = poll.shares.filter(|qty| *qty >= Decimal::ZERO) {
            self.matched_shares_lower_bound = Some(
                self.matched_shares_lower_bound
                    .map_or(qty, |old| old.max(qty)),
            );
        }
        Ok(())
    }
}

/// 必须基于锁内快照调用；旧 JSON 中仍可验证的同单证据在覆盖前恢复。
pub fn pm_order_constraints(
    info: Option<&Value>,
    order_id: &str,
    asset_id: &str,
    funder: &str,
    poll: Option<&OrderPoll>,
) -> Result<PmOrderConstraints> {
    let mut constraints = PmOrderConstraints {
        version: 1,
        order_id: order_id.into(),
        asset_id: asset_id.into(),
        funder: funder.to_ascii_lowercase(),
        associated_trade_ids: BTreeSet::new(),
        matched_shares_lower_bound: None,
    };
    constraints.validate()?;
    if let Some(info) = info {
        for path in [
            "/pm_order_constraints",
            "/fill_evidence/pm_order_constraints",
        ] {
            if let Some(value) = info.pointer(path).filter(|value| !value.is_null()) {
                let previous: PmOrderConstraints = serde_json::from_value(value.clone())?;
                previous.validate()?;
                if previous.order_id != order_id
                    || previous.asset_id != asset_id
                    || !previous.funder.eq_ignore_ascii_case(funder)
                {
                    return Err(Error::msg("polymarket order constraint identity mismatch"));
                }
                constraints
                    .associated_trade_ids
                    .extend(previous.associated_trade_ids);
                if let Some(qty) = previous.matched_shares_lower_bound {
                    constraints.matched_shares_lower_bound = Some(
                        constraints
                            .matched_shares_lower_bound
                            .map_or(qty, |old| old.max(qty)),
                    );
                }
            }
        }
        for path in ["/order_poll", "/fill_evidence/poll"] {
            if let Some(value) = info.pointer(path).filter(|value| !value.is_null()) {
                let previous: OrderPoll = serde_json::from_value(value.clone())?;
                constraints.absorb_poll(&previous)?;
            }
        }
    }
    if let Some(poll) = poll {
        constraints.absorb_poll(poll)?;
    }
    Ok(constraints)
}

/// 本次固定窗口扫描的证据；旧 fills 不能替代本轮实际查到的成交集合。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PmTradeScan {
    pub version: u8,
    pub funder: String,
    pub asset_id: String,
    pub order_id: String,
    pub after: i64,
    pub before: i64,
    pub next_cursor: String,
    pub trade_ids: Vec<String>,
}

impl PmTradeScan {
    pub fn validate(&self) -> Result<()> {
        let ids: HashSet<_> = self.trade_ids.iter().collect();
        if self.version != 2
            || self.after < 0
            || self.before.checked_sub(self.after) != Some(300)
            || self.funder.is_empty()
            || self.asset_id.is_empty()
            || self.order_id.is_empty()
            || self.next_cursor.is_empty()
            || ids.len() != self.trade_ids.len()
            || self
                .trade_ids
                .iter()
                .any(|id| id.is_empty() || id.starts_with("ack:"))
        {
            return Err(Error::msg("invalid polymarket trade scan evidence"));
        }
        Ok(())
    }
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
            restore_pm_fee_rate(&mut merged)?;
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
    let missing_pm_order = platform == POLYMARKET
        && !poll.found
        && poll.status == "not_found"
        && matches!(
            poll.raw.get("lookup_missing").and_then(|v| v.as_str()),
            Some("http_404" | "null_body")
        );
    if poll.order_id.is_none() || (!poll.found && !missing_pm_order) {
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
    let state = if platform == POLYMARKET {
        crate::platforms::polymarket::normalized_order_status(&poll.status)
            .unwrap_or("unknown")
            .to_string()
    } else {
        poll.status.to_ascii_lowercase()
    };
    let cancelled = if platform == POLYMARKET {
        state == "cancelled"
    } else {
        cancellation_status(&state)
    };
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
    if missing_pm_order {
        let Some(scan) = &evidence.pm_scan else {
            return Ok(LegResolution::Pending("trade_scan_evidence_missing"));
        };
        scan.validate()?;
        if Some(scan.order_id.as_str()) != poll.order_id.as_deref()
            || trades
                .values()
                .any(|fill| fill.coin.as_deref() != Some(scan.asset_id.as_str()))
        {
            return Err(Error::msg("polymarket scan identity mismatch"));
        }
        if !evidence.page_complete || !evidence.history_complete || scan.next_cursor != "LTE=" {
            return Ok(LegResolution::Pending("trade_pages_incomplete"));
        }
        let scanned: HashSet<_> = scan.trade_ids.iter().map(String::as_str).collect();
        if scanned.len() != trades.len() || trades.keys().any(|id| !scanned.contains(id.as_str())) {
            return Ok(LegResolution::Pending("trade_scan_snapshot_mismatch"));
        }
        // 缺单时采用当轮完整扫描集合；空集合仍须通过下方历史成交约束，才能按零成交失败收口。
    } else if platform == POLYMARKET {
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
        } else if !evidence.page_complete
            || !evidence.history_complete
            || !evidence
                .outcome_scan
                .as_ref()
                .is_some_and(|scan| scan.has_coverage())
        {
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
    if platform == POLYMARKET {
        if let Some(known) = &evidence.pm_order_constraints {
            known.validate()?;
            if poll.order_id.as_deref() != Some(known.order_id.as_str())
                || trades
                    .values()
                    .any(|fill| fill.coin.as_deref() != Some(known.asset_id.as_str()))
            {
                return Err(Error::msg(
                    "polymarket order constraints do not match trades",
                ));
            }
            if known
                .associated_trade_ids
                .iter()
                .any(|id| !trades.contains_key(id))
            {
                return Ok(LegResolution::Pending("known_associated_trade_missing"));
            }
            let observed: Decimal = trades.values().map(|fill| fill.shares).sum();
            if known
                .matched_shares_lower_bound
                .is_some_and(|lower| observed < lower)
            {
                return Ok(LegResolution::Pending("known_matched_quantity_incomplete"));
            }
        }
    }
    if shares.is_zero() {
        return Ok(LegResolution::Terminal {
            status: if (missing_pm_order && trades.is_empty())
                || trades
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

fn pm_snapshot_rate(fill: &TradeFill, snapshot: &Value) -> Result<Decimal> {
    let rate = snapshot
        .get("rate")
        .and_then(crate::platforms::parse_decimal)
        .ok_or_else(|| Error::msg("fee calculation snapshot missing rate"))?;
    if rate < Decimal::ZERO || rate > Decimal::ONE {
        return Err(Error::msg("invalid fee calculation snapshot rate"));
    }
    match snapshot.get("source").and_then(Value::as_str) {
        // 旧快照没有 version/bps/token_id，继续按原始已批准系数计算。
        Some("clob-markets") => {}
        Some("common" | "env") => {
            if snapshot.get("version").and_then(Value::as_u64) != Some(1)
                || snapshot
                    .get("bps")
                    .and_then(crate::platforms::parse_decimal)
                    != Some(rate * Decimal::from(10_000))
                || !snapshot
                    .get("token_id")
                    .and_then(Value::as_str)
                    .is_some_and(|token| {
                        !token.trim().is_empty() && Some(token) == fill.coin.as_deref()
                    })
                || !snapshot
                    .get("condition_id")
                    .and_then(Value::as_str)
                    .is_some_and(|condition| !condition.trim().is_empty())
            {
                return Err(Error::msg(
                    "invalid fee calculation snapshot identity or bps",
                ));
            }
        }
        _ => return Err(Error::msg("invalid fee calculation snapshot source")),
    }
    Ok(rate)
}

/// 只恢复规范化费率，不把估算费用写成接口实收 fee；无快照的历史值保持原状。
pub(crate) fn restore_pm_fee_rate(fill: &mut TradeFill) -> Result<()> {
    if let Some(snapshot) = fill.raw.get("fee_calculation") {
        fill.fee_rate_bps = Some(pm_snapshot_rate(fill, snapshot)? * Decimal::from(10_000));
    } else if fill.fee.is_none() && fill.raw.get("role").and_then(Value::as_str) == Some("maker") {
        fill.fee_rate_bps = Some(Decimal::ZERO);
    }
    Ok(())
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
            let rate = pm_snapshot_rate(fill, snapshot)?;
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
#[path = "../tests/unit/reconcile.rs"]
mod tests;

pub fn cancellation_status(status: &str) -> bool {
    let normalized = status.to_ascii_lowercase().replace(['_', '-', ' '], "");
    matches!(
        normalized.as_str(),
        "cancelled" | "canceled" | "expired" | "unmatched" | "rejected"
    ) || normalized.ends_with("canceled")
        || normalized.ends_with("cancelled")
        || normalized.ends_with("rejected")
}
