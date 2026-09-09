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
        if scan.trade_ids.is_empty() {
            return Ok(LegResolution::Pending("zero_fill_not_proven"));
        }
        let scanned: HashSet<_> = scan.trade_ids.iter().map(String::as_str).collect();
        if scanned.len() != trades.len() || trades.keys().any(|id| !scanned.contains(id.as_str())) {
            return Ok(LegResolution::Pending("trade_scan_snapshot_mismatch"));
        }
        // FAK 缺单时采用当轮可见的非空终态集合，不虚构 order 撮合量或关联列表。
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
            pm_scan: None,
            pm_order_constraints: None,
            outcome_scan: None,
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

    fn missing_pm_evidence(source: &str, ids: &[&str]) -> FillEvidence {
        let mut evidence = pm_evidence();
        evidence.poll = OrderPoll {
            status: "not_found".into(),
            order_id: Some("777".into()),
            raw: json!({"lookup_missing":source}),
            ..Default::default()
        };
        evidence.pm_scan = Some(PmTradeScan {
            version: 2,
            funder: "funder".into(),
            asset_id: "token".into(),
            order_id: "777".into(),
            after: 1_700_000_000,
            before: 1_700_000_300,
            next_cursor: "LTE=".into(),
            trade_ids: ids.iter().map(|id| (*id).into()).collect(),
        });
        evidence
    }

    #[test]
    fn missing_order_retains_previously_known_trade_constraints() {
        let known = pm_evidence().poll;
        let constraints =
            pm_order_constraints(None, "777", "token", "funder", Some(&known)).unwrap();
        let mut evidence = missing_pm_evidence("null_body", &["one"]);
        evidence.pm_order_constraints = Some(constraints);
        let one = fill("one", "6", FillFinality::Confirmed);
        assert_eq!(
            resolve_leg(POLYMARKET, &[one.clone()], &evidence).unwrap(),
            LegResolution::Pending("known_associated_trade_missing")
        );
        evidence
            .pm_scan
            .as_mut()
            .unwrap()
            .trade_ids
            .push("two".into());
        let two = fill("two", "4", FillFinality::Failed);
        assert!(
            matches!(resolve_leg(POLYMARKET, &[one, two], &evidence).unwrap(),
            LegResolution::Terminal { status: "matched", shares, .. } if shares == d("6"))
        );
    }

    #[test]
    fn order_constraints_restore_and_only_accumulate_reliable_fields() {
        let known = pm_evidence().poll;
        let mut partial = known.clone();
        partial.shares = Some(d("6"));
        partial.raw = json!({"associate_trades":["one"]});
        let info = json!({"order_poll": known, "fill_evidence":{"poll":partial}});
        let first = pm_order_constraints(Some(&info), "777", "token", "FUNDER", None).unwrap();
        assert_eq!(
            first.associated_trade_ids,
            BTreeSet::from(["one".into(), "two".into()])
        );
        assert_eq!(first.matched_shares_lower_bound, Some(d("10")));
        let mut weaker = partial.clone();
        weaker.raw = json!({"associate_trades":["three",null]});
        weaker.shares = Some(d("-1"));
        let stored = json!({"pm_order_constraints":first});
        assert_eq!(
            pm_order_constraints(Some(&stored), "777", "token", "funder", Some(&weaker)).unwrap(),
            first
        );
        weaker.raw = json!({"associate_trades":["three"]});
        weaker.shares = None;
        let grown =
            pm_order_constraints(Some(&stored), "777", "token", "funder", Some(&weaker)).unwrap();
        assert!(grown.associated_trade_ids.contains("three"));
        assert_eq!(grown.matched_shares_lower_bound, Some(d("10")));
        weaker.order_id = Some("other".into());
        assert_eq!(
            pm_order_constraints(Some(&stored), "777", "token", "funder", Some(&weaker)).unwrap(),
            first
        );
        for invalid in [json!({"version":99}), json!(false), json!({"version":1})] {
            assert!(pm_order_constraints(
                Some(&json!({"pm_order_constraints":invalid})),
                "777",
                "token",
                "funder",
                None
            )
            .is_err());
        }
        assert!(pm_order_constraints(Some(&stored), "other", "token", "funder", None).is_err());
        let empty = pm_order_constraints(
            Some(&json!({"pm_order_constraints":null})),
            "777",
            "token",
            "funder",
            None,
        )
        .unwrap();
        assert!(!empty.has_execution());
        let mut legacy: Value = serde_json::to_value(pm_evidence()).unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("pm_order_constraints");
        assert!(serde_json::from_value::<FillEvidence>(legacy)
            .unwrap()
            .pm_order_constraints
            .is_none());
    }

    #[test]
    fn known_matched_lower_bound_is_not_ack_or_original_quantity() {
        let mut known = pm_evidence().poll;
        known.raw = json!({});
        known.status = "live".into();
        let mut evidence = missing_pm_evidence("http_404", &["one"]);
        evidence.pm_order_constraints =
            Some(pm_order_constraints(None, "777", "token", "funder", Some(&known)).unwrap());
        let one = fill("one", "6", FillFinality::Confirmed);
        assert_eq!(
            resolve_leg(POLYMARKET, &[one.clone()], &evidence).unwrap(),
            LegResolution::Pending("known_matched_quantity_incomplete")
        );
        evidence
            .pm_scan
            .as_mut()
            .unwrap()
            .trade_ids
            .push("two".into());
        assert!(
            matches!(resolve_leg(POLYMARKET,&[one.clone(),fill("two","4",FillFinality::Failed)],&evidence).unwrap(),LegResolution::Terminal{shares,..} if shares==d("6"))
        );
        evidence.pm_scan.as_mut().unwrap().trade_ids.pop();
        evidence.pm_order_constraints = None;
        evidence.expected_shares = Some(d("100"));
        evidence.poll.original_shares = Some(d("100"));
        assert!(
            matches!(resolve_leg(POLYMARKET,&[one],&evidence).unwrap(),LegResolution::Terminal{shares,..} if shares==d("6"))
        );
    }

    #[test]
    fn pm_order_terminal_whitelist_does_not_change_outcome_cancellation() {
        for (status, terminal) in [
            ("ORDER_STATUS_MATCHED", true),
            ("ORDER_STATUS_CANCELED_MARKET_RESOLVED", true),
            ("FUTURE_CANCELED", false),
            ("ORDER_STATUS_INVALID", false),
        ] {
            let mut evidence = pm_evidence();
            evidence.poll.status = status.into();
            let result = resolve_leg(
                POLYMARKET,
                &[
                    fill("one", "6", FillFinality::Confirmed),
                    fill("two", "4", FillFinality::Confirmed),
                ],
                &evidence,
            )
            .unwrap();
            assert_eq!(
                matches!(result, LegResolution::Terminal { .. }),
                terminal,
                "{status}"
            );
        }
        assert!(cancellation_status("futureCanceled"));
    }

    #[test]
    fn missing_pm_order_finalizes_only_confirmed_quantity_or_all_failed() {
        for source in ["http_404", "null_body"] {
            for (states, expected_status, expected_shares, expected_fee) in [
                (
                    [FillFinality::Confirmed, FillFinality::Confirmed],
                    "matched",
                    "10",
                    "0.02",
                ),
                (
                    [FillFinality::Confirmed, FillFinality::Failed],
                    "matched",
                    "6",
                    "0.01",
                ),
                (
                    [FillFinality::Failed, FillFinality::Failed],
                    "failed",
                    "0",
                    "0",
                ),
            ] {
                let evidence = missing_pm_evidence(source, &["one", "two"]);
                let one = fill("one", "6", states[0]);
                let two = fill("two", "4", states[1]);
                let resolution =
                    resolve_leg(POLYMARKET, &[one.clone(), two, one], &evidence).unwrap();
                assert!(
                    matches!(resolution, LegResolution::Terminal { status, shares, fee, price, .. }
                    if status == expected_status && shares == d(expected_shares)
                        && fee == d(expected_fee) && price == if shares.is_zero() { Decimal::ZERO } else { d("0.5") })
                );
            }
        }
    }

    #[test]
    fn missing_pm_order_waits_for_nonempty_current_scan_and_finality() {
        let evidence = missing_pm_evidence("http_404", &["one"]);
        let confirmed = fill("one", "6", FillFinality::Confirmed);
        let mut incomplete = evidence.clone();
        incomplete.page_complete = false;
        incomplete.history_complete = false;
        incomplete.pm_scan.as_mut().unwrap().next_cursor = "next".into();
        assert_eq!(
            resolve_leg(POLYMARKET, &[confirmed.clone()], &incomplete).unwrap(),
            LegResolution::Pending("trade_pages_incomplete")
        );
        assert_eq!(
            resolve_leg(
                POLYMARKET,
                &[fill("one", "6", FillFinality::Pending)],
                &evidence
            )
            .unwrap(),
            LegResolution::Pending("trade_confirmation_pending")
        );
        let empty = missing_pm_evidence("null_body", &[]);
        for observations in [vec![], vec![confirmed.clone()]] {
            assert_eq!(
                resolve_leg(POLYMARKET, &observations, &empty).unwrap(),
                LegResolution::Pending("zero_fill_not_proven")
            );
        }
        assert_eq!(
            resolve_leg(POLYMARKET, &[], &evidence).unwrap(),
            LegResolution::Pending("trade_scan_snapshot_mismatch")
        );
        let old_fill = fill("old", "1", FillFinality::Confirmed);
        assert_eq!(
            resolve_leg(POLYMARKET, &[confirmed.clone(), old_fill], &evidence).unwrap(),
            LegResolution::Pending("trade_scan_snapshot_mismatch")
        );
        let mut no_fee = confirmed;
        no_fee.fee = None;
        assert_eq!(
            resolve_leg(POLYMARKET, &[no_fee], &evidence).unwrap(),
            LegResolution::Pending("fee_evidence_missing")
        );
    }

    #[test]
    fn missing_pm_order_rejects_wrong_identity_or_unproven_missing_response() {
        let evidence = missing_pm_evidence("http_404", &["one"]);
        let confirmed = fill("one", "6", FillFinality::Confirmed);
        for field in ["order", "token"] {
            let mut wrong = confirmed.clone();
            if field == "order" {
                wrong.order_id = Some("other".into());
            } else {
                wrong.coin = Some("other".into());
            }
            assert!(resolve_leg(POLYMARKET, &[wrong], &evidence).is_err());
        }
        let mut invalid = evidence.clone();
        invalid.pm_scan.as_mut().unwrap().before += 1;
        assert!(resolve_leg(POLYMARKET, &[confirmed.clone()], &invalid).is_err());
        let mut old = serde_json::to_value(&evidence).unwrap();
        old.as_object_mut().unwrap().remove("pm_scan");
        let old: FillEvidence = serde_json::from_value(old).unwrap();
        assert_eq!(
            resolve_leg(POLYMARKET, &[confirmed.clone()], &old).unwrap(),
            LegResolution::Pending("trade_scan_evidence_missing")
        );
        for source in ["http_500", "timeout", "malformed"] {
            let missing = missing_pm_evidence(source, &["one"]);
            assert_eq!(
                resolve_leg(POLYMARKET, &[confirmed.clone()], &missing).unwrap(),
                LegResolution::Pending("order_not_found")
            );
        }
        assert_eq!(
            resolve_leg(OUTCOME, &[confirmed], &evidence).unwrap(),
            LegResolution::Pending("order_not_found")
        );
    }

    #[test]
    fn found_pm_order_still_requires_order_state_associations_and_quantity() {
        let fills = [
            fill("one", "6", FillFinality::Confirmed),
            fill("two", "4", FillFinality::Failed),
        ];
        for (field, expected) in [
            ("state", "order_still_open"),
            ("associations", "associated_trades_missing"),
            ("missing", "associated_trade_missing"),
            ("extra", "order_trade_snapshot_mismatch"),
            ("quantity", "matched_quantity_incomplete"),
        ] {
            let mut evidence = pm_evidence();
            evidence.pm_scan = missing_pm_evidence("http_404", &["one", "two"]).pm_scan;
            match field {
                "state" => evidence.poll.status = "live".into(),
                "associations" => evidence.poll.raw = json!({}),
                "missing" => {
                    evidence.poll.associated_trades.push("three".into());
                    evidence.poll.raw = json!({"associate_trades":["one","two","three"]});
                }
                "extra" => {
                    evidence.poll.associated_trades = vec!["one".into()];
                    evidence.poll.raw = json!({"associate_trades":["one"]});
                }
                _ => evidence.poll.shares = Some(d("11")),
            }
            assert_eq!(
                resolve_leg(POLYMARKET, &fills, &evidence).unwrap(),
                LegResolution::Pending(expected)
            );
        }
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
            pm_scan: None,
            pm_order_constraints: None,
            outcome_scan: None,
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
        assert_eq!(
            resolve_leg(OUTCOME, &[one.clone()], &evidence).unwrap(),
            LegResolution::Pending("fill_history_incomplete")
        );
        evidence.outcome_scan = Some(
            serde_json::from_value(json!({
                "version":2,"tokenId":"#5160","submittedAtMs":100,"endTime":1000,
                "cursor":100,"seenIds":[],"historyChecked":true,"historyLowerBound":99,
                "historyComplete":true,"scannedCount":1,"complete":true,"phase":"complete",
                "account":"0xaccount","terminalObservedAtMs":999,"scanValid":true
            }))
            .unwrap(),
        );
        assert!(
            matches!(resolve_leg(OUTCOME,&[one.clone()],&evidence).unwrap(),LegResolution::Terminal{shares,..} if shares==d("6"))
        );
        let reloaded: FillEvidence =
            serde_json::from_value(serde_json::to_value(&evidence).unwrap()).unwrap();
        assert_eq!(
            resolve_leg(OUTCOME, &[one.clone()], &reloaded).unwrap(),
            LegResolution::Pending("fill_history_incomplete")
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
