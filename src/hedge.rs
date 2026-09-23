use crate::book::{BookStore, Level};
use crate::calc::{
    align_hedge_price, below_venue_mins, estimate_taker_fee, floor_shares, FeeContext,
};
use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::{TokenRef, Topic};
use crate::error::{Error, Result};
use crate::platforms::polymarket::market_buy_base_units;
use crate::platforms::OrderSide;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HedgeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct HedgeAction {
    pub platform: String,
    pub token_id: String,
    pub label: String,
    pub side: HedgeSide,
    pub shares: Decimal,
    pub cap_price: Decimal,
    pub fee: Decimal,
    pub marginal_value: Decimal,
}

pub type Positions = HashMap<String, HashMap<String, Decimal>>;

/// PM 保持最终限价本金加已有估计费；Outcome 按最终 cap 计本金和 builder 买费。
pub(crate) fn hedge_buy_required(
    platform: &str,
    shares: Decimal,
    cap: Decimal,
    fee: Decimal,
    fees: &FeeContext,
) -> Result<Decimal> {
    if shares <= Decimal::ZERO
        || !shares.fract().is_zero()
        || cap <= Decimal::ZERO
        || cap >= Decimal::ONE
        || fee < Decimal::ZERO
        || fees.outcome_builder_rate < Decimal::ZERO
    {
        return Err(Error::msg("invalid rebalance buy funding inputs"));
    }
    let principal = match platform {
        POLYMARKET => {
            let (maker, _) = market_buy_base_units(shares, cap)?;
            // maker 是整分金额，先去掉基础单位尾零，避免无谓压缩 Decimal 值域。
            let cents = i128::try_from(maker / 10_000)
                .map_err(|_| Error::msg("rebalance buy funding outside decimal range"))?;
            Decimal::try_from_i128_with_scale(cents, 2)
                .map_err(|_| Error::msg("rebalance buy funding outside decimal range"))?
        }
        OUTCOME => shares
            .checked_mul(cap)
            .ok_or_else(|| Error::msg("rebalance buy funding outside decimal range"))?,
        _ => return Err(Error::msg("unsupported rebalance buy platform")),
    };
    let funding_fee = if platform == OUTCOME {
        principal
            .checked_mul(fees.outcome_builder_rate)
            .ok_or_else(|| Error::msg("rebalance buy funding outside decimal range"))?
    } else {
        fee
    };
    principal
        .checked_add(funding_fee)
        .ok_or_else(|| Error::msg("rebalance buy funding outside decimal range"))
}

struct Depth {
    avg: Decimal,
    worst: Decimal,
    worst_plus_two: Decimal,
    filled: Decimal,
}

struct Candidate {
    action: HedgeAction,
    required_usdc: Option<Decimal>,
}

struct CandidateGroup {
    excess: String,
    deficit: String,
    qty_needed: Decimal,
    candidates: Vec<Candidate>,
}

pub(crate) struct HedgeCandidates {
    groups: Vec<CandidateGroup>,
}

impl HedgeCandidates {
    /// 只查询已通过盘口、最小下单量和资金计算校验的买候选平台，稳定去重。
    pub(crate) fn buy_platforms(&self) -> Vec<String> {
        let mut platforms = Vec::new();
        for candidate in self.groups.iter().flat_map(|group| &group.candidates) {
            if candidate.required_usdc.is_some() && !platforms.contains(&candidate.action.platform)
            {
                platforms.push(candidate.action.platform.clone());
            }
        }
        platforms
    }

    pub(crate) fn select(self, balances: &HashMap<String, Decimal>) -> Vec<HedgeAction> {
        self.groups
            .into_iter()
            .filter_map(|group| {
                // 各组独立判定，不聚合或扣减余额；缺余额按 0 处理，卖候选始终保留。
                let best = group
                    .candidates
                    .into_iter()
                    .filter(|candidate| {
                        candidate.required_usdc.is_none_or(|required| {
                            balances
                                .get(&candidate.action.platform)
                                .copied()
                                .unwrap_or(Decimal::ZERO)
                                >= required
                        })
                    })
                    // 组内保持 sell → buy，max_by 同分取后者，即买入。
                    .max_by(|a, b| a.action.marginal_value.cmp(&b.action.marginal_value))?;
                tracing::info!(
                    excess = %group.excess,
                    deficit = %group.deficit,
                    qty_needed = %group.qty_needed,
                    chosen = %format!("{} {} {}", best.action.platform, match best.action.side {
                        HedgeSide::Buy => "BUY",
                        HedgeSide::Sell => "SELL",
                    }, best.action.label),
                    shares = %best.action.shares,
                    cap = %best.action.cap_price,
                    marginal_value = %best.action.marginal_value,
                    "hedge chose higher marginal value"
                );
                Some(best.action)
            })
            .collect()
    }
}

struct Imbalance<'a> {
    excess_platform: &'a str,
    excess_token: &'a TokenRef,
    deficit_platform: &'a str,
    deficit_token: &'a TokenRef,
    qty_needed: Decimal,
}

fn imbalance_for<'a>(
    topic: &'a Topic,
    pm_label: &str,
    out_label: &str,
    diff: Decimal,
    min_qty: Decimal,
) -> Option<Imbalance<'a>> {
    if diff.abs() <= min_qty {
        return None;
    }
    let qty_needed = floor_shares(diff.abs());
    if qty_needed <= Decimal::ZERO {
        return None;
    }
    let out_token = topic.token(OUTCOME, out_label)?;
    let pm_token = topic.token(POLYMARKET, pm_label)?;
    // diff>0: PM[pm_label] 多于 Outcome 互补腿，多余在 PM，缺失在 Outcome。
    if diff > Decimal::ZERO {
        Some(Imbalance {
            excess_platform: POLYMARKET,
            excess_token: pm_token,
            deficit_platform: OUTCOME,
            deficit_token: out_token,
            qty_needed,
        })
    } else {
        Some(Imbalance {
            excess_platform: OUTCOME,
            excess_token: out_token,
            deficit_platform: POLYMARKET,
            deficit_token: pm_token,
            qty_needed,
        })
    }
}

/// 可能下单的 token：每个超额的多余腿（卖）和跨平台缺失腿（买），去重。
/// 不含同平台对立腿。
pub fn hedge_order_tokens(
    topic: &Topic,
    positions: &Positions,
    min_qty: Decimal,
) -> Vec<(String, String)> {
    let labels = topic.labels();
    let Some((diff1, diff2)) = position_diffs(positions, &labels) else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut tokens = Vec::new();
    for (pm_label, out_label, diff) in [
        (labels[0].as_str(), labels[1].as_str(), diff1),
        (labels[1].as_str(), labels[0].as_str(), diff2),
    ] {
        let Some(imb) = imbalance_for(topic, pm_label, out_label, diff, min_qty) else {
            continue;
        };
        for token in [imb.excess_token, imb.deficit_token] {
            let key = (token.platform.clone(), token.token_id.clone());
            if seen.insert(key.clone()) {
                tokens.push(key);
            }
        }
    }
    tokens
}

pub fn position_diffs(positions: &Positions, labels: &[String]) -> Option<(Decimal, Decimal)> {
    if labels.len() != 2 {
        return None;
    }
    let empty = HashMap::new();
    let pm = positions.get(POLYMARKET).unwrap_or(&empty);
    let out = positions.get(OUTCOME).unwrap_or(&empty);
    let l1 = &labels[0];
    let l2 = &labels[1];
    let diff1 = *pm.get(l1).unwrap_or(&Decimal::ZERO) - *out.get(l2).unwrap_or(&Decimal::ZERO);
    let diff2 = *pm.get(l2).unwrap_or(&Decimal::ZERO) - *out.get(l1).unwrap_or(&Decimal::ZERO);
    Some((diff1, diff2))
}

/// `None` 仅表示标签不是二元市场，不能计算差额。缺平台视为 0 持仓。
pub fn needs_rebalance(positions: &Positions, labels: &[String], min_qty: Decimal) -> Option<bool> {
    let (d1, d2) = position_diffs(positions, labels)?;
    Some(d1.abs() > min_qty || d2.abs() > min_qty)
}

pub fn plan_hedge(
    topic: &Topic,
    positions: &Positions,
    books: &BookStore,
    balances: &HashMap<String, Decimal>,
    fees: &FeeContext,
    min_qty: Decimal,
    now: Instant,
    stale: Duration,
) -> Vec<HedgeAction> {
    hedge_candidates(topic, positions, books, fees, min_qty, now, stale).select(balances)
}

pub(crate) fn hedge_candidates(
    topic: &Topic,
    positions: &Positions,
    books: &BookStore,
    fees: &FeeContext,
    min_qty: Decimal,
    now: Instant,
    stale: Duration,
) -> HedgeCandidates {
    let mut groups = Vec::new();
    let labels = topic.labels();
    let Some((diff1, diff2)) = position_diffs(positions, &labels) else {
        return HedgeCandidates { groups };
    };
    for (pm_label, out_label, diff) in [
        (labels[0].as_str(), labels[1].as_str(), diff1),
        (labels[1].as_str(), labels[0].as_str(), diff2),
    ] {
        if let Some(group) = hedge_one(
            topic, pm_label, out_label, diff, books, fees, min_qty, now, stale,
        ) {
            groups.push(group);
        }
    }
    HedgeCandidates { groups }
}

fn hedge_one(
    topic: &Topic,
    pm_label: &str,
    out_label: &str,
    diff: Decimal,
    books: &BookStore,
    fees: &FeeContext,
    min_qty: Decimal,
    now: Instant,
    stale: Duration,
) -> Option<CandidateGroup> {
    let imb = imbalance_for(topic, pm_label, out_label, diff, min_qty)?;
    let mut candidates = Vec::new();
    if let Some(sell) = eval_sell(
        imb.excess_platform,
        &imb.excess_token.token_id,
        &imb.excess_token.label,
        imb.qty_needed,
        books,
        fees,
        now,
        stale,
    ) {
        candidates.push(sell);
    }
    if let Some(buy) = eval_buy(
        imb.deficit_platform,
        &imb.deficit_token.token_id,
        &imb.deficit_token.label,
        imb.qty_needed,
        books,
        fees,
        now,
        stale,
    ) {
        candidates.push(buy);
    }
    Some(CandidateGroup {
        excess: format!("{}.{}", imb.excess_platform, imb.excess_token.label),
        deficit: format!("{}.{}", imb.deficit_platform, imb.deficit_token.label),
        qty_needed: imb.qty_needed,
        candidates,
    })
}

/// 差额超过股数门槛，但按现价两边都低于交易所最小名义/股数，无法再下对冲单。
/// 盘口缺失或过期时返回 false，避免把暂时看不到的深度误标完成。
pub fn leftover_untradeable(
    topic: &Topic,
    positions: &Positions,
    books: &BookStore,
    min_qty: Decimal,
    now: Instant,
    stale: Duration,
) -> bool {
    let labels = topic.labels();
    let Some((diff1, diff2)) = position_diffs(positions, &labels) else {
        return false;
    };
    let mut any = false;
    for (pm_label, out_label, diff) in [
        (labels[0].as_str(), labels[1].as_str(), diff1),
        (labels[1].as_str(), labels[0].as_str(), diff2),
    ] {
        if diff.abs() <= min_qty {
            continue;
        }
        let qty = floor_shares(diff.abs());
        if qty <= Decimal::ZERO {
            continue;
        }
        any = true;
        let Some(imb) = imbalance_for(topic, pm_label, out_label, diff, min_qty) else {
            return false;
        };
        match (
            side_below_venue_min(
                imb.excess_platform,
                &imb.excess_token.token_id,
                false,
                qty,
                books,
                now,
                stale,
            ),
            side_below_venue_min(
                imb.deficit_platform,
                &imb.deficit_token.token_id,
                true,
                qty,
                books,
                now,
                stale,
            ),
        ) {
            (Some(true), Some(true)) => {}
            _ => return false,
        }
    }
    any
}

fn eval_sell(
    platform: &str,
    token_id: &str,
    label: &str,
    qty_needed: Decimal,
    books: &BookStore,
    fees: &FeeContext,
    now: Instant,
    stale: Duration,
) -> Option<Candidate> {
    let depth = walk_book(platform, token_id, false, qty_needed, books, now, stale)?;
    let qty = floor_shares(depth.filled);
    if qty <= Decimal::ZERO {
        return None;
    }
    let fee = estimate_taker_fee(platform, OrderSide::Sell, qty, depth.avg, fees);
    let revenue = depth.avg * qty - fee;
    let trade_cost = depth.worst * qty;
    if below_venue_mins(platform, false, qty, trade_cost) {
        tracing::warn!(
            platform,
            %qty,
            %trade_cost,
            "hedge sell below venue min, skip"
        );
        return None;
    }
    Some(Candidate {
        action: HedgeAction {
            platform: platform.into(),
            token_id: token_id.into(),
            label: label.into(),
            side: HedgeSide::Sell,
            shares: qty,
            cap_price: align_hedge_price(
                platform,
                false,
                depth.worst,
                polymarket_tick(books, platform, token_id),
            )?,
            fee,
            marginal_value: revenue,
        },
        required_usdc: None,
    })
}

fn eval_buy(
    platform: &str,
    token_id: &str,
    label: &str,
    qty_needed: Decimal,
    books: &BookStore,
    fees: &FeeContext,
    now: Instant,
    stale: Duration,
) -> Option<Candidate> {
    let depth = walk_book(platform, token_id, true, qty_needed, books, now, stale)?;
    let qty = floor_shares(depth.filled);
    if qty <= Decimal::ZERO {
        return None;
    }
    let fee = estimate_taker_fee(platform, OrderSide::Buy, qty, depth.avg, fees);
    let cost = depth.avg * qty + fee;
    let trade_cost = depth.worst * qty;
    if below_venue_mins(platform, true, qty, trade_cost) {
        return None;
    }
    // 买入 cap 取最差价与再后两档的较大值，给 IOC/FAK 留出行走空间。
    let cap = align_hedge_price(
        platform,
        true,
        depth.worst.max(depth.worst_plus_two),
        polymarket_tick(books, platform, token_id),
    )?;
    let required = hedge_buy_required(platform, qty, cap, fee, fees).ok()?;
    Some(Candidate {
        action: HedgeAction {
            platform: platform.into(),
            token_id: token_id.into(),
            label: label.into(),
            side: HedgeSide::Buy,
            shares: qty,
            cap_price: cap,
            fee,
            // 无论补 PM 还是 Outcome，新增配对部分都承担 Outcome 结算准备。
            marginal_value: qty * (Decimal::ONE - fees.outcome_taker_rate) - cost,
        },
        required_usdc: Some(required),
    })
}

/// 有新鲜盘口时，判断按现有深度下单是否仍低于交易所最小股数/名义金额。
/// 盘口缺失或过期返回 `None`，避免把暂时看不到的深度误标完成。
fn side_below_venue_min(
    platform: &str,
    token_id: &str,
    buy: bool,
    qty: Decimal,
    books: &BookStore,
    now: Instant,
    stale: Duration,
) -> Option<bool> {
    let depth = walk_book(platform, token_id, buy, qty, books, now, stale)?;
    let filled = floor_shares(depth.filled);
    if filled <= Decimal::ZERO {
        return Some(true);
    }
    let walk_notional = depth.worst * filled;
    if below_venue_mins(platform, buy, filled, walk_notional) {
        return Some(true);
    }
    let raw_cap = if buy {
        depth.worst.max(depth.worst_plus_two)
    } else {
        depth.worst
    };
    let cap = align_hedge_price(
        platform,
        buy,
        raw_cap,
        polymarket_tick(books, platform, token_id),
    )?;
    Some(below_venue_mins(platform, buy, filled, cap * filled))
}

fn walk_book(
    platform: &str,
    token_id: &str,
    buy: bool,
    need_qty: Decimal,
    books: &BookStore,
    now: Instant,
    stale: Duration,
) -> Option<Depth> {
    let book = books.get(platform, token_id)?;
    if !book.is_fresh(stale, now) {
        return None;
    }
    let levels = if buy { &book.asks } else { &book.bids };
    walk_levels(levels, need_qty)
}

fn polymarket_tick(books: &BookStore, platform: &str, token_id: &str) -> Option<Decimal> {
    if platform != POLYMARKET {
        return None;
    }
    books
        .get(platform, token_id)
        .and_then(|book| book.tick_size)
}

fn walk_levels(levels: &[Level], need_qty: Decimal) -> Option<Depth> {
    let mut filled = Decimal::ZERO;
    let mut total = Decimal::ZERO;
    let mut worst = Decimal::ZERO;
    let mut worst_idx = 0usize;
    for (idx, level) in levels.iter().enumerate() {
        if level.price <= Decimal::ZERO || level.size <= Decimal::ZERO {
            continue;
        }
        let take = (need_qty - filled).min(level.size);
        filled += take;
        total += take * level.price;
        worst = level.price;
        worst_idx = idx;
        if filled >= need_qty {
            break;
        }
    }
    if filled <= Decimal::ZERO {
        return None;
    }
    let next_idx = (worst_idx + 2).min(levels.len().saturating_sub(1));
    let worst_plus_two = levels
        .get(next_idx)
        .map(|level| level.price)
        .unwrap_or(worst);
    Some(Depth {
        avg: total / filled,
        worst,
        worst_plus_two,
        filled,
    })
}

#[cfg(test)]
#[path = "../tests/unit/hedge.rs"]
mod tests;
