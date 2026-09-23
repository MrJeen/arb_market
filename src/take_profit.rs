use crate::book::{BookStore, Level};
use crate::calc::{
    align_hedge_price, below_venue_mins, estimate_taker_fee, floor_shares, min_trade_amount,
    min_trade_cost, FeeContext,
};
use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::Topic;
use crate::hedge::Positions;
use crate::platforms::OrderSide;
use rust_decimal::Decimal;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TakeProfitAction {
    pub platform: String,
    pub token_id: String,
    pub label: String,
    pub shares: Decimal,
    pub cap_price: Decimal,
    pub fee: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TakeProfitPlan {
    pub actions: [TakeProfitAction; 2],
    pub shares: Decimal,
    pub gross_revenue: Decimal,
    pub total_fee: Decimal,
    pub gain: Decimal,
}

/// 选择收益最高的互补持仓方向，同时卖出 PM 标签与 Outcome 反向标签。
///
/// `min_gain` 是整笔计划的最低净收益；等于门槛时仍会返回计划。
pub fn plan_take_profit(
    topic: &Topic,
    positions: &Positions,
    books: &BookStore,
    fees: &FeeContext,
    min_gain: Decimal,
    now: Instant,
    stale: Duration,
) -> Option<TakeProfitPlan> {
    let labels = topic.labels();
    if labels.len() != 2
        || fees.polymarket_fee_rate < Decimal::ZERO
        || fees.outcome_taker_rate < Decimal::ZERO
        || fees.outcome_builder_rate < Decimal::ZERO
    {
        return None;
    }

    [
        (labels[0].as_str(), labels[1].as_str()),
        (labels[1].as_str(), labels[0].as_str()),
    ]
    .into_iter()
    .filter_map(|(pm_label, out_label)| {
        evaluate_pair(
            topic, positions, books, fees, min_gain, now, stale, pm_label, out_label,
        )
    })
    .max_by(|a, b| a.gain.cmp(&b.gain))
}

#[allow(clippy::too_many_arguments)]
fn evaluate_pair(
    topic: &Topic,
    positions: &Positions,
    books: &BookStore,
    fees: &FeeContext,
    min_gain: Decimal,
    now: Instant,
    stale: Duration,
    pm_label: &str,
    out_label: &str,
) -> Option<TakeProfitPlan> {
    let pm_token = topic.token(POLYMARKET, pm_label)?;
    let out_token = topic.token(OUTCOME, out_label)?;
    let pm_position = position(positions, POLYMARKET, pm_label);
    let out_position = position(positions, OUTCOME, out_label);
    if pm_position <= Decimal::ZERO || out_position <= Decimal::ZERO {
        return None;
    }

    let pm_book = books.get(POLYMARKET, &pm_token.token_id)?;
    let out_book = books.get(OUTCOME, &out_token.token_id)?;
    if !pm_book.is_fresh(stale, now) || !out_book.is_fresh(stale, now) {
        return None;
    }

    let limit = floor_shares(pm_position.min(out_position));
    if limit <= Decimal::ZERO {
        return None;
    }

    let valid = |level: &&Level| level.price > Decimal::ZERO && level.size > Decimal::ZERO;
    let mut pm_levels = pm_book.bids.iter().filter(valid);
    let mut out_levels = out_book.bids.iter().filter(valid);
    let mut pm_level = pm_levels.next()?;
    let mut out_level = out_levels.next()?;
    let mut pm_left = pm_level.size;
    let mut out_left = out_level.size;
    let mut lo = Decimal::ZERO;
    let mut pm_revenue = Decimal::ZERO;
    let mut out_revenue = Decimal::ZERO;
    let mut best: Option<TakeProfitPlan> = None;

    while lo < limit {
        let step = pm_left.min(out_left).min(limit - lo);
        let hi = lo + step;
        let pm_cap = align_hedge_price(POLYMARKET, false, pm_level.price, pm_book.tick_size)?;
        let out_cap = align_hedge_price(OUTCOME, false, out_level.price, None)?;
        // (lo, hi] 内最差价与 cap 固定；小数深度必须先累积，不能逐档取整。
        let mut first = (floor_shares(lo) + Decimal::ONE).max(Decimal::ONE);
        for (platform, cap) in [(POLYMARKET, pm_cap), (OUTCOME, out_cap)] {
            first = first
                .max(min_trade_amount(platform, false).ceil())
                .max((min_trade_cost(platform, false) / cap).ceil());
        }
        let last = floor_shares(hi);
        if first <= last {
            // 固定档位内 gain = A*q + B + C/q，非负 PM 费率保证 C >= 0。
            // 因而只需检查可行整数两端；入口拒绝负费率以维持这个前提。
            for qty in [first, last] {
                if below_venue_mins(POLYMARKET, false, qty, pm_cap * qty)
                    || below_venue_mins(OUTCOME, false, qty, out_cap * qty)
                {
                    continue;
                }
                let pm_gross = pm_revenue + (qty - lo) * pm_level.price;
                let out_gross = out_revenue + (qty - lo) * out_level.price;
                let pm_fee =
                    estimate_taker_fee(POLYMARKET, OrderSide::Sell, qty, pm_gross / qty, fees);
                let out_fee =
                    estimate_taker_fee(OUTCOME, OrderSide::Sell, qty, out_gross / qty, fees);
                let gross_revenue = pm_gross + out_gross;
                let total_fee = pm_fee + out_fee;
                // 仍与放弃的毛兑付 q 比较，不用未核实的结算费准备放宽止盈。
                let gain = gross_revenue - total_fee - qty;
                if gain < min_gain
                    || best.as_ref().is_some_and(|plan| {
                        gain < plan.gain || (gain == plan.gain && qty <= plan.shares)
                    })
                {
                    continue;
                }
                best = Some(TakeProfitPlan {
                    actions: [
                        TakeProfitAction {
                            platform: POLYMARKET.into(),
                            token_id: pm_token.token_id.clone(),
                            label: pm_token.label.clone(),
                            shares: qty,
                            cap_price: pm_cap,
                            fee: pm_fee,
                        },
                        TakeProfitAction {
                            platform: OUTCOME.into(),
                            token_id: out_token.token_id.clone(),
                            label: out_token.label.clone(),
                            shares: qty,
                            cap_price: out_cap,
                            fee: out_fee,
                        },
                    ],
                    shares: qty,
                    gross_revenue,
                    total_fee,
                    gain,
                });
            }
        }
        pm_revenue += step * pm_level.price;
        out_revenue += step * out_level.price;
        lo = hi;
        pm_left -= step;
        out_left -= step;
        if pm_left.is_zero() {
            let Some(next) = pm_levels.next() else {
                break;
            };
            pm_level = next;
            pm_left = next.size;
        }
        if out_left.is_zero() {
            let Some(next) = out_levels.next() else {
                break;
            };
            out_level = next;
            out_left = next.size;
        }
    }
    best
}

fn position(positions: &Positions, platform: &str, label: &str) -> Decimal {
    positions
        .get(platform)
        .and_then(|by_label| {
            by_label.get(label).copied().or_else(|| {
                by_label
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case(label))
                    .map(|(_, value)| *value)
            })
        })
        .unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
#[path = "../tests/unit/take_profit.rs"]
mod tests;
