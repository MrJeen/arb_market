use crate::book::{BookStore, Level};
use crate::calc::{
    align_hedge_price, estimate_taker_fee, floor_shares, min_trade_amount, min_trade_cost,
    FeeContext,
};
use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::Topic;
use rust_decimal::Decimal;
use std::collections::HashMap;
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
    pub marginal_value: Decimal,
}

pub type Positions = HashMap<String, HashMap<String, Decimal>>;

struct Depth {
    avg: Decimal,
    worst: Decimal,
    worst_plus_two: Decimal,
    filled: Decimal,
}

struct Candidate {
    action: HedgeAction,
}

pub fn position_diffs(positions: &Positions, labels: &[String]) -> Option<(Decimal, Decimal)> {
    if labels.len() != 2 {
        return None;
    }
    let pm = positions.get(POLYMARKET)?;
    let out = positions.get(OUTCOME)?;
    let l1 = &labels[0];
    let l2 = &labels[1];
    let diff1 = *pm.get(l1).unwrap_or(&Decimal::ZERO) - *out.get(l2).unwrap_or(&Decimal::ZERO);
    let diff2 = *pm.get(l2).unwrap_or(&Decimal::ZERO) - *out.get(l1).unwrap_or(&Decimal::ZERO);
    Some((diff1, diff2))
}

pub fn needs_rebalance(positions: &Positions, labels: &[String], min_qty: Decimal) -> bool {
    match position_diffs(positions, labels) {
        Some((d1, d2)) => d1.abs() > min_qty || d2.abs() > min_qty,
        None => false,
    }
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
    let labels = topic.labels();
    let Some((diff1, diff2)) = position_diffs(positions, &labels) else {
        return Vec::new();
    };
    let mut actions = Vec::new();
    if let Some(action) = hedge_one(
        topic, &labels[0], &labels[1], diff1, books, balances, fees, min_qty, now, stale,
    ) {
        actions.push(action);
    }
    if let Some(action) = hedge_one(
        topic, &labels[1], &labels[0], diff2, books, balances, fees, min_qty, now, stale,
    ) {
        actions.push(action);
    }
    actions
}

fn hedge_one(
    topic: &Topic,
    pm_label: &str,
    out_label: &str,
    diff: Decimal,
    books: &BookStore,
    balances: &HashMap<String, Decimal>,
    fees: &FeeContext,
    min_qty: Decimal,
    now: Instant,
    stale: Duration,
) -> Option<HedgeAction> {
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
    let (excess_platform, excess_token, deficit_platform, deficit_token) = if diff > Decimal::ZERO {
        (POLYMARKET, pm_token, OUTCOME, out_token)
    } else {
        (OUTCOME, out_token, POLYMARKET, pm_token)
    };

    let mut candidates = Vec::new();
    if let Some(sell) = eval_sell(
        excess_platform,
        &excess_token.token_id,
        &excess_token.label,
        qty_needed,
        books,
        fees,
        now,
        stale,
    ) {
        candidates.push(sell);
    }
    if let Some(buy) = eval_buy(
        deficit_platform,
        &deficit_token.token_id,
        &deficit_token.label,
        qty_needed,
        books,
        balances,
        fees,
        now,
        stale,
    ) {
        candidates.push(buy);
    }
    let best = candidates
        .into_iter()
        .max_by(|a, b| a.action.marginal_value.cmp(&b.action.marginal_value))?;
    tracing::info!(
        excess = %format!("{}.{}", excess_platform, excess_token.label),
        deficit = %format!("{}.{}", deficit_platform, deficit_token.label),
        qty_needed = %qty_needed,
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
    let fee = estimate_taker_fee(platform, qty, depth.avg, fees);
    let revenue = depth.avg * qty - fee;
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
            marginal_value: revenue,
        },
    })
}

fn eval_buy(
    platform: &str,
    token_id: &str,
    label: &str,
    qty_needed: Decimal,
    books: &BookStore,
    balances: &HashMap<String, Decimal>,
    fees: &FeeContext,
    now: Instant,
    stale: Duration,
) -> Option<Candidate> {
    let depth = walk_book(platform, token_id, true, qty_needed, books, now, stale)?;
    let qty = floor_shares(depth.filled);
    if qty <= Decimal::ZERO {
        return None;
    }
    let fee = estimate_taker_fee(platform, qty, depth.avg, fees);
    let cost = depth.avg * qty + fee;
    let trade_cost = depth.worst * qty;
    if trade_cost < min_trade_cost(platform) || qty < min_trade_amount(platform) {
        return None;
    }
    let balance = balances.get(platform).copied().unwrap_or(Decimal::ZERO);
    if balance < cost {
        return None;
    }
    // 买入 cap 取最差价与再后两档的较大值，给 IOC/FAK 留出行走空间。
    let cap = depth.worst.max(depth.worst_plus_two);
    Some(Candidate {
        action: HedgeAction {
            platform: platform.into(),
            token_id: token_id.into(),
            label: label.into(),
            side: HedgeSide::Buy,
            shares: qty,
            cap_price: align_hedge_price(
                platform,
                true,
                cap,
                polymarket_tick(books, platform, token_id),
            )?,
            // 补齐后锁定兑付 $1/share，边际价值 = 锁定兑付 - 买入成本。
            marginal_value: qty - cost,
        },
    })
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
    books.get(platform, token_id).and_then(|book| book.tick_size)
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
mod tests {
    use super::*;
    use crate::book::BookStore;
    use crate::domain::{TokenRef, Topic, TopicKey};
    use std::str::FromStr;
    use uuid::Uuid;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn fees() -> FeeContext {
        FeeContext {
            polymarket_fee_rate: Decimal::ZERO,
            outcome_taker_rate: Decimal::ZERO,
            extra_cost_multiplier: d("1.3"),
        }
    }

    fn token(platform: &str, id: &str, label: &str) -> TokenRef {
        TokenRef {
            platform: platform.into(),
            token_id: id.into(),
            label: label.into(),
            option_id: "1".into(),
            condition_id: None,
            asset_id: None,
            side_index: None,
            neg_risk: None,
            fees_enabled: None,
            fee_rate: None,
        }
    }

    fn topic() -> Topic {
        Topic {
            key: TopicKey::new(Uuid::nil(), 0),
            title: "t".into(),
            market_title: "m".into(),
            end_date: None,
            tokens: vec![
                token(POLYMARKET, "pm-yes", "yes"),
                token(POLYMARKET, "pm-no", "no"),
                token(OUTCOME, "#10", "no"),
                token(OUTCOME, "#11", "yes"),
            ],
        }
    }

    fn imbalanced_positions(pm_yes: &str, out_no: &str) -> Positions {
        let mut positions = Positions::new();
        positions.insert(
            POLYMARKET.into(),
            HashMap::from([("yes".into(), d(pm_yes)), ("no".into(), d("0"))]),
        );
        positions.insert(
            OUTCOME.into(),
            HashMap::from([("no".into(), d(out_no)), ("yes".into(), d("0"))]),
        );
        positions
    }

    fn plan(
        books: &BookStore,
        balances: &HashMap<String, Decimal>,
        now: Instant,
        pm_yes: &str,
        out_no: &str,
    ) -> Vec<HedgeAction> {
        plan_hedge(
            &topic(),
            &imbalanced_positions(pm_yes, out_no),
            books,
            balances,
            &fees(),
            d("1.5"),
            now,
            Duration::from_secs(5),
        )
    }

    #[test]
    fn buys_missing_when_only_ask_exists() {
        let mut books = BookStore::default();
        let now = Instant::now();
        books.replace_snapshot(
            OUTCOME,
            "#10",
            vec![],
            vec![Level {
                price: d("0.4"),
                size: d("40"),
            }],
            1,
            now,
        );
        let mut balances = HashMap::new();
        balances.insert(OUTCOME.into(), d("100"));
        let actions = plan(&books, &balances, now, "31", "6");
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].platform, OUTCOME);
        assert_eq!(actions[0].side, HedgeSide::Buy);
        assert_eq!(actions[0].shares, floor_shares(d("25")));
        // 锁定 25 - 成本 10 = 15
        assert_eq!(actions[0].marginal_value, d("15"));
    }

    #[test]
    fn sells_excess_when_sell_has_higher_marginal_value() {
        let mut books = BookStore::default();
        let now = Instant::now();
        books.replace_snapshot(
            OUTCOME,
            "#10",
            vec![],
            vec![Level {
                price: d("0.80"),
                size: d("40"),
            }],
            1,
            now,
        );
        books.replace_snapshot(
            POLYMARKET,
            "pm-yes",
            vec![Level {
                price: d("0.50"),
                size: d("40"),
            }],
            vec![],
            1,
            now,
        );
        books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
        let mut balances = HashMap::new();
        balances.insert(OUTCOME.into(), d("100"));
        let actions = plan(&books, &balances, now, "31", "6");
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].platform, POLYMARKET);
        assert_eq!(actions[0].side, HedgeSide::Sell);
        assert_eq!(actions[0].shares, floor_shares(d("25")));
        // 卖出回收 12.5 > 买入锁定 25 - 20 = 5
        assert_eq!(actions[0].marginal_value, d("12.5"));
    }

    #[test]
    fn buys_missing_when_buy_has_higher_marginal_value() {
        let mut books = BookStore::default();
        let now = Instant::now();
        books.replace_snapshot(
            OUTCOME,
            "#10",
            vec![],
            vec![Level {
                price: d("0.25"),
                size: d("50"),
            }],
            1,
            now,
        );
        books.replace_snapshot(
            POLYMARKET,
            "pm-yes",
            vec![Level {
                price: d("0.30"),
                size: d("50"),
            }],
            vec![],
            1,
            now,
        );
        books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
        let mut balances = HashMap::new();
        balances.insert(OUTCOME.into(), d("100"));
        let actions = plan(&books, &balances, now, "46", "6");
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].platform, OUTCOME);
        assert_eq!(actions[0].side, HedgeSide::Buy);
        assert_eq!(actions[0].shares, floor_shares(d("40")));
        // 买入锁定 40 - 10 = 30 > 卖出回收 12
        assert_eq!(actions[0].marginal_value, d("30"));
    }

    #[test]
    fn skips_when_neither_side_has_depth() {
        let books = BookStore::default();
        let now = Instant::now();
        let actions = plan(&books, &HashMap::new(), now, "10", "6");
        assert!(actions.is_empty());
    }
}
