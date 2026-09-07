use crate::book::{BookStore, Level};
use crate::calc::{
    align_hedge_price, below_venue_mins, estimate_taker_fee, floor_shares, FeeContext,
};
use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::{TokenRef, Topic};
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

struct Depth {
    avg: Decimal,
    worst: Decimal,
    worst_plus_two: Decimal,
    filled: Decimal,
}

struct Candidate {
    action: HedgeAction,
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
        excess = %format!("{}.{}", imb.excess_platform, imb.excess_token.label),
        deficit = %format!("{}.{}", imb.deficit_platform, imb.deficit_token.label),
        qty_needed = %imb.qty_needed,
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
    let fee = estimate_taker_fee(platform, qty, depth.avg, fees);
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
    if below_venue_mins(platform, true, qty, trade_cost) {
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
            fee,
            // 补齐后锁定兑付 $1/share，边际价值 = 锁定兑付 - 买入成本。
            marginal_value: qty - cost,
        },
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
    fn hedge_order_tokens_only_excess_and_cross_deficit() {
        let tokens = hedge_order_tokens(&topic(), &imbalanced_positions("31", "6"), d("1.5"));
        let mut keys: Vec<_> = tokens.iter().map(|(p, t)| (p.as_str(), t.as_str())).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![(OUTCOME, "#10"), (POLYMARKET, "pm-yes")]
        );
        assert!(!tokens.iter().any(|(p, t)| p == POLYMARKET && t == "pm-no"));
        assert!(!tokens.iter().any(|(p, t)| p == OUTCOME && t == "#11"));
    }

    #[test]
    fn hedge_order_tokens_empty_when_balanced() {
        let tokens = hedge_order_tokens(&topic(), &imbalanced_positions("6", "6"), d("1.5"));
        assert!(tokens.is_empty());
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
        assert_eq!(actions[0].fee, Decimal::ZERO);
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
        assert_eq!(actions[0].fee, Decimal::ZERO);
    }

    #[test]
    fn hedge_buy_and_sell_set_taker_fee() {
        let fees = FeeContext {
            polymarket_fee_rate: d("0.07"),
            outcome_taker_rate: d("0.00035"),
            extra_cost_multiplier: Decimal::ONE,
        };
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
        let buy = plan_hedge(
            &topic(),
            &imbalanced_positions("31", "6"),
            &books,
            &balances,
            &fees,
            d("1.5"),
            now,
            Duration::from_secs(5),
        );
        assert_eq!(buy[0].side, HedgeSide::Buy);
        assert_eq!(
            buy[0].fee,
            estimate_taker_fee(OUTCOME, buy[0].shares, d("0.4"), &fees)
        );
        assert!(buy[0].fee > Decimal::ZERO);

        let mut sell_books = BookStore::default();
        sell_books.replace_snapshot(
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
        sell_books.replace_snapshot(
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
        sell_books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
        let sell = plan_hedge(
            &topic(),
            &imbalanced_positions("31", "6"),
            &sell_books,
            &balances,
            &fees,
            d("1.5"),
            now,
            Duration::from_secs(5),
        );
        assert_eq!(sell[0].side, HedgeSide::Sell);
        assert_eq!(
            sell[0].fee,
            estimate_taker_fee(POLYMARKET, sell[0].shares, d("0.50"), &fees)
        );
        assert!(sell[0].fee > Decimal::ZERO);
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

    #[test]
    fn buys_other_side_when_outcome_sell_below_min() {
        let mut books = BookStore::default();
        let now = Instant::now();
        books.replace_snapshot(
            OUTCOME,
            "#10",
            vec![Level {
                price: d("0.05"),
                size: d("100"),
            }],
            vec![],
            1,
            now,
        );
        books.replace_snapshot(
            POLYMARKET,
            "pm-yes",
            vec![],
            vec![Level {
                price: d("0.40"),
                size: d("100"),
            }],
            1,
            now,
        );
        books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
        let mut balances = HashMap::new();
        balances.insert(POLYMARKET.into(), d("100"));
        let actions = plan(&books, &balances, now, "6", "16");
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].platform, POLYMARKET);
        assert_eq!(actions[0].side, HedgeSide::Buy);
        assert_eq!(actions[0].shares, floor_shares(d("10")));
    }

    #[test]
    fn leftover_untradeable_when_both_sides_below_min() {
        let mut books = BookStore::default();
        let now = Instant::now();
        books.replace_snapshot(
            OUTCOME,
            "#10",
            vec![Level {
                price: d("0.05"),
                size: d("100"),
            }],
            vec![],
            1,
            now,
        );
        books.replace_snapshot(
            POLYMARKET,
            "pm-yes",
            vec![],
            vec![Level {
                price: d("0.40"),
                size: d("100"),
            }],
            1,
            now,
        );
        books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
        // Outcome 多 10 股：卖 10 * 0.05 = 0.50 < 1U；买 PM 10 股 >= 5，名义 4U，仍可买。
        assert!(!leftover_untradeable(
            &topic(),
            &imbalanced_positions("6", "16"),
            &books,
            d("1.5"),
            now,
            Duration::from_secs(5),
        ));
        // 买 PM 只要 3 股 < 5，两边都做不了。
        books.replace_snapshot(
            POLYMARKET,
            "pm-yes",
            vec![],
            vec![Level {
                price: d("0.40"),
                size: d("100"),
            }],
            1,
            now,
        );
        assert!(leftover_untradeable(
            &topic(),
            &imbalanced_positions("6", "9"),
            &books,
            d("1.5"),
            now,
            Duration::from_secs(5),
        ));
    }

    #[test]
    fn leftover_untradeable_false_when_book_missing() {
        let books = BookStore::default();
        let now = Instant::now();
        assert!(!leftover_untradeable(
            &topic(),
            &imbalanced_positions("6", "16"),
            &books,
            d("1.5"),
            now,
            Duration::from_secs(5),
        ));
    }

    #[test]
    fn skips_outcome_sell_below_min_notional() {
        let mut books = BookStore::default();
        let now = Instant::now();
        books.replace_snapshot(
            OUTCOME,
            "#10",
            vec![Level {
                price: d("0.20"),
                size: d("10"),
            }],
            vec![],
            1,
            now,
        );
        let actions = plan(&books, &HashMap::new(), now, "6", "10");
        assert!(actions.is_empty());
    }

    #[test]
    fn sells_pm_below_five_shares() {
        let mut books = BookStore::default();
        let now = Instant::now();
        books.replace_snapshot(
            POLYMARKET,
            "pm-yes",
            vec![Level {
                price: d("0.40"),
                size: d("10"),
            }],
            vec![],
            1,
            now,
        );
        books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
        let actions = plan(&books, &HashMap::new(), now, "9", "6");
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].platform, POLYMARKET);
        assert_eq!(actions[0].side, HedgeSide::Sell);
        assert_eq!(actions[0].shares, floor_shares(d("3")));
    }

    #[test]
    fn missing_platform_counts_as_zero_and_both_zero_skips() {
        let mut only_pm = Positions::new();
        only_pm.insert(POLYMARKET.into(), HashMap::from([("yes".into(), d("10"))]));
        assert_eq!(
            needs_rebalance(&only_pm, &["yes".into(), "no".into()], d("1.5")),
            Some(true)
        );
        assert_eq!(
            needs_rebalance(&Positions::new(), &["yes".into(), "no".into()], d("1.5")),
            Some(false)
        );
        assert!(needs_rebalance(&only_pm, &["yes".into()], d("1.5")).is_none());
        assert_eq!(
            needs_rebalance(
                &imbalanced_positions("10", "10"),
                &["yes".into(), "no".into()],
                d("1.5")
            ),
            Some(false)
        );
        assert_eq!(
            needs_rebalance(
                &imbalanced_positions("31", "6"),
                &["yes".into(), "no".into()],
                d("1.5")
            ),
            Some(true)
        );
    }

    #[test]
    fn plans_hedge_when_one_leg_failed_as_zero() {
        let mut books = BookStore::default();
        let now = Instant::now();
        books.replace_snapshot(
            POLYMARKET,
            "pm-yes",
            vec![Level {
                price: d("0.50"),
                size: d("100"),
            }],
            vec![],
            1,
            now,
        );
        books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
        let mut failed_other = Positions::new();
        failed_other.insert(
            POLYMARKET.into(),
            HashMap::from([("yes".into(), d("100")), ("no".into(), d("0"))]),
        );
        failed_other.insert(
            OUTCOME.into(),
            HashMap::from([("no".into(), d("0")), ("yes".into(), d("0"))]),
        );
        assert_eq!(
            needs_rebalance(&failed_other, &["yes".into(), "no".into()], d("1.5")),
            Some(true)
        );
        let actions = plan_hedge(
            &topic(),
            &failed_other,
            &books,
            &HashMap::new(),
            &fees(),
            d("1.5"),
            now,
            Duration::from_secs(5),
        );
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].platform, POLYMARKET);
        assert_eq!(actions[0].side, HedgeSide::Sell);
        assert_eq!(actions[0].shares, floor_shares(d("100")));
    }
}
