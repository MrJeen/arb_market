use crate::book::{BookStore, Level};
use crate::calc::{
    align_hedge_price, below_venue_mins, estimate_taker_fee, floor_shares, FeeContext,
};
use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::Topic;
use crate::hedge::Positions;
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

#[derive(Debug, Clone, Copy)]
struct SellDepth {
    avg: Decimal,
    worst: Decimal,
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
    if labels.len() != 2 {
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

    let qty = floor_shares(
        pm_position
            .min(out_position)
            .min(bid_quantity(&pm_book.bids))
            .min(bid_quantity(&out_book.bids)),
    );
    if qty <= Decimal::ZERO {
        return None;
    }

    let pm_depth = walk_bids(&pm_book.bids, qty)?;
    let out_depth = walk_bids(&out_book.bids, qty)?;
    let pm_cap = align_hedge_price(POLYMARKET, false, pm_depth.worst, pm_book.tick_size)?;
    let out_cap = align_hedge_price(OUTCOME, false, out_depth.worst, None)?;
    if below_venue_mins(POLYMARKET, false, qty, pm_cap * qty)
        || below_venue_mins(OUTCOME, false, qty, out_cap * qty)
    {
        return None;
    }

    let pm_fee = estimate_taker_fee(POLYMARKET, qty, pm_depth.avg, fees);
    let out_fee = estimate_taker_fee(OUTCOME, qty, out_depth.avg, fees);
    let gross_revenue = qty * (pm_depth.avg + out_depth.avg);
    let total_fee = pm_fee + out_fee;
    // 项目仅支持二元市场，一对跨平台互补份额固定兑付 q/1。
    let gain = gross_revenue - total_fee - qty;
    if gain < min_gain {
        return None;
    }

    Some(TakeProfitPlan {
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
    })
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

fn bid_quantity(levels: &[Level]) -> Decimal {
    levels
        .iter()
        .filter(|level| level.price > Decimal::ZERO && level.size > Decimal::ZERO)
        .map(|level| level.size)
        .sum()
}

fn walk_bids(levels: &[Level], qty: Decimal) -> Option<SellDepth> {
    let mut filled = Decimal::ZERO;
    let mut revenue = Decimal::ZERO;
    let mut worst = Decimal::ZERO;
    for level in levels {
        if level.price <= Decimal::ZERO || level.size <= Decimal::ZERO {
            continue;
        }
        let take = (qty - filled).min(level.size);
        filled += take;
        revenue += take * level.price;
        worst = level.price;
        if filled >= qty {
            break;
        }
    }
    (filled == qty).then_some(SellDepth {
        avg: revenue / qty,
        worst,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{TokenRef, TopicKey};
    use std::collections::HashMap;
    use std::str::FromStr;
    use uuid::Uuid;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn token(platform: &str, token_id: &str, label: &str) -> TokenRef {
        TokenRef {
            platform: platform.into(),
            token_id: token_id.into(),
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
            title: "topic".into(),
            market_title: "market".into(),
            end_date: None,
            tokens: vec![
                token(POLYMARKET, "pm-yes", "yes"),
                token(POLYMARKET, "pm-no", "no"),
                token(OUTCOME, "#10", "no"),
                token(OUTCOME, "#11", "yes"),
            ],
        }
    }

    fn positions(pm_yes: &str, pm_no: &str, out_yes: &str, out_no: &str) -> Positions {
        HashMap::from([
            (
                POLYMARKET.into(),
                HashMap::from([("yes".into(), d(pm_yes)), ("no".into(), d(pm_no))]),
            ),
            (
                OUTCOME.into(),
                HashMap::from([("yes".into(), d(out_yes)), ("no".into(), d(out_no))]),
            ),
        ])
    }

    fn fees() -> FeeContext {
        FeeContext {
            polymarket_fee_rate: Decimal::ZERO,
            outcome_taker_rate: Decimal::ZERO,
        }
    }

    fn snapshot(
        books: &mut BookStore,
        platform: &str,
        token: &str,
        bids: &[(&str, &str)],
        now: Instant,
    ) {
        books.replace_snapshot(
            platform,
            token,
            bids.iter()
                .map(|(price, size)| Level {
                    price: d(price),
                    size: d(size),
                })
                .collect(),
            vec![],
            1,
            now,
        );
        if platform == POLYMARKET {
            books.set_tick_size(platform, token, d("0.01"));
        }
    }

    #[test]
    fn walks_multiple_bid_levels_and_uses_worst_cap() {
        let now = Instant::now();
        let mut books = BookStore::default();
        snapshot(
            &mut books,
            POLYMARKET,
            "pm-yes",
            &[("0.65", "3"), ("0.60", "7")],
            now,
        );
        snapshot(
            &mut books,
            OUTCOME,
            "#10",
            &[("0.50", "4"), ("0.45", "6")],
            now,
        );

        let plan = plan_take_profit(
            &topic(),
            &positions("10", "0", "0", "10"),
            &books,
            &fees(),
            d("0"),
            now,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(plan.shares, d("10"));
        assert_eq!(plan.gross_revenue, d("10.85"));
        assert_eq!(plan.gain, d("0.85"));
        assert_eq!(plan.actions[0].cap_price, d("0.60"));
        assert_eq!(plan.actions[1].cap_price, d("0.45"));
    }

    #[test]
    fn limits_quantity_by_positions_and_both_bid_depths_then_floors() {
        let now = Instant::now();
        let mut books = BookStore::default();
        snapshot(&mut books, POLYMARKET, "pm-yes", &[("0.7", "9.8")], now);
        snapshot(&mut books, OUTCOME, "#10", &[("0.5", "7.9")], now);
        let plan = plan_take_profit(
            &topic(),
            &positions("9.2", "0", "0", "8.4"),
            &books,
            &fees(),
            Decimal::ZERO,
            now,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(plan.shares, d("7"));
        assert!(plan.actions.iter().all(|action| action.shares == d("7")));
    }

    #[test]
    fn accepts_gain_exactly_at_point_one_boundary() {
        let now = Instant::now();
        let mut books = BookStore::default();
        snapshot(&mut books, POLYMARKET, "pm-yes", &[("0.61", "10")], now);
        snapshot(&mut books, OUTCOME, "#10", &[("0.40", "10")], now);
        let plan = plan_take_profit(
            &topic(),
            &positions("10", "0", "0", "10"),
            &books,
            &fees(),
            d("0.1"),
            now,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(plan.gain, d("0.10"));
    }

    #[test]
    fn subtracts_both_taker_fees() {
        let now = Instant::now();
        let mut books = BookStore::default();
        snapshot(&mut books, POLYMARKET, "pm-yes", &[("0.65", "10")], now);
        snapshot(&mut books, OUTCOME, "#10", &[("0.50", "10")], now);
        let fees = FeeContext {
            polymarket_fee_rate: d("0.07"),
            outcome_taker_rate: d("0.01"),
        };
        let plan = plan_take_profit(
            &topic(),
            &positions("10", "0", "0", "10"),
            &books,
            &fees,
            Decimal::ZERO,
            now,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(plan.actions[0].fee, d("0.15925"));
        assert_eq!(plan.actions[1].fee, d("0.05"));
        assert_eq!(plan.total_fee, d("0.20925"));
        assert_eq!(plan.gain, d("1.29075"));
    }

    #[test]
    fn chooses_more_profitable_complementary_direction() {
        let now = Instant::now();
        let mut books = BookStore::default();
        snapshot(&mut books, POLYMARKET, "pm-yes", &[("0.60", "10")], now);
        snapshot(&mut books, OUTCOME, "#10", &[("0.45", "10")], now);
        snapshot(&mut books, POLYMARKET, "pm-no", &[("0.70", "10")], now);
        snapshot(&mut books, OUTCOME, "#11", &[("0.50", "10")], now);
        let plan = plan_take_profit(
            &topic(),
            &positions("10", "10", "10", "10"),
            &books,
            &fees(),
            Decimal::ZERO,
            now,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(plan.actions[0].label, "no");
        assert_eq!(plan.actions[1].label, "yes");
        assert_eq!(plan.gain, d("2"));
    }

    #[test]
    fn rejects_non_complementary_or_non_positive_positions() {
        let now = Instant::now();
        let mut non_binary = topic();
        non_binary
            .tokens
            .push(token(POLYMARKET, "pm-maybe", "maybe"));
        assert!(plan_take_profit(
            &non_binary,
            &Positions::new(),
            &BookStore::default(),
            &fees(),
            Decimal::ZERO,
            now,
            Duration::from_secs(5)
        )
        .is_none());

        let mut books = BookStore::default();
        snapshot(&mut books, POLYMARKET, "pm-yes", &[("0.7", "10")], now);
        snapshot(&mut books, OUTCOME, "#10", &[("0.5", "10")], now);
        assert!(plan_take_profit(
            &topic(),
            &positions("10", "0", "0", "0"),
            &books,
            &fees(),
            Decimal::ZERO,
            now,
            Duration::from_secs(5)
        )
        .is_none());
    }

    #[test]
    fn rejects_stale_book_and_outcome_below_venue_minimum() {
        let now = Instant::now();
        let old = now - Duration::from_secs(10);
        let mut stale_books = BookStore::default();
        snapshot(
            &mut stale_books,
            POLYMARKET,
            "pm-yes",
            &[("0.7", "10")],
            old,
        );
        snapshot(&mut stale_books, OUTCOME, "#10", &[("0.5", "10")], now);
        assert!(plan_take_profit(
            &topic(),
            &positions("10", "0", "0", "10"),
            &stale_books,
            &fees(),
            Decimal::ZERO,
            now,
            Duration::from_secs(5)
        )
        .is_none());

        let mut below_min = BookStore::default();
        snapshot(&mut below_min, POLYMARKET, "pm-yes", &[("0.99", "1")], now);
        snapshot(&mut below_min, OUTCOME, "#10", &[("0.50", "1")], now);
        assert!(plan_take_profit(
            &topic(),
            &positions("1", "0", "0", "1"),
            &below_min,
            &fees(),
            Decimal::ZERO,
            now,
            Duration::from_secs(5)
        )
        .is_none());
    }
}
