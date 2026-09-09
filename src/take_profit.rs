use crate::book::{BookStore, Level};
use crate::calc::{
    align_hedge_price, below_venue_mins, estimate_taker_fee, floor_shares, min_trade_amount,
    min_trade_cost, FeeContext,
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
                let pm_fee = estimate_taker_fee(POLYMARKET, qty, pm_gross / qty, fees);
                let out_fee = estimate_taker_fee(OUTCOME, qty, out_gross / qty, fees);
                let gross_revenue = pm_gross + out_gross;
                let total_fee = pm_fee + out_fee;
                // 项目仅支持二元市场，一对跨平台互补份额固定兑付 q/1。
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

    fn partial_plan(
        pm_bids: &[(&str, &str)],
        out_bids: &[(&str, &str)],
        holdings: &str,
        fees: &FeeContext,
        min_gain: &str,
    ) -> Option<TakeProfitPlan> {
        let now = Instant::now();
        let mut books = BookStore::default();
        snapshot(&mut books, POLYMARKET, "pm-yes", pm_bids, now);
        snapshot(&mut books, OUTCOME, "#10", out_bids, now);
        plan_take_profit(
            &topic(),
            &positions(holdings, "0", "0", holdings),
            &books,
            fees,
            d(min_gain),
            now,
            Duration::from_secs(5),
        )
    }

    #[test]
    fn selects_profitable_prefix_instead_of_unprofitable_full_depth() {
        let plan = partial_plan(
            &[("0.65", "10"), ("0.30", "90")],
            &[("0.50", "10"), ("0.30", "90")],
            "100",
            &fees(),
            "0.1",
        )
        .unwrap();
        assert_eq!(plan.shares, d("10"));
        assert_eq!(plan.gain, d("1.50"));
    }

    #[test]
    fn selects_integer_right_of_fractional_breakpoint() {
        let plan = partial_plan(
            &[("0.65", "5.8"), ("0.30", "94.2")],
            &[("0.50", "5.8"), ("0.30", "94.2")],
            "100",
            &fees(),
            "0.1",
        )
        .unwrap();
        assert_eq!(plan.shares, d("6"));
        assert_eq!(plan.gain, d("0.79"));
    }

    #[test]
    fn selects_first_integer_meeting_cap_notional_minimum() {
        let plan = partial_plan(
            &[("0.90", "2"), ("0.65", "98")],
            &[("0.40", "2"), ("0.25", "98")],
            "100",
            &fees(),
            "0.1",
        )
        .unwrap();
        assert_eq!(plan.shares, d("4"));
        assert_eq!(plan.gain, d("0.40"));
        assert_eq!(plan.actions[1].cap_price * plan.shares, d("1"));
        // q=3 的实际收入为 1.05，但 cap × q 仅 .75，不能下单。
        assert!(partial_plan(
            &[("0.90", "2"), ("0.65", "98")],
            &[("0.40", "2"), ("0.25", "98")],
            "3",
            &fees(),
            "0.1",
        )
        .is_none());
    }

    #[test]
    fn accumulates_sub_share_levels() {
        // 非法档位由 BookStore 输入边界拒绝，规划器使用已验证的完整盘口。
        let plan = partial_plan(
            &[("0.80", "0.6"), ("0.75", "0.6"), ("0.70", "0.9")],
            &[("0.65", "0.4"), ("0.60", "0.7"), ("0.55", "1.1")],
            "2.9",
            &fees(),
            "0",
        )
        .unwrap();
        assert_eq!(plan.shares, d("2"));
        assert_eq!(plan.gross_revenue, d("2.665"));
        assert_eq!(plan.actions[0].cap_price, d("0.70"));
        assert_eq!(plan.actions[1].cap_price, d("0.55"));
        assert!(partial_plan(
            &[("0.8", "0.6"), ("0.7", "0.3")],
            &[("0.6", "10")],
            "10",
            &fees(),
            "0",
        )
        .is_none());
    }

    #[test]
    fn equal_gain_prefers_larger_quantity_including_flat_deep_levels() {
        let plan = partial_plan(
            &[("0.70", "5"), ("0.50", "15")],
            &[("0.50", "20")],
            "20",
            &fees(),
            "1",
        )
        .unwrap();
        assert_eq!(plan.gain, d("1"));
        assert_eq!(plan.shares, d("20"));
    }

    #[test]
    fn equal_direction_gain_keeps_last_direction_even_with_fewer_shares() {
        let now = Instant::now();
        let mut books = BookStore::default();
        // labels() 按字典序返回 no、yes；原 max_by 在方向平局时选最后的 yes。
        snapshot(&mut books, POLYMARKET, "pm-yes", &[("0.70", "5")], now);
        snapshot(&mut books, OUTCOME, "#10", &[("0.50", "5")], now);
        snapshot(&mut books, POLYMARKET, "pm-no", &[("0.60", "10")], now);
        snapshot(&mut books, OUTCOME, "#11", &[("0.50", "10")], now);
        let plan = plan_take_profit(
            &topic(),
            &positions("5", "10", "10", "5"),
            &books,
            &fees(),
            Decimal::ZERO,
            now,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(plan.gain, d("1"));
        assert_eq!(plan.shares, d("5"));
        assert_eq!(plan.actions[0].label, "yes");
    }

    #[test]
    fn json_snapshots_restore_take_profit_only_after_complete_book() {
        use crate::platforms::{outcome::apply_ws_book, polymarket::apply_ws_message};
        use serde_json::json;

        let now = Instant::now();
        let mut books = BookStore::default();
        let mut pm_snapshot = json!({
            "event_type": "book", "asset_id": "pm-yes", "timestamp": "100",
            "tick_size": "0.01", "bids": [{"price": "0.65", "size": "10"}],
            "asks": [{"price": "0.8", "size": "10"}]
        });
        assert_eq!(
            apply_ws_message(&mut books, &pm_snapshot, now),
            vec![("pm-yes".into(), true)]
        );
        assert_eq!(
            apply_ws_book(
                &mut books,
                &json!({
                    "channel": "l2Book", "data": {
                        "coin": "#10", "time": 100,
                        "levels": [[{"px": "0.5", "sz": "10", "n": 1}], []]
                    }
                }),
                now
            ),
            Some("#10".into())
        );
        let plan = |books: &BookStore| {
            plan_take_profit(
                &topic(),
                &positions("10", "0", "0", "10"),
                books,
                &fees(),
                d("0.1"),
                now,
                Duration::from_secs(5),
            )
        };
        assert_eq!(plan(&books).unwrap().gain, d("1.5"));
        books.mark_platform_stale(POLYMARKET);
        assert!(plan(&books).is_none());
        assert_eq!(
            apply_ws_message(
                &mut books,
                &json!({
                    "event_type": "price_change", "timestamp": "101", "price_changes": [
                        {"asset_id": "pm-yes", "side": "BUY", "price": "0.65", "size": "10"}
                    ]
                }),
                now
            ),
            vec![]
        );
        assert!(books.get(POLYMARKET, "pm-yes").unwrap().stale);
        assert!(plan(&books).is_none());
        pm_snapshot["timestamp"] = json!("101");
        pm_snapshot["asks"] = json!([]);
        assert!(apply_ws_message(&mut books, &pm_snapshot, now).is_empty());
        assert!(
            plan(&books).is_none(),
            "same-timestamp conflict cannot repair the missing baseline"
        );
        pm_snapshot["timestamp"] = json!("102");
        assert_eq!(
            apply_ws_message(&mut books, &pm_snapshot, now),
            vec![("pm-yes".into(), true)]
        );
        let restored = books.get(POLYMARKET, "pm-yes").unwrap();
        assert!(!restored.stale);
        assert!(restored.asks.is_empty());
        assert_eq!(plan(&books).unwrap().gain, d("1.5"));
    }

    #[test]
    fn rejects_missing_polymarket_tick() {
        let now = Instant::now();
        let mut books = BookStore::default();
        books.replace_snapshot(
            POLYMARKET,
            "pm-yes",
            vec![Level {
                price: d("0.7"),
                size: d("10"),
            }],
            vec![],
            1,
            now,
        );
        snapshot(&mut books, OUTCOME, "#10", &[("0.5", "10")], now);
        assert!(plan_take_profit(
            &topic(),
            &positions("10", "0", "0", "10"),
            &books,
            &fees(),
            Decimal::ZERO,
            now,
            Duration::from_secs(5),
        )
        .is_none());
    }

    #[test]
    fn rejects_either_negative_fee_rate_but_accepts_zero() {
        for (pm_rate, out_rate) in [("-0.01", "0"), ("0", "-0.01"), ("-1", "-1")] {
            assert!(partial_plan(
                &[("0.7", "10")],
                &[("0.5", "10")],
                "10",
                &FeeContext {
                    polymarket_fee_rate: d(pm_rate),
                    outcome_taker_rate: d(out_rate),
                },
                "0",
            )
            .is_none());
        }
        assert!(partial_plan(&[("0.7", "10")], &[("0.5", "10")], "10", &fees(), "0",).is_some());
    }

    #[test]
    fn large_holdings_searches_depth_segments_not_each_share() {
        let plan = partial_plan(
            &[("0.65", "10"), ("0.30", "999999999990")],
            &[("0.50", "10"), ("0.30", "999999999990")],
            "1000000000000",
            &fees(),
            "0.1",
        )
        .unwrap();
        assert_eq!(plan.shares, d("10"));
        assert_eq!(plan.gain, d("1.5"));
        let full = partial_plan(
            &[("0.70", "1000000000000")],
            &[("0.50", "1000000000000")],
            "1000000000000",
            &fees(),
            "0",
        )
        .unwrap();
        assert_eq!(full.shares, d("1000000000000"));
        assert_eq!(full.gain, d("200000000000"));
    }

    // 独立逐 q 重走盘口；不复用生产分段、端点或收入累加状态。
    fn exhaustive_pair(
        books: &BookStore,
        holdings: u32,
        fees: &FeeContext,
        min_gain: Decimal,
    ) -> Option<TakeProfitPlan> {
        fn revenue_and_cap(
            bids: &[Level],
            qty: Decimal,
            platform: &str,
            tick: Option<Decimal>,
        ) -> Option<(Decimal, Decimal)> {
            let mut remaining = qty;
            let mut gross = Decimal::ZERO;
            for level in bids {
                if level.price <= Decimal::ZERO || level.size <= Decimal::ZERO {
                    continue;
                }
                let taken = remaining.min(level.size);
                gross += taken * level.price;
                remaining -= taken;
                if remaining.is_zero() {
                    let cap = align_hedge_price(platform, false, level.price, tick)?;
                    return (!below_venue_mins(platform, false, qty, cap * qty))
                        .then_some((gross, cap));
                }
            }
            None
        }

        let pm = books.get(POLYMARKET, "pm-yes")?;
        let out = books.get(OUTCOME, "#10")?;
        let mut best: Option<TakeProfitPlan> = None;
        for q in 1..=holdings {
            let qty = Decimal::from(q);
            let Some((pm_gross, pm_cap)) = revenue_and_cap(&pm.bids, qty, POLYMARKET, pm.tick_size)
            else {
                continue;
            };
            let Some((out_gross, out_cap)) = revenue_and_cap(&out.bids, qty, OUTCOME, None) else {
                continue;
            };
            let pm_fee = estimate_taker_fee(POLYMARKET, qty, pm_gross / qty, fees);
            let out_fee = estimate_taker_fee(OUTCOME, qty, out_gross / qty, fees);
            let gross_revenue = pm_gross + out_gross;
            let total_fee = pm_fee + out_fee;
            let gain = gross_revenue - total_fee - qty;
            if gain < min_gain || best.as_ref().is_some_and(|plan| plan.gain > gain) {
                continue;
            }
            best = Some(TakeProfitPlan {
                actions: [
                    TakeProfitAction {
                        platform: POLYMARKET.into(),
                        token_id: "pm-yes".into(),
                        label: "yes".into(),
                        shares: qty,
                        cap_price: pm_cap,
                        fee: pm_fee,
                    },
                    TakeProfitAction {
                        platform: OUTCOME.into(),
                        token_id: "#10".into(),
                        label: "no".into(),
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
        best
    }

    #[test]
    fn segmented_search_matches_deterministic_exhaustive_oracle() {
        let now = Instant::now();
        let mut seed = 37_u64;
        let mut next = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            seed >> 32
        };
        for case in 0..80 {
            let mut books = BookStore::default();
            for (platform, token) in [(POLYMARKET, "pm-yes"), (OUTCOME, "#10")] {
                let levels = (0..5)
                    .map(|_| Level {
                        price: Decimal::new((next() % 950 + 10) as i64, 3),
                        size: Decimal::new((next() % 70 + 1) as i64, 1),
                    })
                    .collect();
                books.replace_snapshot(platform, token, levels, vec![], 1, now);
            }
            books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
            for (pm_rate, out_rate) in [("0", "0"), ("0.07", "0.01"), ("2", "0.03")] {
                let fees = FeeContext {
                    polymarket_fee_rate: d(pm_rate),
                    outcome_taker_rate: d(out_rate),
                };
                for holdings in 1..=16 {
                    for min_gain in [d("-2"), d("0"), d("0.1"), d("1")] {
                        let expected = exhaustive_pair(&books, holdings, &fees, min_gain);
                        let actual = plan_take_profit(
                            &topic(),
                            &positions(&holdings.to_string(), "0", "0", &holdings.to_string()),
                            &books,
                            &fees,
                            min_gain,
                            now,
                            Duration::from_secs(5),
                        );
                        let context = format!(
                            "case={case} holdings={holdings} fees={fees:?} min_gain={min_gain}"
                        );
                        assert_eq!(actual.is_some(), expected.is_some(), "{context}");
                        if let (Some(actual), Some(expected)) = (actual, expected) {
                            // 数量与 cap 必须精确相等；仅费用中的循环小数允许末位误差。
                            assert_eq!(actual.shares, expected.shares, "{context}");
                            assert_eq!(actual.gross_revenue, expected.gross_revenue, "{context}");
                            for (a, e) in actual.actions.iter().zip(&expected.actions) {
                                assert_eq!(a.cap_price, e.cap_price, "{context}");
                                assert_eq!(a.shares, e.shares, "{context}");
                                assert!(
                                    (a.fee - e.fee).abs() <= d("0.000000000000000000000001"),
                                    "{context}"
                                );
                            }
                            assert!(
                                (actual.total_fee - expected.total_fee).abs()
                                    <= d("0.000000000000000000000001"),
                                "{context}"
                            );
                            assert!(
                                (actual.gain - expected.gain).abs()
                                    <= d("0.000000000000000000000001"),
                                "{context}"
                            );
                        }
                    }
                }
            }
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
