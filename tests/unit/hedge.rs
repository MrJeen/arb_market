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
        outcome_builder_rate: Decimal::ZERO,
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

fn candidates(books: &BookStore, now: Instant, pm_yes: &str, out_no: &str) -> HedgeCandidates {
    hedge_candidates(
        &topic(),
        &imbalanced_positions(pm_yes, out_no),
        books,
        &fees(),
        d("1.5"),
        now,
        Duration::from_secs(5),
    )
}

#[test]
fn buy_platforms_exclude_absent_and_unusable_candidates() {
    let now = Instant::now();
    for (name, platform, token, pm_qty, out_qty, price, size, age, tick, expected) in [
        (
            "balanced", OUTCOME, "#10", "10", "10", "0.4", "10", 0, "0.01", false,
        ),
        (
            "threshold",
            OUTCOME,
            "#10",
            "11.5",
            "10",
            "0.4",
            "10",
            0,
            "0.01",
            false,
        ),
        (
            "empty depth",
            OUTCOME,
            "#10",
            "10",
            "0",
            "0.4",
            "0",
            0,
            "0.01",
            false,
        ),
        (
            "invalid price",
            OUTCOME,
            "#10",
            "10",
            "0",
            "0",
            "10",
            0,
            "0.01",
            false,
        ),
        (
            "minimum notional",
            OUTCOME,
            "#10",
            "10",
            "0",
            "0.05",
            "10",
            0,
            "0.01",
            false,
        ),
        (
            "minimum shares",
            POLYMARKET,
            "pm-yes",
            "0",
            "3",
            "0.5",
            "10",
            0,
            "0.01",
            false,
        ),
        (
            "missing tick",
            POLYMARKET,
            "pm-yes",
            "0",
            "10",
            "0.4",
            "10",
            0,
            "0",
            false,
        ),
        (
            "stale", OUTCOME, "#10", "10", "0", "0.4", "10", 6, "0.01", false,
        ),
        (
            "valid outcome",
            OUTCOME,
            "#10",
            "10",
            "0",
            "0.4",
            "10",
            0,
            "0.01",
            true,
        ),
        (
            "valid pm", POLYMARKET, "pm-yes", "0", "10", "0.4", "10", 0, "0.01", true,
        ),
    ] {
        let mut books = BookStore::default();
        books.replace_snapshot(
            platform,
            token,
            vec![],
            vec![Level {
                price: d(price),
                size: d(size),
            }],
            1,
            now - Duration::from_secs(age),
        );
        if tick != "0" {
            books.set_tick_size(POLYMARKET, "pm-yes", d(tick));
        }
        let candidates = candidates(&books, now, pm_qty, out_qty);
        let expected_platforms = if expected {
            vec![platform.to_string()]
        } else {
            vec![]
        };
        assert_eq!(candidates.buy_platforms(), expected_platforms, "{name}");
        assert!(candidates.select(&HashMap::new()).is_empty(), "{name}");
    }
}

#[test]
fn buy_platforms_are_stable_and_groups_do_not_share_funding() {
    let now = Instant::now();
    let mut books = BookStore::default();
    for (platform, token) in [
        (POLYMARKET, "pm-no"),
        (POLYMARKET, "pm-yes"),
        (OUTCOME, "#11"),
        (OUTCOME, "#10"),
    ] {
        books.replace_snapshot(
            platform,
            token,
            vec![],
            vec![Level {
                price: d("0.4"),
                size: d("10"),
            }],
            1,
            now,
        );
        if platform == POLYMARKET {
            books.set_tick_size(platform, token, d("0.01"));
        }
    }
    // labels 为 no → yes；分别覆盖同平台去重和两个方向的平台顺序。
    for (pm_no, pm_yes, out_yes, out_no, expected) in [
        ("10", "10", "0", "0", vec![OUTCOME]),
        ("0", "0", "10", "10", vec![POLYMARKET]),
        ("10", "0", "0", "10", vec![OUTCOME, POLYMARKET]),
        ("0", "10", "10", "0", vec![POLYMARKET, OUTCOME]),
    ] {
        let mut positions = imbalanced_positions(pm_yes, out_no);
        positions
            .get_mut(POLYMARKET)
            .unwrap()
            .insert("no".into(), d(pm_no));
        positions
            .get_mut(OUTCOME)
            .unwrap()
            .insert("yes".into(), d(out_yes));
        let candidates = hedge_candidates(
            &topic(),
            &positions,
            &books,
            &fees(),
            d("1.5"),
            now,
            Duration::from_secs(5),
        );
        assert_eq!(candidates.buy_platforms(), expected);
        let expected_tokens: Vec<_> = candidates
            .groups
            .iter()
            .map(|group| {
                assert_eq!(group.candidates.len(), 1);
                assert_eq!(group.candidates[0].required_usdc, Some(d("4")));
                group.candidates[0].action.token_id.clone()
            })
            .collect();
        // 同平台两笔各需 4U，余额 4U 仍各自通过，不能聚合成 8U 或扣减。
        let balances = HashMap::from([(POLYMARKET.into(), d("4")), (OUTCOME.into(), d("4"))]);
        let actions = candidates.select(&balances);
        assert_eq!(actions.len(), 2);
        assert!(actions.iter().all(|action| action.side == HedgeSide::Buy));
        assert_eq!(
            actions
                .iter()
                .map(|action| action.token_id.clone())
                .collect::<Vec<_>>(),
            expected_tokens
        );
        assert_eq!(
            actions[0].token_id,
            if pm_no == "10" { "#11" } else { "pm-no" }
        );
        assert_eq!(
            actions[1].token_id,
            if pm_yes == "10" { "#10" } else { "pm-yes" }
        );
    }
}

#[test]
fn equal_marginal_value_prefers_buy_after_funding_filter() {
    let now = Instant::now();
    let mut books = BookStore::default();
    books.replace_snapshot(
        POLYMARKET,
        "pm-yes",
        vec![Level {
            price: d("0.6"),
            size: d("10"),
        }],
        vec![],
        1,
        now,
    );
    books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
    books.replace_snapshot(
        OUTCOME,
        "#10",
        vec![],
        vec![Level {
            price: d("0.4"),
            size: d("10"),
        }],
        1,
        now,
    );
    for (balances, expected_side) in [
        (HashMap::new(), HedgeSide::Sell),
        (HashMap::from([(OUTCOME.into(), d("4"))]), HedgeSide::Buy),
    ] {
        let candidates = candidates(&books, now, "10", "0");
        let group = &candidates.groups[0];
        assert_eq!(group.candidates.len(), 2);
        assert_eq!(group.candidates[0].action.side, HedgeSide::Sell);
        assert_eq!(group.candidates[0].required_usdc, None);
        assert_eq!(group.candidates[1].action.side, HedgeSide::Buy);
        assert_eq!(
            group.candidates[0].action.marginal_value,
            group.candidates[1].action.marginal_value
        );
        let actions = candidates.select(&balances);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].side, expected_side);
    }
}

#[test]
fn buy_funding_uses_final_cap_and_fee_before_candidate_selection() {
    let now = Instant::now();
    for buy_pm in [false, true] {
        let (buy_platform, buy_token, sell_platform, sell_token, pm_qty, out_qty) = if buy_pm {
            (POLYMARKET, "pm-yes", OUTCOME, "#10", "0", "10")
        } else {
            (OUTCOME, "#10", POLYMARKET, "pm-yes", "10", "0")
        };
        let mut books = BookStore::default();
        books.replace_snapshot(
            buy_platform,
            buy_token,
            vec![],
            [("0.30", "10"), ("0.60", "10"), ("0.90", "10")]
                .into_iter()
                .map(|(p, s)| Level {
                    price: d(p),
                    size: d(s),
                })
                .collect(),
            1,
            now,
        );
        books.replace_snapshot(
            sell_platform,
            sell_token,
            vec![Level {
                price: d("0.40"),
                size: d("10"),
            }],
            vec![],
            1,
            now,
        );
        books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
        let positions = imbalanced_positions(pm_qty, out_qty);
        let fees = FeeContext {
            polymarket_fee_rate: d("0.07"),
            outcome_taker_rate: d("0.001344"),
            outcome_builder_rate: d("0.0003"),
        };
        let fee = estimate_taker_fee(buy_platform, OrderSide::Buy, d("10"), d("0.30"), &fees);
        let required = hedge_buy_required(buy_platform, d("10"), d("0.90"), fee, &fees).unwrap();
        assert_eq!(
            required,
            d("9")
                + if buy_pm {
                    fee
                } else {
                    d("9") * fees.outcome_builder_rate
                }
        );
        for balance in [d("5"), d("9"), required - d("0.00000001"), required] {
            let candidates = hedge_candidates(
                &topic(),
                &positions,
                &books,
                &fees,
                d("1.5"),
                now,
                Duration::from_secs(5),
            );
            assert_eq!(candidates.buy_platforms(), vec![buy_platform]);
            let group = &candidates.groups[0];
            assert_eq!(group.candidates[0].required_usdc, None);
            assert_eq!(group.candidates[1].required_usdc, Some(required));
            let actions = candidates.select(&HashMap::from([(buy_platform.into(), balance)]));
            assert_eq!(actions.len(), 1);
            if balance < required {
                assert_eq!(actions[0].side, HedgeSide::Sell);
            } else {
                assert_eq!(actions[0].side, HedgeSide::Buy);
                assert_eq!(
                    actions[0].marginal_value,
                    d("7") - d("10") * fees.outcome_taker_rate - fee
                );
            }
        }
    }
}

#[test]
fn unrepresentable_pm_buy_does_not_hide_sell_or_mark_dust_complete() {
    let now = Instant::now();
    let mut books = BookStore::default();
    books.replace_snapshot(
        POLYMARKET,
        "pm-yes",
        vec![],
        vec![Level {
            price: d("0.333"),
            size: d("100"),
        }],
        1,
        now,
    );
    books.set_tick_size(POLYMARKET, "pm-yes", d("0.001"));
    books.replace_snapshot(
        OUTCOME,
        "#10",
        vec![Level {
            price: d("0.40"),
            size: d("100"),
        }],
        vec![],
        1,
        now,
    );
    assert!(candidates(&books, now, "0", "7").buy_platforms().is_empty());
    let balances = HashMap::from([(POLYMARKET.into(), d("100"))]);
    let actions = plan(&books, &balances, now, "0", "7");
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].side, HedgeSide::Sell);
    books.replace_snapshot(OUTCOME, "#10", vec![], vec![], 2, now);
    assert!(plan(&books, &balances, now, "0", "7").is_empty());
    assert!(!leftover_untradeable(
        &topic(),
        &imbalanced_positions("0", "7"),
        &books,
        d("1.5"),
        now,
        Duration::from_secs(5)
    ));
}

#[test]
fn buy_funding_rejects_invalid_inputs_and_overflow() {
    for platform in [POLYMARKET, OUTCOME] {
        for (shares, cap, fee) in [
            (d("0"), d("0.5"), d("0")),
            (d("1.5"), d("0.5"), d("0")),
            (d("10"), d("1"), d("0")),
            (d("10"), d("0.5"), d("-1")),
            (Decimal::MAX, Decimal::MAX, d("0")),
        ] {
            assert!(hedge_buy_required(platform, shares, cap, fee, &fees()).is_err());
        }
    }
}

#[test]
fn filling_either_deficit_reserves_only_newly_paired_quantity() {
    let now = Instant::now();
    let fees = FeeContext {
        outcome_taker_rate: d("0.001344"),
        outcome_builder_rate: d("0.0003"),
        ..fees()
    };
    for (platform, token, pm_qty, out_qty) in [
        (POLYMARKET, "pm-yes", "6", "36"),
        (OUTCOME, "#10", "36", "6"),
    ] {
        let mut books = BookStore::default();
        books.replace_snapshot(
            platform,
            token,
            vec![],
            vec![Level {
                price: d("0.4"),
                size: d("10"),
            }],
            1,
            now,
        );
        books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
        let actions = plan_hedge(
            &topic(),
            &imbalanced_positions(pm_qty, out_qty),
            &books,
            &HashMap::from([(platform.into(), d("100"))]),
            &fees,
            d("1.5"),
            now,
            Duration::from_secs(5),
        );
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].side, HedgeSide::Buy);
        assert_eq!(actions[0].shares, d("10"));
        let buy_fee = if platform == OUTCOME {
            d("0.0012")
        } else {
            Decimal::ZERO
        };
        assert_eq!(actions[0].fee, buy_fee);
        assert_eq!(actions[0].marginal_value, d("5.98656") - buy_fee);
    }
    let mut books = BookStore::default();
    books.replace_snapshot(
        OUTCOME,
        "#10",
        vec![Level {
            price: d("0.9608"),
            size: d("30"),
        }],
        vec![],
        1,
        now,
    );
    let sell = eval_sell(
        OUTCOME,
        "#10",
        "no",
        d("30"),
        &books,
        &fees,
        now,
        Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(sell.action.fee, d("0.047386656"));
    assert_eq!(sell.action.marginal_value, d("28.824") - d("0.047386656"));
}

#[test]
fn outcome_cap_funding_ignores_protocol_reserve_and_rejects_builder_overflow() {
    let context = FeeContext {
        outcome_taker_rate: d("0.001344"),
        ..fees()
    };
    assert_eq!(
        hedge_buy_required(OUTCOME, d("30"), d("0.9"), Decimal::ZERO, &context).unwrap(),
        d("27")
    );
    let context = FeeContext {
        outcome_builder_rate: Decimal::MAX,
        ..context
    };
    assert!(hedge_buy_required(OUTCOME, d("30"), d("0.9"), Decimal::ZERO, &context).is_err());
    assert!(hedge_buy_required(POLYMARKET, d("10"), d("0.5"), Decimal::MAX, &fees()).is_err());
}

#[test]
fn hedge_order_tokens_only_excess_and_cross_deficit() {
    let tokens = hedge_order_tokens(&topic(), &imbalanced_positions("31", "6"), d("1.5"));
    let mut keys: Vec<_> = tokens
        .iter()
        .map(|(p, t)| (p.as_str(), t.as_str()))
        .collect();
    keys.sort();
    assert_eq!(keys, vec![(OUTCOME, "#10"), (POLYMARKET, "pm-yes")]);
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
fn empty_balances_force_sell_when_buy_would_otherwise_win() {
    let mut books = BookStore::default();
    let now = Instant::now();
    books.replace_snapshot(
        OUTCOME,
        "#10",
        vec![],
        vec![Level {
            price: d("0.25"),
            size: d("40"),
        }],
        1,
        now,
    );
    books.replace_snapshot(
        POLYMARKET,
        "pm-yes",
        vec![Level {
            price: d("0.30"),
            size: d("40"),
        }],
        vec![],
        1,
        now,
    );
    books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
    let actions = plan(&books, &HashMap::new(), now, "31", "6");
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].platform, POLYMARKET);
    assert_eq!(actions[0].side, HedgeSide::Sell);
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
        outcome_taker_rate: d("0.001344"),
        outcome_builder_rate: d("0.0003"),
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
        estimate_taker_fee(OUTCOME, OrderSide::Buy, buy[0].shares, d("0.4"), &fees)
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
        estimate_taker_fee(
            POLYMARKET,
            OrderSide::Sell,
            sell[0].shares,
            d("0.50"),
            &fees
        )
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
    let candidates = candidates(&books, now, "9", "6");
    assert!(candidates.buy_platforms().is_empty());
    assert_eq!(candidates.groups[0].candidates.len(), 1);
    assert_eq!(candidates.groups[0].candidates[0].required_usdc, None);
    let actions = candidates.select(&HashMap::new());
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
