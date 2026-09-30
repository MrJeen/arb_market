use super::{
    match_backfill, ComplementaryPair, MatchError, TradeFillInput, TradeSide, MATCH_WINDOW_SECS,
};
use crate::exec::arb_calc_excluded;
use rust_decimal::Decimal;
use std::str::FromStr;
use uuid::Uuid;

fn d(raw: &str) -> Decimal {
    Decimal::from_str(raw).unwrap()
}

fn fill(
    ts: i64,
    account: &str,
    token: &str,
    side: TradeSide,
    shares: &str,
    price: &str,
    fee: &str,
    source: &str,
) -> TradeFillInput {
    TradeFillInput {
        ts,
        account: account.to_string(),
        token_id: token.to_string(),
        side,
        shares: d(shares),
        price: d(price),
        fee: d(fee),
        source_id: source.to_string(),
    }
}

fn pair() -> ComplementaryPair {
    ComplementaryPair {
        unified_index: 1,
        market_title: "Arsenal".into(),
        pm_token: "pm-no".into(),
        pm_label: "no".into(),
        out_token: "#14730".into(),
        out_label: "yes".into(),
        condition_id: "0xcondition".into(),
        option_id: "1473".into(),
    }
}

#[test]
fn buy_then_sell_on_the_same_account_offsets() {
    let pairs = [pair()];
    let pm = vec![
        fill(
            1_000,
            "0xaaa",
            "pm-no",
            TradeSide::Buy,
            "50",
            "0.40",
            "1",
            "b1",
        ),
        fill(
            1_010,
            "0xaaa",
            "pm-no",
            TradeSide::Sell,
            "20",
            "0.45",
            "0",
            "s1",
        ),
    ];
    let outcome = vec![fill(
        1_005,
        "0xout",
        "#14730",
        TradeSide::Buy,
        "30",
        "0.60",
        "0.2",
        "o1",
    )];
    let plan = match_backfill(&pairs, &pm, &outcome).unwrap();
    assert!(plan.ignored_pm.is_empty());
    assert_eq!(plan.orders.len(), 1);
    assert_eq!(plan.orders[0].pm_shares, d("30"));
    assert_eq!(plan.orders[0].pm_fee, d("0.6"));
    assert_eq!(plan.orders[0].out_shares, d("30"));
    assert_eq!(plan.orders[0].pm_wallet, "0xaaa");
}

#[test]
fn matches_within_five_minutes_when_shares_differ_by_at_most_tolerance() {
    let pairs = [pair()];
    let pm = vec![fill(
        1_000,
        "0xaaa",
        "pm-no",
        TradeSide::Buy,
        "50",
        "0.40",
        "0.1",
        "b1",
    )];
    let outcome = vec![
        fill(
            1_100,
            "0xout",
            "#14730",
            TradeSide::Buy,
            "20",
            "0.50",
            "0.1",
            "o1",
        ),
        fill(
            1_120,
            "0xout",
            "#14730",
            TradeSide::Buy,
            "30.2",
            "0.70",
            "0.2",
            "o2",
        ),
    ];
    let plan = match_backfill(&pairs, &pm, &outcome).unwrap();
    assert_eq!(plan.orders.len(), 1);
    assert_eq!(plan.orders[0].pm_shares, d("50"));
    assert_eq!(plan.orders[0].out_shares, d("50.2"));
    assert!((plan.orders[0].pm_shares - plan.orders[0].out_shares).abs() <= d("1.5"));
    assert_eq!(plan.orders[0].out_fee, d("0.3"));
    let notional = d("20") * d("0.50") + d("30.2") * d("0.70");
    assert_eq!(plan.orders[0].out_price, (notional / d("50.2")).round_dp(8));
    assert!(plan.orders[0].submitted_at - 1_000 <= MATCH_WINDOW_SECS);
}

#[test]
fn aggregates_same_wallet_outside_the_five_minute_window() {
    let pairs = [pair()];
    let pm = vec![
        fill(
            1_000,
            "0xaaa",
            "pm-no",
            TradeSide::Buy,
            "30",
            "0.40",
            "0.3",
            "b1",
        ),
        fill(
            1_010,
            "0xaaa",
            "pm-no",
            TradeSide::Buy,
            "20",
            "0.50",
            "0.2",
            "b2",
        ),
    ];
    let outcome = vec![fill(
        1_000 + MATCH_WINDOW_SECS + 50,
        "0xout",
        "#14730",
        TradeSide::Buy,
        "50",
        "0.60",
        "0",
        "o1",
    )];
    let plan = match_backfill(&pairs, &pm, &outcome).unwrap();
    assert_eq!(plan.orders.len(), 1);
    assert_eq!(plan.orders[0].pm_wallet, "0xaaa");
    assert_eq!(plan.orders[0].pm_shares, d("50"));
    assert_eq!(plan.orders[0].pm_fee, d("0.5"));
    assert_eq!(plan.orders[0].pm_price, d("0.44"));
    assert_eq!(
        plan.orders[0].pm_sources,
        vec!["b1".to_string(), "b2".to_string()]
    );
}

#[test]
fn different_wallets_stay_on_separate_orders() {
    let pairs = [pair()];
    let pm = vec![
        fill(
            1_000,
            "0xaaa",
            "pm-no",
            TradeSide::Buy,
            "50",
            "0.40",
            "0",
            "a",
        ),
        fill(
            1_010,
            "0xbbb",
            "pm-no",
            TradeSide::Buy,
            "50",
            "0.41",
            "0",
            "b",
        ),
    ];
    let outcome = vec![
        fill(
            1_000,
            "0xout",
            "#14730",
            TradeSide::Buy,
            "50",
            "0.60",
            "0",
            "o1",
        ),
        fill(
            1_010,
            "0xout",
            "#14730",
            TradeSide::Buy,
            "50",
            "0.61",
            "0",
            "o2",
        ),
    ];
    let plan = match_backfill(&pairs, &pm, &outcome).unwrap();
    assert_eq!(plan.orders.len(), 2);
    assert_eq!(plan.orders[0].pm_wallet, "0xaaa");
    assert_eq!(plan.orders[1].pm_wallet, "0xbbb");
    assert_ne!(plan.orders[0].pm_wallet, plan.orders[1].pm_wallet);
}

#[test]
fn extra_polymarket_position_is_ignored() {
    let pairs = [pair()];
    let pm = vec![
        fill(
            1_000,
            "0xaaa",
            "pm-no",
            TradeSide::Buy,
            "50",
            "0.40",
            "0",
            "a",
        ),
        fill(
            1_020,
            "0xbbb",
            "pm-no",
            TradeSide::Buy,
            "50",
            "0.41",
            "0",
            "b",
        ),
    ];
    let outcome = vec![fill(
        1_000,
        "0xout",
        "#14730",
        TradeSide::Buy,
        "50",
        "0.60",
        "0",
        "o1",
    )];
    let plan = match_backfill(&pairs, &pm, &outcome).unwrap();
    assert_eq!(plan.orders.len(), 1);
    assert_eq!(plan.orders[0].pm_wallet, "0xaaa");
    assert_eq!(plan.ignored_pm.len(), 1);
    assert_eq!(plan.ignored_pm[0].wallet, "0xbbb");
    assert_eq!(plan.ignored_pm[0].shares, d("50"));
    assert_eq!(plan.ignored_pm[0].token_id, "pm-no");
}

#[test]
fn unmatched_outcome_above_tolerance_fails() {
    let pairs = [pair()];
    let pm = vec![fill(
        1_000,
        "0xaaa",
        "pm-no",
        TradeSide::Buy,
        "10",
        "0.40",
        "0",
        "a",
    )];
    let outcome = vec![fill(
        1_000,
        "0xout",
        "#14730",
        TradeSide::Buy,
        "20",
        "0.60",
        "0",
        "o1",
    )];
    let err = match_backfill(&pairs, &pm, &outcome).unwrap_err();
    assert_eq!(
        err,
        MatchError::OutcomeUnmatched {
            token_id: "#14730".into(),
            shares: d("10"),
        }
    );
}

#[test]
fn sell_larger_than_buys_fails() {
    let pairs = [pair()];
    let pm = vec![
        fill(
            1_000,
            "0xaaa",
            "pm-no",
            TradeSide::Buy,
            "10",
            "0.40",
            "0",
            "b",
        ),
        fill(
            1_010,
            "0xaaa",
            "pm-no",
            TradeSide::Sell,
            "12",
            "0.40",
            "0",
            "s",
        ),
    ];
    let err = match_backfill(&pairs, &pm, &[]).unwrap_err();
    match err {
        MatchError::Oversell { excess, .. } => assert_eq!(excess, d("2")),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn backfill_event_is_excluded_from_arb_calculation() {
    let excluded = Uuid::parse_str("019f2452-6b31-7c38-8c53-4d5867edd46d").unwrap();
    assert!(arb_calc_excluded(excluded));
    assert!(!arb_calc_excluded(Uuid::nil()));
}
