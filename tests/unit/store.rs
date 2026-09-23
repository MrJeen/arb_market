use super::*;
use rust_decimal::prelude::FromStr;

fn d(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}

#[test]
fn lifecycle_action_accepts_supported_actions() {
    assert!(validate_lifecycle_action("take_profit").is_ok());
    assert!(validate_lifecycle_action("rebalance").is_ok());
}

#[test]
fn lifecycle_action_rejects_unknown_actions() {
    assert!(validate_lifecycle_action("settlement").is_err());
    assert!(validate_lifecycle_action("").is_err());
}

#[test]
fn settled_actuals_use_remaining_token_payouts_without_locked_double_count() {
    let rows = vec![
        (
            "BUY".into(),
            POLYMARKET.into(),
            "pm-yes".into(),
            d("10"),
            d("0.4"),
            d("0"),
        ),
        (
            "BUY".into(),
            OUTCOME.into(),
            "#5161".into(),
            d("8"),
            d("0.5"),
            d("0"),
        ),
        (
            "SELL".into(),
            POLYMARKET.into(),
            "pm-yes".into(),
            d("2"),
            d("0.6"),
            d("0"),
        ),
    ];
    let payouts = std::collections::HashMap::from([
        ((POLYMARKET.into(), "pm-yes".into()), Decimal::ONE),
        ((OUTCOME.into(), "#5161".into()), Decimal::ONE),
    ]);
    let (cost, rev, profit) = compute_settled_actuals(&rows, &payouts).unwrap();
    assert_eq!(cost, d("8"));
    assert_eq!(rev, d("17.2"));
    assert_eq!(profit, d("9.2"));
}

#[test]
fn settled_actuals_use_fractional_outcome_payout_regardless_of_pm_winner() {
    let rows = vec![
        (
            "BUY".into(),
            POLYMARKET.into(),
            "pm-yes".into(),
            d("10"),
            d("0.4"),
            Decimal::ZERO,
        ),
        (
            "BUY".into(),
            OUTCOME.into(),
            "#5161".into(),
            d("10"),
            d("0.55"),
            Decimal::ZERO,
        ),
    ];
    for (pm_payout, expected_rev, expected_profit) in [
        (Decimal::ZERO, d("5"), d("-4.5")),
        (Decimal::ONE, d("15"), d("5.5")),
    ] {
        let payouts = std::collections::HashMap::from([
            ((POLYMARKET.into(), "pm-yes".into()), pm_payout),
            ((OUTCOME.into(), "#5161".into()), d("0.5")),
        ]);
        assert_eq!(
            compute_settled_actuals(&rows, &payouts).unwrap(),
            (d("9.5"), expected_rev, expected_profit)
        );
    }
}

#[test]
fn settled_actuals_reject_missing_payout_and_negative_position() {
    let buy = vec![(
        "BUY".into(),
        OUTCOME.into(),
        "#5160".into(),
        d("1"),
        d("0.4"),
        d("0"),
    )];
    assert!(compute_settled_actuals(&buy, &std::collections::HashMap::new()).is_err());
    let sell = vec![(
        "SELL".into(),
        OUTCOME.into(),
        "#5160".into(),
        d("1"),
        d("0.4"),
        d("0"),
    )];
    assert!(compute_settled_actuals(&sell, &std::collections::HashMap::new()).is_err());
}

#[test]
fn actuals_lock_opposite_labels_and_add_sell_rev() {
    let rows = vec![
        (
            "BUY".into(),
            POLYMARKET.into(),
            "yes".into(),
            d("10"),
            d("0.4"),
            d("0.1"),
        ),
        (
            "BUY".into(),
            OUTCOME.into(),
            "no".into(),
            d("8"),
            d("0.5"),
            d("0"),
        ),
        (
            "SELL".into(),
            POLYMARKET.into(),
            "yes".into(),
            d("2"),
            d("0.6"),
            d("0"),
        ),
    ];
    let (cost, rev, profit) = compute_actuals(&rows);
    assert_eq!(cost.to_string(), "8.1");
    assert_eq!(rev.to_string(), "9.2");
    assert_eq!(profit.to_string(), "1.1");
}

#[test]
fn actuals_same_labels_do_not_lock() {
    let rows = vec![
        (
            "BUY".into(),
            POLYMARKET.into(),
            "yes".into(),
            d("10"),
            d("0.4"),
            d("0"),
        ),
        (
            "BUY".into(),
            OUTCOME.into(),
            "yes".into(),
            d("8"),
            d("0.5"),
            d("0"),
        ),
    ];
    let (cost, rev, profit) = compute_actuals(&rows);
    assert_eq!(cost, d("8"));
    assert_eq!(rev, Decimal::ZERO);
    assert_eq!(profit, d("-8"));
}

#[test]
fn actuals_lock_both_complementary_directions() {
    let rows = vec![
        (
            "BUY".into(),
            POLYMARKET.into(),
            "yes".into(),
            d("10"),
            d("0.4"),
            d("0"),
        ),
        (
            "BUY".into(),
            OUTCOME.into(),
            "no".into(),
            d("8"),
            d("0.5"),
            d("0"),
        ),
        (
            "BUY".into(),
            POLYMARKET.into(),
            "no".into(),
            d("3"),
            d("0.3"),
            d("0"),
        ),
        (
            "BUY".into(),
            OUTCOME.into(),
            "yes".into(),
            d("5"),
            d("0.6"),
            d("0"),
        ),
    ];
    let (cost, rev, profit) = compute_actuals(&rows);
    assert_eq!(cost, d("11.9"));
    assert_eq!(rev, d("11"));
    assert_eq!(profit, d("-0.9"));
}

#[test]
fn actuals_partial_sell_reduces_its_label_position() {
    let rows = vec![
        (
            "BUY".into(),
            POLYMARKET.into(),
            "yes".into(),
            d("100"),
            d("0.4"),
            d("0"),
        ),
        (
            "BUY".into(),
            OUTCOME.into(),
            "no".into(),
            d("100"),
            d("0.5"),
            d("0"),
        ),
        (
            "SELL".into(),
            POLYMARKET.into(),
            "yes".into(),
            d("50"),
            d("0.4"),
            d("0"),
        ),
    ];
    let (cost, rev, profit) = compute_actuals(&rows);
    assert_eq!(cost, d("90"));
    assert_eq!(rev, d("70"));
    assert_eq!(profit, d("-20"));
}
