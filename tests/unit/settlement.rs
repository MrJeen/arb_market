use super::*;
use serde_json::json;

#[test]
fn polymarket_winner_produces_token_payouts() {
    let status = parse_polymarket_settlement(&json!({
        "closed": true,
        "accepting_orders": false,
        "enable_order_book": false,
        "tokens": [
            {"token_id": "yes", "outcome": "Yes", "winner": true, "price": "1"},
            {"token_id": "no", "outcome": "No", "winner": false, "price": "0"}
        ]
    }))
    .unwrap();
    assert_eq!(
        status,
        SettlementStatus::Settled {
            payouts: vec![
                SettlementPayout {
                    token_id: "yes".into(),
                    payout: Decimal::ONE
                },
                SettlementPayout {
                    token_id: "no".into(),
                    payout: Decimal::ZERO
                },
            ]
        }
    );
}

#[test]
fn polymarket_prices_are_exact_and_independent_of_winner() {
    for (a, b) in [
        (json!("0.5"), json!(0.5)),
        (json!("0.3"), json!("0.7")),
        (json!("0"), json!("1")),
    ] {
        let response = json!({"tokens":[{"token_id":"a","winner":true,"price":a},{"token_id":"b","winner":false,"price":b}]});
        let SettlementStatus::Settled { payouts } = parse_polymarket_settlement(&response).unwrap()
        else {
            panic!("expected settlement")
        };
        assert_eq!(payouts[0].payout, parse_decimal(&a).unwrap());
        assert_eq!(payouts[1].payout, parse_decimal(&b).unwrap());
        for bad in [
            Value::Null,
            json!("NaN"),
            json!(true),
            json!("-0.1"),
            json!("1.1"),
            json!("0.123"),
        ] {
            let mut invalid = response.clone();
            invalid["tokens"][0]["price"] = bad;
            assert!(parse_polymarket_settlement(&invalid).is_err());
        }
        let mut missing = response.clone();
        missing["tokens"][0]
            .as_object_mut()
            .unwrap()
            .remove("price");
        assert!(parse_polymarket_settlement(&missing).is_err());
        for id in ["a", " "] {
            let mut invalid = response.clone();
            invalid["tokens"][1]["token_id"] = json!(id);
            assert!(parse_polymarket_settlement(&invalid).is_err());
        }
        let mut no_winner = response.clone();
        no_winner["tokens"][0]["winner"] = json!(false);
        assert_eq!(
            parse_polymarket_settlement(&no_winner).unwrap(),
            SettlementStatus::Unavailable
        );
        let mut multiple = response;
        multiple["tokens"][1]["winner"] = json!(true);
        assert!(parse_polymarket_settlement(&multiple).is_err());
    }
}

#[test]
fn polymarket_numeric_price_preserves_decimal_precision() {
    let response: Value = serde_json::from_str(r#"{"tokens":[{"token_id":"a","winner":true,"price":0.3000000000000000000000000001},{"token_id":"b","winner":false,"price":0.6999999999999999999999999999}]}"#).unwrap();
    let SettlementStatus::Settled { payouts } = parse_polymarket_settlement(&response).unwrap()
    else {
        panic!("expected settled")
    };
    assert_eq!(
        payouts[0].payout,
        "0.3000000000000000000000000001".parse::<Decimal>().unwrap()
    );
}

#[test]
fn polymarket_requires_all_trading_flags() {
    let open = json!({
        "closed": false,
        "accepting_orders": true,
        "enable_order_book": true,
        "tokens": [{"token_id": "yes", "winner": false, "price": "0"}]
    });
    assert_eq!(
        parse_polymarket_settlement(&open).unwrap(),
        SettlementStatus::TradableUnsettled
    );
    for field in ["closed", "accepting_orders", "enable_order_book"] {
        let mut unavailable = open.clone();
        unavailable.as_object_mut().unwrap().remove(field);
        assert_eq!(
            parse_polymarket_settlement(&unavailable).unwrap(),
            SettlementStatus::Unavailable
        );
    }
}

#[test]
fn polymarket_closed_without_winner_is_unavailable() {
    let status = parse_polymarket_settlement(&json!({
        "closed": true,
        "accepting_orders": false,
        "enable_order_book": false,
        "tokens": [{"token_id": "yes", "winner": false, "price": "0"}]
    }))
    .unwrap();
    assert_eq!(status, SettlementStatus::Unavailable);
}

#[test]
fn polymarket_rejects_ambiguous_or_malformed_tokens() {
    assert!(parse_polymarket_settlement(&json!({"tokens": []})).is_err());
    assert!(parse_polymarket_settlement(&json!({
        "tokens": [{"token_id": "yes"}]
    }))
    .is_err());
    assert!(parse_polymarket_settlement(&json!({
        "tokens": [
            {"token_id": "yes", "winner": true, "price": "1"},
            {"token_id": "no", "winner": true, "price": "1"}
        ]
    }))
    .is_err());
}

#[test]
fn outcome_null_is_unsettled() {
    assert_eq!(
        parse_outcome_settlement(95, &Value::Null).unwrap(),
        OutcomeSettlement::Unsettled
    );
}

#[test]
fn outcome_fractions_produce_protocol_payouts() {
    for (value, first, second) in [
        (json!("0"), "0", "1"),
        (json!("0.25"), "0.25", "0.75"),
        (json!(0.5), "0.5", "0.5"),
        (json!("0.75"), "0.75", "0.25"),
        (json!("1"), "1", "0"),
    ] {
        assert_eq!(
            parse_outcome_settlement(95, &json!({"settleFraction": value})).unwrap(),
            OutcomeSettlement::Settled {
                payouts: vec![
                    SettlementPayout {
                        token_id: "#950".into(),
                        payout: first.parse().unwrap()
                    },
                    SettlementPayout {
                        token_id: "#951".into(),
                        payout: second.parse().unwrap()
                    },
                ]
            }
        );
    }
}

#[test]
fn fractional_detection_only_fires_between_zero_and_one() {
    for binary in [json!("0"), json!("1"), json!(0), json!(1)] {
        let settled = parse_outcome_settlement(95, &json!({"settleFraction": binary})).unwrap();
        assert!(!settled.is_fractional(), "{binary} must stay binary");
    }
    for fractional in [json!("0.25"), json!(0.5), json!("0.999")] {
        let settled = parse_outcome_settlement(95, &json!({"settleFraction": fractional})).unwrap();
        assert!(settled.is_fractional(), "{fractional} must be flagged");
    }
    assert!(!OutcomeSettlement::Unsettled.is_fractional());
}

#[test]
fn outcome_rejects_out_of_range_or_malformed_settlement() {
    for malformed in [
        json!({}),
        json!({"settleFraction": "not-a-decimal"}),
        json!({"settleFraction": "-0.1"}),
        json!({"settleFraction": "1.1"}),
    ] {
        assert!(parse_outcome_settlement(95, &malformed).is_err());
    }
}
