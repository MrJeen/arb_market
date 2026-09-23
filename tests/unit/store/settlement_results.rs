use super::*;
#[test]
fn source_replay_rejects_unknown_versions_and_tampering() {
    let evidence = json!({"version":1,"market_id":"m","source":"clob_market_price","response":{"condition_id":"m","tokens":[{"token_id":"a","winner":true,"price":"0.3"},{"token_id":"b","winner":false,"price":"0.7"}]}});
    let payouts = vec![
        SettlementPayout {
            token_id: "a".into(),
            payout: "0.3".parse().unwrap(),
        },
        SettlementPayout {
            token_id: "b".into(),
            payout: "0.7".parse().unwrap(),
        },
    ];
    assert!(verify_source(POLYMARKET, "m", "clob_market_price", 1, &evidence, &payouts).is_ok());
    for (source, version) in [
        ("clob_market_price", 2),
        ("clob_market_winner", 1),
        ("unknown", 1),
    ] {
        assert!(verify_source(POLYMARKET, "m", source, version, &evidence, &payouts).is_err());
    }
    for field in ["version", "source", "market_id", "response"] {
        let mut bad = evidence.clone();
        bad[field] = Value::Null;
        assert!(verify_source(POLYMARKET, "m", "clob_market_price", 1, &bad, &payouts).is_err());
    }
    let mut wrong = payouts;
    wrong[0].payout = Decimal::ONE;
    wrong[1].payout = Decimal::ZERO;
    assert!(verify_source(POLYMARKET, "m", "clob_market_price", 1, &evidence, &wrong).is_err());
}

#[test]
fn payout_normalization_rejects_missing_duplicate_and_invalid_values() {
    let good = vec![
        SettlementPayout {
            token_id: "a".into(),
            payout: Decimal::ONE,
        },
        SettlementPayout {
            token_id: "b".into(),
            payout: Decimal::ZERO,
        },
    ];
    let mut reversed = good.clone();
    reversed.reverse();
    assert_eq!(
        normalize(good.clone()).unwrap(),
        normalize(reversed).unwrap()
    );
    assert!(normalize(vec![]).is_err());
    let mut bad = good.clone();
    bad[1].token_id = "a".into();
    assert!(normalize(bad).is_err());
    let mut bad = good.clone();
    bad[1].payout = Decimal::ONE;
    assert!(normalize(bad).is_err());
    let mut bad = good;
    bad[0].payout = -Decimal::ONE;
    assert!(normalize(bad).is_err());
}
