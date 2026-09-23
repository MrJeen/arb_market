use super::*;
use serde_json::json;
fn setup(action: &str, side: &str) -> (Baseline, Vec<Leg>) {
    let at = DateTime::from_timestamp(1000, 0).unwrap();
    let old = Leg {
        valuation: ProjectionLeg {
            id: 1,
            status: "completed".into(),
            side: "BUY".into(),
            platform: "polymarket".into(),
            label: "yes".into(),
            token_id: "token".into(),
            wallet_address: None,
            submitted_at: Some(at - chrono::Duration::seconds(50)),
            last_order_info: None,
            actual_shares: Some(Decimal::TEN),
            actual_price: Some(Decimal::new(5, 1)),
            actual_fee: Some(Decimal::ZERO),
        },
        claim_id: None,
        intent: "arb_buy".into(),
        created_at: at - chrono::Duration::seconds(100),
        funder: Some("funder".into()),
        req_shares: Decimal::TEN,
        req_price: Decimal::ONE,
        req_fee: Decimal::ZERO,
        observations: json!([]),
        execution_safe: true,
    };
    let p = actuals::project(std::slice::from_ref(&old.valuation));
    let (c, r, pf) = p.actuals().unwrap();
    let baseline = Baseline::capture(
        Uuid::new_v4(),
        action,
        at,
        (Some(c), Some(r), Some(pf)),
        p.evidence(),
        vec![old.clone()],
    )
    .unwrap();
    let mut current = old.clone();
    current.valuation.id = 2;
    current.valuation.side = side.into();
    current.valuation.status = "pending".into();
    current.valuation.submitted_at = None;
    current.valuation.actual_shares = None;
    current.valuation.actual_price = None;
    current.valuation.actual_fee = None;
    current.created_at = at;
    current.claim_id = Some(baseline.claim_id);
    current.intent = action.into();
    (baseline, vec![old, current])
}
fn refresh(b: &mut Baseline, legs: &[Leg]) {
    b.refresh(
        Some(b.claim_id),
        Some(&b.action.clone()),
        Some(b.claimed_at),
        legs,
    );
}
fn value(b: &Baseline, seconds: i64) -> Option<Decimal> {
    b.profit_at(
        b.claim_id,
        &b.action,
        b.claimed_at,
        b.claimed_at + chrono::Duration::seconds(seconds),
        Duration::from_secs(300),
        Duration::from_secs(300),
    )
}
#[test]
fn normal_wait_matrix_fixed_baseline_and_deadline() {
    for (action, side) in [
        ("take_profit", "SELL"),
        ("rebalance", "BUY"),
        ("rebalance", "SELL"),
    ] {
        let (mut b, mut legs) = setup(action, side);
        refresh(&mut b, &legs);
        assert_eq!(value(&b, 299), Some(b.profit));
        assert_eq!(value(&b, 300), None);
        legs[1].valuation.status = "actived".into();
        legs[1].valuation.submitted_at = Some(b.claimed_at + chrono::Duration::seconds(250));
        refresh(&mut b, &legs);
        assert_eq!(value(&b, 549), Some(b.profit));
        assert_eq!(value(&b, 550), None);
        legs[1].valuation.submitted_at = Some(b.claimed_at + chrono::Duration::seconds(500));
        refresh(&mut b, &legs);
        assert_eq!(value(&b, 599), Some(b.profit));
        assert_eq!(value(&b, 600), None);
        let mut done = legs[1].clone();
        done.valuation.id = 3;
        done.valuation.status = "completed".into();
        done.valuation.actual_shares = Some(Decimal::ONE);
        done.valuation.actual_price = Some(Decimal::new(5, 1));
        done.valuation.actual_fee = Some(Decimal::ZERO);
        legs.push(done);
        refresh(&mut b, &legs);
        assert_eq!(value(&b, 599), Some(b.profit));
    }
}
#[test]
fn all_legs_bad_evidence_matrix_is_permanent() {
    for case in 0..21 {
        let (mut b, mut legs) = setup("rebalance", "BUY");
        // Pending is first: no early success can conceal later bad evidence.
        legs.swap(0, 1);
        match case {
            0 => legs[0].valuation.status = "unknown".into(),
            1 => legs[0].claim_id = Some(Uuid::new_v4()),
            2 => legs[0].intent = "arb_buy".into(),
            3 => legs[0].valuation.actual_shares = Some(-Decimal::ONE),
            4 => legs[0].valuation.actual_price = Some(Decimal::TEN),
            5 => legs[0].valuation.actual_fee = Some(-Decimal::ONE),
            6 => legs[0].execution_safe = false,
            7 => legs[1].valuation.actual_price = None,
            8 => legs[1].valuation.status = "pending".into(),
            9 => legs[1].valuation.token_id = "changed".into(),
            10 => legs[1].created_at = b.claimed_at,
            11 => legs[1].observations = json!([{"new":true}]),
            12 => {
                legs.remove(1);
            }
            13 => legs[0].valuation.status = "completed".into(),
            14 => legs[0].valuation.platform = "outcome".into(),
            15 => legs[0].valuation.status = "actived".into(),
            17 => legs[0].valuation.token_id = "other-token".into(),
            18 => legs[0].funder = Some("other-funder".into()),
            19 => legs[0].valuation.wallet_address = Some("other-wallet".into()),
            20 => legs[0].valuation.label = "other-label".into(),
            _ => legs[0].created_at = b.claimed_at - chrono::Duration::seconds(1),
        }
        refresh(&mut b, &legs);
        assert!(value(&b, 1).is_none(), "case {case}");
        let (_, mut restored) = setup("rebalance", "BUY");
        restored[1].claim_id = Some(b.claim_id);
        refresh(&mut b, &restored);
        assert!(value(&b, 1).is_none());
    }
}
#[test]
fn capture_respects_postgres_cost_scale_without_rounding_profit() {
    let (prior, mut legs) = setup("rebalance", "BUY");
    legs[0].valuation.actual_shares = Some("1.12345678".parse().unwrap());
    legs[0].valuation.actual_price = Some("0.12345678".parse().unwrap());
    let p = actuals::project(&[legs[0].valuation.clone()]);
    let (cost, rev, profit) = p.actuals().unwrap();
    let stored =
        cost.round_dp_with_strategy(8, rust_decimal::RoundingStrategy::MidpointAwayFromZero);
    assert_ne!(stored, cost);
    let mut b = Baseline::capture(
        prior.claim_id,
        "rebalance",
        prior.claimed_at,
        (Some(stored), Some(rev), Some(profit)),
        p.evidence(),
        vec![legs[0].clone()],
    )
    .unwrap();
    refresh(&mut b, &legs);
    assert_eq!(value(&b, 1), Some(profit));
    assert_eq!(b.cost, cost);
}

#[test]
fn corrupt_baseline_and_claim_never_authorize() {
    let (mut b, legs) = setup("take_profit", "SELL");
    refresh(&mut b, &legs);
    for bad in [json!(null), json!([]), json!(true), json!({"version":"1"})] {
        assert!(parse(&bad).is_none());
    }
    for field in [
        "version",
        "profit",
        "claimed_at",
        "eligible",
        "waiting",
        "legs",
    ] {
        let mut encoded = serde_json::to_value(&b).unwrap();
        encoded[field] = json!("broken");
        assert!(parse(&encoded).is_none_or(|v| value(&v, 1).is_none()));
    }
    assert!(b
        .profit_at(
            Uuid::new_v4(),
            &b.action,
            b.claimed_at,
            b.claimed_at,
            Duration::from_secs(300),
            Duration::from_secs(300)
        )
        .is_none());
    let mut encoded = serde_json::to_value(&b).unwrap();
    encoded["profit"] = json!("999");
    assert!(value(&parse(&encoded).unwrap(), 1).is_none());
}
