use super::*;
fn d(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}
fn leg(id: i64, side: &str, qty: &str) -> ProjectionLeg {
    ProjectionLeg {
        id,
        status: "completed".into(),
        side: side.into(),
        platform: OUTCOME.into(),
        label: "yes".into(),
        token_id: "#5160".into(),
        wallet_address: Some("wallet-a".into()),
        submitted_at: DateTime::from_timestamp(1000 + id, 0),
        last_order_info: Some(
            json!({"fee_estimate":{"version":1,"fee_model":"out_usdc_taker_close_v1","outcome_id":"516","token_ids":["#5160","#5161"],"taker_rate":"0.001344","builder_rate":"0.0003","settlement_builder_rate":"0","user_fees_fetched_at":"2000-01-01T00:00:00Z","outcome_meta_fetched_at":"2000-01-01T00:00:00Z"}}),
        ),
        actual_shares: Some(d(qty)),
        actual_price: Some(d("0.5")),
        actual_fee: Some(Decimal::ZERO),
    }
}
fn fee(rows: &[ProjectionLeg]) -> Decimal {
    let p = project(rows);
    let (cost, rev, profit) = p.actuals().unwrap();
    assert_eq!(rev - cost, profit);
    Decimal::from_str(p.evidence()["estimated_fee"].as_str().unwrap()).unwrap()
}
#[test]
fn reserve_tracks_remaining_without_cumulative_deduction() {
    let buy = leg(1, "BUY", "30");
    assert_eq!(fee(std::slice::from_ref(&buy)), d("0.04932"));
    assert_eq!(fee(std::slice::from_ref(&buy)), d("0.04932"));
    assert_eq!(
        project(std::slice::from_ref(&buy)).actuals().unwrap().1,
        -d("0.04932")
    );
    assert_eq!(fee(&[buy.clone(), leg(2, "SELL", "15")]), d("0.02466"));
    let mut sell = leg(2, "SELL", "30");
    sell.last_order_info = None;
    let mut buy = buy;
    buy.last_order_info = None;
    assert_eq!(fee(&[buy, sell]), Decimal::ZERO);
}
#[test]
fn snapshot_selection_is_frozen_and_wallet_token_scoped() {
    let buy = leg(1, "BUY", "30");
    let mut sell = leg(2, "SELL", "15");
    sell.last_order_info.as_mut().unwrap()["fee_estimate"]["builder_rate"] = json!("0.001");
    assert_eq!(fee(&[buy.clone(), sell.clone()]), d("0.03516"));
    sell.wallet_address = Some("wallet-b".into());
    assert!(project(&[buy.clone(), sell]).actuals().is_none());
    let mut other = leg(3, "BUY", "2");
    other.token_id = "#5161".into();
    other.last_order_info = None;
    assert!(project(&[buy, other]).actuals().is_none());
}
#[test]
fn malformed_or_unresolved_evidence_never_defaults_to_zero() {
    for field in [
        "version",
        "fee_model",
        "outcome_id",
        "token_ids",
        "taker_rate",
        "builder_rate",
        "user_fees_fetched_at",
    ] {
        let mut buy = leg(1, "BUY", "30");
        buy.last_order_info.as_mut().unwrap()["fee_estimate"][field] = json!("invalid");
        assert!(project(&[buy]).actuals().is_none(), "{field}");
    }
    for rate in ["-0.1", "1.1", "NaN"] {
        let mut buy = leg(1, "BUY", "30");
        buy.last_order_info.as_mut().unwrap()["fee_estimate"]["taker_rate"] = json!(rate);
        assert!(project(&[buy]).actuals().is_none());
    }
    let mut buy = leg(1, "BUY", "0");
    buy.last_order_info = None;
    assert_eq!(fee(&[buy.clone()]), Decimal::ZERO);
    buy.actual_shares = None;
    assert!(project(&[buy.clone()]).actuals().is_none());
    buy.actual_shares = Some(Decimal::ZERO);
    buy.status = "pending".into();
    assert!(project(&[buy]).actuals().is_none());
}
#[test]
fn failed_and_cancelled_partial_fills_are_real_trades() {
    let mut buy = leg(1, "BUY", "30");
    buy.status = "failed".into();
    let mut sell = leg(2, "SELL", "15");
    sell.status = "cancelled".into();
    assert_eq!(fee(&[buy, sell]), d("0.02466"));
}
#[test]
fn actual_payout_zero_and_fraction_replace_the_reserve_basis() {
    let buy = leg(1, "BUY", "30");
    assert_eq!(fee(std::slice::from_ref(&buy)), d("0.04932"));
    let rows = vec![(
        "BUY".into(),
        OUTCOME.into(),
        buy.token_id.clone(),
        d("30"),
        d("0.5"),
        Decimal::ZERO,
    )];
    for payout in [Decimal::ZERO, d("0.37"), Decimal::ONE] {
        let payouts =
            std::collections::HashMap::from([((OUTCOME.into(), buy.token_id.clone()), payout)]);
        let (cost, gross, profit) = super::super::compute_settled_actuals(&rows, &payouts).unwrap();
        assert_eq!(cost, d("15"));
        assert_eq!(gross, d("30") * payout);
        assert_eq!(profit, gross - cost);
    }
}
#[test]
fn source_order_uses_submission_then_leg_id_not_snapshot_age() {
    let mut older = leg(1, "BUY", "10");
    let mut newer = leg(2, "BUY", "20");
    newer.last_order_info.as_mut().unwrap()["fee_estimate"]["taker_rate"] = json!("0.002");
    older.submitted_at = newer.submitted_at;
    assert_eq!(fee(&[newer.clone(), older.clone()]), d("0.069"));
    newer.last_order_info.as_mut().unwrap()["fee_estimate"]["version"] = json!(2);
    assert_eq!(fee(&[newer, older]), d("0.04932"));
}
fn fallback(now: DateTime<Utc>) -> FallbackContext {
    let frozen = leg(1, "BUY", "30");
    BTreeMap::from([(
        ("wallet-a".into(), "#5160".into()),
        LatestFee {
            wallet: "wallet-a".into(),
            token: "#5160".into(),
            snapshot: frozen.last_order_info.unwrap()["fee_estimate"].clone(),
            valid_until: now + chrono::Duration::seconds(100),
        },
    )])
}

#[test]
fn fallback_only_missing_submitted_evidence_and_never_mutates_legs() {
    let now = Utc::now();
    let context = fallback(now);
    for info in [None, Some(json!({})), Some(json!({"other":"evidence"}))] {
        let mut buy = leg(1, "BUY", "30");
        buy.last_order_info = info.clone();
        let result = project_with_fallback(&[buy.clone()], &context, now);
        assert!(result.actuals().is_some());
        assert_eq!(
            result.evidence()["groups"][0]["source_kind"],
            "latest_valid_fallback"
        );
        assert!(result.evidence()["groups"][0]
            .get("source_leg_id")
            .is_none());
        assert!(result.evidence()["fallback_valid_until"].is_i64());
        assert_eq!(buy.last_order_info, info);
        assert!(project(&[buy.clone()]).actuals().is_none());
        buy.submitted_at = None;
        assert!(project_with_fallback(&[buy], &context, now)
            .actuals()
            .is_none());
    }
    for info in [
        json!(null),
        json!([]),
        json!("bad"),
        json!({"fee_estimate":null}),
        json!({"fee_estimate":{}}),
    ] {
        let mut buy = leg(1, "BUY", "30");
        buy.last_order_info = Some(info);
        assert!(project_with_fallback(&[buy], &context, now)
            .actuals()
            .is_none());
    }
    let mut bad = leg(2, "BUY", "3");
    bad.last_order_info = Some(json!({"fee_estimate":null}));
    let frozen = leg(1, "BUY", "30");
    let result = project_with_fallback(&[frozen, bad], &context, now);
    assert_eq!(result.evidence()["source_kind"], "frozen");
}

#[test]
fn fallback_identity_expiry_and_mixed_groups() {
    let now = Utc::now();
    let mut buy = leg(1, "BUY", "30");
    buy.last_order_info = None;
    let mut context = fallback(now);
    assert!(project_with_fallback(&[buy.clone()], &BTreeMap::new(), now)
        .actuals()
        .is_none());
    let key = ("wallet-a".into(), "#5160".into());
    for field in ["wallet", "token", "expiry", "rules"] {
        let mut invalid = context.clone();
        let value = invalid.get_mut(&key).unwrap();
        match field {
            "wallet" => value.wallet = "wallet-b".into(),
            "token" => value.token = "#5161".into(),
            "expiry" => value.valid_until = now,
            _ => value.snapshot["fee_model"] = json!("unsupported"),
        }
        assert!(project_with_fallback(&[buy.clone()], &invalid, now)
            .actuals()
            .is_none());
    }
    let mut other = leg(2, "BUY", "10");
    other.token_id = "#5161".into();
    let mixed = project_with_fallback(&[buy.clone(), other.clone()], &context, now);
    assert_eq!(
        mixed.evidence()["groups"][0]["source_kind"],
        "latest_valid_fallback"
    );
    assert_eq!(mixed.evidence()["groups"][1]["source_kind"], "frozen");
    other.last_order_info = None;
    assert!(
        project_with_fallback(&[buy.clone(), other.clone()], &context, now)
            .actuals()
            .is_none()
    );
    let mut second = context[&key].clone();
    second.token = "#5161".into();
    second.valid_until = now + chrono::Duration::seconds(20);
    context.insert(("wallet-a".into(), "#5161".into()), second);
    assert_eq!(
        project_with_fallback(&[buy, other], &context, now).evidence()["fallback_valid_until"],
        json!((now + chrono::Duration::seconds(20)).timestamp())
    );
}

#[test]
fn fallback_rates_recompute_remaining_reserve_without_accumulation() {
    let now = Utc::now();
    let mut context = fallback(now);
    let mut buy = leg(1, "BUY", "30");
    buy.last_order_info = None;
    let mut sell = leg(2, "SELL", "15");
    sell.last_order_info = None;
    let rows = [buy.clone(), sell.clone()];
    let first = project_with_fallback(&rows, &context, now);
    assert_eq!(
        serde_json::from_value::<Decimal>(first.evidence()["estimated_fee"].clone()).unwrap(),
        d("0.02466")
    );
    assert_eq!(
        first.actuals(),
        project_with_fallback(&rows, &context, now).actuals()
    );
    context.values_mut().next().unwrap().snapshot["taker_rate"] = json!("0.002");
    assert_eq!(
        serde_json::from_value::<Decimal>(
            project_with_fallback(&rows, &context, now).evidence()["estimated_fee"].clone()
        )
        .unwrap(),
        d("0.0345")
    );
    sell.actual_shares = Some(d("30"));
    assert_eq!(
        project_with_fallback(&[buy, sell], &BTreeMap::new(), now).evidence()["status"],
        "not_applicable"
    );
}

#[test]
fn unknown_warning_only_on_status_or_reason_change() {
    let old = json!({"status":"unknown","reason":"missing snapshot"});
    assert!(!unknown_changed(&old, "missing snapshot"));
    assert!(unknown_changed(&old, "unresolved leg"));
    assert!(unknown_changed(
        &json!({"status":"estimated"}),
        "missing snapshot"
    ));
    assert!(unknown_changed(&json!({}), "missing snapshot"));
}
