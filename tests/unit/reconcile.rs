use super::*;
use serde_json::json;

fn d(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn fill(id: &str, shares: &str, finality: FillFinality) -> TradeFill {
    TradeFill {
        trade_id: id.into(),
        order_id: Some("777".into()),
        order_ids: vec!["777".into()],
        coin: Some("token".into()),
        shares: d(shares),
        price: d("0.5"),
        fee: Some(d("0.01")),
        fee_token: Some("USDC".into()),
        fee_rate_bps: None,
        finality,
        raw: json!({}),
    }
}

fn pm_evidence() -> FillEvidence {
    FillEvidence {
        poll: OrderPoll {
            found: true,
            status: "matched".into(),
            order_id: Some("777".into()),
            shares: Some(d("10")),
            associated_trades: vec!["one".into(), "two".into()],
            raw: json!({"associate_trades":["one","two"]}),
            ..Default::default()
        },
        page_complete: true,
        history_complete: true,
        expected_shares: Some(d("10")),
        pm_scan: None,
        pm_order_constraints: None,
        outcome_scan: None,
    }
}

#[test]
fn pm_requires_complete_final_trade_set_not_ack_or_order_quantity() {
    let evidence = pm_evidence();
    assert!(matches!(
        resolve_leg(POLYMARKET, &[], &evidence).unwrap(),
        LegResolution::Pending(_)
    ));
    let one = fill("one", "6", FillFinality::Confirmed);
    let mut two = fill("two", "4", FillFinality::Pending);
    assert_eq!(
        resolve_leg(POLYMARKET, &[one.clone(), two.clone()], &evidence).unwrap(),
        LegResolution::Pending("trade_confirmation_pending")
    );
    two.finality = FillFinality::Failed;
    let resolution = resolve_leg(POLYMARKET, &[one.clone(), two.clone(), one], &evidence).unwrap();
    assert!(
        matches!(resolution, LegResolution::Terminal {status:"matched", shares, fee, ..}
            if shares == d("6") && fee == d("0.01"))
    );
    let mut incomplete = evidence.clone();
    incomplete.page_complete = false;
    assert_eq!(
        resolve_leg(POLYMARKET, &[two], &incomplete).unwrap(),
        LegResolution::Pending("trade_pages_incomplete")
    );
}

#[test]
fn pm_all_failed_zero_requires_nonempty_complete_evidence() {
    let evidence = pm_evidence();
    assert!(matches!(resolve_leg(POLYMARKET, &[
            fill("one","6",FillFinality::Failed),fill("two","4",FillFinality::Failed)
        ], &evidence).unwrap(), LegResolution::Terminal{status:"failed",shares,..} if shares.is_zero()));
    let mut empty = evidence;
    empty.poll.associated_trades.clear();
    empty.poll.raw = json!({"associate_trades":[]});
    empty.poll.shares = Some(Decimal::ZERO);
    assert_eq!(
        resolve_leg(POLYMARKET, &[], &empty).unwrap(),
        LegResolution::Pending("zero_fill_not_proven")
    );
    empty.poll.status = "cancelled".into();
    assert!(matches!(
        resolve_leg(POLYMARKET, &[], &empty).unwrap(),
        LegResolution::Terminal {
            status: "cancelled",
            ..
        }
    ));
    empty.poll.raw = json!({"associate_trades":[null]});
    assert_eq!(
        resolve_leg(POLYMARKET, &[], &empty).unwrap(),
        LegResolution::Pending("associated_trades_missing")
    );
}

fn missing_pm_evidence(source: &str, ids: &[&str]) -> FillEvidence {
    let mut evidence = pm_evidence();
    evidence.poll = OrderPoll {
        status: "not_found".into(),
        order_id: Some("777".into()),
        raw: json!({"lookup_missing":source}),
        ..Default::default()
    };
    evidence.pm_scan = Some(PmTradeScan {
        version: 2,
        funder: "funder".into(),
        asset_id: "token".into(),
        order_id: "777".into(),
        after: 1_700_000_000,
        before: 1_700_000_300,
        next_cursor: "LTE=".into(),
        trade_ids: ids.iter().map(|id| (*id).into()).collect(),
    });
    evidence
}

#[test]
fn missing_order_retains_previously_known_trade_constraints() {
    let known = pm_evidence().poll;
    let constraints = pm_order_constraints(None, "777", "token", "funder", Some(&known)).unwrap();
    let mut evidence = missing_pm_evidence("null_body", &["one"]);
    evidence.pm_order_constraints = Some(constraints);
    let one = fill("one", "6", FillFinality::Confirmed);
    assert_eq!(
        resolve_leg(POLYMARKET, &[one.clone()], &evidence).unwrap(),
        LegResolution::Pending("known_associated_trade_missing")
    );
    evidence
        .pm_scan
        .as_mut()
        .unwrap()
        .trade_ids
        .push("two".into());
    let two = fill("two", "4", FillFinality::Failed);
    assert!(
        matches!(resolve_leg(POLYMARKET, &[one, two], &evidence).unwrap(),
            LegResolution::Terminal { status: "matched", shares, .. } if shares == d("6"))
    );
}

#[test]
fn order_constraints_restore_and_only_accumulate_reliable_fields() {
    let known = pm_evidence().poll;
    let mut partial = known.clone();
    partial.shares = Some(d("6"));
    partial.raw = json!({"associate_trades":["one"]});
    let info = json!({"order_poll": known, "fill_evidence":{"poll":partial}});
    let first = pm_order_constraints(Some(&info), "777", "token", "FUNDER", None).unwrap();
    assert_eq!(
        first.associated_trade_ids,
        BTreeSet::from(["one".into(), "two".into()])
    );
    assert_eq!(first.matched_shares_lower_bound, Some(d("10")));
    let mut weaker = partial.clone();
    weaker.raw = json!({"associate_trades":["three",null]});
    weaker.shares = Some(d("-1"));
    let stored = json!({"pm_order_constraints":first});
    assert_eq!(
        pm_order_constraints(Some(&stored), "777", "token", "funder", Some(&weaker)).unwrap(),
        first
    );
    weaker.raw = json!({"associate_trades":["three"]});
    weaker.shares = None;
    let grown =
        pm_order_constraints(Some(&stored), "777", "token", "funder", Some(&weaker)).unwrap();
    assert!(grown.associated_trade_ids.contains("three"));
    assert_eq!(grown.matched_shares_lower_bound, Some(d("10")));
    weaker.order_id = Some("other".into());
    assert_eq!(
        pm_order_constraints(Some(&stored), "777", "token", "funder", Some(&weaker)).unwrap(),
        first
    );
    for invalid in [json!({"version":99}), json!(false), json!({"version":1})] {
        assert!(pm_order_constraints(
            Some(&json!({"pm_order_constraints":invalid})),
            "777",
            "token",
            "funder",
            None
        )
        .is_err());
    }
    assert!(pm_order_constraints(Some(&stored), "other", "token", "funder", None).is_err());
    let empty = pm_order_constraints(
        Some(&json!({"pm_order_constraints":null})),
        "777",
        "token",
        "funder",
        None,
    )
    .unwrap();
    assert!(!empty.has_execution());
    let mut legacy: Value = serde_json::to_value(pm_evidence()).unwrap();
    legacy
        .as_object_mut()
        .unwrap()
        .remove("pm_order_constraints");
    assert!(serde_json::from_value::<FillEvidence>(legacy)
        .unwrap()
        .pm_order_constraints
        .is_none());
}

#[test]
fn known_matched_lower_bound_is_not_ack_or_original_quantity() {
    let mut known = pm_evidence().poll;
    known.raw = json!({});
    known.status = "live".into();
    let mut evidence = missing_pm_evidence("http_404", &["one"]);
    evidence.pm_order_constraints =
        Some(pm_order_constraints(None, "777", "token", "funder", Some(&known)).unwrap());
    let one = fill("one", "6", FillFinality::Confirmed);
    assert_eq!(
        resolve_leg(POLYMARKET, &[one.clone()], &evidence).unwrap(),
        LegResolution::Pending("known_matched_quantity_incomplete")
    );
    evidence
        .pm_scan
        .as_mut()
        .unwrap()
        .trade_ids
        .push("two".into());
    assert!(
        matches!(resolve_leg(POLYMARKET,&[one.clone(),fill("two","4",FillFinality::Failed)],&evidence).unwrap(),LegResolution::Terminal{shares,..} if shares==d("6"))
    );
    evidence.pm_scan.as_mut().unwrap().trade_ids.pop();
    evidence.pm_order_constraints = None;
    evidence.expected_shares = Some(d("100"));
    evidence.poll.original_shares = Some(d("100"));
    assert!(
        matches!(resolve_leg(POLYMARKET,&[one],&evidence).unwrap(),LegResolution::Terminal{shares,..} if shares==d("6"))
    );
}

#[test]
fn pm_order_terminal_whitelist_does_not_change_outcome_cancellation() {
    for (status, terminal) in [
        ("ORDER_STATUS_MATCHED", true),
        ("ORDER_STATUS_CANCELED_MARKET_RESOLVED", true),
        ("FUTURE_CANCELED", false),
        ("ORDER_STATUS_INVALID", false),
    ] {
        let mut evidence = pm_evidence();
        evidence.poll.status = status.into();
        let result = resolve_leg(
            POLYMARKET,
            &[
                fill("one", "6", FillFinality::Confirmed),
                fill("two", "4", FillFinality::Confirmed),
            ],
            &evidence,
        )
        .unwrap();
        assert_eq!(
            matches!(result, LegResolution::Terminal { .. }),
            terminal,
            "{status}"
        );
    }
    assert!(cancellation_status("futureCanceled"));
}

#[test]
fn missing_pm_order_finalizes_only_confirmed_quantity_or_all_failed() {
    for source in ["http_404", "null_body"] {
        for (states, expected_status, expected_shares, expected_fee) in [
            (
                [FillFinality::Confirmed, FillFinality::Confirmed],
                "matched",
                "10",
                "0.02",
            ),
            (
                [FillFinality::Confirmed, FillFinality::Failed],
                "matched",
                "6",
                "0.01",
            ),
            (
                [FillFinality::Failed, FillFinality::Failed],
                "failed",
                "0",
                "0",
            ),
        ] {
            let evidence = missing_pm_evidence(source, &["one", "two"]);
            let one = fill("one", "6", states[0]);
            let two = fill("two", "4", states[1]);
            let resolution = resolve_leg(POLYMARKET, &[one.clone(), two, one], &evidence).unwrap();
            assert!(
                matches!(resolution, LegResolution::Terminal { status, shares, fee, price, .. }
                    if status == expected_status && shares == d(expected_shares)
                        && fee == d(expected_fee) && price == if shares.is_zero() { Decimal::ZERO } else { d("0.5") })
            );
        }
    }
}

#[test]
fn missing_pm_order_requires_current_scan_and_finality() {
    let evidence = missing_pm_evidence("http_404", &["one"]);
    let confirmed = fill("one", "6", FillFinality::Confirmed);
    let mut incomplete = evidence.clone();
    incomplete.page_complete = false;
    incomplete.history_complete = false;
    incomplete.pm_scan.as_mut().unwrap().next_cursor = "next".into();
    assert_eq!(
        resolve_leg(POLYMARKET, &[confirmed.clone()], &incomplete).unwrap(),
        LegResolution::Pending("trade_pages_incomplete")
    );
    assert_eq!(
        resolve_leg(
            POLYMARKET,
            &[fill("one", "6", FillFinality::Pending)],
            &evidence
        )
        .unwrap(),
        LegResolution::Pending("trade_confirmation_pending")
    );
    let empty = missing_pm_evidence("null_body", &[]);
    assert_eq!(
        resolve_leg(POLYMARKET, &[confirmed.clone()], &empty).unwrap(),
        LegResolution::Pending("trade_scan_snapshot_mismatch")
    );
    assert_eq!(
        resolve_leg(POLYMARKET, &[], &evidence).unwrap(),
        LegResolution::Pending("trade_scan_snapshot_mismatch")
    );
    let old_fill = fill("old", "1", FillFinality::Confirmed);
    assert_eq!(
        resolve_leg(POLYMARKET, &[confirmed.clone(), old_fill], &evidence).unwrap(),
        LegResolution::Pending("trade_scan_snapshot_mismatch")
    );
    let mut no_fee = confirmed;
    no_fee.fee = None;
    assert_eq!(
        resolve_leg(POLYMARKET, &[no_fee], &evidence).unwrap(),
        LegResolution::Pending("fee_evidence_missing")
    );
}

#[test]
fn missing_pm_order_empty_scan_fails_only_without_execution_evidence() {
    for source in ["http_404", "null_body"] {
        let empty = missing_pm_evidence(source, &[]);
        let failed = LegResolution::Terminal {
            status: "failed",
            shares: Decimal::ZERO,
            price: Decimal::ZERO,
            fee: Decimal::ZERO,
            fee_sources: vec![],
        };
        assert_eq!(resolve_leg(POLYMARKET, &[], &empty).unwrap(), failed);
        for field in ["page", "history", "cursor"] {
            let mut incomplete = empty.clone();
            match field {
                "page" => incomplete.page_complete = false,
                "history" => incomplete.history_complete = false,
                _ => incomplete.pm_scan.as_mut().unwrap().next_cursor = "next".into(),
            }
            assert_eq!(
                resolve_leg(POLYMARKET, &[], &incomplete).unwrap(),
                LegResolution::Pending("trade_pages_incomplete")
            );
        }
        let mut constrained = empty.clone();
        let mut known = pm_order_constraints(None, "777", "token", "funder", None).unwrap();
        known.matched_shares_lower_bound = Some(Decimal::ZERO);
        constrained.pm_order_constraints = Some(known.clone());
        assert_eq!(resolve_leg(POLYMARKET, &[], &constrained).unwrap(), failed);
        known.matched_shares_lower_bound = Some(d("1"));
        constrained.pm_order_constraints = Some(known.clone());
        assert_eq!(
            resolve_leg(POLYMARKET, &[], &constrained).unwrap(),
            LegResolution::Pending("known_matched_quantity_incomplete")
        );
        known.associated_trade_ids.insert("known".into());
        constrained.pm_order_constraints = Some(known);
        assert_eq!(
            resolve_leg(POLYMARKET, &[], &constrained).unwrap(),
            LegResolution::Pending("known_associated_trade_missing")
        );
        for finality in [
            FillFinality::Confirmed,
            FillFinality::Failed,
            FillFinality::Pending,
        ] {
            assert_eq!(
                resolve_leg(POLYMARKET, &[fill("old", "1", finality)], &empty).unwrap(),
                LegResolution::Pending(if finality == FillFinality::Pending {
                    "trade_confirmation_pending"
                } else {
                    "trade_scan_snapshot_mismatch"
                })
            );
        }
    }
}

#[test]
fn missing_pm_order_rejects_wrong_identity_or_unproven_missing_response() {
    let evidence = missing_pm_evidence("http_404", &["one"]);
    let confirmed = fill("one", "6", FillFinality::Confirmed);
    for field in ["order", "token"] {
        let mut wrong = confirmed.clone();
        if field == "order" {
            wrong.order_id = Some("other".into());
        } else {
            wrong.coin = Some("other".into());
        }
        assert!(resolve_leg(POLYMARKET, &[wrong], &evidence).is_err());
    }
    let mut invalid = evidence.clone();
    invalid.pm_scan.as_mut().unwrap().before += 1;
    assert!(resolve_leg(POLYMARKET, &[confirmed.clone()], &invalid).is_err());
    let mut old = serde_json::to_value(&evidence).unwrap();
    old.as_object_mut().unwrap().remove("pm_scan");
    let old: FillEvidence = serde_json::from_value(old).unwrap();
    assert_eq!(
        resolve_leg(POLYMARKET, &[confirmed.clone()], &old).unwrap(),
        LegResolution::Pending("trade_scan_evidence_missing")
    );
    for source in ["http_500", "timeout", "malformed"] {
        let missing = missing_pm_evidence(source, &["one"]);
        assert_eq!(
            resolve_leg(POLYMARKET, &[confirmed.clone()], &missing).unwrap(),
            LegResolution::Pending("order_not_found")
        );
    }
    assert_eq!(
        resolve_leg(OUTCOME, &[confirmed], &evidence).unwrap(),
        LegResolution::Pending("order_not_found")
    );
}

#[test]
fn found_pm_order_still_requires_order_state_associations_and_quantity() {
    let fills = [
        fill("one", "6", FillFinality::Confirmed),
        fill("two", "4", FillFinality::Failed),
    ];
    for (field, expected) in [
        ("state", "order_still_open"),
        ("associations", "associated_trades_missing"),
        ("missing", "associated_trade_missing"),
        ("extra", "order_trade_snapshot_mismatch"),
        ("quantity", "matched_quantity_incomplete"),
    ] {
        let mut evidence = pm_evidence();
        evidence.pm_scan = missing_pm_evidence("http_404", &["one", "two"]).pm_scan;
        match field {
            "state" => evidence.poll.status = "live".into(),
            "associations" => evidence.poll.raw = json!({}),
            "missing" => {
                evidence.poll.associated_trades.push("three".into());
                evidence.poll.raw = json!({"associate_trades":["one","two","three"]});
            }
            "extra" => {
                evidence.poll.associated_trades = vec!["one".into()];
                evidence.poll.raw = json!({"associate_trades":["one"]});
            }
            _ => evidence.poll.shares = Some(d("11")),
        }
        assert_eq!(
            resolve_leg(POLYMARKET, &fills, &evidence).unwrap(),
            LegResolution::Pending(expected)
        );
    }
}

#[test]
fn outcome_does_not_use_remaining_size_or_incomplete_history_as_zero_fill() {
    let mut evidence = FillEvidence {
        poll: OrderPoll {
            found: true,
            status: "canceled".into(),
            order_id: Some("777".into()),
            original_shares: Some(d("10")),
            remaining_shares: Some(d("4")),
            ..Default::default()
        },
        page_complete: true,
        history_complete: false,
        expected_shares: None,
        pm_scan: None,
        pm_order_constraints: None,
        outcome_scan: None,
    };
    assert_eq!(
        resolve_leg(OUTCOME, &[], &evidence).unwrap(),
        LegResolution::Pending("fill_history_incomplete")
    );
    let one = fill("one", "6", FillFinality::Confirmed);
    assert_eq!(
        resolve_leg(OUTCOME, &[one.clone()], &evidence).unwrap(),
        LegResolution::Pending("fill_history_incomplete")
    );
    evidence.history_complete = true;
    assert_eq!(
        resolve_leg(OUTCOME, &[one.clone()], &evidence).unwrap(),
        LegResolution::Pending("fill_history_incomplete")
    );
    evidence.outcome_scan = Some(
        serde_json::from_value(json!({
            "version":2,"tokenId":"#5160","submittedAtMs":100,"endTime":1000,
            "cursor":100,"seenIds":[],"historyChecked":true,"historyLowerBound":99,
            "historyComplete":true,"scannedCount":1,"complete":true,"phase":"complete",
            "account":"0xaccount","terminalObservedAtMs":999,"scanValid":true
        }))
        .unwrap(),
    );
    assert!(
        matches!(resolve_leg(OUTCOME,&[one.clone()],&evidence).unwrap(),LegResolution::Terminal{shares,..} if shares==d("6"))
    );
    let reloaded: FillEvidence =
        serde_json::from_value(serde_json::to_value(&evidence).unwrap()).unwrap();
    assert_eq!(
        resolve_leg(OUTCOME, &[one.clone()], &reloaded).unwrap(),
        LegResolution::Pending("fill_history_incomplete")
    );
    evidence.history_complete = false;
    evidence.expected_shares = Some(d("6"));
    assert!(
        matches!(resolve_leg(OUTCOME,&[one],&evidence).unwrap(),LegResolution::Terminal{shares,..} if shares==d("6"))
    );
    evidence.poll.found = false;
    assert_eq!(
        resolve_leg(OUTCOME, &[], &evidence).unwrap(),
        LegResolution::Pending("order_not_found")
    );
}

#[test]
fn actual_zero_is_not_missing_fee_and_unknown_currency_waits() {
    let mut trade = fill("one", "100", FillFinality::Confirmed);
    trade.fee = Some(Decimal::ZERO);
    assert_eq!(
        accounting_fee(OUTCOME, &trade).unwrap(),
        Some((Decimal::ZERO, "actual"))
    );
    trade.fee = None;
    assert_eq!(accounting_fee(OUTCOME, &trade).unwrap(), None);
    trade.fee = Some(d("0.01"));
    trade.fee_token = Some("OTHER".into());
    assert_eq!(accounting_fee(OUTCOME, &trade).unwrap(), None);
}

#[test]
fn pm_unverified_taker_rate_does_not_become_actual_fee() {
    let mut trade = fill("one", "100", FillFinality::Confirmed);
    trade.fee = None;
    trade.fee_token = None;
    trade.fee_rate_bps = Some(d("700"));
    trade.raw = json!({"role":"taker"});
    assert_eq!(accounting_fee(POLYMARKET, &trade).unwrap(), None);
    trade.raw = json!({"role":"maker"});
    assert_eq!(
        accounting_fee(POLYMARKET, &trade).unwrap(),
        Some((Decimal::ZERO, "calculated_maker_zero"))
    );
}

#[test]
fn approved_market_snapshot_calculates_without_strategy_multiplier() {
    let mut trade = fill("one", "100", FillFinality::Confirmed);
    trade.fee = None;
    trade.fee_token = None;
    trade.raw = json!({"role":"taker","fee_calculation":{
        "source":"clob-markets","rate":"0.07",
        "condition_id":"condition","observed_at_ms":1,"currency":"pUSD",
        "valuation":"1 USD","rounding":"midpoint_away_from_zero_5dp"
    }});
    assert_eq!(
        accounting_fee(POLYMARKET, &trade).unwrap(),
        Some((d("1.75"), "calculated"))
    );
    trade.raw["fee_calculation"]["rate"] = json!("0.05");
    assert_eq!(
        accounting_fee(POLYMARKET, &trade).unwrap(),
        Some((d("1.25"), "calculated"))
    );
    trade.raw["fee_calculation"]["rate"] = json!("0.0000006");
    assert_eq!(
        accounting_fee(POLYMARKET, &trade).unwrap(),
        Some((d("0.00002"), "calculated")),
        "round half away from zero at five places"
    );
    let previous = trade.clone();
    trade.raw["fee_calculation"]["rate"] = json!("0.01");
    assert_eq!(
        merge_observation(&previous, &trade).unwrap().raw["fee_calculation"],
        previous.raw["fee_calculation"]
    );
    trade.fee = Some(d("0.3"));
    trade.fee_token = Some("pUSD".into());
    assert_eq!(
        accounting_fee(POLYMARKET, &trade).unwrap(),
        Some((d("0.3"), "actual"))
    );
    trade.fee = None;
    trade.raw["fee_calculation"]["rate"] = json!("-1");
    assert!(accounting_fee(POLYMARKET, &trade).is_err());
}

fn pm_fee_snapshot(source: &str, rate: &str) -> Value {
    let mut snapshot = json!({
        "version": 1, "source": source, "rate": rate,
        "bps": (d(rate) * Decimal::from(10_000)).to_string(),
        "condition_id": "condition", "token_id": "token",
        "event_id": "event", "unified_index": 0, "fetched_at_ms": 1
    });
    if source == "env" {
        snapshot["config_key"] = json!("PM_FEE_BPS");
        snapshot["fallback_reason"] = json!("common_unavailable");
    }
    snapshot
}

#[test]
fn common_and_env_snapshots_calculate_and_restore_without_actual_fee() {
    for source in ["common", "env"] {
        for (rate, expected) in [
            ("0", "0"),
            ("0.07", "1.75"),
            ("1", "25"),
            ("0.0000006", "0.00002"),
        ] {
            let mut trade = fill("one", "100", FillFinality::Confirmed);
            trade.fee = None;
            trade.fee_token = None;
            trade.raw = json!({"role": "taker", "fee_calculation": pm_fee_snapshot(source, rate)});
            assert_eq!(
                accounting_fee(POLYMARKET, &trade).unwrap(),
                Some((d(expected), "calculated")),
                "{source} {rate}"
            );
            restore_pm_fee_rate(&mut trade).unwrap();
            assert_eq!(trade.fee_rate_bps, Some(d(rate) * Decimal::from(10_000)));
            assert_eq!(trade.fee, None);
            assert_eq!(trade.fee_token, None);
        }
    }
}

#[test]
fn invalid_snapshots_fail_validation_without_mutating_legacy_rate() {
    for source in ["common", "env"] {
        for (field, value) in [
            ("source", json!("rest")),
            ("source", Value::Null),
            ("rate", json!("-0.01")),
            ("rate", json!("1.01")),
            ("rate", json!("broken")),
            ("rate", Value::Null),
            ("version", json!(2)),
            ("version", json!("1")),
            ("version", Value::Null),
            ("bps", json!("701")),
            ("bps", Value::Null),
            ("token_id", json!("other")),
            ("token_id", Value::Null),
            ("condition_id", json!("  ")),
            ("condition_id", Value::Null),
        ] {
            let mut trade = fill("one", "100", FillFinality::Confirmed);
            trade.fee = None;
            trade.fee_rate_bps = Some(d("123"));
            trade.raw =
                json!({"role": "taker", "fee_calculation": pm_fee_snapshot(source, "0.07")});
            trade.raw["fee_calculation"][field] = value;
            assert!(
                accounting_fee(POLYMARKET, &trade).is_err(),
                "{source} {field}"
            );
            assert!(restore_pm_fee_rate(&mut trade).is_err(), "{source} {field}");
            assert_eq!(trade.fee_rate_bps, Some(d("123")));
        }
        let mut trade = fill("one", "100", FillFinality::Confirmed);
        trade.fee = None;
        trade.coin = None;
        trade.raw["fee_calculation"] = pm_fee_snapshot(source, "0.07");
        assert!(accounting_fee(POLYMARKET, &trade).is_err());
        assert!(restore_pm_fee_rate(&mut trade).is_err());
    }
}

#[test]
fn actual_fee_and_maker_zero_precede_invalid_calculation_snapshot() {
    let mut trade = fill("one", "100", FillFinality::Confirmed);
    trade.raw = json!({"role": "maker", "fee_calculation": {"source": "invalid"}});
    trade.fee_token = Some("pUSD".into());
    assert_eq!(
        accounting_fee(POLYMARKET, &trade).unwrap(),
        Some((d("0.01"), "actual"))
    );
    trade.raw["role"] = json!("taker");
    assert_eq!(
        accounting_fee(POLYMARKET, &trade).unwrap(),
        Some((d("0.01"), "actual"))
    );
    trade.fee_token = Some("OTHER".into());
    assert_eq!(accounting_fee(POLYMARKET, &trade).unwrap(), None);
    trade.fee = None;
    trade.raw["role"] = json!("maker");
    assert_eq!(
        accounting_fee(POLYMARKET, &trade).unwrap(),
        Some((Decimal::ZERO, "calculated_maker_zero"))
    );
}

#[test]
fn restored_fee_rates_keep_legacy_values_and_maker_zero_rule() {
    let mut trade = fill("one", "100", FillFinality::Confirmed);
    trade.fee = None;
    trade.raw = json!({"role": "taker"});
    for legacy in [None, Some(d("700"))] {
        trade.fee_rate_bps = legacy;
        restore_pm_fee_rate(&mut trade).unwrap();
        assert_eq!(trade.fee_rate_bps, legacy);
        assert_eq!(accounting_fee(POLYMARKET, &trade).unwrap(), None);
    }
    trade.raw["role"] = json!("maker");
    restore_pm_fee_rate(&mut trade).unwrap();
    assert_eq!(trade.fee_rate_bps, Some(Decimal::ZERO));
    trade.fee = Some(d("0.01"));
    trade.fee_rate_bps = Some(d("123"));
    restore_pm_fee_rate(&mut trade).unwrap();
    assert_eq!(trade.fee_rate_bps, Some(d("123")));
    trade.raw["fee_calculation"] = json!({"source": "clob-markets", "rate": "0.07"});
    restore_pm_fee_rate(&mut trade).unwrap();
    assert_eq!(trade.fee_rate_bps, Some(d("700")));
    assert_eq!(trade.fee, Some(d("0.01")));
}

#[test]
fn confirmed_merge_freezes_snapshot_and_restores_its_bps() {
    for source in ["common", "env", "clob-markets"] {
        let mut previous = fill("one", "100", FillFinality::Confirmed);
        previous.fee = None;
        previous.fee_token = None;
        previous.raw = json!({"role": "taker", "fee_calculation": pm_fee_snapshot(source, "0.07")});
        if source == "clob-markets" {
            previous.raw["fee_calculation"] = json!({"source": source, "rate": "0.07"});
        }
        let mut incoming = previous.clone();
        incoming.raw["fee_calculation"] = pm_fee_snapshot("env", "0.05");
        incoming.fee_rate_bps = Some(d("500"));
        let merged = merge_observation(&previous, &incoming).unwrap();
        assert_eq!(
            merged.raw["fee_calculation"],
            previous.raw["fee_calculation"]
        );
        assert_eq!(merged.fee_rate_bps, Some(d("700")));
        assert_eq!(merged.fee, None);
        assert_eq!(
            accounting_fee(POLYMARKET, &merged).unwrap(),
            Some((d("1.75"), "calculated"))
        );
        incoming
            .raw
            .as_object_mut()
            .unwrap()
            .remove("fee_calculation");
        incoming.fee_rate_bps = None;
        let merged = merge_observation(&previous, &incoming).unwrap();
        assert_eq!(merged.fee_rate_bps, Some(d("700")));
        assert_eq!(
            merged.raw["fee_calculation"],
            previous.raw["fee_calculation"]
        );
    }
}

#[test]
fn confirmed_observation_does_not_regress_or_silently_change() {
    let confirmed = fill("one", "5", FillFinality::Confirmed);
    let mut incoming = confirmed.clone();
    incoming.finality = FillFinality::Pending;
    assert_eq!(
        merge_observation(&confirmed, &incoming).unwrap().finality,
        FillFinality::Confirmed
    );
    incoming.finality = FillFinality::Failed;
    assert!(merge_observation(&confirmed, &incoming).is_err());
    incoming.finality = FillFinality::Confirmed;
    incoming.shares = d("4");
    assert!(merge_observation(&confirmed, &incoming).is_err());
}
