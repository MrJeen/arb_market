use super::*;
const WALLET: &str = "0x1111111111111111111111111111111111111111";
fn event(tid: u64, time: u64) -> Value {
    json!({"dir":"Settlement","coin":"#5160","tid":tid,"hash":"0xabc","time":time,
            "px":"0","sz":"2","fee":"0","feeToken":"USDC"})
}
#[test]
fn same_millisecond_position_chain_and_partial_settlements() {
    let buy = |tid, start: &str| {
        json!({"coin":"#5160","dir":"Buy","side":"B",
            "oid":tid,"tid":tid,"sz":"2","px":"0.4","startPosition":start,"time":100})
    };
    let state = SettlementScan::new(WALLET, "#5160", 99, 200).unwrap();
    let page = apply_page(
        &json!([buy(1, "2"), buy(90, "0"), event(100, 101), event(101, 101)]),
        &state,
    )
    .unwrap();
    let done = apply_page(&json!([event(200, 98)]), &page.progress)
        .unwrap()
        .progress;
    assert!(validate_ownership(&done, &done.trades).is_ok());
    let mut residual = done.clone();
    residual.events.pop();
    assert!(validate_ownership(&residual, &residual.trades).is_err());
    let mut ambiguous = done.clone();
    ambiguous.trades[0].start_position = Decimal::ZERO;
    assert!(validate_ownership(&ambiguous, &ambiguous.trades).is_err());
}
#[test]
fn distinct_usdc_deltas_share_transaction_hash() {
    let rows = json!([
        {"time":100,"hash":"same","delta":{"type":"accountActivationGas","amount":"1.0","token":"USDC"}},
        {"time":100,"hash":"same","delta":{"type":"send","amount":"2","token":"USDC"}}
    ]);
    assert!(validate_transfer_ledger(&rows, "#5160", 99, 200).is_ok());
    let rows: Vec<_> = (0..500).map(|_| rows[0].clone()).collect();
    assert!(validate_transfer_ledger(&json!(rows), "#5160", 99, 200).is_err());
}
#[test]
fn strict_settlement_allows_zero_price() {
    assert_eq!(
        parse_settlement_event(&event(1, 100), "#5160").unwrap().px,
        Decimal::ZERO
    );
    for key in [
        "dir", "coin", "tid", "hash", "time", "px", "sz", "fee", "feeToken",
    ] {
        let mut bad = event(1, 100);
        bad.as_object_mut().unwrap().remove(key);
        assert!(parse_settlement_event(&bad, "#5160").is_err(), "{key}");
    }
    let mut bad = event(1, 100);
    bad["fee"] = json!("-1");
    assert!(parse_settlement_event(&bad, "#5160").is_err());
}
#[test]
fn dedup_conflict_is_atomic() {
    let state = SettlementScan::new(WALLET, "#5160", 100, 200).unwrap();
    let mut bad = event(1, 100);
    bad["sz"] = json!("3");
    assert!(apply_page(&json!([event(1, 100), bad]), &state).is_err());
    assert!(state.events.is_empty());
    let page = apply_page(&json!([event(1, 100), event(1, 100)]), &state).unwrap();
    assert_eq!(page.events.len(), 1);
    assert!(!page.progress.complete);
    let done = apply_page(&json!([event(2, 99)]), &page.progress).unwrap();
    assert!(done.progress.history_complete);
    assert!(
        apply_page(&json!([]), &page.progress)
            .unwrap()
            .progress
            .complete
    );
    assert!(
        !apply_page(&json!([]), &page.progress)
            .unwrap()
            .progress
            .history_complete
    );
}
#[test]
fn full_other_coin_page_and_same_millisecond_stall() {
    let state = SettlementScan::new(WALLET, "#5160", 100, 3000).unwrap();
    let rows: Vec<_> = (0..PAGE_SIZE)
        .map(|i| json!({"tid":i,"time":100,"coin":"BTC"}))
        .collect();
    let page = apply_page(&json!(rows), &state).unwrap();
    assert!(page.events.is_empty());
    assert!(!page.progress.complete);
    assert!(apply_page(&json!(rows), &page.progress).is_err());
}
#[test]
fn ownership_and_ledger_fail_closed() {
    assert!(validate_wallet("not-a-wallet").is_err());
    assert!(validate_transfer_ledger(
        &json!([{"time":100,"hash":"x","delta":{"type":"spotTransfer","token":"#5160"}}]),
        "#5160",
        100,
        200
    )
    .is_err());
    assert!(validate_transfer_ledger(
        &json!([{"time":100,"hash":"x","delta":{"type":"unknown","usdc":"2"}}]),
        "#5160",
        100,
        200
    )
    .is_err());
    let buy = json!({"coin":"#5160","dir":"Buy","side":"B","oid":1,"tid":1,"sz":"2","px":"0.4","startPosition":"0","time":100});
    let state = SettlementScan::new(WALLET, "#5160", 99, 200).unwrap();
    let page = apply_page(&json!([buy, event(2, 101)]), &state).unwrap();
    let done = apply_page(&json!([event(3, 98)]), &page.progress)
        .unwrap()
        .progress;
    assert!(validate_ownership(&done, &done.trades).is_ok());
    assert!(validate_ownership(&done, &[]).is_err());
    let mut bad = done.clone();
    bad.trades[0].start_position = Decimal::ONE;
    assert!(validate_ownership(&bad, &bad.trades).is_err());
}
