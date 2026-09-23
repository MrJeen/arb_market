use super::*;

#[test]
fn validates_nonnegative_decimal_without_environment_mutation() {
    assert_eq!(
        parse_nonnegative_decimal("TAKE_PROFIT_MIN_GAIN", Some("0.25"), "0.1").unwrap(),
        Decimal::new(25, 2)
    );
    assert!(
        parse_nonnegative_decimal("TAKE_PROFIT_MIN_GAIN", Some("-0.01"), "0.1")
            .unwrap_err()
            .to_string()
            .contains("must be non-negative")
    );
}

#[test]
fn validates_polymarket_fee_bps_boundaries() {
    assert_eq!(parse_polymarket_fee_bps(None).unwrap(), Decimal::from(700));
    for raw in ["0", "700", "10000"] {
        assert_eq!(
            parse_polymarket_fee_bps(Some(raw)).unwrap(),
            Decimal::from_str(raw).unwrap()
        );
    }
    for raw in ["-1", "10000.01", "bad", ""] {
        assert!(parse_polymarket_fee_bps(Some(raw)).is_err());
    }
}

#[test]
fn parses_enabled_platforms() {
    let set = parse_enabled_platforms(Some("polymarket, outcome".into())).unwrap();
    assert!(set.contains(POLYMARKET));
    assert!(set.contains(OUTCOME));
}

#[test]
fn rejects_unknown_platform_name() {
    let err = parse_enabled_platforms(Some("polymarket,typo".into())).unwrap_err();
    assert!(err.to_string().contains("unknown platform typo"));
}

#[test]
fn parses_funder_json() {
    let raw = r#"{"0xAbc":{"walletPrivateKey":"0x1","isWalletV2":true,"service":"a"}}"#;
    let funders = parse_funders(raw.into()).unwrap();
    assert_eq!(funders.len(), 1);
    assert!(funders[0].is_wallet_v2);
    assert_eq!(funders[0].service.as_deref(), Some("a"));
}

#[test]
fn parses_funder_array() {
    let raw =
        r#"[{"funderAddress":"0xAbc","walletPrivateKey":"0x1","isWalletV2":true,"service":"a"}]"#;
    let funders = parse_funders(raw.into()).unwrap();
    assert_eq!(funders.len(), 1);
    assert_eq!(funders[0].funder_address, "0xAbc");
    assert!(funders[0].is_wallet_v2);
}

#[test]
fn normalizes_builder_address() {
    assert_eq!(
        normalize_builder_address("0xAB5DBC057628BC18523C4CDFC0E1E2EBDBECB704").unwrap(),
        "0xab5dbc057628bc18523c4cdfc0e1e2ebdbecb704"
    );
    assert!(normalize_builder_address("not-an-address").is_err());
}

#[test]
fn parses_funder_file() {
    let path = std::env::temp_dir().join("market_arb_funders_test.json");
    std::fs::write(&path, r#"{"0xDef":{"walletPrivateKey":"0x2"}}"#).unwrap();
    let funders = parse_funders_file(path.to_str().unwrap()).unwrap();
    assert_eq!(funders[0].funder_address, "0xDef");
    let _ = std::fs::remove_file(path);
}

#[test]
fn funder_object_keeps_json_key_order() {
    let raw = r#"{
            "0xaaa":{"walletPrivateKey":"0x1"},
            "0xbbb":{"walletPrivateKey":"0x2"},
            "0xccc":{"walletPrivateKey":"0x3"}
        }"#;
    let funders = parse_funders(raw.into()).unwrap();
    assert_eq!(
        funders
            .iter()
            .map(|f| f.funder_address.as_str())
            .collect::<Vec<_>>(),
        ["0xaaa", "0xbbb", "0xccc"]
    );
}
