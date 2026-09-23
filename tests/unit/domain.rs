use super::*;

#[test]
fn encodes_outcome_ids() {
    assert_eq!(side_coin(516, 0), "#5160");
    assert_eq!(side_coin(516, 1), "#5161");
    assert_eq!(side_coin(9, 0), "#90");
    assert_eq!(side_asset_id(516, 0), 100_005_160);
    assert_eq!(side_balance_coin(516, 0), "+5160");
    assert_eq!(parse_side_coin("#5160"), Some((516, 0)));
    assert_eq!(parse_side_coin("+12110"), Some((1211, 0)));
    assert_eq!(parse_side_coin("#90"), Some((9, 0)));
    assert_eq!(parse_side_coin("#5162"), None);
}

#[test]
fn extracts_market_identity_from_topic() {
    let token = |platform: &str, option_id: &str, condition_id: Option<&str>| TokenRef {
        platform: platform.into(),
        token_id: "token".into(),
        label: "yes".into(),
        option_id: option_id.into(),
        condition_id: condition_id.map(str::to_owned),
        asset_id: None,
        side_index: None,
        neg_risk: None,
        fees_enabled: None,
        fee_rate: None,
    };
    let topic = Topic {
        key: TopicKey::new(Uuid::nil(), 1),
        title: String::new(),
        market_title: String::new(),
        end_date: None,
        tokens: vec![
            token(POLYMARKET, "pm-market", Some("condition")),
            token(OUTCOME, "516", None),
        ],
    };

    let identity = topic.market_identity().unwrap();
    assert_eq!(identity.require(POLYMARKET).unwrap(), "condition");
    assert_eq!(identity.require(OUTCOME).unwrap(), "516");
    assert_eq!(
        identity.iter().collect::<Vec<_>>(),
        vec![(OUTCOME, "516"), (POLYMARKET, "condition")]
    );
}

#[test]
fn market_identity_normalizes_platform_and_market_id() {
    let identity = MarketIdentity::new(" Outcome ", " 516 ").unwrap();
    assert_eq!(identity.get("outcome"), Some("516"));
    assert_eq!(identity.get(" OUTCOME "), Some("516"));
    assert_eq!(
        identity.iter().collect::<Vec<_>>(),
        vec![("outcome", "516")]
    );
}

#[test]
fn market_identity_rejects_empty_values() {
    assert!(MarketIdentity::new(" ", "market").is_err());
    assert!(MarketIdentity::new("outcome", " ").is_err());
}

#[test]
fn rejects_duplicate_outcome_side_or_label() {
    let option = |outcomes| PlatformOption {
        title: String::new(),
        platform: OUTCOME.into(),
        option_id: "516".into(),
        condition_id: None,
        outcomes,
        fees_enabled: None,
        fee_schedule: None,
        neg_risk: None,
    };
    let spec = |token_id: &str, label: &str, side_index| OutcomeSpec {
        token_id: token_id.into(),
        label: label.into(),
        index_set: None,
        asset_id: None,
        side_index: Some(side_index),
        neg_risk: None,
    };
    assert!(validate_outcome_option(&option(vec![
        spec("#5160", "yes", 0),
        spec("#5160", "no", 0),
    ]))
    .is_err());
    assert!(validate_outcome_option(&option(vec![
        spec("#5160", "yes", 0),
        spec("#5161", "YES", 1),
    ]))
    .is_err());
}

#[test]
fn rejects_mismatched_asset_id() {
    let option = PlatformOption {
        title: String::new(),
        platform: OUTCOME.into(),
        option_id: "516".into(),
        condition_id: None,
        outcomes: vec![
            OutcomeSpec {
                token_id: "#5160".into(),
                label: "yes".into(),
                index_set: Some(0),
                asset_id: Some(1),
                side_index: Some(0),
                neg_risk: None,
            },
            OutcomeSpec {
                token_id: "#5161".into(),
                label: "no".into(),
                index_set: Some(1),
                asset_id: Some(100_005_161),
                side_index: Some(1),
                neg_risk: None,
            },
        ],
        fees_enabled: None,
        fee_schedule: None,
        neg_risk: None,
    };
    assert!(validate_outcome_option(&option).is_err());
}
