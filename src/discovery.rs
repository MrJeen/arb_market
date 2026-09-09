use crate::domain::{tradable_topics, CatalogEvent, Topic, TopicKey, UnifiedOption};
use crate::error::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::collections::HashSet;
use uuid::Uuid;

type CatalogEventRow = (Uuid, String, Option<DateTime<Utc>>, serde_json::Value);

pub async fn load_active_topics(pool: &PgPool, enabled: &HashSet<String>) -> Result<Vec<Topic>> {
    let path = unified_options_platforms_path(enabled);
    let rows: Vec<CatalogEventRow> = sqlx::query_as(
        "SELECT id, title, end_date, unified_options
         FROM events
         WHERE status = 'active'
           AND jsonb_path_exists(unified_options, $1::jsonpath)",
    )
    .bind(&path)
    .fetch_all(pool)
    .await?;
    let mut topics = Vec::new();
    for row in rows {
        let id = row.0;
        let event = match catalog_event_from_row(row) {
            Ok(event) => event,
            Err(err) => {
                tracing::warn!(event_id = %id, error = %err, "invalid unified_options");
                continue;
            }
        };
        topics.extend(tradable_topics(&event, enabled));
    }
    Ok(topics)
}

/// Loads one topic from the event catalog, including inactive historical events.
pub async fn load_topic(
    pool: &PgPool,
    key: TopicKey,
    enabled: &HashSet<String>,
) -> Result<Option<Topic>> {
    let row: Option<CatalogEventRow> = sqlx::query_as(
        "SELECT id, title, end_date, unified_options
         FROM events
         WHERE id = $1",
    )
    .bind(key.event_id)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let event = match catalog_event_from_row(row) {
        Ok(event) => event,
        Err(err) => {
            tracing::warn!(event_id = %key.event_id, error = %err, "invalid unified_options");
            return Ok(None);
        }
    };
    Ok(topic_for_key(&event, key, enabled))
}

/// 成交费率只依赖本腿 PM 市场，不要求另一平台目录或可交易 Topic 完整。
pub(crate) async fn load_pm_fee_rate(
    pool: &PgPool,
    key: TopicKey,
    condition_id: &str,
    token_id: &str,
) -> Result<Option<rust_decimal::Decimal>> {
    let raw: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT unified_options FROM events WHERE id = $1")
            .bind(key.event_id)
            .fetch_optional(pool)
            .await?;
    raw.map(|raw| pm_fee_rate_from_catalog(&raw, key.unified_index, condition_id, token_id))
        .transpose()
        .map(Option::flatten)
}

fn pm_fee_rate_from_catalog(
    raw: &serde_json::Value,
    index: i32,
    condition_id: &str,
    token_id: &str,
) -> Result<Option<rust_decimal::Decimal>> {
    use crate::error::Error;
    let invalid = || Error::msg("invalid COMMON polymarket fee catalog");
    let options = raw.as_array().ok_or_else(invalid)?;
    let mut found = None;
    for option in options {
        let option_index = option
            .get("index")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(invalid)?;
        if option_index != i64::from(index) {
            continue;
        }
        if found.is_some() {
            return Err(Error::msg("duplicate COMMON unified index"));
        }
        found = Some(option);
    }
    let Some(option) = found else {
        return Ok(None);
    };
    let platforms = match option.get("platformOptions") {
        None => return Ok(None),
        Some(value) => value.as_array().ok_or_else(invalid)?,
    };
    let mut matched = None;
    for platform in platforms {
        let name = platform
            .get("platform")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(invalid)?;
        if name != crate::config::POLYMARKET {
            continue;
        }
        if matched.is_some() {
            return Err(Error::msg("duplicate COMMON polymarket option"));
        }
        matched = Some(platform);
    }
    let Some(pm) = matched else {
        return Ok(None);
    };
    if !pm
        .get("conditionId")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|id| id.eq_ignore_ascii_case(condition_id))
    {
        return Err(Error::msg("COMMON polymarket condition identity mismatch"));
    }
    let outcomes = pm
        .get("outcomes")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(invalid)?;
    let mut matches = 0;
    for outcome in outcomes {
        let id = outcome.get("tokenId").ok_or_else(invalid)?;
        let id = match id {
            serde_json::Value::String(id) => id.clone(),
            serde_json::Value::Number(id) => id.to_string(),
            _ => return Err(invalid()),
        };
        matches += usize::from(id == token_id);
    }
    if matches != 1 {
        return Err(Error::msg("COMMON polymarket token identity mismatch"));
    }
    match pm.get("feesEnabled") {
        Some(serde_json::Value::Bool(false)) => return Ok(Some(rust_decimal::Decimal::ZERO)),
        None | Some(serde_json::Value::Null | serde_json::Value::Bool(true)) => {}
        _ => return Err(invalid()),
    }
    let schedule = match pm.get("feeSchedule") {
        None | Some(serde_json::Value::Null) => return Ok(None),
        Some(value) => value.as_object().ok_or_else(invalid)?,
    };
    let rate = match schedule.get("rate") {
        None | Some(serde_json::Value::Null) => return Ok(None),
        Some(value) => crate::platforms::parse_decimal(value).ok_or_else(invalid)?,
    };
    validate_pm_fee_rate(rate).map(Some)
}

pub(crate) fn validate_pm_fee_rate(rate: rust_decimal::Decimal) -> Result<rust_decimal::Decimal> {
    if !(rust_decimal::Decimal::ZERO..=rust_decimal::Decimal::ONE).contains(&rate) {
        return Err(crate::error::Error::msg(
            "COMMON polymarket fee rate must be between 0 and 1",
        ));
    }
    Ok(rate)
}

fn catalog_event_from_row(row: CatalogEventRow) -> serde_json::Result<CatalogEvent> {
    let (id, title, end_date, unified_options) = row;
    Ok(CatalogEvent {
        id,
        title,
        end_date,
        unified_options: serde_json::from_value::<Vec<UnifiedOption>>(unified_options)?,
    })
}

fn topic_for_key(event: &CatalogEvent, key: TopicKey, enabled: &HashSet<String>) -> Option<Topic> {
    tradable_topics(event, enabled)
        .into_iter()
        .find(|topic| topic.key == key)
}

/// 同一 unified option 上同时出现所有 enabled 平台。平台名来自配置，不写死。
fn unified_options_platforms_path(enabled: &HashSet<String>) -> String {
    if enabled.is_empty() {
        return "$[*] ? (false)".to_string();
    }
    let mut preds: Vec<String> = enabled
        .iter()
        .map(|platform| {
            let escaped = escape_jsonpath_string(platform);
            format!(r#"exists(@."platformOptions"[*]."platform" ? (@ == "{escaped}"))"#)
        })
        .collect();
    preds.sort();
    format!("$[*] ? ({})", preds.join(" && "))
}

fn escape_jsonpath_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{OUTCOME, POLYMARKET};
    use rust_decimal::Decimal;
    use serde_json::json;

    fn pm_fee_catalog() -> serde_json::Value {
        // 仅包含 PM 腿所需数据：无需 Outcome 平台或完整 Topic。
        json!([{
            "index": 7,
            "platformOptions": [{
                "platform": POLYMARKET,
                "conditionId": "0xcond",
                "feesEnabled": true,
                "feeSchedule": {"rate": 0.07},
                "outcomes": [{"tokenId": "111"}, {"tokenId": 222}]
            }]
        }])
    }

    #[test]
    fn pm_fee_catalog_supports_standalone_pm_and_nonpositional_index() {
        let mut raw = pm_fee_catalog();
        raw.as_array_mut().unwrap().insert(0, json!({"index": 3}));

        assert_eq!(
            pm_fee_rate_from_catalog(&raw, 7, "0xCOND", "111").unwrap(),
            Some(Decimal::new(7, 2))
        );
        assert_eq!(
            pm_fee_rate_from_catalog(&raw, 7, "0xcond", "222").unwrap(),
            Some(Decimal::new(7, 2))
        );
        assert_eq!(
            pm_fee_rate_from_catalog(&raw, 1, "0xcond", "111").unwrap(),
            None
        );
    }

    #[test]
    fn pm_fee_catalog_accepts_numeric_and_string_rates_including_boundaries() {
        for (value, expected) in [
            (json!(0.07), Decimal::new(7, 2)),
            (json!("0.07"), Decimal::new(7, 2)),
            (json!(0), Decimal::ZERO),
            (json!("0"), Decimal::ZERO),
            (json!(1), Decimal::ONE),
            (json!("1"), Decimal::ONE),
        ] {
            let mut raw = pm_fee_catalog();
            raw[0]["platformOptions"][0]["feeSchedule"]["rate"] = value.clone();
            assert_eq!(
                pm_fee_rate_from_catalog(&raw, 7, "0xcond", "111").unwrap(),
                Some(expected),
                "rate={value}"
            );
        }
    }

    #[test]
    fn pm_fee_catalog_disabled_fees_override_invalid_or_missing_schedule() {
        for schedule in [
            json!(null),
            json!("invalid schedule"),
            json!([]),
            json!({"rate": "not a decimal"}),
            json!({"rate": -0.01}),
            json!({"rate": 1.01}),
        ] {
            let mut raw = pm_fee_catalog();
            let pm = &mut raw[0]["platformOptions"][0];
            pm["feesEnabled"] = json!(false);
            pm["feeSchedule"] = schedule.clone();
            assert_eq!(
                pm_fee_rate_from_catalog(&raw, 7, "0xcond", "111").unwrap(),
                Some(Decimal::ZERO),
                "schedule={schedule}"
            );
        }
        let mut raw = pm_fee_catalog();
        let pm = raw[0]["platformOptions"][0].as_object_mut().unwrap();
        pm.insert("feesEnabled".into(), json!(false));
        pm.remove("feeSchedule");
        assert_eq!(
            pm_fee_rate_from_catalog(&raw, 7, "0xcond", "111").unwrap(),
            Some(Decimal::ZERO)
        );
    }

    #[test]
    fn pm_fee_catalog_missing_option_platform_or_rate_returns_none() {
        let mut without_pm = pm_fee_catalog();
        without_pm[0]["platformOptions"][0]["platform"] = json!(OUTCOME);
        let mut without_schedule = pm_fee_catalog();
        without_schedule[0]["platformOptions"][0]
            .as_object_mut()
            .unwrap()
            .remove("feeSchedule");
        let mut null_schedule = pm_fee_catalog();
        null_schedule[0]["platformOptions"][0]["feeSchedule"] = json!(null);
        let mut without_rate = pm_fee_catalog();
        without_rate[0]["platformOptions"][0]["feeSchedule"] = json!({});
        let mut null_rate = pm_fee_catalog();
        null_rate[0]["platformOptions"][0]["feeSchedule"]["rate"] = json!(null);

        for raw in [
            json!([]),
            json!([{"index": 3}]),
            json!([{"index": 7}]),
            json!([{"index": 7, "platformOptions": []}]),
            without_pm,
            without_schedule,
            null_schedule,
            without_rate,
            null_rate,
        ] {
            assert_eq!(
                pm_fee_rate_from_catalog(&raw, 7, "0xcond", "111").unwrap(),
                None,
                "catalog={raw}"
            );
        }
    }

    #[test]
    fn pm_fee_catalog_absent_or_null_fees_enabled_still_uses_rate() {
        let mut raw = pm_fee_catalog();
        raw[0]["platformOptions"][0]
            .as_object_mut()
            .unwrap()
            .remove("feesEnabled");
        assert_eq!(
            pm_fee_rate_from_catalog(&raw, 7, "0xcond", "111").unwrap(),
            Some(Decimal::new(7, 2))
        );
        raw[0]["platformOptions"][0]["feesEnabled"] = json!(null);
        assert_eq!(
            pm_fee_rate_from_catalog(&raw, 7, "0xcond", "111").unwrap(),
            Some(Decimal::new(7, 2))
        );
    }

    #[test]
    fn pm_fee_catalog_rejects_invalid_json_structure() {
        for raw in [
            json!(null),
            json!({}),
            json!("not an array"),
            json!([null]),
            json!([{}]),
            json!([{"index": "7"}]),
            json!([{"index": 7.5}]),
            json!([{"index": 7, "platformOptions": null}]),
            json!([{"index": 7, "platformOptions": {}}]),
            json!([{"index": 7, "platformOptions": [null]}]),
            json!([{"index": 7, "platformOptions": [{}]}]),
            json!([{"index": 7, "platformOptions": [{"platform": 1}]}]),
        ] {
            assert!(
                pm_fee_rate_from_catalog(&raw, 7, "0xcond", "111").is_err(),
                "catalog={raw}"
            );
        }
    }

    #[test]
    fn pm_fee_catalog_rejects_invalid_rate_and_fees_enabled() {
        for (path, value) in [
            ("/0/platformOptions/0/feeSchedule", json!([])),
            ("/0/platformOptions/0/feeSchedule", json!("invalid")),
            ("/0/platformOptions/0/feeSchedule/rate", json!("invalid")),
            ("/0/platformOptions/0/feeSchedule/rate", json!("")),
            ("/0/platformOptions/0/feeSchedule/rate", json!(true)),
            ("/0/platformOptions/0/feeSchedule/rate", json!({})),
            ("/0/platformOptions/0/feeSchedule/rate", json!([])),
            ("/0/platformOptions/0/feeSchedule/rate", json!(-0.01)),
            ("/0/platformOptions/0/feeSchedule/rate", json!(1.01)),
            ("/0/platformOptions/0/feeSchedule/rate", json!("-0.01")),
            ("/0/platformOptions/0/feeSchedule/rate", json!("1.01")),
            ("/0/platformOptions/0/feesEnabled", json!("false")),
            ("/0/platformOptions/0/feesEnabled", json!(0)),
            ("/0/platformOptions/0/feesEnabled", json!({})),
            ("/0/platformOptions/0/feesEnabled", json!([])),
        ] {
            let mut raw = pm_fee_catalog();
            *raw.pointer_mut(path).unwrap() = value.clone();
            assert!(
                pm_fee_rate_from_catalog(&raw, 7, "0xcond", "111").is_err(),
                "path={path}, value={value}"
            );
        }
    }

    #[test]
    fn pm_fee_catalog_rejects_duplicate_index_pm_or_matching_token() {
        let mut duplicate_index = pm_fee_catalog();
        duplicate_index
            .as_array_mut()
            .unwrap()
            .push(json!({"index": 7}));
        let mut duplicate_pm = pm_fee_catalog();
        let pm = duplicate_pm[0]["platformOptions"][0].clone();
        duplicate_pm[0]["platformOptions"]
            .as_array_mut()
            .unwrap()
            .push(pm);
        let mut duplicate_token = pm_fee_catalog();
        duplicate_token[0]["platformOptions"][0]["outcomes"] =
            json!([{"tokenId": "111"}, {"tokenId": 111}]);

        for raw in [duplicate_index, duplicate_pm, duplicate_token] {
            assert!(
                pm_fee_rate_from_catalog(&raw, 7, "0xcond", "111").is_err(),
                "catalog={raw}"
            );
        }
    }

    #[test]
    fn pm_fee_catalog_rejects_invalid_pm_identity_even_when_fees_disabled() {
        for (field, value) in [
            ("conditionId", json!(null)),
            ("conditionId", json!(123)),
            ("conditionId", json!("0xother")),
            ("outcomes", json!(null)),
            ("outcomes", json!({})),
            ("outcomes", json!([])),
            ("outcomes", json!([{}])),
            ("outcomes", json!([{"tokenId": null}])),
            ("outcomes", json!([{"tokenId": true}])),
            ("outcomes", json!([{"tokenId": {}}])),
            ("outcomes", json!([{"tokenId": "other"}])),
            ("outcomes", json!([{"tokenId": "111"}, {"tokenId": null}])),
        ] {
            for enabled in [true, false] {
                let mut raw = pm_fee_catalog();
                raw[0]["platformOptions"][0][field] = value.clone();
                raw[0]["platformOptions"][0]["feesEnabled"] = json!(enabled);
                assert!(
                    pm_fee_rate_from_catalog(&raw, 7, "0xcond", "111").is_err(),
                    "field={field}, value={value}, feesEnabled={enabled}"
                );
            }
        }
        let raw = pm_fee_catalog();
        assert!(pm_fee_rate_from_catalog(&raw, 7, "0xother", "111").is_err());
        assert!(pm_fee_rate_from_catalog(&raw, 7, "0xcond", "333").is_err());
        assert!(pm_fee_rate_from_catalog(&raw, 7, "0xcond", "0111").is_err());
    }

    #[test]
    fn pm_fee_catalog_ignores_unrelated_platform_fee_data() {
        let mut raw = pm_fee_catalog();
        raw[0]["platformOptions"].as_array_mut().unwrap().insert(
            0,
            json!({
                "platform": OUTCOME,
                "feesEnabled": "invalid",
                "feeSchedule": {"rate": -42},
                "outcomes": null
            }),
        );
        raw[0]["platformOptions"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "platform": "unrelated",
                "feesEnabled": {},
                "feeSchedule": "invalid"
            }));
        assert_eq!(
            pm_fee_rate_from_catalog(&raw, 7, "0xcond", "111").unwrap(),
            Some(Decimal::new(7, 2))
        );
    }

    #[test]
    fn validates_pm_fee_rate_inclusive_unit_interval() {
        for rate in [Decimal::ZERO, Decimal::new(7, 2), Decimal::ONE] {
            assert_eq!(validate_pm_fee_rate(rate).unwrap(), rate);
        }
        for rate in [Decimal::new(-1, 2), Decimal::new(101, 2)] {
            assert!(validate_pm_fee_rate(rate).is_err(), "rate={rate}");
        }
    }

    #[test]
    fn discovers_paired_binary_markets() {
        let unified = json!([{
            "index": 0,
            "title": "will it happen",
            "platformOptions": [
                {
                    "platform": "polymarket",
                    "optionId": "0xabc",
                    "conditionId": "0xcond",
                    "negRisk": true,
                    "outcomes": [
                        {"tokenId": "111", "label": "Yes"},
                        {"tokenId": "222", "label": "No"}
                    ]
                },
                {
                    "platform": "outcome",
                    "optionId": 516,
                    "outcomes": [
                        {"tokenId": "#5160", "label": "Yes", "assetId": 100005160, "sideIndex": 0},
                        {"tokenId": "#5161", "label": "No", "assetId": 100005161, "sideIndex": 1}
                    ]
                }
            ]
        }]);
        let options: Vec<UnifiedOption> = serde_json::from_value(unified).unwrap();
        let event = CatalogEvent {
            id: Uuid::nil(),
            title: "evt".into(),
            end_date: None,
            unified_options: options,
        };
        let enabled = HashSet::from([POLYMARKET.to_string(), OUTCOME.to_string()]);
        let topics = tradable_topics(&event, &enabled);
        assert_eq!(topics.len(), 1);
        assert!(topics[0].token(POLYMARKET, "yes").is_some());
        assert_eq!(
            topics[0].token(OUTCOME, "yes").unwrap().asset_id,
            Some(100_005_160)
        );
    }

    #[test]
    fn skips_polymarket_market_without_condition_id() {
        let unified = json!([{
            "index": 0,
            "title": "missing identity",
            "platformOptions": [
                {
                    "platform": "polymarket",
                    "optionId": "0xabc",
                    "outcomes": [
                        {"tokenId": "111", "label": "Yes"},
                        {"tokenId": "222", "label": "No"}
                    ]
                },
                {
                    "platform": "outcome",
                    "optionId": 516,
                    "outcomes": [
                        {"tokenId": "#5160", "label": "Yes", "sideIndex": 0},
                        {"tokenId": "#5161", "label": "No", "sideIndex": 1}
                    ]
                }
            ]
        }]);
        let event = CatalogEvent {
            id: Uuid::nil(),
            title: "evt".into(),
            end_date: None,
            unified_options: serde_json::from_value(unified).unwrap(),
        };
        let enabled = HashSet::from([POLYMARKET.to_string(), OUTCOME.to_string()]);
        assert!(tradable_topics(&event, &enabled).is_empty());
    }

    #[test]
    fn copies_polymarket_fee_schedule_rate() {
        let unified = json!([{
            "index": 0,
            "title": "will it happen",
            "platformOptions": [
                {
                    "platform": "polymarket",
                    "optionId": "0xabc",
                    "conditionId": "0xcond",
                    "feesEnabled": true,
                    "feeSchedule": { "rate": 0.07, "exponent": 1 },
                    "outcomes": [
                        {"tokenId": "111", "label": "Yes"},
                        {"tokenId": "222", "label": "No"}
                    ]
                },
                {
                    "platform": "outcome",
                    "optionId": 516,
                    "outcomes": [
                        {"tokenId": "#5160", "label": "Yes", "assetId": 100005160, "sideIndex": 0},
                        {"tokenId": "#5161", "label": "No", "assetId": 100005161, "sideIndex": 1}
                    ]
                }
            ]
        }]);
        let options: Vec<UnifiedOption> = serde_json::from_value(unified).unwrap();
        let event = CatalogEvent {
            id: Uuid::nil(),
            title: "evt".into(),
            end_date: None,
            unified_options: options,
        };
        let enabled = HashSet::from([POLYMARKET.to_string(), OUTCOME.to_string()]);
        let topics = tradable_topics(&event, &enabled);
        let pm = topics[0].token(POLYMARKET, "yes").unwrap();
        assert_eq!(pm.fees_enabled, Some(true));
        assert_eq!(pm.fee_rate.unwrap().to_string(), "0.07");
        assert_eq!(topics[0].polymarket_fee_rate().unwrap().to_string(), "0.07");
    }

    #[test]
    fn disabled_fee_schedule_is_zero() {
        let unified = json!([{
            "index": 0,
            "title": "will it happen",
            "platformOptions": [
                {
                    "platform": "polymarket",
                    "optionId": "0xabc",
                    "conditionId": "0xcond",
                    "feesEnabled": false,
                    "feeSchedule": { "rate": 0.07 },
                    "outcomes": [
                        {"tokenId": "111", "label": "Yes"},
                        {"tokenId": "222", "label": "No"}
                    ]
                },
                {
                    "platform": "outcome",
                    "optionId": 516,
                    "outcomes": [
                        {"tokenId": "#5160", "label": "Yes", "assetId": 100005160, "sideIndex": 0},
                        {"tokenId": "#5161", "label": "No", "assetId": 100005161, "sideIndex": 1}
                    ]
                }
            ]
        }]);
        let options: Vec<UnifiedOption> = serde_json::from_value(unified).unwrap();
        let event = CatalogEvent {
            id: Uuid::nil(),
            title: "evt".into(),
            end_date: None,
            unified_options: options,
        };
        let enabled = HashSet::from([POLYMARKET.to_string(), OUTCOME.to_string()]);
        let topics = tradable_topics(&event, &enabled);
        assert_eq!(topics[0].polymarket_fee_rate().unwrap().to_string(), "0");
    }

    #[test]
    fn selects_exact_topic_from_multiple_unified_options() {
        let unified = json!([
            {
                "index": 3,
                "title": "first market",
                "platformOptions": [
                    {
                        "platform": "polymarket",
                        "optionId": "0xfirst",
                        "conditionId": "0xfirst-cond",
                        "outcomes": [
                            {"tokenId": "first-yes", "label": "Yes"},
                            {"tokenId": "first-no", "label": "No"}
                        ]
                    },
                    {
                        "platform": "outcome",
                        "optionId": 516,
                        "outcomes": [
                            {"tokenId": "#5160", "label": "Yes", "sideIndex": 0},
                            {"tokenId": "#5161", "label": "No", "sideIndex": 1}
                        ]
                    }
                ]
            },
            {
                "index": 7,
                "title": "selected market",
                "platformOptions": [
                    {
                        "platform": "polymarket",
                        "optionId": "0xselected",
                        "conditionId": "0xselected-cond",
                        "outcomes": [
                            {"tokenId": "selected-yes", "label": "Yes"},
                            {"tokenId": "selected-no", "label": "No"}
                        ]
                    },
                    {
                        "platform": "outcome",
                        "optionId": 517,
                        "outcomes": [
                            {"tokenId": "#5170", "label": "Yes", "sideIndex": 0},
                            {"tokenId": "#5171", "label": "No", "sideIndex": 1}
                        ]
                    }
                ]
            }
        ]);
        let event_id = Uuid::new_v4();
        let event = CatalogEvent {
            id: event_id,
            title: "event".into(),
            end_date: None,
            unified_options: serde_json::from_value(unified).unwrap(),
        };
        let enabled = HashSet::from([POLYMARKET.to_string(), OUTCOME.to_string()]);

        let topic = topic_for_key(&event, TopicKey::new(event_id, 7), &enabled).unwrap();

        assert_eq!(topic.key, TopicKey::new(event_id, 7));
        assert_eq!(topic.market_title, "selected market");
        assert_eq!(
            topic.token(POLYMARKET, "yes").unwrap().token_id,
            "selected-yes"
        );
        assert!(topic_for_key(&event, TopicKey::new(event_id, 9), &enabled).is_none());
    }

    #[test]
    fn jsonpath_requires_all_enabled_platforms_on_one_option() {
        let both = HashSet::from([POLYMARKET.to_string(), OUTCOME.to_string()]);
        let path = unified_options_platforms_path(&both);
        assert!(path.contains(r#"@ == "outcome""#));
        assert!(path.contains(r#"@ == "polymarket""#));
        assert!(path.contains(" && "));
        let only_pm = HashSet::from([POLYMARKET.to_string()]);
        assert_eq!(
            unified_options_platforms_path(&only_pm),
            r#"$[*] ? (exists(@."platformOptions"[*]."platform" ? (@ == "polymarket")))"#
        );
        assert_eq!(
            unified_options_platforms_path(&HashSet::new()),
            "$[*] ? (false)"
        );
    }
}
