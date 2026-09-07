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
    use serde_json::json;

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
