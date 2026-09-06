use crate::domain::{tradable_topics, CatalogEvent, Topic, UnifiedOption};
use crate::error::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::collections::HashSet;
use uuid::Uuid;

pub async fn load_active_topics(pool: &PgPool, enabled: &HashSet<String>) -> Result<Vec<Topic>> {
    let path = unified_options_platforms_path(enabled);
    let rows: Vec<(Uuid, String, Option<DateTime<Utc>>, serde_json::Value)> = sqlx::query_as(
        "SELECT id, title, end_date, unified_options
         FROM events
         WHERE status = 'active'
           AND jsonb_path_exists(unified_options, $1::jsonpath)",
    )
    .bind(&path)
    .fetch_all(pool)
    .await?;
    let mut topics = Vec::new();
    for (id, title, end_date, unified) in rows {
        let options: Vec<UnifiedOption> = match serde_json::from_value(unified) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(event_id = %id, error = %err, "invalid unified_options");
                continue;
            }
        };
        let event = CatalogEvent {
            id,
            title,
            end_date,
            unified_options: options,
        };
        topics.extend(tradable_topics(&event, enabled));
    }
    Ok(topics)
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
    fn copies_polymarket_fee_schedule_rate() {
        let unified = json!([{
            "index": 0,
            "title": "will it happen",
            "platformOptions": [
                {
                    "platform": "polymarket",
                    "optionId": "0xabc",
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
