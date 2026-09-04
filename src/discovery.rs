use crate::domain::{tradable_topics, CatalogEvent, Topic, UnifiedOption};
use crate::error::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::collections::HashSet;
use uuid::Uuid;

pub async fn load_active_topics(pool: &PgPool, enabled: &HashSet<String>) -> Result<Vec<Topic>> {
    let rows: Vec<(Uuid, String, Option<DateTime<Utc>>, serde_json::Value)> = sqlx::query_as(
        "SELECT id, title, end_date, unified_options
         FROM events
         WHERE status = 'active'
           AND unified_options IS NOT NULL",
    )
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
}
