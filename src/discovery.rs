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
#[path = "../tests/unit/discovery.rs"]
mod tests;
