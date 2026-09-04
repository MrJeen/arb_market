use crate::config::{OUTCOME, POLYMARKET};
use crate::error::{Error, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub const OUTCOME_ASSET_BASE: u64 = 100_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TopicKey {
    pub event_id: Uuid,
    pub unified_index: i32,
}

impl TopicKey {
    pub fn new(event_id: Uuid, unified_index: i32) -> Self {
        Self {
            event_id,
            unified_index,
        }
    }

    pub fn as_str(&self) -> String {
        format!("{}:{}", self.event_id, self.unified_index)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedOption {
    pub index: i32,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub platform_options: Vec<PlatformOption>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlatformOption {
    #[serde(default)]
    pub title: String,
    pub platform: String,
    #[serde(deserialize_with = "de_stringish")]
    pub option_id: String,
    #[serde(default, deserialize_with = "de_opt_stringish")]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub outcomes: Vec<OutcomeSpec>,
    #[serde(default)]
    pub fees_enabled: Option<bool>,
    #[serde(default)]
    pub fee_schedule: Option<FeeSchedule>,
    #[serde(default)]
    pub neg_risk: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeeSchedule {
    #[serde(default)]
    pub rate: Option<Decimal>,
    #[serde(default)]
    pub exponent: Option<Decimal>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeSpec {
    #[serde(deserialize_with = "de_stringish")]
    pub token_id: String,
    pub label: String,
    #[serde(default)]
    pub index_set: Option<i32>,
    #[serde(default)]
    pub asset_id: Option<u64>,
    #[serde(default)]
    pub side_index: Option<u8>,
    #[serde(default)]
    pub neg_risk: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct CatalogEvent {
    pub id: Uuid,
    pub title: String,
    pub end_date: Option<chrono::DateTime<chrono::Utc>>,
    pub unified_options: Vec<UnifiedOption>,
}

#[derive(Debug, Clone)]
pub struct TokenRef {
    pub platform: String,
    pub token_id: String,
    pub label: String,
    pub option_id: String,
    pub condition_id: Option<String>,
    pub asset_id: Option<u64>,
    pub side_index: Option<u8>,
    pub neg_risk: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub key: TopicKey,
    pub title: String,
    pub market_title: String,
    pub end_date: Option<chrono::DateTime<chrono::Utc>>,
    pub tokens: Vec<TokenRef>,
}

impl Topic {
    pub fn token(&self, platform: &str, label: &str) -> Option<&TokenRef> {
        let label = label.to_ascii_lowercase();
        self.tokens.iter().find(|t| {
            t.platform == platform && t.label.to_ascii_lowercase() == label
        })
    }

    pub fn labels(&self) -> Vec<String> {
        let mut set = HashSet::new();
        for token in &self.tokens {
            set.insert(token.label.to_ascii_lowercase());
        }
        let mut labels: Vec<_> = set.into_iter().collect();
        labels.sort();
        labels
    }
}

pub fn side_coin(outcome_id: u64, side_index: u8) -> String {
    format!("#{outcome_id}{side_index}")
}

pub fn side_asset_id(outcome_id: u64, side_index: u8) -> u64 {
    OUTCOME_ASSET_BASE + outcome_id * 10 + u64::from(side_index)
}

pub fn parse_side_coin(coin: &str) -> Option<(u64, u8)> {
    let rest = coin.strip_prefix('#')?;
    if rest.is_empty() {
        return None;
    }
    let side_index = rest.chars().last()?.to_digit(10)? as u8;
    if side_index > 1 {
        return None;
    }
    let outcome_id = rest[..rest.len() - 1].parse().ok()?;
    Some((outcome_id, side_index))
}

pub fn validate_outcome_option(option: &PlatformOption) -> Result<()> {
    let outcome_id: u64 = option
        .option_id
        .parse()
        .map_err(|_| Error::msg(format!("invalid outcome optionId {}", option.option_id)))?;
    if option.outcomes.len() != 2 {
        return Err(Error::msg("outcome market must have exactly two sides"));
    }
    for spec in &option.outcomes {
        let side_index = spec
            .side_index
            .or_else(|| parse_side_coin(&spec.token_id).map(|(_, s)| s))
            .ok_or_else(|| Error::msg("missing outcome sideIndex"))?;
        if side_index > 1 {
            return Err(Error::msg("outcome sideIndex must be 0 or 1"));
        }
        let expected_coin = side_coin(outcome_id, side_index);
        if spec.token_id != expected_coin {
            return Err(Error::msg(format!(
                "outcome tokenId {} != expected {expected_coin}",
                spec.token_id
            )));
        }
        let expected_asset = side_asset_id(outcome_id, side_index);
        if let Some(asset_id) = spec.asset_id {
            if asset_id != expected_asset {
                return Err(Error::msg(format!(
                    "outcome assetId {asset_id} != expected {expected_asset}"
                )));
            }
        }
    }
    Ok(())
}

pub fn tradable_topics(event: &CatalogEvent, enabled: &HashSet<String>) -> Vec<Topic> {
    let mut topics = Vec::new();
    for option in &event.unified_options {
        match build_topic(event, option, enabled) {
            Ok(Some(topic)) => topics.push(topic),
            Ok(None) => {}
            Err(err) => tracing::warn!(
                event_id = %event.id,
                unified_index = option.index,
                error = %err,
                "skip unified option"
            ),
        }
    }
    topics
}

fn build_topic(
    event: &CatalogEvent,
    option: &UnifiedOption,
    enabled: &HashSet<String>,
) -> Result<Option<Topic>> {
    let mut by_platform: HashMap<String, &PlatformOption> = HashMap::new();
    for po in &option.platform_options {
        let platform = po.platform.trim().to_ascii_lowercase();
        if !enabled.contains(&platform) {
            continue;
        }
        if po.outcomes.len() != 2 {
            continue;
        }
        if platform == OUTCOME {
            validate_outcome_option(po)?;
        }
        by_platform.insert(platform, po);
    }
    if !(by_platform.contains_key(POLYMARKET) && by_platform.contains_key(OUTCOME)) {
        return Ok(None);
    }
    let mut label_sets = Vec::new();
    let mut tokens = Vec::new();
    for (platform, po) in &by_platform {
        let mut labels = HashSet::new();
        for spec in &po.outcomes {
            let label = spec.label.trim().to_ascii_lowercase();
            if label.is_empty() || spec.token_id.trim().is_empty() {
                return Ok(None);
            }
            labels.insert(label.clone());
            tokens.push(TokenRef {
                platform: platform.clone(),
                token_id: spec.token_id.clone(),
                label,
                option_id: po.option_id.clone(),
                condition_id: po.condition_id.clone(),
                asset_id: spec.asset_id.or_else(|| {
                    if platform == OUTCOME {
                        parse_side_coin(&spec.token_id).map(|(id, side)| side_asset_id(id, side))
                    } else {
                        None
                    }
                }),
                side_index: spec.side_index.or_else(|| {
                    parse_side_coin(&spec.token_id).map(|(_, side)| side)
                }),
                neg_risk: spec.neg_risk.or(po.neg_risk),
            });
        }
        label_sets.push(labels);
    }
    let first = &label_sets[0];
    if label_sets.iter().any(|set| set != first) || first.len() != 2 {
        return Ok(None);
    }
    Ok(Some(Topic {
        key: TopicKey::new(event.id, option.index),
        title: event.title.clone(),
        market_title: option.title.clone(),
        end_date: event.end_date,
        tokens,
    }))
}

fn de_stringish<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        other => Err(serde::de::Error::custom(format!(
            "expected string or number, got {other}"
        ))),
    }
}

fn de_opt_stringish<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) if s.is_empty() => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s)),
        Some(serde_json::Value::Number(n)) => Ok(Some(n.to_string())),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected string or number, got {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_outcome_ids() {
        assert_eq!(side_coin(516, 0), "#5160");
        assert_eq!(side_coin(516, 1), "#5161");
        assert_eq!(side_coin(9, 0), "#90");
        assert_eq!(side_asset_id(516, 0), 100_005_160);
        assert_eq!(parse_side_coin("#5160"), Some((516, 0)));
        assert_eq!(parse_side_coin("#90"), Some((9, 0)));
        assert_eq!(parse_side_coin("#5162"), None);
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
}
