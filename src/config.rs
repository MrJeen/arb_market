use crate::error::{Error, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

pub const POLYMARKET: &str = "polymarket";
pub const OUTCOME: &str = "outcome";

#[derive(Debug, Clone)]
pub struct Config {
    pub common_postgres_uri: String,
    pub app_postgres_uri: String,
    pub enabled_platforms: HashSet<String>,
    pub enable_buy: bool,
    pub discovery_interval: Duration,
    pub reconcile_interval: Duration,
    pub hedge_interval: Duration,
    pub book_stale: Duration,
    pub book_resync: Duration,
    pub book_resync_batch: usize,
    pub arb_min_profit: Decimal,
    pub arb_min_apr: Decimal,
    pub arb_cost_limit: Decimal,
    pub extra_cost_multiplier: Decimal,
    pub min_rebalance_qty: Decimal,
    pub polymarket_fee_bps_prior: Decimal,
    pub outcome_taker_fee_rate: Decimal,
    pub polymarket_clob_url: String,
    pub polymarket_ws_url: String,
    pub polymarket_funders: Vec<PolymarketFunderConfig>,
    pub hyperliquid_info_url: String,
    pub hyperliquid_exchange_url: String,
    pub hyperliquid_ws_url: String,
    pub hyperliquid_mainnet: bool,
    pub outcome_agent_private_key: Option<String>,
    pub outcome_account_address: Option<String>,
    pub nats_url: Option<String>,
    pub nats_token: Option<String>,
    pub nats_subject: String,
    pub nats_channel: String,
    pub cat: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketFunderConfig {
    pub funder_address: String,
    pub wallet_private_key: String,
    pub is_wallet_v2: bool,
    pub service: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();
        let enabled = parse_enabled_platforms(env_opt("ENABLED_PLATFORMS"));
        let funders = load_funders()?;
        Ok(Self {
            common_postgres_uri: env_required("COMMON_POSTGRES_URI")?,
            app_postgres_uri: env_required("APP_POSTGRES_URI")?,
            enabled_platforms: enabled,
            enable_buy: env_bool("ENABLE_BUY", false),
            discovery_interval: Duration::from_secs(env_u64("DISCOVERY_INTERVAL_SECS", 30)),
            reconcile_interval: Duration::from_secs(env_u64("RECONCILE_INTERVAL_SECS", 2)),
            hedge_interval: Duration::from_secs(env_u64("HEDGE_INTERVAL_SECS", 5)),
            book_stale: Duration::from_millis(env_u64("BOOK_STALE_MS", 5000)),
            book_resync: Duration::from_secs(env_u64("BOOK_RESYNC_SECS", 10)),
            book_resync_batch: env_u64("BOOK_RESYNC_BATCH", 80) as usize,
            arb_min_profit: env_decimal("ARB_MIN_PROFIT", "3")?,
            arb_min_apr: env_decimal("ARB_MIN_APR", "0")?,
            arb_cost_limit: env_decimal("ARB_COST_LIMIT", "100")?,
            extra_cost_multiplier: env_decimal("ARB_EXTRA_COST_MULTIPLIER", "1.3")?,
            min_rebalance_qty: env_decimal("MIN_REBALANCE_QTY", "1.5")?,
            polymarket_fee_bps_prior: env_decimal("POLYMARKET_FEE_BPS_PRIOR", "0")?,
            outcome_taker_fee_rate: env_decimal("OUTCOME_TAKER_FEE_RATE", "0.00035")?,
            polymarket_clob_url: env_or(
                "POLYMARKET_CLOB_URL",
                "https://clob.polymarket.com",
            ),
            polymarket_ws_url: env_or(
                "POLYMARKET_WS_URL",
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            ),
            polymarket_funders: funders,
            hyperliquid_info_url: env_or(
                "HYPERLIQUID_INFO_URL",
                "https://api.hyperliquid.xyz/info",
            ),
            hyperliquid_exchange_url: env_or(
                "HYPERLIQUID_EXCHANGE_URL",
                "https://api.hyperliquid.xyz/exchange",
            ),
            hyperliquid_ws_url: env_or("HYPERLIQUID_WS_URL", "wss://api.hyperliquid.xyz/ws"),
            hyperliquid_mainnet: env_bool("HYPERLIQUID_MAINNET", true),
            outcome_agent_private_key: env_opt("OUTCOME_AGENT_PRIVATE_KEY"),
            outcome_account_address: env_opt("OUTCOME_ACCOUNT_ADDRESS"),
            nats_url: env_opt("NATS_URL"),
            nats_token: env_opt("NATS_TOKEN"),
            nats_subject: env_or("NATS_TG_SUBJECT", "tg.notification"),
            nats_channel: env_or("NATS_TG_CHANNEL", "ARB"),
            cat: env_or("CAT", "market-arb"),
        })
    }

    pub fn platform_enabled(&self, platform: &str) -> bool {
        self.enabled_platforms.contains(platform)
    }
}

pub fn parse_enabled_platforms(raw: Option<String>) -> HashSet<String> {
    let text = raw.unwrap_or_default();
    let parts: Vec<String> = text
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    let selected = if parts.is_empty() {
        vec![POLYMARKET.to_string(), OUTCOME.to_string()]
    } else {
        parts
    };
    selected
        .into_iter()
        .filter(|p| p == POLYMARKET || p == OUTCOME || true)
        .collect()
}

const DEFAULT_FUNDER_KEYS_FILE: &str = "polymarket_funders.json";

fn load_funders() -> Result<Vec<PolymarketFunderConfig>> {
    if let Some(path) = env_opt("POLYMARKET_FUNDER_KEYS_FILE") {
        return parse_funders_file(&path);
    }
    if let Some(raw) = env_opt("POLYMARKET_FUNDER_PRIVATE_KEYS") {
        if raw != "{}" {
            return parse_funders(raw);
        }
    }
    if Path::new(DEFAULT_FUNDER_KEYS_FILE).is_file() {
        return parse_funders_file(DEFAULT_FUNDER_KEYS_FILE);
    }
    Ok(Vec::new())
}

fn parse_funders_file(path: &str) -> Result<Vec<PolymarketFunderConfig>> {
    let text = fs::read_to_string(path).map_err(|e| Error::Config(format!("read {path}: {e}")))?;
    parse_funders(text)
}

fn parse_funders(raw: String) -> Result<Vec<PolymarketFunderConfig>> {
    let text = raw.trim();
    if text.is_empty() || text == "{}" || text == "[]" {
        return Ok(Vec::new());
    }
    let parsed: serde_json::Value = serde_json::from_str(text)
        .map_err(|_| Error::Config("funder keys must be a JSON object or array".into()))?;
    if let Some(arr) = parsed.as_array() {
        return parse_funder_array(arr);
    }
    let obj = parsed
        .as_object()
        .ok_or_else(|| Error::Config("funder keys must be a JSON object or array".into()))?;
    let mut out = Vec::new();
    for (funder, value) in obj {
        let Some(item) = parse_funder_value(funder, value) else {
            continue;
        };
        out.push(item);
    }
    Ok(out)
}

fn parse_funder_array(arr: &[serde_json::Value]) -> Result<Vec<PolymarketFunderConfig>> {
    let mut out = Vec::new();
    for value in arr {
        let Some(map) = value.as_object() else {
            continue;
        };
        let funder = map
            .get("funderAddress")
            .or_else(|| map.get("funder"))
            .or_else(|| map.get("address"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let Some(item) = parse_funder_value(funder, value) else {
            continue;
        };
        out.push(item);
    }
    Ok(out)
}

fn parse_funder_value(funder: &str, value: &serde_json::Value) -> Option<PolymarketFunderConfig> {
    let (key, is_wallet_v2, service) = match value {
        serde_json::Value::String(s) => (s.clone(), false, None),
        serde_json::Value::Object(map) => {
            let key = map
                .get("walletPrivateKey")
                .or_else(|| map.get("privateKey"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let is_wallet_v2 = map
                .get("isWalletV2")
                .or_else(|| map.get("is_wallet_v2"))
                .and_then(|v| match v {
                    serde_json::Value::Bool(b) => Some(*b),
                    serde_json::Value::String(s) => {
                        Some(matches!(s.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
                    }
                    _ => None,
                })
                .unwrap_or(false);
            let service = map
                .get("service")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            (key, is_wallet_v2, service)
        }
        _ => return None,
    };
    if funder.trim().is_empty() || key.trim().is_empty() {
        return None;
    }
    Some(PolymarketFunderConfig {
        funder_address: funder.trim().to_string(),
        wallet_private_key: key.trim().to_string(),
        is_wallet_v2,
        service,
    })
}

fn env_required(key: &str) -> Result<String> {
    env_opt(key).ok_or_else(|| Error::Config(format!("missing {key}")))
}

fn env_opt(key: &str) -> Option<String> {
    env::var(key).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

fn env_or(key: &str, default: &str) -> String {
    env_opt(key).unwrap_or_else(|| default.to_string())
}

fn env_bool(key: &str, default: bool) -> bool {
    match env_opt(key) {
        Some(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        None => default,
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    env_opt(key)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_decimal(key: &str, default: &str) -> Result<Decimal> {
    let text = env_opt(key).unwrap_or_else(|| default.to_string());
    Decimal::from_str(&text).map_err(|_| Error::Config(format!("invalid decimal {key}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_enabled_platforms() {
        let set = parse_enabled_platforms(Some("polymarket, outcome".into()));
        assert!(set.contains(POLYMARKET));
        assert!(set.contains(OUTCOME));
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
        let raw = r#"[{"funderAddress":"0xAbc","walletPrivateKey":"0x1","isWalletV2":true,"service":"a"}]"#;
        let funders = parse_funders(raw.into()).unwrap();
        assert_eq!(funders.len(), 1);
        assert_eq!(funders[0].funder_address, "0xAbc");
        assert!(funders[0].is_wallet_v2);
    }

    #[test]
    fn parses_funder_file() {
        let path = std::env::temp_dir().join("market_arb_funders_test.json");
        std::fs::write(&path, r#"{"0xDef":{"walletPrivateKey":"0x2"}}"#).unwrap();
        let funders = parse_funders_file(path.to_str().unwrap()).unwrap();
        assert_eq!(funders[0].funder_address, "0xDef");
        let _ = std::fs::remove_file(path);
    }
}
