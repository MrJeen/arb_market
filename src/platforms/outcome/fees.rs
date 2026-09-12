//! Outcome long-only 估费规则；不是实际成交或结算账务证据。
use crate::domain::{side_asset_id, side_coin, OUTCOME_ASSET_BASE};
use crate::error::{Error, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub const FEE_REFRESH_INTERVAL: Duration = Duration::from_secs(300);
pub const FEE_MAX_AGE: Duration = Duration::from_secs(900);

#[derive(Debug, Clone)]
struct SourceTime {
    monotonic: Instant,
    utc: DateTime<Utc>,
}

impl SourceTime {
    fn now() -> Self {
        Self {
            monotonic: Instant::now(),
            utc: Utc::now(),
        }
    }
    fn fresh_at(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.monotonic) < FEE_MAX_AGE
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AccountRules {
    base_rate: Decimal,
    discount: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MarketRules {
    scale: Decimal,
    taker_rate: Decimal,
}

#[derive(Debug, Clone)]
pub struct OutcomeFeeSnapshot {
    pub taker_rate: Decimal,
    pub builder_rate: Decimal,
    outcome_id: u64,
    account: AccountRules,
    market: MarketRules,
    account_version: u64,
    market_version: u64,
    account_time: SourceTime,
    market_time: SourceTime,
}

impl OutcomeFeeSnapshot {
    #[cfg(test)]
    pub(crate) fn expire_test(&mut self) {
        self.account_time.monotonic = Instant::now() - FEE_MAX_AGE;
    }

    pub fn source_ages_ms(&self) -> (u64, u64) {
        let now = Instant::now();
        (
            now.saturating_duration_since(self.account_time.monotonic)
                .as_millis() as u64,
            now.saturating_duration_since(self.market_time.monotonic)
                .as_millis() as u64,
        )
    }

    pub fn is_fresh(&self) -> bool {
        let now = Instant::now();
        self.account_time.fresh_at(now) && self.market_time.fresh_at(now)
    }

    /// Bound persisted estimates by both original wall-clock deadlines and remaining
    /// monotonic lifetime. Recomputing a projection never renews its fee sources.
    pub fn valid_until(&self) -> Option<DateTime<Utc>> {
        self.valid_until_at(Instant::now(), Utc::now())
    }

    fn valid_until_at(&self, instant: Instant, utc: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut deadline =
            utc.checked_add_signed(chrono::Duration::seconds(FEE_MAX_AGE.as_secs() as i64))?;
        for source in [&self.account_time, &self.market_time] {
            let remaining =
                FEE_MAX_AGE.checked_sub(instant.saturating_duration_since(source.monotonic))?;
            if remaining.is_zero() {
                return None;
            }
            let wall = source
                .utc
                .checked_add_signed(chrono::Duration::seconds(FEE_MAX_AGE.as_secs() as i64))?;
            let monotonic = utc.checked_add_signed(chrono::Duration::from_std(remaining).ok()?)?;
            deadline = deadline.min(wall).min(monotonic);
        }
        // Persist whole Unix seconds conservatively; exact expiry is unavailable.
        let deadline = DateTime::from_timestamp(deadline.timestamp(), 0)?;
        (deadline > utc).then_some(deadline)
    }

    /// 成功续期不改变经济规则；账户或本市场参数变化后旧确认失效。
    pub fn same_rules(&self, other: &Self) -> bool {
        self.outcome_id == other.outcome_id
            && self.account_version == other.account_version
            && self.market_version == other.market_version
            && self.account == other.account
            && self.market == other.market
            && self.taker_rate == other.taker_rate
            && self.builder_rate == other.builder_rate
    }

    pub fn estimate_json(&self) -> Value {
        json!({
            "version": 1,
            "fee_model": "out_usdc_taker_close_v1",
            "outcome_id": self.outcome_id.to_string(),
            "token_ids": [side_coin(self.outcome_id, 0), side_coin(self.outcome_id, 1)],
            "venue": "out", "quote_token": "USDC", "fee_scale": "1",
            "account_version": self.account_version.to_string(),
            "market_version": self.market_version.to_string(),
            "user_fees_fetched_at": self.account_time.utc.to_rfc3339(),
            "outcome_meta_fetched_at": self.market_time.utc.to_rfc3339(),
            "max_age_secs": FEE_MAX_AGE.as_secs(),
            "user_spot_cross_rate": self.account.base_rate.to_string(),
            "active_referral_discount": self.account.discount.to_string(),
            "deployer_fee_scale": self.market.scale.to_string(),
            "taker_rate": self.taker_rate.to_string(),
            "builder_rate": self.builder_rate.to_string(),
            "protocol_open_rate": "0",
            "settlement_reserve_rate": self.taker_rate.to_string(),
            "settlement_fee_policy": "estimated_from_taker_close",
            "settlement_builder_rate": "0"
        })
    }
}

#[derive(Clone)]
pub(super) struct ParsedAccount {
    rules: AccountRules,
    time: SourceTime,
}

#[derive(Clone)]
pub(super) struct ParsedMarkets {
    rules: BTreeMap<u64, std::result::Result<MarketRules, &'static str>>,
    time: SourceTime,
}

#[derive(Default, Clone)]
pub(super) struct FeeCache {
    snapshot: Option<std::sync::Arc<CombinedSnapshot>>,
    reported_available: bool,
}

#[derive(Clone)]
struct CombinedSnapshot {
    account: ParsedAccount,
    markets: ParsedMarkets,
    account_version: u64,
    market_versions: BTreeMap<u64, u64>,
    generation: u64,
    builder_rate: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeLookupError {
    Missing,
    Expired,
    MarketMissing,
    MarketInvalid(&'static str),
    InvalidIdentity,
    CacheLock,
}

impl FeeLookupError {
    pub fn detail(self) -> &'static str {
        match self {
            Self::MarketInvalid(detail) => detail,
            _ => self.reason(),
        }
    }

    pub fn reason(self) -> &'static str {
        match self {
            Self::Missing => "current_fee_snapshot_missing",
            Self::Expired => "current_fee_snapshot_expired",
            Self::MarketMissing => "current_fee_market_missing",
            Self::MarketInvalid(_) => "unsupported_fee_market",
            Self::InvalidIdentity => "invalid_fee_market_identity",
            Self::CacheLock => "fee_cache_lock_error",
        }
    }
}

impl std::fmt::Display for FeeLookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason())
    }
}
impl std::error::Error for FeeLookupError {}

impl FeeCache {
    #[cfg(test)]
    pub(super) fn get(&self, outcome_id: u64) -> Result<OutcomeFeeSnapshot> {
        self.lookup(outcome_id)
            .map_err(|err| Error::msg(err.reason()))
    }

    pub(super) fn lookup(
        &self,
        outcome_id: u64,
    ) -> std::result::Result<OutcomeFeeSnapshot, FeeLookupError> {
        let combined = self.snapshot.as_ref().ok_or(FeeLookupError::Missing)?;
        let now = Instant::now();
        if !combined.account.time.fresh_at(now) || !combined.markets.time.fresh_at(now) {
            return Err(FeeLookupError::Expired);
        }
        let market = combined
            .markets
            .rules
            .get(&outcome_id)
            .ok_or(FeeLookupError::MarketMissing)?
            .as_ref()
            .map_err(|reason| FeeLookupError::MarketInvalid(reason))?;
        Ok(OutcomeFeeSnapshot {
            taker_rate: market.taker_rate,
            builder_rate: combined.builder_rate,
            outcome_id,
            account: combined.account.rules.clone(),
            market: market.clone(),
            account_version: combined.account_version,
            market_version: combined.market_versions[&outcome_id],
            account_time: combined.account.time.clone(),
            market_time: combined.markets.time.clone(),
        })
    }

    pub(super) fn publish(
        &mut self,
        account: ParsedAccount,
        markets: ParsedMarkets,
        builder_rate: Decimal,
    ) -> Result<()> {
        let old = self.snapshot.as_ref();
        let generation = old
            .map_or(Some(1), |s| s.generation.checked_add(1))
            .ok_or_else(|| Error::msg("outcome fee version exhausted"))?;
        let account_version = old
            .filter(|s| s.account.rules == account.rules)
            .map_or(generation, |s| s.account_version);
        let market_versions = markets
            .rules
            .iter()
            .map(|(id, rules)| {
                let version = old
                    .filter(|s| s.markets.rules.get(id) == Some(rules))
                    .and_then(|s| s.market_versions.get(id))
                    .copied()
                    .unwrap_or(generation);
                (*id, version)
            })
            .collect();
        self.snapshot = Some(std::sync::Arc::new(CombinedSnapshot {
            account,
            markets,
            account_version,
            market_versions,
            generation,
            builder_rate,
        }));
        self.report_availability();
        let snapshot = self.snapshot.as_ref().unwrap();
        tracing::debug!(
            service = "outcome",
            api = "fee_snapshot",
            generation,
            account_version,
            markets = snapshot.markets.rules.len(),
            unavailable_markets = snapshot
                .markets
                .rules
                .values()
                .filter(|v| v.is_err())
                .count(),
            "outcome fee snapshot refreshed"
        );
        Ok(())
    }

    pub(super) fn report_availability(&mut self) {
        let now = Instant::now();
        let available = self.snapshot.as_ref().is_some_and(|s| {
            s.account.time.fresh_at(now)
                && s.markets.time.fresh_at(now)
                && s.markets.rules.values().any(|r| r.is_ok())
        });
        if available != self.reported_available {
            tracing::info!(
                service = "outcome",
                api = "fee_snapshot",
                available,
                "outcome fee availability changed"
            );
            self.reported_available = available;
        }
    }

    #[cfg(test)]
    pub(crate) fn expire_test(&mut self) {
        if let Some(snapshot) = &mut self.snapshot {
            let snapshot = std::sync::Arc::make_mut(snapshot);
            snapshot.account.time.monotonic = Instant::now() - FEE_MAX_AGE;
            snapshot.markets.time.monotonic = Instant::now() - FEE_MAX_AGE;
        }
    }

    #[cfg(test)]
    pub(crate) fn install_test(&mut self, id: u64, taker_rate: Decimal, builder_rate: Decimal) {
        let account = ParsedAccount {
            rules: AccountRules {
                base_rate: taker_rate / Decimal::TWO,
                discount: Decimal::ZERO,
            },
            time: SourceTime::now(),
        };
        let mut rules = self
            .snapshot
            .as_ref()
            .map(|s| s.markets.rules.clone())
            .unwrap_or_default();
        rules.insert(
            id,
            Ok(MarketRules {
                scale: Decimal::ONE,
                taker_rate,
            }),
        );
        self.publish(
            account,
            ParsedMarkets {
                rules,
                time: SourceTime::now(),
            },
            builder_rate,
        )
        .unwrap();
    }
}

fn decimal(value: Option<&Value>) -> Option<Decimal> {
    match value? {
        Value::String(s) => Decimal::from_str_exact(s).ok(),
        Value::Number(n) => Decimal::from_str_exact(&n.to_string()).ok(),
        _ => None,
    }
}

pub(super) fn parse_account(value: &Value) -> Result<ParsedAccount> {
    let base_rate = decimal(value.get("userSpotCrossRate"))
        .filter(|v| *v >= Decimal::ZERO)
        .ok_or_else(|| Error::msg("userFees invalid userSpotCrossRate"))?;
    let discount = decimal(value.get("activeReferralDiscount"))
        .filter(|v| *v >= Decimal::ZERO && *v <= Decimal::ONE)
        .ok_or_else(|| Error::msg("userFees invalid activeReferralDiscount"))?;
    Ok(ParsedAccount {
        rules: AccountRules {
            base_rate,
            discount,
        },
        time: SourceTime::now(),
    })
}

pub(super) fn parse_markets(value: &Value, account: &ParsedAccount) -> Result<ParsedMarkets> {
    let outcomes = value
        .get("outcomes")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::msg("outcomeMeta invalid outcomes array"))?;
    let top_supported = decimal(value.get("feeScale")) == Some(Decimal::ONE);
    let mut rules = BTreeMap::new();
    for outcome in outcomes {
        let id = outcome
            .get("outcome")
            .and_then(Value::as_u64)
            .filter(|id| *id > 0 && *id <= (u64::MAX - OUTCOME_ASSET_BASE - 1) / 10)
            .ok_or_else(|| Error::msg("outcomeMeta invalid outcome identity"))?;
        let rule = parse_market(outcome, id, top_supported, &account.rules);
        if rules.insert(id, rule).is_some() {
            return Err(Error::msg("outcomeMeta duplicate outcome identity"));
        }
    }
    Ok(ParsedMarkets {
        rules,
        time: SourceTime::now(),
    })
}

fn parse_market(
    value: &Value,
    id: u64,
    top_supported: bool,
    account: &AccountRules,
) -> std::result::Result<MarketRules, &'static str> {
    if !top_supported {
        return Err("outcomeMeta unsupported feeScale");
    }
    if value.get("venue").and_then(Value::as_str) != Some("out") {
        return Err("outcomeMeta unsupported venue");
    }
    if value.get("quoteToken").and_then(Value::as_str) != Some("USDC") {
        return Err("outcomeMeta unsupported quoteToken");
    }
    let sides = value
        .get("sideSpecs")
        .and_then(Value::as_array)
        .filter(|s| s.len() == 2)
        .ok_or("outcomeMeta invalid sideSpecs")?;
    for (index, side) in sides.iter().enumerate() {
        if !side.is_object()
            || side
                .get("name")
                .and_then(Value::as_str)
                .is_none_or(|name| name.is_empty())
            || side
                .get("sideIndex")
                .is_some_and(|v| v.as_u64() != Some(index as u64))
            || side
                .get("tokenId")
                .is_some_and(|v| v.as_str() != Some(side_coin(id, index as u8).as_str()))
            || side
                .get("assetId")
                .is_some_and(|v| v.as_u64() != Some(side_asset_id(id, index as u8)))
        {
            return Err("outcomeMeta invalid side identity");
        }
    }
    let scale = decimal(value.get("deployerFeeScale"))
        .filter(|s| *s >= Decimal::ZERO && *s <= Decimal::TEN)
        .ok_or("outcomeMeta invalid deployerFeeScale")?;
    // 仅当前已验证的 out/USDC 模型；推荐折扣一次，不能再乘固定 2。
    let taker_rate = account
        .base_rate
        .checked_mul(Decimal::ONE - account.discount)
        .and_then(|r| r.checked_mul(scale + scale.max(Decimal::ONE)))
        .filter(|r| *r >= Decimal::ZERO && *r < Decimal::ONE)
        .ok_or("outcomeMeta invalid derived taker rate")?;
    Ok(MarketRules { scale, taker_rate })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn account() -> ParsedAccount {
        parse_account(&json!({"userSpotCrossRate":"0.0007", "activeReferralDiscount":"0.04"}))
            .unwrap()
    }
    pub(super) fn meta() -> Value {
        json!({"feeScale":"1.0", "outcomes":[{"outcome":516,"venue":"out","quoteToken":"USDC","deployerFeeScale":"1.0","sideSpecs":[{"name":"Yes"},{"name":"No"}]}]})
    }
    fn publish(cache: &mut FeeCache, value: &Value) {
        let account = account();
        let markets = parse_markets(value, &account).unwrap();
        cache.publish(account, markets, Decimal::ZERO).unwrap();
    }
    #[test]
    fn persisted_deadline_uses_both_sources_and_never_renews() {
        let mut cache = FeeCache::default();
        publish(&mut cache, &meta());
        let mut snapshot = cache.get(516).unwrap();
        let instant = Instant::now();
        let utc = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        for account_old in [true, false] {
            snapshot.account_time = SourceTime {
                monotonic: instant,
                utc,
            };
            snapshot.market_time = SourceTime {
                monotonic: instant,
                utc,
            };
            let old = if account_old {
                &mut snapshot.account_time
            } else {
                &mut snapshot.market_time
            };
            old.monotonic = instant - Duration::from_secs(100);
            old.utc = utc - chrono::Duration::seconds(100);
            let deadline = utc + chrono::Duration::seconds(800);
            assert_eq!(snapshot.valid_until_at(instant, utc), Some(deadline));
            assert_eq!(
                snapshot.valid_until_at(
                    instant + Duration::from_secs(50),
                    utc + chrono::Duration::seconds(50)
                ),
                Some(deadline)
            );
            assert_eq!(
                snapshot.valid_until_at(instant + Duration::from_secs(800), deadline),
                None
            );
            // Forward/backward wall-clock movement cannot extend monotonic TTL.
            assert!(
                snapshot
                    .valid_until_at(instant, utc - chrono::Duration::seconds(50))
                    .unwrap()
                    <= deadline
            );
            assert_eq!(snapshot.valid_until_at(instant, deadline), None);
        }
    }

    #[test]
    fn sample_scale_and_discount_boundaries() {
        for (scale, expected) in [("0", "0.000672"), ("1", "0.001344"), ("10", "0.01344")] {
            let mut value = meta();
            value["outcomes"][0]["deployerFeeScale"] = json!(scale);
            let mut cache = FeeCache::default();
            publish(&mut cache, &value);
            assert_eq!(
                cache.get(516).unwrap().taker_rate,
                Decimal::from_str_exact(expected).unwrap()
            );
        }
        for (discount, expected) in [("0", "0.0014"), ("1", "0")] {
            let account = parse_account(
                &json!({"userSpotCrossRate":"0.0007", "activeReferralDiscount":discount}),
            )
            .unwrap();
            let markets = parse_markets(&meta(), &account).unwrap();
            let mut cache = FeeCache::default();
            cache.publish(account, markets, Decimal::ZERO).unwrap();
            assert_eq!(
                cache.get(516).unwrap().taker_rate,
                Decimal::from_str_exact(expected).unwrap()
            );
        }
    }
    #[test]
    fn invalid_account_and_market_inputs_fail_closed() {
        for field in ["userSpotCrossRate", "activeReferralDiscount"] {
            for bad in [
                json!(null),
                json!(true),
                json!({}),
                json!("NaN"),
                json!("-1"),
                json!("1e999"),
            ] {
                let mut value =
                    json!({"userSpotCrossRate":"0.0007","activeReferralDiscount":"0.04"});
                value[field] = bad;
                assert!(parse_account(&value).is_err());
            }
        }
        assert!(
            parse_account(&json!({"userSpotCrossRate":"1", "activeReferralDiscount":"1.01"}))
                .is_err()
        );
        for field in ["venue", "quoteToken", "deployerFeeScale", "sideSpecs"] {
            let mut value = meta();
            value["outcomes"][0][field] = Value::Null;
            let mut cache = FeeCache::default();
            publish(&mut cache, &value);
            assert!(cache.get(516).is_err());
        }
        for scale in [json!("-0.1"), json!("10.1"), json!(true), json!("NaN")] {
            let mut value = meta();
            value["outcomes"][0]["deployerFeeScale"] = scale;
            let mut cache = FeeCache::default();
            publish(&mut cache, &value);
            assert!(cache.get(516).is_err());
        }
        for top in [Value::Null, json!("2"), json!(true)] {
            let mut value = meta();
            value["feeScale"] = top;
            let mut cache = FeeCache::default();
            publish(&mut cache, &value);
            assert!(cache.get(516).is_err());
        }
        for base in ["1", "79228162514264337593543950335"] {
            let account =
                parse_account(&json!({"userSpotCrossRate":base,"activeReferralDiscount":"0"}))
                    .unwrap();
            assert!(parse_markets(&meta(), &account).unwrap().rules[&516].is_err());
        }
    }
    #[test]
    fn identities_and_single_market_invalidation() {
        let mut value = meta();
        let mut second = value["outcomes"][0].clone();
        second["outcome"] = json!(517);
        value["outcomes"].as_array_mut().unwrap().push(second);
        let mut cache = FeeCache::default();
        publish(&mut cache, &value);
        let first = cache.get(516).unwrap();
        value["outcomes"][1]["sideSpecs"][0]["tokenId"] = json!("#5160");
        publish(&mut cache, &value);
        assert!(cache.get(517).is_err());
        assert!(first.same_rules(&cache.get(516).unwrap()));
        value["outcomes"][1]["outcome"] = json!(516);
        assert!(parse_markets(&value, &account()).is_err());
        value["outcomes"][1]["outcome"] = json!("517");
        assert!(parse_markets(&value, &account()).is_err());
    }
    #[test]
    fn concurrent_readers_observe_one_atomic_generation() {
        let mut cache = FeeCache::default();
        publish(&mut cache, &meta());
        let cache = std::sync::Arc::new(std::sync::RwLock::new(cache));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    for _ in 0..500 {
                        let snapshot = cache.read().unwrap().get(516).unwrap();
                        assert_eq!(
                            snapshot.taker_rate,
                            snapshot.account.base_rate
                                * (Decimal::ONE - snapshot.account.discount)
                                * (snapshot.market.scale + snapshot.market.scale.max(Decimal::ONE))
                        );
                        assert!(snapshot.is_fresh());
                    }
                })
            })
            .collect();
        for i in 0..100 {
            let mut value = meta();
            value["outcomes"][0]["deployerFeeScale"] = json!(if i % 2 == 0 { "1" } else { "2" });
            let mut next = cache.read().unwrap().clone();
            publish(&mut next, &value);
            *cache.write().unwrap() = next;
        }
        for reader in readers {
            reader.join().unwrap();
        }
    }

    #[test]
    fn account_rules_change_invalidates_even_when_derived_rate_is_same() {
        let mut cache = FeeCache::default();
        publish(&mut cache, &meta());
        let old = cache.get(516).unwrap();
        let changed =
            parse_account(&json!({"userSpotCrossRate":"0.001344", "activeReferralDiscount":"0.5"}))
                .unwrap();
        let markets = parse_markets(&meta(), &changed).unwrap();
        cache.publish(changed, markets, Decimal::ZERO).unwrap();
        let new = cache.get(516).unwrap();
        assert_eq!(old.taker_rate, new.taker_rate);
        assert!(!old.same_rules(&new));
        cache.expire_test();
        assert!(cache.get(516).is_err());
        publish(&mut cache, &meta());
        assert!(cache.get(516).unwrap().is_fresh());
    }

    #[test]
    fn renewals_versions_and_both_source_expiry() {
        let mut cache = FeeCache::default();
        let mut value = meta();
        publish(&mut cache, &value);
        let old = cache.get(516).unwrap();
        publish(&mut cache, &value);
        let renewed = cache.get(516).unwrap();
        assert!(old.same_rules(&renewed));
        assert!(renewed.account_time.monotonic >= old.account_time.monotonic);
        value["outcomes"][0]["deployerFeeScale"] = json!("2");
        publish(&mut cache, &value);
        assert!(!old.same_rules(&cache.get(516).unwrap()));
        value["outcomes"][0]["deployerFeeScale"] = json!("1");
        publish(&mut cache, &value);
        assert!(!old.same_rules(&cache.get(516).unwrap()));
        let snapshot = cache.get(516).unwrap();
        for account_source in [true, false] {
            let mut stale = snapshot.clone();
            let source = if account_source {
                &mut stale.account_time
            } else {
                &mut stale.market_time
            };
            source.monotonic = Instant::now() - FEE_MAX_AGE;
            assert!(!stale.is_fresh());
        }
        let instant = Instant::now();
        let time = SourceTime {
            monotonic: instant,
            utc: Utc::now(),
        };
        assert!(time.fresh_at(instant + FEE_MAX_AGE - Duration::from_nanos(1)));
        assert!(!time.fresh_at(instant + FEE_MAX_AGE));
        assert_eq!(
            Decimal::from_str_exact(old.estimate_json()["taker_rate"].as_str().unwrap()).unwrap(),
            Decimal::new(1344, 6)
        );
    }
}
