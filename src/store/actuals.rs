//! Frozen settlement reserve estimates, never actual fee or payout evidence.
use super::compute_actuals;
use crate::{config::OUTCOME, domain::side_coin};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::{collections::BTreeMap, str::FromStr};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, sqlx::FromRow)]
pub struct ProjectionLeg {
    pub id: i64,
    pub status: String,
    pub side: String,
    pub platform: String,
    pub label: String,
    pub token_id: String,
    pub wallet_address: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub last_order_info: Option<Value>,
    pub actual_shares: Option<Decimal>,
    pub actual_price: Option<Decimal>,
    pub actual_fee: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub enum Projection {
    Ready {
        actuals: (Decimal, Decimal, Decimal),
        evidence: Value,
    },
    Unknown {
        reason: String,
    },
}

impl Projection {
    pub fn actuals(&self) -> Option<(Decimal, Decimal, Decimal)> {
        match self {
            Self::Ready { actuals, .. } => Some(*actuals),
            Self::Unknown { .. } => None,
        }
    }
    pub fn evidence(&self) -> Value {
        match self {
            Self::Ready { evidence, .. } => evidence.clone(),
            Self::Unknown { reason } => {
                json!({"version":1,"status":"unknown","basis":"frozen_outcome_sell_payout_one","stale":true,"reason":reason,"computed_at":Utc::now()})
            }
        }
    }
}

pub(super) fn unknown_changed(previous: &Value, reason: &str) -> bool {
    previous.get("status").and_then(Value::as_str) != Some("unknown")
        || previous.get("reason").and_then(Value::as_str) != Some(reason)
}

/// Immutable, identity-bound cache evidence; never written back to a historical leg.
#[derive(Debug, Clone)]
pub struct LatestFee {
    pub wallet: String,
    pub token: String,
    pub snapshot: Value,
    pub valid_until: DateTime<Utc>,
}

pub type FallbackContext = BTreeMap<(String, String), LatestFee>;
pub type FeeResolver<'a> = dyn Fn(&str, &str) -> Option<LatestFee> + Send + Sync + 'a;

fn missing_snapshot(leg: &ProjectionLeg) -> bool {
    leg.submitted_at.is_some()
        && match &leg.last_order_info {
            None => true,
            Some(Value::Object(info)) => !info.contains_key("fee_estimate"),
            _ => false,
        }
}

fn snapshot(leg: &ProjectionLeg) -> Option<(Decimal, Decimal, Value)> {
    leg.submitted_at?;
    parse_snapshot(
        leg.last_order_info.as_ref()?.get("fee_estimate")?,
        &leg.token_id,
    )
}

pub(super) fn valid_existing_snapshot(leg: &ProjectionLeg) -> bool {
    leg.platform != OUTCOME
        || match &leg.last_order_info {
            None => true,
            Some(Value::Object(info)) => info
                .get("fee_estimate")
                .is_none_or(|value| parse_snapshot(value, &leg.token_id).is_some()),
            _ => false,
        }
}

pub(super) fn valid_pending_snapshot(leg: &ProjectionLeg) -> bool {
    if leg.platform != OUTCOME {
        return true;
    }
    leg.wallet_address
        .as_ref()
        .is_some_and(|w| !w.trim().is_empty())
        && leg
            .last_order_info
            .as_ref()
            .and_then(|v| v.get("fee_estimate"))
            .and_then(|v| parse_snapshot(v, &leg.token_id))
            .is_some()
}

fn parse_snapshot(v: &Value, token: &str) -> Option<(Decimal, Decimal, Value)> {
    if v.get("version")?.as_u64()? != 1
        || v.get("fee_model")?.as_str()? != "out_usdc_taker_close_v1"
    {
        return None;
    }
    let outcome = v.get("outcome_id")?.as_str()?.parse::<u64>().ok()?;
    let tokens = [side_coin(outcome, 0), side_coin(outcome, 1)];
    if !tokens.iter().any(|candidate| candidate == token) || v.get("token_ids")? != &json!(tokens) {
        return None;
    }
    let rate = |key| {
        let value = Decimal::from_str(v.get(key)?.as_str()?).ok()?;
        (value >= Decimal::ZERO && value <= Decimal::ONE).then_some(value)
    };
    let taker = rate("taker_rate")?;
    let builder = rate("builder_rate")?;
    if taker >= Decimal::ONE {
        return None;
    }
    // These timestamps identify the frozen source; admission freshness TTL does not apply.
    for key in ["user_fees_fetched_at", "outcome_meta_fetched_at"] {
        DateTime::parse_from_rfc3339(v.get(key)?.as_str()?).ok()?;
    }
    Some((taker, builder, v.clone()))
}

pub fn project(legs: &[ProjectionLeg]) -> Projection {
    project_with_fallback(legs, &FallbackContext::new(), Utc::now())
}

/// Pure projection: all cache observations and the calculation time are explicit.
pub fn project_with_fallback(
    legs: &[ProjectionLeg],
    fallback: &FallbackContext,
    now: DateTime<Utc>,
) -> Projection {
    match calculate(legs, fallback, now) {
        Ok((actuals, evidence)) => Projection::Ready { actuals, evidence },
        Err(reason) => Projection::Unknown { reason },
    }
}

fn calculate(
    legs: &[ProjectionLeg],
    fallback: &FallbackContext,
    now: DateTime<Utc>,
) -> Result<((Decimal, Decimal, Decimal), Value), String> {
    let mut rows = Vec::new();
    let mut groups: BTreeMap<(String, String), (Decimal, Vec<&ProjectionLeg>)> = BTreeMap::new();
    for leg in legs {
        let invalid = || format!("leg {} has unresolved or invalid trade evidence", leg.id);
        if !matches!(
            leg.status.as_str(),
            "matched" | "completed" | "failed" | "cancelled"
        ) {
            return Err(invalid());
        }
        let qty = leg.actual_shares.ok_or_else(invalid)?;
        if qty < Decimal::ZERO {
            return Err(invalid());
        }
        if qty.is_zero() {
            continue;
        }
        let price = leg.actual_price.ok_or_else(invalid)?;
        let fee = leg.actual_fee.ok_or_else(invalid)?;
        if price < Decimal::ZERO || !matches!(leg.side.as_str(), "BUY" | "SELL") {
            return Err(invalid());
        }
        rows.push((
            leg.side.clone(),
            leg.platform.clone(),
            leg.label.clone(),
            qty,
            price,
            fee,
        ));
        if leg.platform == OUTCOME {
            let wallet = leg
                .wallet_address
                .as_ref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(invalid)?
                .trim()
                .to_ascii_lowercase();
            let entry = groups.entry((wallet, leg.token_id.clone())).or_default();
            entry.0 += if leg.side == "SELL" { -qty } else { qty };
            entry.1.push(leg);
        }
    }
    let (cost, gross, _) = compute_actuals(&rows);
    let mut reserve = Decimal::ZERO;
    let mut evidence = Vec::new();
    let mut fallback_valid_until: Option<DateTime<Utc>> = None;
    for ((wallet, token), (qty, candidates)) in groups {
        if qty < Decimal::ZERO {
            return Err(format!("negative Outcome position for {wallet}/{token}"));
        }
        if qty.is_zero() {
            continue;
        }
        let frozen = candidates
            .iter()
            .filter_map(|leg| snapshot(leg).map(|(t, b, s)| (*leg, t, b, s)))
            .max_by_key(|(leg, ..)| (leg.submitted_at, leg.id));
        let (taker, builder, mut source) = if let Some((leg, t, b, snapshot)) = frozen {
            (
                t,
                b,
                json!({"source_kind":"frozen","source_leg_id":leg.id,"source_submitted_at":leg.submitted_at,"source_snapshot":snapshot}),
            )
        } else {
            // A corrupt candidate must never be disguised as missing historical evidence.
            if !candidates.iter().all(|leg| missing_snapshot(leg)) {
                return Err(format!(
                    "invalid or unsubmitted fee snapshot for {wallet}/{token}"
                ));
            }
            let latest = fallback
                .get(&(wallet.clone(), token.clone()))
                .filter(|v| {
                    v.wallet.trim().eq_ignore_ascii_case(&wallet)
                        && v.token == token
                        && v.valid_until > now
                })
                .ok_or_else(|| {
                    format!(
                        "latest fee unavailable, expired or identity mismatch for {wallet}/{token}"
                    )
                })?;
            let (t, b, snapshot) = parse_snapshot(&latest.snapshot, &token)
                .ok_or_else(|| format!("invalid latest fee snapshot for {wallet}/{token}"))?;
            fallback_valid_until = Some(
                fallback_valid_until.map_or(latest.valid_until, |old| old.min(latest.valid_until)),
            );
            (
                t,
                b,
                json!({"source_kind":"latest_valid_fallback","valid_until":latest.valid_until.timestamp(),"source_snapshot":snapshot}),
            )
        };
        // Sell at payout=1 includes the original builder rate, not settlement_builder_rate.
        qty.checked_mul(taker.checked_add(builder).ok_or("rate overflow")?)
            .ok_or("reserve overflow")?;
        let fee = crate::calc::estimate_outcome_fee(
            qty,
            crate::platforms::OrderSide::Sell,
            &crate::calc::FeeContext {
                polymarket_fee_rate: Decimal::ZERO,
                outcome_taker_rate: taker,
                outcome_builder_rate: builder,
            },
        );
        reserve = reserve.checked_add(fee).ok_or("reserve sum overflow")?;
        source.as_object_mut().unwrap().extend(json!({"wallet":wallet,"token":token,"quantity":qty,"taker_rate":taker,"builder_rate":builder,"estimated_fee":fee}).as_object().unwrap().clone());
        evidence.push(source);
    }
    let rev = gross.checked_sub(reserve).ok_or("revenue overflow")?;
    let profit = rev.checked_sub(cost).ok_or("profit overflow")?;
    Ok((
        (cost, rev, profit),
        json!({"version":1,"status":if evidence.is_empty(){"not_applicable"}else{"estimated"},"basis":if legs.is_empty(){"empty"}else if fallback_valid_until.is_some(){"outcome_sell_payout_one"}else{"frozen_outcome_sell_payout_one"},"stale":false,"estimated_fee":reserve,"payout_assumption":"1","groups":evidence,"source_kind":if fallback_valid_until.is_some(){"latest_valid_fallback"}else{"frozen"},"fallback_valid_until":fallback_valid_until.map(|time|time.timestamp()),"computed_at":now}),
    ))
}

#[cfg(test)]
#[path = "../../tests/unit/store/actuals.rs"]
mod tests;
