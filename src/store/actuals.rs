//! Frozen settlement reserve estimates, never actual fee or payout evidence.
use super::compute_actuals;
use crate::{config::OUTCOME, domain::side_coin};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::{collections::BTreeMap, str::FromStr};

#[derive(Debug, Clone, sqlx::FromRow)]
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

fn snapshot(leg: &ProjectionLeg) -> Option<(Decimal, Decimal, Value)> {
    let v = leg.last_order_info.as_ref()?.get("fee_estimate")?;
    if v.get("version")?.as_u64()? != 1
        || v.get("fee_model")?.as_str()? != "out_usdc_taker_close_v1"
    {
        return None;
    }
    let outcome = v.get("outcome_id")?.as_str()?.parse::<u64>().ok()?;
    let tokens = [side_coin(outcome, 0), side_coin(outcome, 1)];
    if !tokens.contains(&leg.token_id) || v.get("token_ids")? != &json!(tokens) {
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
    leg.submitted_at?;
    Some((taker, builder, v.clone()))
}

pub fn project(legs: &[ProjectionLeg]) -> Projection {
    match calculate(legs) {
        Ok((actuals, evidence)) => Projection::Ready { actuals, evidence },
        Err(reason) => Projection::Unknown { reason },
    }
}

fn calculate(legs: &[ProjectionLeg]) -> Result<((Decimal, Decimal, Decimal), Value), String> {
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
                .to_ascii_lowercase();
            let entry = groups.entry((wallet, leg.token_id.clone())).or_default();
            entry.0 += if leg.side == "SELL" { -qty } else { qty };
            entry.1.push(leg);
        }
    }
    let (cost, gross, _) = compute_actuals(&rows);
    let mut reserve = Decimal::ZERO;
    let mut evidence = Vec::new();
    for ((wallet, token), (qty, candidates)) in groups {
        if qty < Decimal::ZERO {
            return Err(format!("negative Outcome position for {wallet}/{token}"));
        }
        if qty.is_zero() {
            continue;
        }
        let (leg, taker, builder, source) = candidates
            .into_iter()
            .filter_map(|leg| snapshot(leg).map(|(t, b, s)| (leg, t, b, s)))
            .max_by_key(|(leg, ..)| (leg.submitted_at, leg.id))
            .ok_or_else(|| format!("missing valid frozen fee snapshot for {wallet}/{token}"))?;
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
        evidence.push(json!({"wallet":wallet,"token":token,"quantity":qty,"taker_rate":taker,"builder_rate":builder,"estimated_fee":fee,"source_leg_id":leg.id,"source_submitted_at":leg.submitted_at,"source_snapshot":source}));
    }
    let rev = gross.checked_sub(reserve).ok_or("revenue overflow")?;
    let profit = rev.checked_sub(cost).ok_or("profit overflow")?;
    Ok((
        (cost, rev, profit),
        json!({"version":1,"status":if evidence.is_empty(){"not_applicable"}else{"estimated"},"basis":if legs.is_empty(){"empty"}else{"frozen_outcome_sell_payout_one"},"stale":false,"estimated_fee":reserve,"payout_assumption":"1","groups":evidence,"computed_at":Utc::now()}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }
    fn leg(id: i64, side: &str, qty: &str) -> ProjectionLeg {
        ProjectionLeg {
            id,
            status: "completed".into(),
            side: side.into(),
            platform: OUTCOME.into(),
            label: "yes".into(),
            token_id: "#5160".into(),
            wallet_address: Some("wallet-a".into()),
            submitted_at: DateTime::from_timestamp(1000 + id, 0),
            last_order_info: Some(
                json!({"fee_estimate":{"version":1,"fee_model":"out_usdc_taker_close_v1","outcome_id":"516","token_ids":["#5160","#5161"],"taker_rate":"0.001344","builder_rate":"0.0003","settlement_builder_rate":"0","user_fees_fetched_at":"2000-01-01T00:00:00Z","outcome_meta_fetched_at":"2000-01-01T00:00:00Z"}}),
            ),
            actual_shares: Some(d(qty)),
            actual_price: Some(d("0.5")),
            actual_fee: Some(Decimal::ZERO),
        }
    }
    fn fee(rows: &[ProjectionLeg]) -> Decimal {
        let p = project(rows);
        let (cost, rev, profit) = p.actuals().unwrap();
        assert_eq!(rev - cost, profit);
        Decimal::from_str(p.evidence()["estimated_fee"].as_str().unwrap()).unwrap()
    }
    #[test]
    fn reserve_tracks_remaining_without_cumulative_deduction() {
        let buy = leg(1, "BUY", "30");
        assert_eq!(fee(std::slice::from_ref(&buy)), d("0.04932"));
        assert_eq!(fee(std::slice::from_ref(&buy)), d("0.04932"));
        assert_eq!(
            project(std::slice::from_ref(&buy)).actuals().unwrap().1,
            -d("0.04932")
        );
        assert_eq!(fee(&[buy.clone(), leg(2, "SELL", "15")]), d("0.02466"));
        let mut sell = leg(2, "SELL", "30");
        sell.last_order_info = None;
        let mut buy = buy;
        buy.last_order_info = None;
        assert_eq!(fee(&[buy, sell]), Decimal::ZERO);
    }
    #[test]
    fn snapshot_selection_is_frozen_and_wallet_token_scoped() {
        let buy = leg(1, "BUY", "30");
        let mut sell = leg(2, "SELL", "15");
        sell.last_order_info.as_mut().unwrap()["fee_estimate"]["builder_rate"] = json!("0.001");
        assert_eq!(fee(&[buy.clone(), sell.clone()]), d("0.03516"));
        sell.wallet_address = Some("wallet-b".into());
        assert!(project(&[buy.clone(), sell]).actuals().is_none());
        let mut other = leg(3, "BUY", "2");
        other.token_id = "#5161".into();
        other.last_order_info = None;
        assert!(project(&[buy, other]).actuals().is_none());
    }
    #[test]
    fn malformed_or_unresolved_evidence_never_defaults_to_zero() {
        for field in [
            "version",
            "fee_model",
            "outcome_id",
            "token_ids",
            "taker_rate",
            "builder_rate",
            "user_fees_fetched_at",
        ] {
            let mut buy = leg(1, "BUY", "30");
            buy.last_order_info.as_mut().unwrap()["fee_estimate"][field] = json!("invalid");
            assert!(project(&[buy]).actuals().is_none(), "{field}");
        }
        for rate in ["-0.1", "1.1", "NaN"] {
            let mut buy = leg(1, "BUY", "30");
            buy.last_order_info.as_mut().unwrap()["fee_estimate"]["taker_rate"] = json!(rate);
            assert!(project(&[buy]).actuals().is_none());
        }
        let mut buy = leg(1, "BUY", "0");
        buy.last_order_info = None;
        assert_eq!(fee(&[buy.clone()]), Decimal::ZERO);
        buy.actual_shares = None;
        assert!(project(&[buy.clone()]).actuals().is_none());
        buy.actual_shares = Some(Decimal::ZERO);
        buy.status = "pending".into();
        assert!(project(&[buy]).actuals().is_none());
    }
    #[test]
    fn failed_and_cancelled_partial_fills_are_real_trades() {
        let mut buy = leg(1, "BUY", "30");
        buy.status = "failed".into();
        let mut sell = leg(2, "SELL", "15");
        sell.status = "cancelled".into();
        assert_eq!(fee(&[buy, sell]), d("0.02466"));
    }
    #[test]
    fn actual_payout_zero_and_fraction_replace_the_reserve_basis() {
        let buy = leg(1, "BUY", "30");
        assert_eq!(fee(std::slice::from_ref(&buy)), d("0.04932"));
        let rows = vec![(
            "BUY".into(),
            OUTCOME.into(),
            buy.token_id.clone(),
            d("30"),
            d("0.5"),
            Decimal::ZERO,
        )];
        for payout in [Decimal::ZERO, d("0.37"), Decimal::ONE] {
            let payouts =
                std::collections::HashMap::from([((OUTCOME.into(), buy.token_id.clone()), payout)]);
            let (cost, gross, profit) =
                super::super::compute_settled_actuals(&rows, &payouts).unwrap();
            assert_eq!(cost, d("15"));
            assert_eq!(gross, d("30") * payout);
            assert_eq!(profit, gross - cost);
        }
    }
    #[test]
    fn source_order_uses_submission_then_leg_id_not_snapshot_age() {
        let mut older = leg(1, "BUY", "10");
        let mut newer = leg(2, "BUY", "20");
        newer.last_order_info.as_mut().unwrap()["fee_estimate"]["taker_rate"] = json!("0.002");
        older.submitted_at = newer.submitted_at;
        assert_eq!(fee(&[newer.clone(), older.clone()]), d("0.069"));
        newer.last_order_info.as_mut().unwrap()["fee_estimate"]["version"] = json!(2);
        assert_eq!(fee(&[newer, older]), d("0.04932"));
    }
    #[test]
    fn unknown_warning_only_on_status_or_reason_change() {
        let old = json!({"status":"unknown","reason":"missing snapshot"});
        assert!(!unknown_changed(&old, "missing snapshot"));
        assert!(unknown_changed(&old, "unresolved leg"));
        assert!(unknown_changed(
            &json!({"status":"estimated"}),
            "missing snapshot"
        ));
        assert!(unknown_changed(&json!({}), "missing snapshot"));
    }
}
