//! Claim-scoped risk evidence. Never used as current actuals or notification amounts.
use super::actuals::{self, ProjectionLeg};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct Leg {
    pub valuation: ProjectionLeg,
    pub claim_id: Option<Uuid>,
    pub intent: String,
    pub created_at: DateTime<Utc>,
    pub funder: Option<String>,
    pub req_shares: Decimal,
    pub req_price: Decimal,
    pub req_fee: Decimal,
    // Normalized execution observations, not paging/diagnostic text.
    pub observations: Value,
    pub execution_safe: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Baseline {
    version: u64,
    claim_id: Uuid,
    action: String,
    claimed_at: DateTime<Utc>,
    cost: Decimal,
    rev: Decimal,
    profit: Decimal,
    projection: Value,
    legs: Vec<Leg>,
    invalidated: bool,
    eligible: bool,
    waiting: Vec<Wait>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wait {
    created_at: DateTime<Utc>,
    submitted_at: Option<DateTime<Utc>>,
}

fn ready(value: &Value) -> bool {
    value["version"] == 1
        && value["stale"] == false
        && matches!(
            value["status"].as_str(),
            Some("estimated" | "not_applicable")
        )
}

// Previously selected fallback is durable event evidence, not a live cache lookup.
fn historical_fallback(projection: &Value) -> actuals::FallbackContext {
    projection["groups"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|group| {
            if group["source_kind"] != "latest_valid_fallback" {
                return None;
            }
            let wallet = group["wallet"].as_str()?.to_owned();
            let token = group["token"].as_str()?.to_owned();
            Some((
                (wallet.clone(), token.clone()),
                actuals::LatestFee {
                    wallet,
                    token,
                    snapshot: group["source_snapshot"].clone(),
                    valid_until: DateTime::<Utc>::MAX_UTC,
                },
            ))
        })
        .collect()
}

fn valid_fields(leg: &Leg) -> bool {
    let v = &leg.valuation;
    matches!(v.platform.as_str(), "polymarket" | "outcome")
        && matches!(v.side.as_str(), "BUY" | "SELL")
        && !v.token_id.is_empty()
        && !v.label.is_empty()
        && (v.platform != "polymarket" || leg.funder.as_ref().is_some_and(|f| !f.trim().is_empty()))
        && leg.req_shares > Decimal::ZERO
        && leg.req_price > Decimal::ZERO
        && leg.req_price <= Decimal::ONE
        && leg.req_fee >= Decimal::ZERO
        && v.actual_shares
            .is_none_or(|q| q >= Decimal::ZERO && q <= leg.req_shares)
        && v.actual_price
            .is_none_or(|p| p >= Decimal::ZERO && p <= Decimal::ONE)
        && v.actual_fee.is_none_or(|f| f >= Decimal::ZERO)
        && v.submitted_at.is_none_or(|t| t >= leg.created_at)
        && leg.execution_safe
        && actuals::valid_existing_snapshot(v)
}

fn nonnegative_positions(legs: &[ProjectionLeg]) -> bool {
    let mut positions = BTreeMap::<(&str, &str, Option<&str>), Decimal>::new();
    for leg in legs {
        let qty = leg.actual_shares.unwrap_or(Decimal::ZERO);
        let net = positions
            .entry((&leg.platform, &leg.token_id, leg.wallet_address.as_deref()))
            .or_default();
        let Some(next) = (if leg.side == "SELL" {
            net.checked_sub(qty)
        } else {
            net.checked_add(qty)
        }) else {
            return false;
        };
        *net = next;
    }
    positions.values().all(|q| *q >= Decimal::ZERO)
}

fn terminal(leg: &Leg) -> bool {
    let v = &leg.valuation;
    valid_fields(leg)
        && matches!(
            v.status.as_str(),
            "matched" | "completed" | "failed" | "cancelled"
        )
        && v.actual_shares.is_some()
        && (v.actual_shares == Some(Decimal::ZERO)
            || (v.actual_price.is_some_and(|p| p > Decimal::ZERO) && v.actual_fee.is_some()))
}

impl Baseline {
    pub(super) fn capture(
        claim_id: Uuid,
        action: &str,
        claimed_at: DateTime<Utc>,
        amounts: (Option<Decimal>, Option<Decimal>, Option<Decimal>),
        mut projection: Value,
        legs: Vec<Leg>,
    ) -> Option<Self> {
        projection.as_object_mut()?.remove("lifecycle_risk");
        let (stored_cost, rev, profit) = (amounts.0?, amounts.1?, amounts.2?);
        let valuations: Vec<_> = legs.iter().map(|leg| leg.valuation.clone()).collect();
        if !legs.iter().all(terminal) || !nonnegative_positions(&valuations) {
            return None;
        }
        let (cost, expected_rev, expected_profit) = actuals::project_with_fallback(
            &valuations,
            &historical_fallback(&projection),
            claimed_at,
        )
        .actuals()?;
        // Only cost is NUMERIC(20,8); PostgreSQL rounds ties away from zero.
        // Keep exact event cost internally without changing any stored accounting.
        if stored_cost
            != cost.round_dp_with_strategy(8, rust_decimal::RoundingStrategy::MidpointAwayFromZero)
            || rev != expected_rev
            || profit != expected_profit
        {
            return None;
        }
        let baseline = Self {
            version: 1,
            claim_id,
            action: action.into(),
            claimed_at,
            cost,
            rev,
            profit,
            projection,
            legs,
            invalidated: false,
            eligible: false,
            waiting: Vec::new(),
        };
        baseline.valid().then_some(baseline)
    }

    fn valid(&self) -> bool {
        self.version == 1
            && matches!(self.action.as_str(), "take_profit" | "rebalance")
            && !self.invalidated
            && ready(&self.projection)
            && self.rev.checked_sub(self.cost) == Some(self.profit)
            && !self.legs.is_empty()
            && self.legs.iter().all(terminal)
            && self
                .legs
                .iter()
                .all(|leg| leg.created_at <= self.claimed_at && leg.claim_id != Some(self.claim_id))
            && self
                .legs
                .iter()
                .map(|leg| leg.valuation.id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == self.legs.len()
    }

    pub(super) fn refresh(
        &mut self,
        claim_id: Option<Uuid>,
        action: Option<&str>,
        claimed_at: Option<DateTime<Utc>>,
        legs: &[Leg],
    ) {
        self.eligible = false;
        self.waiting.clear();
        if !self.valid()
            || claim_id != Some(self.claim_id)
            || action != Some(self.action.as_str())
            || claimed_at != Some(self.claimed_at)
        {
            self.invalidated = true;
            return;
        }
        let old: BTreeMap<_, _> = self.legs.iter().map(|l| (l.valuation.id, l)).collect();
        let outside: Vec<_> = legs
            .iter()
            .filter(|l| l.claim_id != Some(self.claim_id))
            .collect();
        // Once an unrelated input changes, restoring it cannot revive this claim.
        if outside.len() != old.len()
            || outside
                .iter()
                .any(|l| old.get(&l.valuation.id).copied() != Some(*l))
        {
            self.invalidated = true;
            return;
        }
        let mut projected = Vec::new();
        for leg in legs {
            if !valid_fields(leg) {
                self.invalidated = true;
                return;
            }
            let mut v = leg.valuation.clone();
            if leg.claim_id == Some(self.claim_id) {
                if old.contains_key(&v.id)
                    || !self.legs.iter().any(|prior| {
                        let p = &prior.valuation;
                        p.platform == v.platform
                            && p.token_id == v.token_id
                            && p.label == v.label
                            && p.wallet_address == v.wallet_address
                            && prior.funder == leg.funder
                    })
                    || leg.intent != self.action
                    || leg.created_at < self.claimed_at
                {
                    self.invalidated = true;
                    return;
                }
                if matches!(v.status.as_str(), "pending" | "actived") {
                    if (v.status == "actived" && v.submitted_at.is_none())
                        || !actuals::valid_pending_snapshot(&v)
                    {
                        self.invalidated = true;
                        return;
                    }
                    self.waiting.push(Wait {
                        created_at: leg.created_at,
                        submitted_at: v.submitted_at,
                    });
                    // Validate known partial executions too; never hide a negative position.
                    v.status = "completed".into();
                    v.actual_shares = Some(v.actual_shares.unwrap_or(Decimal::ZERO));
                    if v.actual_shares == Some(Decimal::ZERO) {
                        v.actual_price = Some(Decimal::ZERO);
                        v.actual_fee = Some(Decimal::ZERO);
                    } else if v.actual_price.is_none() || v.actual_fee.is_none() {
                        self.invalidated = true;
                        return;
                    }
                } else if !terminal(leg) {
                    self.invalidated = true;
                    return;
                }
            } else if !terminal(leg) {
                self.invalidated = true;
                return;
            }
            projected.push(v);
        }
        if !nonnegative_positions(&projected)
            || actuals::project_with_fallback(
                &projected,
                &historical_fallback(&self.projection),
                self.claimed_at,
            )
            .actuals()
            .is_none()
        {
            self.invalidated = true;
            return;
        }
        self.eligible = !self.waiting.is_empty();
    }

    pub(super) fn eligible(&self) -> bool {
        self.eligible && !self.invalidated
    }

    pub(super) fn profit_at(
        &self,
        claim: Uuid,
        action: &str,
        claimed_at: DateTime<Utc>,
        now: DateTime<Utc>,
        pending: Duration,
        submitted: Duration,
    ) -> Option<Decimal> {
        if !self.valid()
            || !self.eligible
            || self.waiting.is_empty()
            || self.claim_id != claim
            || self.action != action
            || self.claimed_at != claimed_at
        {
            return None;
        }
        let pending = chrono::Duration::from_std(pending).ok()?;
        let submitted = chrono::Duration::from_std(submitted).ok()?;
        // Fixed claim cap permits initial creation + confirmation, never rolling retries.
        let cap = claimed_at
            .checked_add_signed(pending)?
            .checked_add_signed(submitted)?;
        if now < claimed_at || now >= cap {
            return None;
        }
        for wait in &self.waiting {
            if wait.created_at < claimed_at || wait.created_at > now {
                return None;
            }
            let deadline = match wait.submitted_at {
                Some(at) if at >= wait.created_at && at <= now => {
                    at.checked_add_signed(submitted)?
                }
                Some(_) => return None,
                None => wait.created_at.checked_add_signed(pending)?,
            };
            if now >= deadline {
                return None;
            }
        }
        Some(self.profit)
    }
}

pub(super) fn poll_safe(poll: &crate::platforms::OrderPoll) -> bool {
    // Missing/unknown remote status is not ordinary exchange confirmation waiting.
    poll.found
        && matches!(
            poll.status.to_ascii_lowercase().as_str(),
            "open"
                | "live"
                | "pending"
                | "matched"
                | "filled"
                | "cancelled"
                | "canceled"
                | "delayed"
                | "unmatched"
        )
        && [poll.shares, poll.original_shares, poll.remaining_shares]
            .into_iter()
            .all(|q| q.is_none_or(|q| q >= Decimal::ZERO))
        && poll
            .price
            .is_none_or(|p| p >= Decimal::ZERO && p <= Decimal::ONE)
        && poll.fee.is_none_or(|f| f >= Decimal::ZERO)
        && !poll
            .shares
            .zip(poll.original_shares)
            .is_some_and(|(q, original)| q > original)
}

pub(super) fn reconciliation_safe(
    platform: &str,
    fills: &[crate::platforms::TradeFill],
    evidence: &crate::reconcile::FillEvidence,
) -> bool {
    if !poll_safe(&evidence.poll) || !fills.iter().all(|f| fill_safe(platform, f)) {
        return false;
    }
    // A pending trade cannot mask a different malformed/contradictory execution.
    let Some(observed) = fills
        .iter()
        .try_fold(Decimal::ZERO, |sum, f| sum.checked_add(f.shares))
    else {
        return false;
    };
    if evidence.poll.original_shares.is_some_and(|q| observed > q) {
        return false;
    }
    if platform == "polymarket" {
        if let Some(scan) = &evidence.pm_scan {
            if scan.validate().is_err() {
                return false;
            }
            if evidence.page_complete
                && (scan.trade_ids.len() != fills.len()
                    || fills.iter().any(|f| !scan.trade_ids.contains(&f.trade_id)))
            {
                return false;
            }
        }
        if evidence.page_complete && evidence.poll.status.eq_ignore_ascii_case("matched") {
            if evidence.poll.shares != Some(observed)
                || evidence.poll.associated_trades.len() != fills.len()
                || fills
                    .iter()
                    .any(|f| !evidence.poll.associated_trades.contains(&f.trade_id))
            {
                return false;
            }
        }
    } else if evidence.page_complete
        && evidence.history_complete
        && evidence.poll.status.eq_ignore_ascii_case("filled")
        && evidence
            .expected_shares
            .or(evidence.poll.original_shares)
            .is_some_and(|q| q != observed)
    {
        return false;
    }
    true
}

pub(super) fn fill_safe(platform: &str, fill: &crate::platforms::TradeFill) -> bool {
    fill.shares > Decimal::ZERO
        && fill.price > Decimal::ZERO
        && fill.price <= Decimal::ONE
        && fill.fee.is_none_or(|fee| fee >= Decimal::ZERO)
        && fill.fee_rate_bps.is_none_or(|fee| fee >= Decimal::ZERO)
        && match fill.finality {
            crate::platforms::FillFinality::Confirmed => {
                crate::reconcile::accounting_fee(platform, fill)
                    .ok()
                    .flatten()
                    .is_some()
            }
            crate::platforms::FillFinality::Pending => true,
            _ => false,
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn setup(action: &str, side: &str) -> (Baseline, Vec<Leg>) {
        let at = DateTime::from_timestamp(1000, 0).unwrap();
        let old = Leg {
            valuation: ProjectionLeg {
                id: 1,
                status: "completed".into(),
                side: "BUY".into(),
                platform: "polymarket".into(),
                label: "yes".into(),
                token_id: "token".into(),
                wallet_address: None,
                submitted_at: Some(at - chrono::Duration::seconds(50)),
                last_order_info: None,
                actual_shares: Some(Decimal::TEN),
                actual_price: Some(Decimal::new(5, 1)),
                actual_fee: Some(Decimal::ZERO),
            },
            claim_id: None,
            intent: "arb_buy".into(),
            created_at: at - chrono::Duration::seconds(100),
            funder: Some("funder".into()),
            req_shares: Decimal::TEN,
            req_price: Decimal::ONE,
            req_fee: Decimal::ZERO,
            observations: json!([]),
            execution_safe: true,
        };
        let p = actuals::project(std::slice::from_ref(&old.valuation));
        let (c, r, pf) = p.actuals().unwrap();
        let baseline = Baseline::capture(
            Uuid::new_v4(),
            action,
            at,
            (Some(c), Some(r), Some(pf)),
            p.evidence(),
            vec![old.clone()],
        )
        .unwrap();
        let mut current = old.clone();
        current.valuation.id = 2;
        current.valuation.side = side.into();
        current.valuation.status = "pending".into();
        current.valuation.submitted_at = None;
        current.valuation.actual_shares = None;
        current.valuation.actual_price = None;
        current.valuation.actual_fee = None;
        current.created_at = at;
        current.claim_id = Some(baseline.claim_id);
        current.intent = action.into();
        (baseline, vec![old, current])
    }
    fn refresh(b: &mut Baseline, legs: &[Leg]) {
        b.refresh(
            Some(b.claim_id),
            Some(&b.action.clone()),
            Some(b.claimed_at),
            legs,
        );
    }
    fn value(b: &Baseline, seconds: i64) -> Option<Decimal> {
        b.profit_at(
            b.claim_id,
            &b.action,
            b.claimed_at,
            b.claimed_at + chrono::Duration::seconds(seconds),
            Duration::from_secs(300),
            Duration::from_secs(300),
        )
    }
    #[test]
    fn normal_wait_matrix_fixed_baseline_and_deadline() {
        for (action, side) in [
            ("take_profit", "SELL"),
            ("rebalance", "BUY"),
            ("rebalance", "SELL"),
        ] {
            let (mut b, mut legs) = setup(action, side);
            refresh(&mut b, &legs);
            assert_eq!(value(&b, 299), Some(b.profit));
            assert_eq!(value(&b, 300), None);
            legs[1].valuation.status = "actived".into();
            legs[1].valuation.submitted_at = Some(b.claimed_at + chrono::Duration::seconds(250));
            refresh(&mut b, &legs);
            assert_eq!(value(&b, 549), Some(b.profit));
            assert_eq!(value(&b, 550), None);
            legs[1].valuation.submitted_at = Some(b.claimed_at + chrono::Duration::seconds(500));
            refresh(&mut b, &legs);
            assert_eq!(value(&b, 599), Some(b.profit));
            assert_eq!(value(&b, 600), None);
            let mut done = legs[1].clone();
            done.valuation.id = 3;
            done.valuation.status = "completed".into();
            done.valuation.actual_shares = Some(Decimal::ONE);
            done.valuation.actual_price = Some(Decimal::new(5, 1));
            done.valuation.actual_fee = Some(Decimal::ZERO);
            legs.push(done);
            refresh(&mut b, &legs);
            assert_eq!(value(&b, 599), Some(b.profit));
        }
    }
    #[test]
    fn all_legs_bad_evidence_matrix_is_permanent() {
        for case in 0..21 {
            let (mut b, mut legs) = setup("rebalance", "BUY");
            // Pending is first: no early success can conceal later bad evidence.
            legs.swap(0, 1);
            match case {
                0 => legs[0].valuation.status = "unknown".into(),
                1 => legs[0].claim_id = Some(Uuid::new_v4()),
                2 => legs[0].intent = "arb_buy".into(),
                3 => legs[0].valuation.actual_shares = Some(-Decimal::ONE),
                4 => legs[0].valuation.actual_price = Some(Decimal::TEN),
                5 => legs[0].valuation.actual_fee = Some(-Decimal::ONE),
                6 => legs[0].execution_safe = false,
                7 => legs[1].valuation.actual_price = None,
                8 => legs[1].valuation.status = "pending".into(),
                9 => legs[1].valuation.token_id = "changed".into(),
                10 => legs[1].created_at = b.claimed_at,
                11 => legs[1].observations = json!([{"new":true}]),
                12 => {
                    legs.remove(1);
                }
                13 => legs[0].valuation.status = "completed".into(),
                14 => legs[0].valuation.platform = "outcome".into(),
                15 => legs[0].valuation.status = "actived".into(),
                17 => legs[0].valuation.token_id = "other-token".into(),
                18 => legs[0].funder = Some("other-funder".into()),
                19 => legs[0].valuation.wallet_address = Some("other-wallet".into()),
                20 => legs[0].valuation.label = "other-label".into(),
                _ => legs[0].created_at = b.claimed_at - chrono::Duration::seconds(1),
            }
            refresh(&mut b, &legs);
            assert!(value(&b, 1).is_none(), "case {case}");
            let (_, mut restored) = setup("rebalance", "BUY");
            restored[1].claim_id = Some(b.claim_id);
            refresh(&mut b, &restored);
            assert!(value(&b, 1).is_none());
        }
    }
    #[test]
    fn capture_respects_postgres_cost_scale_without_rounding_profit() {
        let (prior, mut legs) = setup("rebalance", "BUY");
        legs[0].valuation.actual_shares = Some("1.12345678".parse().unwrap());
        legs[0].valuation.actual_price = Some("0.12345678".parse().unwrap());
        let p = actuals::project(&[legs[0].valuation.clone()]);
        let (cost, rev, profit) = p.actuals().unwrap();
        let stored =
            cost.round_dp_with_strategy(8, rust_decimal::RoundingStrategy::MidpointAwayFromZero);
        assert_ne!(stored, cost);
        let mut b = Baseline::capture(
            prior.claim_id,
            "rebalance",
            prior.claimed_at,
            (Some(stored), Some(rev), Some(profit)),
            p.evidence(),
            vec![legs[0].clone()],
        )
        .unwrap();
        refresh(&mut b, &legs);
        assert_eq!(value(&b, 1), Some(profit));
        assert_eq!(b.cost, cost);
    }

    #[test]
    fn corrupt_baseline_and_claim_never_authorize() {
        let (mut b, legs) = setup("take_profit", "SELL");
        refresh(&mut b, &legs);
        for bad in [json!(null), json!([]), json!(true), json!({"version":"1"})] {
            assert!(parse(&bad).is_none());
        }
        for field in [
            "version",
            "profit",
            "claimed_at",
            "eligible",
            "waiting",
            "legs",
        ] {
            let mut encoded = serde_json::to_value(&b).unwrap();
            encoded[field] = json!("broken");
            assert!(parse(&encoded).is_none_or(|v| value(&v, 1).is_none()));
        }
        assert!(b
            .profit_at(
                Uuid::new_v4(),
                &b.action,
                b.claimed_at,
                b.claimed_at,
                Duration::from_secs(300),
                Duration::from_secs(300)
            )
            .is_none());
        let mut encoded = serde_json::to_value(&b).unwrap();
        encoded["profit"] = json!("999");
        assert!(value(&parse(&encoded).unwrap(), 1).is_none());
    }
}

pub(super) fn parse(value: &Value) -> Option<Baseline> {
    serde_json::from_value(value.clone()).ok()
}
