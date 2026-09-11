use crate::error::{Error, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 平台市场当前的结算/可交易状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SettlementStatus {
    Settled { payouts: Vec<SettlementPayout> },
    TradableUnsettled,
    Unavailable,
}

impl SettlementStatus {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Settled { .. } => "settled",
            Self::TradableUnsettled => "tradable_unsettled",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Outcome 的结算查询本身无法判断未结算市场是否仍可交易。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum OutcomeSettlement {
    Settled { payouts: Vec<SettlementPayout> },
    Unsettled,
}

impl OutcomeSettlement {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Settled { .. } => "settled",
            Self::Unsettled => "unsettled",
        }
    }

    /// HIP-4 分数兑付落在 `(0,1)` 时，跨平台互补一对不再兑付 $1，套利与对冲的估值前提失效。
    /// 只用于观测：调用方据此告警，不改变入账口径。
    pub fn is_fractional(&self) -> bool {
        match self {
            Self::Settled { payouts } => payouts
                .iter()
                .any(|payout| payout.payout != Decimal::ZERO && payout.payout != Decimal::ONE),
            Self::Unsettled => false,
        }
    }
}

/// 单个平台 token 的每股结算金额。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementPayout {
    pub token_id: String,
    pub payout: Decimal,
}

pub fn parse_polymarket_settlement(value: &Value) -> Result<SettlementStatus> {
    let tokens = value
        .get("tokens")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::msg("polymarket market response missing tokens"))?;

    if tokens.is_empty() {
        return Err(Error::msg("polymarket market response has no tokens"));
    }

    let mut parsed = Vec::with_capacity(tokens.len());
    let mut winner_count = 0usize;
    for token in tokens {
        let token_id = token
            .get("token_id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| Error::msg("polymarket market token missing token_id"))?;
        let winner = token
            .get("winner")
            .and_then(Value::as_bool)
            .ok_or_else(|| Error::msg("polymarket market token missing winner"))?;
        if parsed.iter().any(|(id, _)| id == token_id) {
            return Err(Error::msg("polymarket duplicate settlement token"));
        }
        winner_count += usize::from(winner);
        parsed.push((token_id.to_string(), winner));
    }

    if winner_count > 1 {
        return Err(Error::msg("polymarket market has multiple winners"));
    }
    if winner_count == 1 {
        if parsed.len() != 2 {
            return Err(Error::msg("settled polymarket market must have two tokens"));
        }
        // winner 只证明终态；每个 token 的兑付必须来自原始 price。
        let payouts: Vec<SettlementPayout> = parsed
            .into_iter()
            .zip(tokens)
            .map(|((token_id, _), token)| {
                let payout = token
                    .get("price")
                    .and_then(parse_decimal)
                    .filter(|p| (Decimal::ZERO..=Decimal::ONE).contains(p))
                    .ok_or_else(|| Error::msg("invalid or missing polymarket settlement price"))?;
                Ok(SettlementPayout { token_id, payout })
            })
            .collect::<Result<_>>()?;
        if payouts[0].payout + payouts[1].payout != Decimal::ONE {
            return Err(Error::msg("polymarket settlement prices must sum to one"));
        }
        return Ok(SettlementStatus::Settled { payouts });
    }

    let tradable = value.get("closed").and_then(Value::as_bool) == Some(false)
        && value.get("accepting_orders").and_then(Value::as_bool) == Some(true)
        && value.get("enable_order_book").and_then(Value::as_bool) == Some(true);
    Ok(if tradable {
        SettlementStatus::TradableUnsettled
    } else {
        SettlementStatus::Unavailable
    })
}

pub fn parse_outcome_settlement(outcome_id: u64, value: &Value) -> Result<OutcomeSettlement> {
    if value.is_null() {
        return Ok(OutcomeSettlement::Unsettled);
    }
    if let Some(identity) = value.get("outcome").filter(|v| !v.is_null()) {
        if identity
            .as_u64()
            .or_else(|| identity.as_str().and_then(|s| s.parse().ok()))
            != Some(outcome_id)
        {
            return Err(Error::msg("outcome settlement response identity mismatch"));
        }
    }
    let fraction = value
        .get("settleFraction")
        .or_else(|| value.get("settle_fraction"))
        .and_then(parse_decimal)
        .ok_or_else(|| Error::msg("outcome settled response missing settleFraction"))?;
    if !(Decimal::ZERO..=Decimal::ONE).contains(&fraction) {
        return Err(Error::msg("outcome settleFraction must be between 0 and 1"));
    }
    let complement = Decimal::ONE
        .checked_sub(fraction)
        .ok_or_else(|| Error::msg("outcome settleFraction is outside decimal range"))?;
    // HIP-4 按原始分数兑付：side 0 得 fraction，side 1 得其补数。
    let (side0_payout, side1_payout) = (fraction, complement);
    Ok(OutcomeSettlement::Settled {
        payouts: vec![
            SettlementPayout {
                token_id: crate::domain::side_coin(outcome_id, 0),
                payout: side0_payout,
            },
            SettlementPayout {
                token_id: crate::domain::side_coin(outcome_id, 1),
                payout: side1_payout,
            },
        ],
    })
}

fn parse_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(raw) => raw.parse().ok(),
        Value::Number(raw) => raw.to_string().parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn polymarket_winner_produces_token_payouts() {
        let status = parse_polymarket_settlement(&json!({
            "closed": true,
            "accepting_orders": false,
            "enable_order_book": false,
            "tokens": [
                {"token_id": "yes", "outcome": "Yes", "winner": true, "price": "1"},
                {"token_id": "no", "outcome": "No", "winner": false, "price": "0"}
            ]
        }))
        .unwrap();
        assert_eq!(
            status,
            SettlementStatus::Settled {
                payouts: vec![
                    SettlementPayout {
                        token_id: "yes".into(),
                        payout: Decimal::ONE
                    },
                    SettlementPayout {
                        token_id: "no".into(),
                        payout: Decimal::ZERO
                    },
                ]
            }
        );
    }

    #[test]
    fn polymarket_prices_are_exact_and_independent_of_winner() {
        for (a, b) in [
            (json!("0.5"), json!(0.5)),
            (json!("0.3"), json!("0.7")),
            (json!("0"), json!("1")),
        ] {
            let response = json!({"tokens":[{"token_id":"a","winner":true,"price":a},{"token_id":"b","winner":false,"price":b}]});
            let SettlementStatus::Settled { payouts } =
                parse_polymarket_settlement(&response).unwrap()
            else {
                panic!("expected settlement")
            };
            assert_eq!(payouts[0].payout, parse_decimal(&a).unwrap());
            assert_eq!(payouts[1].payout, parse_decimal(&b).unwrap());
            for bad in [
                Value::Null,
                json!("NaN"),
                json!(true),
                json!("-0.1"),
                json!("1.1"),
                json!("0.123"),
            ] {
                let mut invalid = response.clone();
                invalid["tokens"][0]["price"] = bad;
                assert!(parse_polymarket_settlement(&invalid).is_err());
            }
            let mut missing = response.clone();
            missing["tokens"][0]
                .as_object_mut()
                .unwrap()
                .remove("price");
            assert!(parse_polymarket_settlement(&missing).is_err());
            for id in ["a", " "] {
                let mut invalid = response.clone();
                invalid["tokens"][1]["token_id"] = json!(id);
                assert!(parse_polymarket_settlement(&invalid).is_err());
            }
            let mut no_winner = response.clone();
            no_winner["tokens"][0]["winner"] = json!(false);
            assert_eq!(
                parse_polymarket_settlement(&no_winner).unwrap(),
                SettlementStatus::Unavailable
            );
            let mut multiple = response;
            multiple["tokens"][1]["winner"] = json!(true);
            assert!(parse_polymarket_settlement(&multiple).is_err());
        }
    }

    #[test]
    fn polymarket_numeric_price_preserves_decimal_precision() {
        let response: Value = serde_json::from_str(r#"{"tokens":[{"token_id":"a","winner":true,"price":0.3000000000000000000000000001},{"token_id":"b","winner":false,"price":0.6999999999999999999999999999}]}"#).unwrap();
        let SettlementStatus::Settled { payouts } = parse_polymarket_settlement(&response).unwrap()
        else {
            panic!("expected settled")
        };
        assert_eq!(
            payouts[0].payout,
            "0.3000000000000000000000000001".parse::<Decimal>().unwrap()
        );
    }

    #[test]
    fn polymarket_requires_all_trading_flags() {
        let open = json!({
            "closed": false,
            "accepting_orders": true,
            "enable_order_book": true,
            "tokens": [{"token_id": "yes", "winner": false, "price": "0"}]
        });
        assert_eq!(
            parse_polymarket_settlement(&open).unwrap(),
            SettlementStatus::TradableUnsettled
        );
        for field in ["closed", "accepting_orders", "enable_order_book"] {
            let mut unavailable = open.clone();
            unavailable.as_object_mut().unwrap().remove(field);
            assert_eq!(
                parse_polymarket_settlement(&unavailable).unwrap(),
                SettlementStatus::Unavailable
            );
        }
    }

    #[test]
    fn polymarket_closed_without_winner_is_unavailable() {
        let status = parse_polymarket_settlement(&json!({
            "closed": true,
            "accepting_orders": false,
            "enable_order_book": false,
            "tokens": [{"token_id": "yes", "winner": false, "price": "0"}]
        }))
        .unwrap();
        assert_eq!(status, SettlementStatus::Unavailable);
    }

    #[test]
    fn polymarket_rejects_ambiguous_or_malformed_tokens() {
        assert!(parse_polymarket_settlement(&json!({"tokens": []})).is_err());
        assert!(parse_polymarket_settlement(&json!({
            "tokens": [{"token_id": "yes"}]
        }))
        .is_err());
        assert!(parse_polymarket_settlement(&json!({
            "tokens": [
                {"token_id": "yes", "winner": true, "price": "1"},
                {"token_id": "no", "winner": true, "price": "1"}
            ]
        }))
        .is_err());
    }

    #[test]
    fn outcome_null_is_unsettled() {
        assert_eq!(
            parse_outcome_settlement(95, &Value::Null).unwrap(),
            OutcomeSettlement::Unsettled
        );
    }

    #[test]
    fn outcome_fractions_produce_protocol_payouts() {
        for (value, first, second) in [
            (json!("0"), "0", "1"),
            (json!("0.25"), "0.25", "0.75"),
            (json!(0.5), "0.5", "0.5"),
            (json!("0.75"), "0.75", "0.25"),
            (json!("1"), "1", "0"),
        ] {
            assert_eq!(
                parse_outcome_settlement(95, &json!({"settleFraction": value})).unwrap(),
                OutcomeSettlement::Settled {
                    payouts: vec![
                        SettlementPayout {
                            token_id: "#950".into(),
                            payout: first.parse().unwrap()
                        },
                        SettlementPayout {
                            token_id: "#951".into(),
                            payout: second.parse().unwrap()
                        },
                    ]
                }
            );
        }
    }

    #[test]
    fn fractional_detection_only_fires_between_zero_and_one() {
        for binary in [json!("0"), json!("1"), json!(0), json!(1)] {
            let settled = parse_outcome_settlement(95, &json!({"settleFraction": binary})).unwrap();
            assert!(!settled.is_fractional(), "{binary} must stay binary");
        }
        for fractional in [json!("0.25"), json!(0.5), json!("0.999")] {
            let settled =
                parse_outcome_settlement(95, &json!({"settleFraction": fractional})).unwrap();
            assert!(settled.is_fractional(), "{fractional} must be flagged");
        }
        assert!(!OutcomeSettlement::Unsettled.is_fractional());
    }

    #[test]
    fn outcome_rejects_out_of_range_or_malformed_settlement() {
        for malformed in [
            json!({}),
            json!({"settleFraction": "not-a-decimal"}),
            json!({"settleFraction": "-0.1"}),
            json!({"settleFraction": "1.1"}),
        ] {
            assert!(parse_outcome_settlement(95, &malformed).is_err());
        }
    }
}
