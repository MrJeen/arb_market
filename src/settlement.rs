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
            .filter(|id| !id.is_empty())
            .ok_or_else(|| Error::msg("polymarket market token missing token_id"))?;
        let winner = token
            .get("winner")
            .and_then(Value::as_bool)
            .ok_or_else(|| Error::msg("polymarket market token missing winner"))?;
        winner_count += usize::from(winner);
        parsed.push((token_id.to_string(), winner));
    }

    if winner_count > 1 {
        return Err(Error::msg("polymarket market has multiple winners"));
    }
    if winner_count == 1 {
        let payouts = parsed
            .into_iter()
            .map(|(token_id, winner)| SettlementPayout {
                token_id,
                payout: if winner { Decimal::ONE } else { Decimal::ZERO },
            })
            .collect();
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
    let fraction = value
        .get("settleFraction")
        .or_else(|| value.get("settle_fraction"))
        .and_then(parse_decimal)
        .ok_or_else(|| Error::msg("outcome settled response missing settleFraction"))?;
    let complement = Decimal::ONE - fraction;
    // 项目只按二元胜负归一化；相等时按 Outcome 约定选择 side 0。
    let side0_wins = fraction >= complement;
    Ok(OutcomeSettlement::Settled {
        payouts: vec![
            SettlementPayout {
                token_id: crate::domain::side_coin(outcome_id, 0),
                payout: if side0_wins {
                    Decimal::ONE
                } else {
                    Decimal::ZERO
                },
            },
            SettlementPayout {
                token_id: crate::domain::side_coin(outcome_id, 1),
                payout: if side0_wins {
                    Decimal::ZERO
                } else {
                    Decimal::ONE
                },
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
                {"token_id": "yes", "outcome": "Yes", "winner": true},
                {"token_id": "no", "outcome": "No", "winner": false}
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
    fn polymarket_requires_all_trading_flags() {
        let open = json!({
            "closed": false,
            "accepting_orders": true,
            "enable_order_book": true,
            "tokens": [{"token_id": "yes", "winner": false}]
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
            "tokens": [{"token_id": "yes", "winner": false}]
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
                {"token_id": "yes", "winner": true},
                {"token_id": "no", "winner": true}
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
    fn outcome_binary_fractions_produce_binary_payouts() {
        for (fraction, first, second) in [
            ("0", Decimal::ZERO, Decimal::ONE),
            ("1", Decimal::ONE, Decimal::ZERO),
            ("0.25", Decimal::ZERO, Decimal::ONE),
            ("1.1", Decimal::ONE, Decimal::ZERO),
            ("-0.1", Decimal::ZERO, Decimal::ONE),
        ] {
            assert_eq!(
                parse_outcome_settlement(95, &json!({"settleFraction": fraction})).unwrap(),
                OutcomeSettlement::Settled {
                    payouts: vec![
                        SettlementPayout {
                            token_id: "#950".into(),
                            payout: first
                        },
                        SettlementPayout {
                            token_id: "#951".into(),
                            payout: second
                        },
                    ]
                }
            );
        }
    }

    #[test]
    fn outcome_tie_selects_side_zero_and_rejects_malformed_settlement() {
        assert_eq!(
            parse_outcome_settlement(95, &json!({"settleFraction": "0.5"})).unwrap(),
            OutcomeSettlement::Settled {
                payouts: vec![
                    SettlementPayout {
                        token_id: "#950".into(),
                        payout: Decimal::ONE
                    },
                    SettlementPayout {
                        token_id: "#951".into(),
                        payout: Decimal::ZERO
                    },
                ]
            }
        );

        for malformed in [json!({}), json!({"settleFraction": "not-a-decimal"})] {
            assert!(parse_outcome_settlement(95, &malformed).is_err());
        }
    }
}
