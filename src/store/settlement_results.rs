use super::*;
use crate::settlement::SettlementPayout;
use sqlx::{Postgres, Transaction};

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_replay_rejects_unknown_versions_and_tampering() {
        let evidence = json!({"version":1,"market_id":"m","source":"clob_market_price","response":{"condition_id":"m","tokens":[{"token_id":"a","winner":true,"price":"0.3"},{"token_id":"b","winner":false,"price":"0.7"}]}});
        let payouts = vec![
            SettlementPayout {
                token_id: "a".into(),
                payout: "0.3".parse().unwrap(),
            },
            SettlementPayout {
                token_id: "b".into(),
                payout: "0.7".parse().unwrap(),
            },
        ];
        assert!(
            verify_source(POLYMARKET, "m", "clob_market_price", 1, &evidence, &payouts).is_ok()
        );
        for (source, version) in [
            ("clob_market_price", 2),
            ("clob_market_winner", 1),
            ("unknown", 1),
        ] {
            assert!(verify_source(POLYMARKET, "m", source, version, &evidence, &payouts).is_err());
        }
        for field in ["version", "source", "market_id", "response"] {
            let mut bad = evidence.clone();
            bad[field] = Value::Null;
            assert!(
                verify_source(POLYMARKET, "m", "clob_market_price", 1, &bad, &payouts).is_err()
            );
        }
        let mut wrong = payouts;
        wrong[0].payout = Decimal::ONE;
        wrong[1].payout = Decimal::ZERO;
        assert!(verify_source(POLYMARKET, "m", "clob_market_price", 1, &evidence, &wrong).is_err());
    }

    #[test]
    fn payout_normalization_rejects_missing_duplicate_and_invalid_values() {
        let good = vec![
            SettlementPayout {
                token_id: "a".into(),
                payout: Decimal::ONE,
            },
            SettlementPayout {
                token_id: "b".into(),
                payout: Decimal::ZERO,
            },
        ];
        let mut reversed = good.clone();
        reversed.reverse();
        assert_eq!(
            normalize(good.clone()).unwrap(),
            normalize(reversed).unwrap()
        );
        assert!(normalize(vec![]).is_err());
        let mut bad = good.clone();
        bad[1].token_id = "a".into();
        assert!(normalize(bad).is_err());
        let mut bad = good.clone();
        bad[1].payout = Decimal::ONE;
        assert!(normalize(bad).is_err());
        let mut bad = good;
        bad[0].payout = -Decimal::ONE;
        assert!(normalize(bad).is_err());
    }
}

fn normalize(mut payouts: Vec<SettlementPayout>) -> Result<Vec<SettlementPayout>> {
    payouts.sort_by(|a, b| a.token_id.cmp(&b.token_id));
    if payouts.len() != 2
        || payouts.iter().any(|p| {
            p.token_id.trim().is_empty() || !(Decimal::ZERO..=Decimal::ONE).contains(&p.payout)
        })
        || payouts[0].token_id == payouts[1].token_id
        || payouts[0].payout + payouts[1].payout != Decimal::ONE
    {
        return Err(Error::msg("invalid binary settlement payout vector"));
    }
    Ok(payouts)
}

async fn validate(
    tx: &mut Transaction<'_, Postgres>,
    order_id: i64,
    platform: &str,
    market: &str,
    payouts: Vec<SettlementPayout>,
) -> Result<Vec<SettlementPayout>> {
    let payouts = normalize(payouts)?;
    let identity: Option<String> = sqlx::query_scalar(
        "SELECT market_id FROM arb_order_market_identities WHERE order_id=$1 AND platform=$2",
    )
    .bind(order_id)
    .bind(platform)
    .fetch_optional(&mut **tx)
    .await?;
    if identity.as_deref() != Some(market) {
        return Err(Error::msg("settlement market identity mismatch or missing"));
    }
    if platform == OUTCOME {
        let id: u64 = market
            .parse()
            .map_err(|_| Error::msg("invalid outcome market identity"))?;
        for side in 0..2 {
            if !payouts
                .iter()
                .any(|p| p.token_id == crate::domain::side_coin(id, side))
            {
                return Err(Error::msg("outcome settlement token mismatch"));
            }
        }
    } else if platform != POLYMARKET {
        return Err(Error::msg(
            "unsupported settlement platform or unproven PM payout",
        ));
    }
    let tokens: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT token_id FROM legs WHERE order_id=$1 AND platform=$2")
            .bind(order_id)
            .bind(platform)
            .fetch_all(&mut **tx)
            .await?;
    if tokens
        .iter()
        .any(|t| !payouts.iter().any(|p| &p.token_id == t))
    {
        return Err(Error::msg("settlement token does not cover durable legs"));
    }
    Ok(payouts)
}

fn legacy_source(
    platform: &str,
    source: &str,
    version: i32,
    market: &str,
    evidence: &Value,
) -> Result<bool> {
    if platform != POLYMARKET || source != "clob_market_winner" {
        return Ok(false);
    }
    if version != 1
        || evidence.get("version").and_then(Value::as_i64) != Some(1)
        || evidence.get("source").and_then(Value::as_str) != Some(source)
        || evidence.get("market_id").and_then(Value::as_str) != Some(market)
    {
        return Err(Error::msg("unsupported legacy settlement source/version"));
    }
    Ok(true)
}

// 所有消费路径重放同一源响应，封装版本不代表 payout 语义。
fn verify_source(
    platform: &str,
    market: &str,
    source: &str,
    version: i32,
    evidence: &Value,
    payouts: &[SettlementPayout],
) -> Result<()> {
    let expected = if platform == POLYMARKET {
        "clob_market_price"
    } else if platform == OUTCOME {
        "settledOutcome"
    } else {
        return Err(Error::msg("unsupported settlement platform"));
    };
    if version != 1
        || source != expected
        || evidence.get("version").and_then(Value::as_i64) != Some(1)
        || evidence.get("source").and_then(Value::as_str) != Some(source)
        || evidence.get("market_id").and_then(Value::as_str) != Some(market)
    {
        return Err(Error::msg(
            "unsupported or inconsistent settlement source/version",
        ));
    }
    let response = &evidence["response"];
    let parsed = if platform == POLYMARKET {
        if response.get("condition_id").and_then(Value::as_str) != Some(market) {
            return Err(Error::msg("PM source response identity mismatch"));
        }
        match crate::settlement::parse_polymarket_settlement(response)? {
            crate::settlement::SettlementStatus::Settled { payouts } => payouts,
            _ => return Err(Error::msg("source response is not settled")),
        }
    } else {
        let id: u64 = market
            .parse()
            .map_err(|_| Error::msg("invalid outcome identity"))?;
        if response.get("request_outcome").and_then(Value::as_u64) != Some(id) {
            return Err(Error::msg("Outcome request identity mismatch"));
        }
        match crate::settlement::parse_outcome_settlement(id, response)? {
            crate::settlement::OutcomeSettlement::Settled { payouts } => payouts,
            _ => return Err(Error::msg("source response is not settled")),
        }
    };
    if normalize(parsed)?.as_slice() != payouts {
        return Err(Error::msg("source response payout mismatch"));
    }
    Ok(())
}

impl Store {
    pub async fn platform_settlement_result(
        &self,
        order_id: i64,
        platform: &str,
        market: &str,
        endpoint: &str,
    ) -> Result<Option<Vec<SettlementPayout>>> {
        let mut tx = self.pool.begin().await?;
        let row: Option<(String,String,Value,String,Value,i32)> = sqlx::query_as("SELECT market_id,endpoint,payouts,source,evidence,evidence_version FROM order_platform_settlement_results WHERE order_id=$1 AND platform=$2")
            .bind(order_id).bind(platform).fetch_optional(&mut *tx).await?;
        let result =
            if let Some((saved_market, saved_endpoint, payouts, source, evidence, version)) = row {
                if saved_market != market || saved_endpoint != endpoint {
                    return Err(Error::msg("saved settlement identity conflict"));
                }
                if legacy_source(platform, &source, version, market, &evidence)? {
                    tx.commit().await?;
                    return Ok(None);
                }
                let payouts: Vec<SettlementPayout> = serde_json::from_value(payouts)?;
                verify_source(
                    platform,
                    market,
                    &source,
                    version,
                    &evidence,
                    &normalize(payouts.clone())?,
                )?;
                Some(validate(&mut tx, order_id, platform, market, payouts).await?)
            } else {
                None
            };
        tx.commit().await?;
        Ok(result)
    }

    pub async fn save_platform_settlement_result(
        &self,
        order_id: i64,
        platform: &str,
        market: &str,
        endpoint: &str,
        payouts: &[SettlementPayout],
        response: &Value,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let settled: bool = sqlx::query_scalar(
            "SELECT settled_at IS NOT NULL OR position_status NOT IN ('watching','settlement_pending') FROM arb_orders WHERE id=$1 FOR UPDATE",
        )
        .bind(order_id)
        .fetch_one(&mut *tx)
        .await?;
        if settled {
            return Err(Error::msg("cannot add market evidence to settled order"));
        }
        let payouts = validate(&mut tx, order_id, platform, market, payouts.to_vec()).await?;
        let source = if platform == POLYMARKET {
            "clob_market_price"
        } else {
            "settledOutcome"
        };
        let evidence = json!({"version":1,"market_id":market,"source":source,"response":response});
        verify_source(platform, market, source, 1, &evidence, &payouts)?;
        let inserted = sqlx::query("INSERT INTO order_platform_settlement_results(order_id,platform,market_id,endpoint,source,payouts,evidence,evidence_version) VALUES($1,$2,$3,$4,$5,$6,$7,1) ON CONFLICT DO NOTHING")
            .bind(order_id).bind(platform).bind(market).bind(endpoint).bind(source).bind(serde_json::to_value(&payouts)?).bind(&evidence).execute(&mut *tx).await?.rows_affected();
        if inserted == 0 {
            let (m,e,p,old_source,old_evidence,version,observed): (String,String,Value,String,Value,i32,chrono::DateTime<chrono::Utc>) = sqlx::query_as("SELECT market_id,endpoint,payouts,source,evidence,evidence_version,observed_at FROM order_platform_settlement_results WHERE order_id=$1 AND platform=$2")
                .bind(order_id).bind(platform).fetch_one(&mut *tx).await?;
            if m != market || e != endpoint {
                return Err(Error::msg("conflicting durable settlement identity"));
            }
            if legacy_source(platform, &old_source, version, market, &old_evidence)? {
                // 父单锁使升级与最终化互斥；旧证据只作审计，绝不反造 price。
                let mut upgraded = evidence.clone();
                upgraded["previous_evidence"] = json!({"source":old_source,"payouts":p,"evidence":old_evidence,"evidence_version":version,"observed_at":observed});
                sqlx::query("UPDATE order_platform_settlement_results SET source=$3,payouts=$4,evidence=$5,evidence_version=1,observed_at=NOW() WHERE order_id=$1 AND platform=$2")
                    .bind(order_id).bind(platform).bind(source).bind(serde_json::to_value(&payouts)?).bind(upgraded).execute(&mut *tx).await?;
                tracing::info!(
                    order_id,
                    platform,
                    market_id = market,
                    source,
                    "legacy settlement evidence upgraded; original audit retained"
                );
            } else {
                let old_payouts = normalize(serde_json::from_value(p)?)?;
                verify_source(
                    platform,
                    market,
                    &old_source,
                    version,
                    &old_evidence,
                    &old_payouts,
                )?;
                if old_payouts != payouts {
                    return Err(Error::msg("conflicting durable settlement result"));
                }
            }
        }
        // Do not clear an in-flight claim: reconciliation owns its eventual release.
        sqlx::query("UPDATE arb_orders SET position_status='settlement_pending',settlement_pending_since=COALESCE(settlement_pending_since,NOW()),settlement_pending_source=COALESCE(settlement_pending_source,$2),settlement_pending_result=COALESCE(settlement_pending_result,$3),updated_at=NOW() WHERE id=$1")
            .bind(order_id).bind(source).bind(evidence).execute(&mut *tx).await?;
        tx.commit().await?;
        if inserted > 0 {
            tracing::info!(
                order_id,
                platform,
                market_id = market,
                source,
                "platform settlement evidence persisted; trading stopped"
            );
        }
        Ok(())
    }
}

pub(super) async fn durable_payouts(
    tx: &mut Transaction<'_, Postgres>,
    order_id: i64,
) -> Result<(std::collections::HashMap<(String, String), Decimal>, Value)> {
    let rows: Vec<(String,String,Value,Value,String,i32)> = sqlx::query_as("SELECT platform,market_id,payouts,evidence,source,evidence_version FROM order_platform_settlement_results WHERE order_id=$1 ORDER BY platform")
        .bind(order_id).fetch_all(&mut **tx).await?;
    if rows.len() != 2 {
        return Err(Error::msg("both durable platform results required"));
    }
    let mut map = std::collections::HashMap::new();
    let mut references = json!({});
    for (platform, market, payouts, evidence, source, version) in rows {
        let payouts: Vec<SettlementPayout> = serde_json::from_value(payouts)?;
        verify_source(
            &platform,
            &market,
            &source,
            version,
            &evidence,
            &normalize(payouts.clone())?,
        )?;
        for p in validate(tx, order_id, &platform, &market, payouts).await? {
            map.insert((platform.clone(), p.token_id), p.payout);
        }
        references[&platform] = evidence;
    }
    Ok((map, references))
}
