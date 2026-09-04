use crate::domain::TopicKey;
use crate::error::Result;
use crate::hedge::Positions;
use crate::config::{OUTCOME, POLYMARKET};
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Store {
    pub pool: PgPool,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ArbOrderRow {
    pub id: i64,
    pub event_id: Uuid,
    pub unified_index: i32,
    pub status: String,
    pub rebalance_status: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LegRow {
    pub id: i64,
    pub order_id: i64,
    pub platform: String,
    pub token_id: String,
    pub label: String,
    pub side: String,
    pub intent: String,
    pub funder_address: Option<String>,
    pub wallet_address: Option<String>,
    pub service: Option<String>,
    pub req_price: Option<Decimal>,
    pub req_shares: Option<Decimal>,
    pub client_order_id: Option<String>,
    pub third_order_id: Option<String>,
    pub status: String,
}

impl Store {
    pub async fn connect(uri: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(uri)
            .await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| crate::error::Error::msg(e.to_string()))?;
        Ok(())
    }

    pub async fn has_active_topic(&self, key: TopicKey) -> Result<bool> {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM arb_orders
             WHERE event_id = $1 AND unified_index = $2
               AND status IN ('pending','actived')
             LIMIT 1",
        )
        .bind(key.event_id)
        .bind(key.unified_index)
        .fetch_optional(&self.pool)
        .await?;
        Ok(exists.is_some())
    }

    pub async fn insert_order(
        &self,
        key: TopicKey,
        title: &str,
        market_title: &str,
        end_date: Option<chrono::DateTime<chrono::Utc>>,
        rev: Decimal,
        profit: Decimal,
        cost: Decimal,
        fills: &Value,
    ) -> Result<i64> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO arb_orders (
                event_id, unified_index, title, market_title, end_date,
                estimated_rev, estimated_profit, estimated_cost, fills, status
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending')
             RETURNING id",
        )
        .bind(key.event_id)
        .bind(key.unified_index)
        .bind(title)
        .bind(market_title)
        .bind(end_date)
        .bind(rev)
        .bind(profit)
        .bind(cost)
        .bind(fills)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn insert_leg(
        &self,
        order_id: i64,
        platform: &str,
        token_id: &str,
        label: &str,
        side: &str,
        intent: &str,
        funder: Option<&str>,
        wallet: Option<&str>,
        service: Option<&str>,
        req_price: Decimal,
        req_shares: Decimal,
        req_fee: Decimal,
        client_order_id: Option<&str>,
    ) -> Result<i64> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO legs (
                order_id, platform, token_id, label, side, intent,
                funder_address, wallet_address, service, req_price, req_shares, req_fee,
                client_order_id, status
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'pending')
             RETURNING id",
        )
        .bind(order_id)
        .bind(platform)
        .bind(token_id)
        .bind(label)
        .bind(side)
        .bind(intent)
        .bind(funder)
        .bind(wallet)
        .bind(service)
        .bind(req_price)
        .bind(req_shares)
        .bind(req_fee)
        .bind(client_order_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn insert_envelope(&self, leg_id: i64, order_hash: &str, payload: &Value) -> Result<()> {
        sqlx::query(
            "INSERT INTO signed_envelopes (leg_id, order_hash, payload) VALUES ($1,$2,$3)",
        )
        .bind(leg_id)
        .bind(order_hash)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        let client_id = payload
            .get("cloid")
            .and_then(|v| v.as_str())
            .unwrap_or(order_hash);
        sqlx::query("UPDATE legs SET client_order_id = $2, updated_at = NOW() WHERE id = $1")
            .bind(leg_id)
            .bind(client_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_leg_submitted(
        &self,
        leg_id: i64,
        status: &str,
        third_order_id: Option<&str>,
        info: &Value,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE legs SET status = $2, third_order_id = COALESCE($3, third_order_id),
                    last_order_info = $4, updated_at = NOW()
             WHERE id = $1",
        )
        .bind(leg_id)
        .bind(status)
        .bind(third_order_id)
        .bind(info)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_leg_fill(
        &self,
        leg_id: i64,
        status: &str,
        third_order_id: Option<&str>,
        price: Decimal,
        shares: Decimal,
        fee: Decimal,
        info: &Value,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE legs SET status = $2, third_order_id = COALESCE($3, third_order_id),
                    actual_price = $4, actual_shares = $5, actual_fee = $6,
                    last_order_info = $7, updated_at = NOW()
             WHERE id = $1",
        )
        .bind(leg_id)
        .bind(status)
        .bind(third_order_id)
        .bind(price)
        .bind(shares)
        .bind(fee)
        .bind(info)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_order_status(&self, order_id: i64, status: &str) -> Result<()> {
        sqlx::query(
            "UPDATE arb_orders SET status = $2, updated_at = NOW(),
                    completed_at = CASE WHEN $2 = 'completed' THEN NOW() ELSE completed_at END
             WHERE id = $1",
        )
        .bind(order_id)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn open_legs(&self) -> Result<Vec<LegRow>> {
        let rows = sqlx::query_as::<_, LegRow>(
            "SELECT id, order_id, platform, token_id, label, side, intent,
                    funder_address, wallet_address, service, req_price, req_shares,
                    client_order_id, third_order_id, status
             FROM legs
             WHERE status IN ('pending','unknown','actived')
             ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn completed_unbalanced_orders(&self) -> Result<Vec<ArbOrderRow>> {
        let rows = sqlx::query_as::<_, ArbOrderRow>(
            "SELECT id, event_id, unified_index, status, rebalance_status
             FROM arb_orders
             WHERE status = 'completed' AND rebalance_status IN ('pending','actived')
             ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn upsert_fill(
        &self,
        leg_id: i64,
        third_order_id: Option<&str>,
        trade_id: Option<&str>,
        shares: Decimal,
        price: Decimal,
        fee: Decimal,
        fee_rate_bps: Option<Decimal>,
        raw: &Value,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO fills (leg_id, third_order_id, trade_id, shares, price, fee, fee_rate_bps, raw)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
             ON CONFLICT (leg_id, trade_id, third_order_id)
             DO UPDATE SET shares = EXCLUDED.shares, price = EXCLUDED.price, fee = EXCLUDED.fee,
                           fee_rate_bps = EXCLUDED.fee_rate_bps, raw = EXCLUDED.raw, updated_at = NOW()",
        )
        .bind(leg_id)
        .bind(third_order_id.unwrap_or(""))
        .bind(trade_id.unwrap_or(""))
        .bind(shares)
        .bind(price)
        .bind(fee)
        .bind(fee_rate_bps)
        .bind(raw)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn positions_for_order(&self, order_id: i64) -> Result<Positions> {
        let rows: Vec<(String, String, String, Option<Decimal>)> = sqlx::query_as(
            "SELECT platform, label, side, actual_shares
             FROM legs
             WHERE order_id = $1 AND status IN ('matched','completed','cancelled')",
        )
        .bind(order_id)
        .fetch_all(&self.pool)
        .await?;
        let mut positions = Positions::new();
        for (platform, label, side, shares) in rows {
            let qty = shares.unwrap_or(Decimal::ZERO);
            let label = label.to_ascii_lowercase();
            let entry = positions.entry(platform).or_default();
            let slot = entry.entry(label).or_insert(Decimal::ZERO);
            if side.eq_ignore_ascii_case("SELL") {
                *slot -= qty;
            } else {
                *slot += qty;
            }
        }
        Ok(positions)
    }

    pub async fn update_actuals(
        &self,
        order_id: i64,
        cost: Decimal,
        rev: Decimal,
        profit: Decimal,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE arb_orders
             SET actual_cost = $2, actual_rev = $3, actual_profit = $4, updated_at = NOW()
             WHERE id = $1",
        )
        .bind(order_id)
        .bind(cost)
        .bind(rev)
        .bind(profit)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_rebalance(&self, order_id: i64, status: &str) -> Result<()> {
        sqlx::query(
            "UPDATE arb_orders SET rebalance_status = $2, updated_at = NOW(),
                    rebalanced_at = CASE WHEN $2 = 'completed' THEN NOW() ELSE rebalanced_at END
             WHERE id = $1",
        )
        .bind(order_id)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn has_open_rebalance_legs(&self, order_id: i64) -> Result<bool> {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM legs
             WHERE order_id = $1
               AND intent = 'rebalance'
               AND status IN ('pending','unknown','actived')
             LIMIT 1",
        )
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(exists.is_some())
    }

    pub async fn fail_stale_pending_without_envelope(&self, timeout: Duration) -> Result<u64> {
        let secs = timeout.as_secs() as i64;
        let result = sqlx::query(
            "UPDATE legs SET status = 'failed', updated_at = NOW(),
                    last_order_info = jsonb_build_object('reason','pending_timeout_no_envelope')
             WHERE status = 'pending'
               AND created_at < NOW() - make_interval(secs => $1)
               AND NOT EXISTS (
                 SELECT 1 FROM signed_envelopes e WHERE e.leg_id = legs.id
               )",
        )
        .bind(secs)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn count_active_orders(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM arb_orders WHERE status IN ('pending','actived')",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    pub async fn count_stale_unknown_legs(&self, timeout: Duration) -> Result<i64> {
        let secs = timeout.as_secs() as i64;
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM legs
             WHERE status = 'unknown'
               AND updated_at < NOW() - make_interval(secs => $1)",
        )
        .bind(secs)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    pub async fn buy_funder_for_token(
        &self,
        order_id: i64,
        platform: &str,
        token_id: &str,
    ) -> Result<Option<String>> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT funder_address FROM legs
             WHERE order_id = $1 AND platform = $2 AND token_id = $3
               AND status = 'matched' AND UPPER(side) = 'BUY'
               AND funder_address IS NOT NULL
             ORDER BY id DESC LIMIT 1",
        )
        .bind(order_id)
        .bind(platform)
        .bind(token_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|item| item.0))
    }

    pub async fn refresh_order_actuals(&self, order_id: i64) -> Result<()> {
        let rows: Vec<(String, String, Option<Decimal>, Option<Decimal>, Option<Decimal>)> =
            sqlx::query_as(
                "SELECT side, platform, actual_shares, actual_price, actual_fee
                 FROM legs
                 WHERE order_id = $1 AND status IN ('matched','completed')",
            )
            .bind(order_id)
            .fetch_all(&self.pool)
            .await?;
        let rows: Vec<(String, String, Decimal, Decimal, Decimal)> = rows
            .into_iter()
            .map(|(side, platform, shares, price, fee)| {
                (
                    side,
                    platform,
                    shares.unwrap_or(Decimal::ZERO),
                    price.unwrap_or(Decimal::ZERO),
                    fee.unwrap_or(Decimal::ZERO),
                )
            })
            .collect();
        let (cost, rev, profit) = compute_actuals(&rows);
        self.update_actuals(order_id, cost, rev, profit).await
    }
}

pub fn compute_actuals(rows: &[(String, String, Decimal, Decimal, Decimal)]) -> (Decimal, Decimal, Decimal) {
    let mut cost = Decimal::ZERO;
    let mut rev = Decimal::ZERO;
    let mut buy_pm = Decimal::ZERO;
    let mut buy_out = Decimal::ZERO;
    for (side, platform, shares, price, fee) in rows {
        if side.eq_ignore_ascii_case("SELL") {
            rev += *shares * *price - *fee;
        } else {
            cost += *shares * *price + *fee;
            if platform == POLYMARKET {
                buy_pm += *shares;
            } else if platform == OUTCOME {
                buy_out += *shares;
            }
        }
    }
    let locked = buy_pm.min(buy_out);
    rev += locked;
    (cost, rev, rev - cost)
}

pub async fn connect_common(uri: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(4)
        .connect(uri)
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn actuals_lock_min_buy_shares_and_add_sell_rev() {
        let rows = vec![
            ("BUY".into(), POLYMARKET.into(), d("10"), d("0.4"), d("0.1")),
            ("BUY".into(), OUTCOME.into(), d("8"), d("0.5"), d("0")),
            ("SELL".into(), POLYMARKET.into(), d("2"), d("0.6"), d("0")),
        ];
        let (cost, rev, profit) = compute_actuals(&rows);
        assert_eq!(cost.to_string(), "8.1");
        assert_eq!(rev.to_string(), "9.2");
        assert_eq!(profit.to_string(), "1.1");
    }
}

