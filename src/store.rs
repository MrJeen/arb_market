use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::{MarketIdentity, TopicKey};
use crate::error::{Error, Result};
use crate::hedge::Positions;
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::time::Duration;
use uuid::Uuid;

type ActualLegRow = (
    String,
    String,
    String,
    Option<Decimal>,
    Option<Decimal>,
    Option<Decimal>,
);

#[derive(Debug, Clone)]
pub struct Store {
    pub pool: PgPool,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ArbOrderRow {
    pub id: i64,
    pub title: String,
    pub event_id: Uuid,
    pub unified_index: i32,
    pub status: String,
    pub rebalance_status: String,
    pub position_status: String,
    pub lifecycle_action: Option<String>,
    pub lifecycle_claim_id: Option<Uuid>,
    pub lifecycle_claimed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub settlement_source: Option<String>,
    pub settlement_result: Option<Value>,
    pub settled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub settlement_pending_since: Option<chrono::DateTime<chrono::Utc>>,
    pub settlement_pending_source: Option<String>,
    pub settlement_pending_result: Option<Value>,
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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClosedLegRef {
    pub id: i64,
    pub order_id: i64,
    pub platform: String,
}

#[derive(Debug, Clone, Copy)]
pub struct NewLeg<'a> {
    pub platform: &'a str,
    pub token_id: &'a str,
    pub label: &'a str,
    pub side: &'a str,
    pub intent: &'a str,
    pub funder: Option<&'a str>,
    pub wallet: Option<&'a str>,
    pub service: Option<&'a str>,
    pub req_price: Decimal,
    pub req_shares: Decimal,
    pub req_fee: Decimal,
    pub client_order_id: Option<&'a str>,
}

impl Store {
    pub async fn connect(uri: &str) -> Result<Self> {
        let pool = PgPoolOptions::new().max_connections(8).connect(uri).await?;
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
        market_identity: &MarketIdentity,
        title: &str,
        market_title: &str,
        end_date: Option<chrono::DateTime<chrono::Utc>>,
        rev: Decimal,
        profit: Decimal,
        cost: Decimal,
        fills: &Value,
    ) -> Result<i64> {
        if market_identity.is_empty() {
            return Err(Error::msg("order market identity must not be empty"));
        }
        let mut tx = self.pool.begin().await?;
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO arb_orders (
                event_id, unified_index, title, market_title, end_date, estimated_rev,
                estimated_profit, estimated_cost, fills, status
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
        .fetch_one(&mut *tx)
        .await?;
        for (platform, market_id) in market_identity.iter() {
            sqlx::query(
                "INSERT INTO arb_order_market_identities (order_id, platform, market_id)
                 VALUES ($1, $2, $3)",
            )
            .bind(id)
            .bind(platform)
            .bind(market_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(id)
    }

    pub async fn market_identities_for_order(&self, order_id: i64) -> Result<MarketIdentity> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT platform, market_id
             FROM arb_order_market_identities
             WHERE order_id = $1
             ORDER BY platform",
        )
        .bind(order_id)
        .fetch_all(&self.pool)
        .await?;
        let mut identity = MarketIdentity::default();
        for (platform, market_id) in rows {
            identity.insert(platform, market_id)?;
        }
        Ok(identity)
    }

    /// Adds missing identities without overwriting existing platform mappings.
    pub async fn backfill_market_identity(
        &self,
        order_id: i64,
        market_identity: &MarketIdentity,
    ) -> Result<bool> {
        if market_identity.is_empty() {
            return Err(Error::msg("backfill market identity must not be empty"));
        }
        let mut tx = self.pool.begin().await?;
        let exists: Option<i64> =
            sqlx::query_scalar("SELECT id FROM arb_orders WHERE id = $1 FOR UPDATE")
                .bind(order_id)
                .fetch_optional(&mut *tx)
                .await?;
        if exists.is_none() {
            return Err(Error::msg(format!("order {order_id} not found")));
        }

        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT platform, market_id
             FROM arb_order_market_identities
             WHERE order_id = $1",
        )
        .bind(order_id)
        .fetch_all(&mut *tx)
        .await?;
        let mut existing = MarketIdentity::default();
        for (platform, market_id) in rows {
            existing.insert(platform, market_id)?;
        }

        let mut inserted = false;
        for (platform, market_id) in market_identity.iter() {
            if let Some(stored) = existing.get(platform) {
                if stored != market_id {
                    return Err(Error::msg(format!(
                        "conflicting {platform} market identity for order {order_id}: stored {stored}, requested {market_id}"
                    )));
                }
                continue;
            }
            sqlx::query(
                "INSERT INTO arb_order_market_identities (order_id, platform, market_id)
                 VALUES ($1, $2, $3)",
            )
            .bind(order_id)
            .bind(platform)
            .bind(market_id)
            .execute(&mut *tx)
            .await?;
            inserted = true;
        }
        tx.commit().await?;
        Ok(inserted)
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

    /// Inserts lifecycle legs only while the caller still owns the claim. The row lock keeps a
    /// concurrent release/reclaim from crossing the ownership check and inserts.
    pub async fn insert_legs_atomic(
        &self,
        order_id: i64,
        expected_action: &str,
        claim_id: Uuid,
        legs: &[NewLeg<'_>],
    ) -> Result<Vec<i64>> {
        validate_lifecycle_action(expected_action)?;
        let mut tx = self.pool.begin().await?;
        let ownership: Option<(Option<String>, Option<Uuid>)> = sqlx::query_as(
            "SELECT lifecycle_action, lifecycle_claim_id
             FROM arb_orders WHERE id = $1 FOR UPDATE",
        )
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await?;
        if ownership
            .as_ref()
            .map(|(action, id)| (action.as_deref(), *id))
            != Some((Some(expected_action), Some(claim_id)))
        {
            return Err(crate::error::Error::msg(format!(
                "lifecycle claim ownership lost for order {order_id}"
            )));
        }

        let mut ids = Vec::with_capacity(legs.len());
        for leg in legs {
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO legs (
                    order_id, platform, token_id, label, side, intent,
                    funder_address, wallet_address, service, req_price, req_shares, req_fee,
                    client_order_id, lifecycle_claim_id, status
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'pending')
                 RETURNING id",
            )
            .bind(order_id)
            .bind(leg.platform)
            .bind(leg.token_id)
            .bind(leg.label)
            .bind(leg.side)
            .bind(leg.intent)
            .bind(leg.funder)
            .bind(leg.wallet)
            .bind(leg.service)
            .bind(leg.req_price)
            .bind(leg.req_shares)
            .bind(leg.req_fee)
            .bind(leg.client_order_id)
            .bind(claim_id)
            .fetch_one(&mut *tx)
            .await?;
            ids.push(id);
        }
        tx.commit().await?;
        Ok(ids)
    }

    pub async fn insert_leg_for_claim(
        &self,
        order_id: i64,
        expected_action: &str,
        claim_id: Uuid,
        leg: &NewLeg<'_>,
    ) -> Result<i64> {
        self.insert_legs_atomic(
            order_id,
            expected_action,
            claim_id,
            std::slice::from_ref(leg),
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| crate::error::Error::msg("lifecycle leg was not inserted"))
    }

    pub async fn insert_envelope(
        &self,
        leg_id: i64,
        order_hash: &str,
        envelope: &Value,
        http_payload: &Value,
        book_snapshot: Option<&Value>,
    ) -> Result<()> {
        let client_id = envelope
            .get("cloid")
            .and_then(|v| v.as_str())
            .unwrap_or(order_hash);
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO signed_envelopes (leg_id, order_hash, payload, book_snapshot)
             VALUES ($1,$2,$3,$4)",
        )
        .bind(leg_id)
        .bind(order_hash)
        .bind(http_payload)
        .bind(book_snapshot)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE legs SET client_order_id = $2, submitted_at = COALESCE(submitted_at, NOW()),
                    updated_at = NOW()
             WHERE id = $1",
        )
        .bind(leg_id)
        .bind(client_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn save_submit_response(&self, leg_id: i64, response: &Value) -> Result<()> {
        sqlx::query(
            "UPDATE signed_envelopes SET submit_response = $2
             WHERE id = (
                 SELECT id FROM signed_envelopes WHERE leg_id = $1 ORDER BY id DESC LIMIT 1
             )",
        )
        .bind(leg_id)
        .bind(response)
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
                    last_order_info = $4, submitted_at = COALESCE(submitted_at, NOW()),
                    updated_at = NOW()
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
                    last_order_info = $7, submitted_at = COALESCE(submitted_at, NOW()),
                    updated_at = NOW()
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

    pub async fn completed_unbalanced_orders(
        &self,
        after_id: i64,
        limit: usize,
    ) -> Result<Vec<ArbOrderRow>> {
        let limit = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
        let query = |after_id: i64| {
            sqlx::query_as::<_, ArbOrderRow>(
                "SELECT id, title, event_id, unified_index, status, rebalance_status,
                        position_status, lifecycle_action,
                        lifecycle_claim_id, lifecycle_claimed_at, settlement_source,
                        settlement_result, settled_at, settlement_pending_since,
                        settlement_pending_source, settlement_pending_result
                 FROM arb_orders
                 WHERE status = 'completed'
                   AND rebalance_status IN ('pending','actived','completed')
                   AND position_status IN ('watching','settlement_pending')
                   AND settled_at IS NULL
                   AND id > $1
                 ORDER BY id
                 LIMIT $2",
            )
            .bind(after_id)
            .bind(limit)
        };
        let mut rows = query(after_id).fetch_all(&self.pool).await?;
        if rows.is_empty() && after_id > 0 {
            rows = query(0).fetch_all(&self.pool).await?;
        }
        Ok(rows)
    }

    pub async fn order_topic_key(&self, order_id: i64) -> Result<TopicKey> {
        let (event_id, unified_index): (Uuid, i32) =
            sqlx::query_as("SELECT event_id, unified_index FROM arb_orders WHERE id = $1")
                .bind(order_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(TopicKey::new(event_id, unified_index))
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
             WHERE order_id = $1 AND status IN ('matched','completed','cancelled','failed')",
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

    /// Atomically reserves a lifecycle action across concurrent workers.
    pub async fn try_claim_lifecycle(&self, order_id: i64, action: &str) -> Result<Option<Uuid>> {
        validate_lifecycle_action(action)?;
        let claim_id = Uuid::new_v4();
        let claimed: Option<Uuid> = sqlx::query_scalar(
            "UPDATE arb_orders
             SET lifecycle_action = $2, lifecycle_claim_id = $3,
                 lifecycle_claimed_at = NOW(), updated_at = NOW()
             WHERE id = $1
               AND position_status = 'watching'
               AND lifecycle_action IS NULL
               AND lifecycle_claim_id IS NULL
               AND lifecycle_claimed_at IS NULL
               AND settled_at IS NULL
             RETURNING lifecycle_claim_id",
        )
        .bind(order_id)
        .bind(action)
        .bind(claim_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(claimed)
    }

    /// Releases only the exact claim owned by the caller, preventing an ABA-era worker from
    /// clearing a newer worker's claim.
    pub async fn release_lifecycle(
        &self,
        order_id: i64,
        action: &str,
        claim_id: Uuid,
    ) -> Result<bool> {
        validate_lifecycle_action(action)?;
        let result = sqlx::query(
            "UPDATE arb_orders
             SET position_status = 'watching', lifecycle_action = NULL,
                 lifecycle_claim_id = NULL, lifecycle_claimed_at = NULL, updated_at = NOW()
             WHERE id = $1 AND settled_at IS NULL
               AND lifecycle_action = $2 AND lifecycle_claim_id = $3",
        )
        .bind(order_id)
        .bind(action)
        .bind(claim_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Stops all lifecycle trading after the first venue confirms settlement.
    /// Returns the durable first-seen timestamp and whether this call entered pending.
    pub async fn mark_settlement_pending(
        &self,
        order_id: i64,
        source: &str,
        result: &Value,
    ) -> Result<Option<(chrono::DateTime<chrono::Utc>, bool)>> {
        let entered: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "UPDATE arb_orders
             SET position_status = 'settlement_pending',
                 settlement_pending_since = NOW(), settlement_pending_source = $2,
                 settlement_pending_result = $3, updated_at = NOW()
             WHERE id = $1 AND position_status = 'watching' AND settled_at IS NULL
               AND lifecycle_action IS NULL
               AND lifecycle_claim_id IS NULL
               AND lifecycle_claimed_at IS NULL
             RETURNING settlement_pending_since",
        )
        .bind(order_id)
        .bind(source)
        .bind(result)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(since) = entered {
            return Ok(Some((since, true)));
        }
        let existing: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT settlement_pending_since FROM arb_orders
             WHERE id = $1 AND position_status = 'settlement_pending' AND settled_at IS NULL",
        )
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await?
        .flatten();
        Ok(existing.map(|since| (since, false)))
    }

    /// First settlement wins; retries do not overwrite the recorded evidence.
    pub async fn mark_position_settled(
        &self,
        order_id: i64,
        source: &str,
        result: &Value,
    ) -> Result<bool> {
        let updated: Option<i64> = sqlx::query_scalar(
            "UPDATE arb_orders
             SET position_status = 'settled', lifecycle_action = NULL,
                 lifecycle_claim_id = NULL, lifecycle_claimed_at = NULL, settlement_source = $2,
                 settlement_result = $3, settled_at = NOW(),
                 updated_at = NOW()
             WHERE id = $1 AND settled_at IS NULL
               AND lifecycle_action IS NULL
               AND lifecycle_claim_id IS NULL
               AND lifecycle_claimed_at IS NULL
             RETURNING id",
        )
        .bind(order_id)
        .bind(source)
        .bind(result)
        .fetch_optional(&self.pool)
        .await?;
        Ok(updated.is_some())
    }

    /// Atomically finalizes a settled position from token-level payouts and all durable fills.
    pub async fn finalize_position_settlement(
        &self,
        order_id: i64,
        source: &str,
        result: &Value,
        payouts: &std::collections::HashMap<(String, String), Decimal>,
    ) -> Result<Option<(Decimal, Decimal, Decimal)>> {
        let mut tx = self.pool.begin().await?;
        let finalizable: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM arb_orders
             WHERE id = $1 AND settled_at IS NULL
               AND lifecycle_action IS NULL
               AND lifecycle_claim_id IS NULL
               AND lifecycle_claimed_at IS NULL
             FOR UPDATE",
        )
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await?;
        if finalizable.is_none() {
            tx.rollback().await?;
            return Ok(None);
        }
        let rows: Vec<ActualLegRow> = sqlx::query_as(
            "SELECT side, platform, token_id, actual_shares, actual_price, actual_fee
             FROM legs
             WHERE order_id = $1
               AND status IN ('matched','completed','cancelled','failed')",
        )
        .bind(order_id)
        .fetch_all(&mut *tx)
        .await?;
        let rows: Vec<_> = rows
            .into_iter()
            .map(|(side, platform, token_id, shares, price, fee)| {
                (
                    side,
                    platform,
                    token_id,
                    shares.unwrap_or(Decimal::ZERO),
                    price.unwrap_or(Decimal::ZERO),
                    fee.unwrap_or(Decimal::ZERO),
                )
            })
            .collect();
        let (cost, rev, profit) = compute_settled_actuals(&rows, payouts)?;
        let updated: Option<i64> = sqlx::query_scalar(
            "UPDATE arb_orders
             SET actual_cost = $2, actual_rev = $3, actual_profit = $4,
                 position_status = 'settled', lifecycle_action = NULL,
                 lifecycle_claim_id = NULL, lifecycle_claimed_at = NULL,
                 settlement_source = $5, settlement_result = $6, settled_at = NOW(),
                 updated_at = NOW()
             WHERE id = $1 AND settled_at IS NULL
               AND lifecycle_action IS NULL
               AND lifecycle_claim_id IS NULL
               AND lifecycle_claimed_at IS NULL
             RETURNING id",
        )
        .bind(order_id)
        .bind(cost)
        .bind(rev)
        .bind(profit)
        .bind(source)
        .bind(result)
        .fetch_optional(&mut *tx)
        .await?;
        if updated.is_none() {
            tx.rollback().await?;
            return Ok(None);
        }
        tx.commit().await?;
        Ok(Some((cost, rev, profit)))
    }

    pub async fn mark_position_closed(&self, order_id: i64) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE arb_orders
             SET position_status = 'closed', updated_at = NOW()
             WHERE id = $1 AND settled_at IS NULL
               AND lifecycle_action IS NULL
               AND lifecycle_claim_id IS NULL
               AND lifecycle_claimed_at IS NULL",
        )
        .bind(order_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn lifecycle_leg_counts(
        &self,
        order_id: i64,
        intent: &str,
        claim_id: Uuid,
    ) -> Result<(i64, i64)> {
        validate_lifecycle_action(intent)?;
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*)::BIGINT,
                    COUNT(*) FILTER (WHERE status IN ('pending','unknown','actived'))::BIGINT
             FROM legs
             WHERE order_id = $1 AND intent = $2 AND lifecycle_claim_id = $3",
        )
        .bind(order_id)
        .bind(intent)
        .bind(claim_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(counts)
    }

    pub async fn lifecycle_claim_has_positive_fill(
        &self,
        order_id: i64,
        intent: &str,
        claim_id: Uuid,
    ) -> Result<bool> {
        validate_lifecycle_action(intent)?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM legs
                 WHERE order_id = $1 AND intent = $2 AND lifecycle_claim_id = $3
                   AND status IN ('matched','completed')
                   AND COALESCE(actual_shares, 0) > 0
             )",
        )
        .bind(order_id)
        .bind(intent)
        .bind(claim_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    pub async fn has_open_lifecycle_legs(&self, order_id: i64) -> Result<bool> {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM legs
             WHERE order_id = $1
               AND intent IN ('rebalance','take_profit')
               AND status IN ('pending','unknown','actived')
             LIMIT 1",
        )
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(exists.is_some())
    }

    pub async fn fail_stale_pending_unsubmitted(&self, timeout: Duration) -> Result<u64> {
        let secs = timeout.as_secs() as i64;
        let result = sqlx::query(
            "UPDATE legs SET status = 'failed', updated_at = NOW(),
                    last_order_info = jsonb_build_object('reason','pending_timeout_unsubmitted')
             WHERE status = 'pending'
               AND submitted_at IS NULL
               AND created_at < NOW() - make_interval(secs => $1)",
        )
        .bind(secs)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn cancel_stale_unknown_without_fills(
        &self,
        timeout: Duration,
    ) -> Result<Vec<ClosedLegRef>> {
        let secs = timeout.as_secs() as i64;
        let rows = sqlx::query_as::<_, ClosedLegRef>(
            "UPDATE legs SET status = 'cancelled',
                    actual_price = 0, actual_shares = 0, actual_fee = 0,
                    last_order_info = COALESCE(last_order_info, '{}'::jsonb)
                        || jsonb_build_object('reason','unknown_timeout_no_fill'),
                    updated_at = NOW()
             WHERE status = 'unknown'
               AND updated_at < NOW() - make_interval(secs => $1)
               AND NOT EXISTS (
                 SELECT 1 FROM fills f
                 WHERE f.leg_id = legs.id AND COALESCE(f.shares, 0) > 0
               )
             RETURNING id, order_id, platform",
        )
        .bind(secs)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn promote_submitted_pending_to_unknown(&self) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE legs SET status = 'unknown', updated_at = NOW(),
                    last_order_info = COALESCE(last_order_info, '{}'::jsonb)
                        || jsonb_build_object('reason','pending_after_submit')
             WHERE status = 'pending'
               AND submitted_at IS NOT NULL",
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn count_active_orders(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM arb_orders
             WHERE status IN ('pending','actived','completed')",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    pub async fn sum_actual_profit(&self) -> Result<Decimal> {
        let profit: Option<Decimal> =
            sqlx::query_scalar("SELECT SUM(actual_profit) FROM arb_orders")
                .fetch_one(&self.pool)
                .await?;
        Ok(profit.unwrap_or(Decimal::ZERO))
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

    /// 本笔套利下单时用的 Polymarket funder，对冲不得换号。
    pub async fn order_pm_funder(&self, order_id: i64) -> Result<Option<String>> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT funder_address FROM legs
             WHERE order_id = $1 AND platform = $2 AND funder_address IS NOT NULL
             ORDER BY CASE WHEN intent = 'arb_buy' THEN 0 ELSE 1 END, id ASC
             LIMIT 1",
        )
        .bind(order_id)
        .bind(POLYMARKET)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|item| item.0))
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

    pub async fn refresh_order_actuals(
        &self,
        order_id: i64,
    ) -> Result<(Decimal, Decimal, Decimal)> {
        let rows: Vec<ActualLegRow> = sqlx::query_as(
            "SELECT side, platform, label, actual_shares, actual_price, actual_fee
                 FROM legs
                 WHERE order_id = $1 AND status IN ('matched','completed')",
        )
        .bind(order_id)
        .fetch_all(&self.pool)
        .await?;
        let rows: Vec<(String, String, String, Decimal, Decimal, Decimal)> = rows
            .into_iter()
            .map(|(side, platform, label, shares, price, fee)| {
                (
                    side,
                    platform,
                    label,
                    shares.unwrap_or(Decimal::ZERO),
                    price.unwrap_or(Decimal::ZERO),
                    fee.unwrap_or(Decimal::ZERO),
                )
            })
            .collect();
        let (cost, rev, profit) = compute_actuals(&rows);
        self.update_actuals(order_id, cost, rev, profit).await?;
        Ok((cost, rev, profit))
    }
}

fn validate_lifecycle_action(action: &str) -> Result<()> {
    match action {
        "take_profit" | "rebalance" => Ok(()),
        _ => Err(crate::error::Error::msg(format!(
            "invalid lifecycle action: {action}"
        ))),
    }
}

fn compute_settled_actuals(
    rows: &[(String, String, String, Decimal, Decimal, Decimal)],
    payouts: &std::collections::HashMap<(String, String), Decimal>,
) -> Result<(Decimal, Decimal, Decimal)> {
    use std::collections::BTreeMap;

    let mut cost = Decimal::ZERO;
    let mut rev = Decimal::ZERO;
    let mut positions = BTreeMap::new();
    for (side, platform, token_id, shares, price, fee) in rows {
        let position = positions
            .entry((platform.clone(), token_id.clone()))
            .or_insert(Decimal::ZERO);
        if side.eq_ignore_ascii_case("SELL") {
            rev += *shares * *price - *fee;
            *position -= *shares;
        } else {
            cost += *shares * *price + *fee;
            *position += *shares;
        }
    }
    for ((platform, token_id), shares) in positions {
        if shares < Decimal::ZERO {
            return Err(Error::msg(format!(
                "negative settled position for {platform}:{token_id}: {shares}"
            )));
        }
        if shares.is_zero() {
            continue;
        }
        let payout = payouts
            .get(&(platform.clone(), token_id.clone()))
            .ok_or_else(|| {
                Error::msg(format!(
                    "missing settlement payout for {platform}:{token_id}"
                ))
            })?;
        rev += shares * *payout;
    }
    Ok((cost, rev, rev - cost))
}

pub fn compute_actuals(
    rows: &[(String, String, String, Decimal, Decimal, Decimal)],
) -> (Decimal, Decimal, Decimal) {
    use std::collections::{BTreeMap, BTreeSet};

    let mut cost = Decimal::ZERO;
    let mut rev = Decimal::ZERO;
    let mut labels = BTreeSet::new();
    let mut positions = BTreeMap::new();
    for (side, platform, label, shares, price, fee) in rows {
        let is_sell = side.eq_ignore_ascii_case("SELL");
        if is_sell {
            rev += *shares * *price - *fee;
        } else {
            cost += *shares * *price + *fee;
        }

        let label = label.to_ascii_lowercase();
        labels.insert(label.clone());
        if platform == POLYMARKET || platform == OUTCOME {
            let position = positions
                .entry((platform.as_str(), label))
                .or_insert(Decimal::ZERO);
            if is_sell {
                *position -= *shares;
            } else {
                *position += *shares;
            }
        }
    }

    // 项目仅支持二元市场，一对跨平台互补份额固定兑付 q/1。
    let locked = if labels.len() == 2 {
        let mut labels = labels.iter();
        let label0 = labels.next().expect("two labels checked");
        let label1 = labels.next().expect("two labels checked");
        let position = |platform, label: &String| {
            positions
                .get(&(platform, label.clone()))
                .copied()
                .unwrap_or(Decimal::ZERO)
                .max(Decimal::ZERO)
        };
        position(POLYMARKET, label0).min(position(OUTCOME, label1))
            + position(POLYMARKET, label1).min(position(OUTCOME, label0))
    } else {
        Decimal::ZERO
    };
    rev += locked;
    (cost, rev, rev - cost)
}

pub async fn connect_common(uri: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new().max_connections(4).connect(uri).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn lifecycle_action_accepts_supported_actions() {
        assert!(validate_lifecycle_action("take_profit").is_ok());
        assert!(validate_lifecycle_action("rebalance").is_ok());
    }

    #[test]
    fn lifecycle_action_rejects_unknown_actions() {
        assert!(validate_lifecycle_action("settlement").is_err());
        assert!(validate_lifecycle_action("").is_err());
    }

    #[test]
    fn settled_actuals_use_remaining_token_payouts_without_locked_double_count() {
        let rows = vec![
            (
                "BUY".into(),
                POLYMARKET.into(),
                "pm-yes".into(),
                d("10"),
                d("0.4"),
                d("0"),
            ),
            (
                "BUY".into(),
                OUTCOME.into(),
                "#5161".into(),
                d("8"),
                d("0.5"),
                d("0"),
            ),
            (
                "SELL".into(),
                POLYMARKET.into(),
                "pm-yes".into(),
                d("2"),
                d("0.6"),
                d("0"),
            ),
        ];
        let payouts = std::collections::HashMap::from([
            ((POLYMARKET.into(), "pm-yes".into()), Decimal::ONE),
            ((OUTCOME.into(), "#5161".into()), Decimal::ONE),
        ]);
        let (cost, rev, profit) = compute_settled_actuals(&rows, &payouts).unwrap();
        assert_eq!(cost, d("8"));
        assert_eq!(rev, d("17.2"));
        assert_eq!(profit, d("9.2"));
    }

    #[test]
    fn settled_actuals_use_fractional_outcome_payout_regardless_of_pm_winner() {
        let rows = vec![
            (
                "BUY".into(),
                POLYMARKET.into(),
                "pm-yes".into(),
                d("10"),
                d("0.4"),
                Decimal::ZERO,
            ),
            (
                "BUY".into(),
                OUTCOME.into(),
                "#5161".into(),
                d("10"),
                d("0.55"),
                Decimal::ZERO,
            ),
        ];
        for (pm_payout, expected_rev, expected_profit) in [
            (Decimal::ZERO, d("5"), d("-4.5")),
            (Decimal::ONE, d("15"), d("5.5")),
        ] {
            let payouts = std::collections::HashMap::from([
                ((POLYMARKET.into(), "pm-yes".into()), pm_payout),
                ((OUTCOME.into(), "#5161".into()), d("0.5")),
            ]);
            assert_eq!(
                compute_settled_actuals(&rows, &payouts).unwrap(),
                (d("9.5"), expected_rev, expected_profit)
            );
        }
    }

    #[test]
    fn settled_actuals_reject_missing_payout_and_negative_position() {
        let buy = vec![(
            "BUY".into(),
            OUTCOME.into(),
            "#5160".into(),
            d("1"),
            d("0.4"),
            d("0"),
        )];
        assert!(compute_settled_actuals(&buy, &std::collections::HashMap::new()).is_err());
        let sell = vec![(
            "SELL".into(),
            OUTCOME.into(),
            "#5160".into(),
            d("1"),
            d("0.4"),
            d("0"),
        )];
        assert!(compute_settled_actuals(&sell, &std::collections::HashMap::new()).is_err());
    }

    #[test]
    fn actuals_lock_opposite_labels_and_add_sell_rev() {
        let rows = vec![
            (
                "BUY".into(),
                POLYMARKET.into(),
                "yes".into(),
                d("10"),
                d("0.4"),
                d("0.1"),
            ),
            (
                "BUY".into(),
                OUTCOME.into(),
                "no".into(),
                d("8"),
                d("0.5"),
                d("0"),
            ),
            (
                "SELL".into(),
                POLYMARKET.into(),
                "yes".into(),
                d("2"),
                d("0.6"),
                d("0"),
            ),
        ];
        let (cost, rev, profit) = compute_actuals(&rows);
        assert_eq!(cost.to_string(), "8.1");
        assert_eq!(rev.to_string(), "9.2");
        assert_eq!(profit.to_string(), "1.1");
    }

    #[test]
    fn actuals_same_labels_do_not_lock() {
        let rows = vec![
            (
                "BUY".into(),
                POLYMARKET.into(),
                "yes".into(),
                d("10"),
                d("0.4"),
                d("0"),
            ),
            (
                "BUY".into(),
                OUTCOME.into(),
                "yes".into(),
                d("8"),
                d("0.5"),
                d("0"),
            ),
        ];
        let (cost, rev, profit) = compute_actuals(&rows);
        assert_eq!(cost, d("8"));
        assert_eq!(rev, Decimal::ZERO);
        assert_eq!(profit, d("-8"));
    }

    #[test]
    fn actuals_lock_both_complementary_directions() {
        let rows = vec![
            (
                "BUY".into(),
                POLYMARKET.into(),
                "yes".into(),
                d("10"),
                d("0.4"),
                d("0"),
            ),
            (
                "BUY".into(),
                OUTCOME.into(),
                "no".into(),
                d("8"),
                d("0.5"),
                d("0"),
            ),
            (
                "BUY".into(),
                POLYMARKET.into(),
                "no".into(),
                d("3"),
                d("0.3"),
                d("0"),
            ),
            (
                "BUY".into(),
                OUTCOME.into(),
                "yes".into(),
                d("5"),
                d("0.6"),
                d("0"),
            ),
        ];
        let (cost, rev, profit) = compute_actuals(&rows);
        assert_eq!(cost, d("11.9"));
        assert_eq!(rev, d("11"));
        assert_eq!(profit, d("-0.9"));
    }

    #[test]
    fn actuals_partial_sell_reduces_its_label_position() {
        let rows = vec![
            (
                "BUY".into(),
                POLYMARKET.into(),
                "yes".into(),
                d("100"),
                d("0.4"),
                d("0"),
            ),
            (
                "BUY".into(),
                OUTCOME.into(),
                "no".into(),
                d("100"),
                d("0.5"),
                d("0"),
            ),
            (
                "SELL".into(),
                POLYMARKET.into(),
                "yes".into(),
                d("50"),
                d("0.4"),
                d("0"),
            ),
        ];
        let (cost, rev, profit) = compute_actuals(&rows);
        assert_eq!(cost, d("90"));
        assert_eq!(rev, d("70"));
        assert_eq!(profit, d("-20"));
    }
}
