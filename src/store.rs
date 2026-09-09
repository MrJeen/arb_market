use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::{MarketIdentity, TopicKey};
use crate::error::{Error, Result};
use crate::hedge::Positions;
use crate::platforms::{OrderPoll, TradeFill};
use crate::reconcile::{
    merge_observation, pm_order_constraints, resolve_leg, FillEvidence, LegResolution,
    PmOrderConstraints, PmTradeScan,
};
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
type TerminalLegRow = (
    String,
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
    pub submitted_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_order_info: Option<Value>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
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

    /// 父单、市场标识与初始交易腿必须一次落库并直接进入 `actived`。
    /// 「`actived` 且没有任何腿」这个中间状态会被 `mark_orders_complete` 判为无成交并取消，
    /// 此后父单永远回不到 `completed`，随后成交的真实持仓将脱离再平衡、止盈和结算管理。
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_actived_order_with_legs(
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
        legs: &[NewLeg<'_>],
    ) -> Result<(i64, Vec<i64>)> {
        if market_identity.is_empty() {
            return Err(Error::msg("order market identity must not be empty"));
        }
        if legs.is_empty() {
            return Err(Error::msg("actived order requires at least one leg"));
        }
        let mut tx = self.pool.begin().await?;
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO arb_orders (
                event_id, unified_index, title, market_title, end_date, estimated_rev,
                estimated_profit, estimated_cost, fills, status
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'actived')
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
        let mut leg_ids = Vec::with_capacity(legs.len());
        for leg in legs {
            let leg_id: i64 = sqlx::query_scalar(
                "INSERT INTO legs (
                    order_id, platform, token_id, label, side, intent,
                    funder_address, wallet_address, service, req_price, req_shares, req_fee,
                    client_order_id, status
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'pending')
                 RETURNING id",
            )
            .bind(id)
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
            .fetch_one(&mut *tx)
            .await?;
            leg_ids.push(leg_id);
        }
        tx.commit().await?;
        Ok((id, leg_ids))
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
        let (_, parent_open) = lock_leg_parent(&mut tx, leg_id).await?;
        let current = read_locked_leg(&mut tx, leg_id).await?;
        if !parent_open || current.status != "pending" || current.submitted_at.is_some() {
            return Err(Error::msg(
                "leg is no longer eligible for initial submission",
            ));
        }
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

    /// 网络提交的迟到响应只能增加审计证据，不能撤销已确认的成交。
    pub async fn record_submission(
        &self,
        leg_id: i64,
        status: &str,
        order_id: Option<&str>,
        evidence: &Value,
        response: &Value,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let (parent_id, parent_open) = lock_leg_parent(&mut tx, leg_id).await?;
        let current = read_locked_leg(&mut tx, leg_id).await?;
        sqlx::query(
            "UPDATE signed_envelopes SET submit_response = $2 WHERE id =
             (SELECT id FROM signed_envelopes WHERE leg_id = $1 ORDER BY id DESC LIMIT 1)",
        )
        .bind(leg_id)
        .bind(response)
        .execute(&mut *tx)
        .await?;
        if !parent_open || !leg_open(&current.status) {
            tx.commit().await?;
            return Ok(());
        }
        let has_observation: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM fills WHERE leg_id = $1)")
                .bind(leg_id)
                .fetch_one(&mut *tx)
                .await?;
        let known_constraints = leg_pm_order_constraints(&current, None)?;
        let has_known_execution = known_constraints
            .as_ref()
            .is_some_and(PmOrderConstraints::has_execution);
        let known = current
            .third_order_id
            .as_deref()
            .or(current.client_order_id.as_deref());
        if let (Some(known), Some(incoming)) = (known, order_id) {
            if known != incoming
                && (Some(known) != current.client_order_id.as_deref()
                    || (current.platform == POLYMARKET && (has_observation || has_known_execution)))
            {
                return Err(Error::msg(
                    "submit response conflicts with recovered order id",
                ));
            }
        }
        let terminal = status == "cancelled" || status == "failed";
        // 找到订单或已有撮合证据后，晚到的拒单不能证明零成交。
        let next_status = if terminal
            && (has_observation || has_known_execution || current.status == "actived")
        {
            current.status.as_str()
        } else if status == "unknown" && current.status == "actived" {
            "actived"
        } else {
            status
        };
        let mut info = current
            .last_order_info
            .unwrap_or_else(|| serde_json::json!({}));
        if current.platform == POLYMARKET
            && known
                .zip(order_id)
                .is_some_and(|(known, incoming)| known != incoming)
        {
            // 尚无成交时允许恢复远端 oid，但旧候选订单的窗口进度不能沿用。
            info["fill_progress"] = Value::Null;
            info["fill_evidence"] = Value::Null;
        }
        if let Some(constraints) = known_constraints {
            info["pm_order_constraints"] =
                if known.zip(order_id).is_some_and(|(old, new)| old != new) {
                    Value::Null
                } else {
                    serde_json::to_value(constraints)?
                };
        }
        if info.pointer("/submission/kind").and_then(Value::as_str) != Some("ack")
            || evidence.get("kind").and_then(Value::as_str) == Some("ack")
        {
            info["submission"] = evidence.clone();
        }
        sqlx::query(
            "UPDATE legs SET status = $2, third_order_id = COALESCE($3,third_order_id),
                 last_order_info = $4, updated_at = clock_timestamp(),
                 actual_shares = CASE WHEN $2 IN ('cancelled','failed') THEN 0 ELSE actual_shares END,
                 actual_price = CASE WHEN $2 IN ('cancelled','failed') THEN 0 ELSE actual_price END,
                 actual_fee = CASE WHEN $2 IN ('cancelled','failed') THEN 0 ELSE actual_fee END
             WHERE id = $1",
        ).bind(leg_id).bind(next_status).bind(order_id).bind(info).execute(&mut *tx).await?;
        refresh_parent_in_tx(&mut tx, parent_id).await?;
        tx.commit().await?;
        Ok(())
    }

    /// 查单恢复身份先落库；后续 HTTP 失败不会丢掉 oid。返回新快照用作回填的并发令牌。
    pub async fn record_order_poll(
        &self,
        leg: &LegRow,
        poll: &OrderPoll,
    ) -> Result<Option<LegRow>> {
        let mut tx = self.pool.begin().await?;
        let (_, parent_open) = lock_leg_parent(&mut tx, leg.id).await?;
        let current = read_locked_leg(&mut tx, leg.id).await?;
        if !parent_open || !leg_open(&current.status) || current.updated_at != leg.updated_at {
            tx.rollback().await?;
            return Ok(None);
        }
        let known_constraints = leg_pm_order_constraints(&current, None)?;
        let has_known_execution = known_constraints
            .as_ref()
            .is_some_and(PmOrderConstraints::has_execution);
        if let (Some(known), Some(incoming)) = (
            current
                .third_order_id
                .as_deref()
                .or(current.client_order_id.as_deref()),
            poll.order_id.as_deref(),
        ) {
            if known != incoming {
                let has_pm_observation = if current.platform == POLYMARKET {
                    sqlx::query_scalar::<_, bool>(
                        "SELECT EXISTS(SELECT 1 FROM fills WHERE leg_id = $1)",
                    )
                    .bind(leg.id)
                    .fetch_one(&mut *tx)
                    .await?
                } else {
                    false
                };
                // 按本地订单哈希落过真实成交后，不能让迟到查单把腿与明细分离。
                if Some(known) != current.client_order_id.as_deref()
                    || has_pm_observation
                    || has_known_execution
                    || (current.platform == POLYMARKET && !poll.found)
                {
                    return Err(Error::msg(
                        "order lookup conflicts with durable order identity",
                    ));
                }
            }
        }
        if poll
            .coin
            .as_deref()
            .is_some_and(|coin| coin != current.token_id)
            || poll.client_order_id.as_deref().is_some_and(|id| {
                current
                    .client_order_id
                    .as_deref()
                    .is_some_and(|known| id != known)
            })
        {
            return Err(Error::msg(
                "order lookup belongs to a different token or client id",
            ));
        }
        let mut info = current
            .last_order_info
            .clone()
            .unwrap_or_else(|| serde_json::json!({}));
        if current.platform == POLYMARKET
            && current
                .third_order_id
                .as_deref()
                .or(current.client_order_id.as_deref())
                .zip(poll.order_id.as_deref())
                .is_some_and(|(known, incoming)| known != incoming)
        {
            info["fill_progress"] = Value::Null;
            info["fill_evidence"] = Value::Null;
            info["pm_order_constraints"] = Value::Null;
        }
        if current.platform == POLYMARKET {
            if let Some(oid) = poll
                .order_id
                .as_deref()
                .or(current.third_order_id.as_deref())
                .or(current.client_order_id.as_deref())
            {
                let funder = current
                    .funder_address
                    .as_deref()
                    .ok_or_else(|| Error::msg("missing polymarket funder"))?;
                let constraints =
                    pm_order_constraints(Some(&info), oid, &current.token_id, funder, Some(poll))?;
                info["pm_order_constraints"] = serde_json::to_value(constraints)?;
            }
        }
        info["order_poll"] = serde_json::to_value(poll)?;
        sqlx::query(
            "UPDATE legs SET third_order_id = COALESCE($2,third_order_id),
                 status = CASE WHEN $3 THEN 'actived' ELSE status END,
                 last_order_info = $4, updated_at = clock_timestamp() WHERE id = $1",
        )
        .bind(leg.id)
        .bind(poll.order_id.as_deref())
        .bind(poll.found)
        .bind(info)
        .execute(&mut *tx)
        .await?;
        let updated = read_locked_leg(&mut tx, leg.id).await?;
        tx.commit().await?;
        Ok(Some(updated))
    }

    pub async fn record_reconciliation_wait(&self, leg: &LegRow, reason: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let (_, parent_open) = lock_leg_parent(&mut tx, leg.id).await?;
        let current = read_locked_leg(&mut tx, leg.id).await?;
        if !parent_open || !leg_open(&current.status) || current.updated_at != leg.updated_at {
            tx.rollback().await?;
            return Ok(());
        }
        let mut info = current
            .last_order_info
            .unwrap_or_else(|| serde_json::json!({}));
        info["waiting_reason"] = serde_json::json!(reason);
        sqlx::query("UPDATE legs SET last_order_info=$2,updated_at=clock_timestamp() WHERE id=$1")
            .bind(leg.id)
            .bind(info)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// 只恢复本页实际出现的同腿成交，不能用缓存补造扫描覆盖；末端事务仍须重新校验。
    pub async fn reconciliation_observations(
        &self,
        leg: &LegRow,
        order_id: &str,
        trade_ids: &[String],
    ) -> Result<Vec<TradeFill>> {
        if trade_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows: Vec<(String, Value)> = sqlx::query_as(
            "SELECT trade_id,raw->'reconciliation_v1' FROM fills
             WHERE leg_id=$1 AND third_order_id=$2 AND trade_id=ANY($3)
               AND raw ? 'reconciliation_v1' AND trade_id NOT LIKE 'ack:%'",
        )
        .bind(leg.id)
        .bind(order_id)
        .bind(trade_ids)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(id, value)| {
                let fill: TradeFill = serde_json::from_value(value)?;
                if fill.trade_id != id
                    || fill.order_id.as_deref() != Some(order_id)
                    || fill.coin.as_deref() != Some(leg.token_id.as_str())
                {
                    return Err(Error::msg(
                        "stored reconciliation observation identity mismatch",
                    ));
                }
                Ok(fill)
            })
            .collect()
    }

    /// 明细、分页进度、腿最终账务和父单完成在同一事务提交。
    pub async fn record_reconciliation(
        &self,
        leg: &LegRow,
        observations: &[TradeFill],
        evidence: &FillEvidence,
        progress: &Value,
    ) -> Result<LegResolution> {
        let mut tx = self.pool.begin().await?;
        let (parent_id, parent_open) = lock_leg_parent(&mut tx, leg.id).await?;
        let current = read_locked_leg(&mut tx, leg.id).await?;
        if !parent_open || !leg_open(&current.status) || current.updated_at != leg.updated_at {
            tx.rollback().await?;
            return Ok(LegResolution::Pending("stale_leg_snapshot"));
        }
        let oid = current
            .third_order_id
            .as_deref()
            .ok_or_else(|| Error::msg("reconciliation requires recovered order id"))?;
        if evidence.poll.order_id.as_deref() != Some(oid) {
            return Err(Error::msg("reconciliation evidence order id mismatch"));
        }
        if current.platform == POLYMARKET {
            if let Some(scan) = &evidence.pm_scan {
                scan.validate()?;
                let persisted: PmTradeScan = serde_json::from_value(progress.clone())?;
                if &persisted != scan
                    || scan.order_id != oid
                    || scan.asset_id != current.token_id
                    || !current
                        .funder_address
                        .as_deref()
                        .is_some_and(|funder| funder.eq_ignore_ascii_case(&scan.funder))
                    || current.submitted_at.map(|time| time.timestamp()) != Some(scan.after)
                    || evidence.page_complete != (scan.next_cursor == "LTE=")
                    || evidence.history_complete != evidence.page_complete
                    || observations.iter().any(|fill| {
                        fill.coin.as_deref() != Some(current.token_id.as_str())
                            || !scan.trade_ids.contains(&fill.trade_id)
                    })
                {
                    return Err(Error::msg(
                        "polymarket scan does not match persisted leg or page",
                    ));
                }
            }
        }
        for incoming in observations {
            if incoming.trade_id.is_empty()
                || incoming.trade_id.starts_with("ack:")
                || incoming.order_id.as_deref() != Some(oid)
            {
                return Err(Error::msg("invalid reconciliation trade identity"));
            }
            let existing: Option<Value> = sqlx::query_scalar(
                "SELECT raw FROM fills WHERE leg_id=$1 AND trade_id=$2 AND third_order_id=$3",
            )
            .bind(leg.id)
            .bind(&incoming.trade_id)
            .bind(oid)
            .fetch_optional(&mut *tx)
            .await?;
            let merged = match existing
                .as_ref()
                .and_then(|raw| raw.get("reconciliation_v1"))
            {
                Some(previous) => {
                    merge_observation(&serde_json::from_value(previous.clone())?, incoming)?
                }
                None => incoming.clone(),
            };
            let accounting = if merged.finality == crate::platforms::FillFinality::Confirmed {
                crate::reconcile::accounting_fee(&current.platform, &merged)?.map(
                    |(fee, source)| serde_json::json!({"fee":fee,"source":source,"currency":"USD"}),
                )
            } else {
                None
            };
            let raw = serde_json::json!({"reconciliation_v1": merged,"accounting":accounting});
            sqlx::query(
                "INSERT INTO fills(leg_id,third_order_id,trade_id,shares,price,fee,fee_rate_bps,raw)
                 VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                 ON CONFLICT(leg_id,trade_id,third_order_id) DO UPDATE SET
                    shares=EXCLUDED.shares,price=EXCLUDED.price,fee=EXCLUDED.fee,
                    fee_rate_bps=EXCLUDED.fee_rate_bps,raw=EXCLUDED.raw,updated_at=NOW()",
            ).bind(leg.id).bind(oid).bind(&merged.trade_id).bind(merged.shares).bind(merged.price)
                .bind(merged.fee).bind(merged.fee_rate_bps).bind(raw).execute(&mut *tx).await?;
        }
        // 老版本占位／未确认费来源不能通过直接 SUM 混入新账务。
        let stored: Vec<Value> = sqlx::query_scalar(
            "SELECT raw->'reconciliation_v1' FROM fills
             WHERE leg_id=$1 AND third_order_id=$2 AND raw ? 'reconciliation_v1'
               AND trade_id NOT LIKE 'ack:%' ORDER BY trade_id",
        )
        .bind(leg.id)
        .bind(oid)
        .fetch_all(&mut *tx)
        .await?;
        let fills: Vec<TradeFill> = stored
            .into_iter()
            .map(serde_json::from_value)
            .collect::<std::result::Result<_, _>>()?;
        let mut effective = evidence.clone();
        // 调用者提供的约束不具权威性，避免遗漏字段或旧快照削弱已持久化事实。
        effective.pm_order_constraints = leg_pm_order_constraints(&current, Some(&evidence.poll))?;
        let resolution = resolve_leg(&current.platform, &fills, &effective)?;
        let mut info = current
            .last_order_info
            .unwrap_or_else(|| serde_json::json!({}));
        if let Some(constraints) = &effective.pm_order_constraints {
            info["pm_order_constraints"] = serde_json::to_value(constraints)?;
        }
        info["fill_progress"] = progress.clone();
        info["fill_evidence"] = serde_json::to_value(&effective)?;
        match &resolution {
            LegResolution::Pending(reason) => {
                info["waiting_reason"] = serde_json::json!(reason);
                sqlx::query(
                    "UPDATE legs SET last_order_info=$2,updated_at=clock_timestamp() WHERE id=$1",
                )
                .bind(leg.id)
                .bind(info)
                .execute(&mut *tx)
                .await?;
            }
            LegResolution::Terminal {
                status,
                shares,
                price,
                fee,
                fee_sources,
            } => {
                info["fee_sources"] = serde_json::json!(fee_sources);
                info["waiting_reason"] = Value::Null;
                sqlx::query(
                    "UPDATE legs SET status=$2,actual_shares=$3,actual_price=$4,actual_fee=$5,
                     last_order_info=$6,updated_at=clock_timestamp() WHERE id=$1",
                )
                .bind(leg.id)
                .bind(status)
                .bind(shares)
                .bind(price)
                .bind(fee)
                .bind(info)
                .execute(&mut *tx)
                .await?;
                refresh_parent_in_tx(&mut tx, parent_id).await?;
            }
        }
        tx.commit().await?;
        Ok(resolution)
    }

    pub async fn complete_orders(&self) -> Result<()> {
        let ids: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM arb_orders WHERE status='actived' ORDER BY id")
                .fetch_all(&self.pool)
                .await?;
        for id in ids {
            let mut tx = self.pool.begin().await?;
            sqlx::query("SELECT id FROM arb_orders WHERE id=$1 FOR UPDATE")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            refresh_parent_in_tx(&mut tx, id).await?;
            tx.commit().await?;
        }
        Ok(())
    }

    pub async fn open_legs(&self) -> Result<Vec<LegRow>> {
        let rows = sqlx::query_as::<_, LegRow>(
            "SELECT id, order_id, platform, token_id, label, side, intent,
                    funder_address, wallet_address, service, req_price, req_shares,
                    client_order_id, third_order_id, status, submitted_at, last_order_info, updated_at
             FROM legs
             WHERE status IN ('pending','unknown','actived')
             ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// 活跃持仓与 `settlement_pending` 使用各自的游标和节奏，因此按状态分别取批次，
    /// 避免等待态订单占用活跃订单的扫描配额。
    async fn scan_orders_by_position_status(
        &self,
        position_status: &str,
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
                   AND position_status = $1
                   AND settled_at IS NULL
                   AND id > $2
                 ORDER BY id
                 LIMIT $3",
            )
            .bind(position_status)
            .bind(after_id)
            .bind(limit)
        };
        let mut rows = query(after_id).fetch_all(&self.pool).await?;
        if rows.is_empty() && after_id > 0 {
            rows = query(0).fetch_all(&self.pool).await?;
        }
        Ok(rows)
    }

    pub async fn completed_unbalanced_orders(
        &self,
        after_id: i64,
        limit: usize,
    ) -> Result<Vec<ArbOrderRow>> {
        self.scan_orders_by_position_status("watching", after_id, limit)
            .await
    }

    pub async fn settlement_pending_orders(
        &self,
        after_id: i64,
        limit: usize,
    ) -> Result<Vec<ArbOrderRow>> {
        self.scan_orders_by_position_status("settlement_pending", after_id, limit)
            .await
    }

    pub async fn order_topic_key(&self, order_id: i64) -> Result<TopicKey> {
        let (event_id, unified_index): (Uuid, i32) =
            sqlx::query_as("SELECT event_id, unified_index FROM arb_orders WHERE id = $1")
                .bind(order_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(TopicKey::new(event_id, unified_index))
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

    /// 诊断用：返回 `position_status`、是否已结算、以及当前持有的 lifecycle 动作。
    /// 行不存在时返回 `None`，用于区分"被并发终态化"和"行已被删除"。
    pub async fn position_state(
        &self,
        order_id: i64,
    ) -> Result<Option<(String, bool, Option<String>)>> {
        let row: Option<(String, bool, Option<String>)> = sqlx::query_as(
            "SELECT position_status, settled_at IS NOT NULL, lifecycle_action
             FROM arb_orders WHERE id = $1",
        )
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
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

    /// 在同一事务里确认净仓位为零、刷新最终账务、完成再平衡并关闭持仓。
    /// 父单行锁阻止新的 lifecycle claim；腿行锁避免回填在账务快照期间改写成交值。
    pub async fn finalize_closed_position(
        &self,
        order_id: i64,
    ) -> Result<Option<(Decimal, Decimal, Decimal)>> {
        let mut tx = self.pool.begin().await?;
        let eligible: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM arb_orders
             WHERE id = $1 AND settled_at IS NULL AND position_status = 'watching'
               AND lifecycle_action IS NULL
               AND lifecycle_claim_id IS NULL
               AND lifecycle_claimed_at IS NULL
             FOR UPDATE",
        )
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await?;
        if eligible.is_none() {
            tx.rollback().await?;
            return Ok(None);
        }

        let rows: Vec<TerminalLegRow> = sqlx::query_as(
            "SELECT status, side, platform, label, actual_shares, actual_price, actual_fee
                 FROM legs WHERE order_id = $1 FOR UPDATE",
        )
        .bind(order_id)
        .fetch_all(&mut *tx)
        .await?;
        if rows
            .iter()
            .any(|(status, ..)| matches!(status.as_str(), "pending" | "unknown" | "actived"))
        {
            tx.rollback().await?;
            return Ok(None);
        }
        let mut positions = Positions::new();
        let mut actual_rows = Vec::new();
        for (status, side, platform, label, shares, price, fee) in rows {
            let shares = shares.unwrap_or(Decimal::ZERO);
            if matches!(
                status.as_str(),
                "matched" | "completed" | "cancelled" | "failed"
            ) {
                let slot = positions
                    .entry(platform.clone())
                    .or_default()
                    .entry(label.to_ascii_lowercase())
                    .or_insert(Decimal::ZERO);
                if side.eq_ignore_ascii_case("SELL") {
                    *slot -= shares;
                } else {
                    *slot += shares;
                }
            }
            if matches!(status.as_str(), "matched" | "completed") {
                actual_rows.push((
                    side,
                    platform,
                    label,
                    shares,
                    price.unwrap_or(Decimal::ZERO),
                    fee.unwrap_or(Decimal::ZERO),
                ));
            }
        }
        if positions
            .values()
            .any(|labels| labels.values().any(|qty| !qty.is_zero()))
        {
            tx.rollback().await?;
            return Ok(None);
        }
        let (cost, rev, profit) = compute_actuals(&actual_rows);
        let updated: Option<i64> = sqlx::query_scalar(
            "UPDATE arb_orders
             SET actual_cost = $2, actual_rev = $3, actual_profit = $4,
                 rebalance_status = 'completed', rebalanced_at = COALESCE(rebalanced_at, NOW()),
                 position_status = 'closed', updated_at = NOW()
             WHERE id = $1 AND settled_at IS NULL AND position_status = 'watching'
               AND lifecycle_action IS NULL
               AND lifecycle_claim_id IS NULL
               AND lifecycle_claimed_at IS NULL
             RETURNING id",
        )
        .bind(order_id)
        .bind(cost)
        .bind(rev)
        .bind(profit)
        .fetch_optional(&mut *tx)
        .await?;
        if updated.is_none() {
            tx.rollback().await?;
            return Ok(None);
        }
        tx.commit().await?;
        Ok(Some((cost, rev, profit)))
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

    /// 提交不明和已受理但未确认的腿均按首次提交时间告警。
    /// 已有部分成交也不能掩盖剩余确认故障；重试不会推迟超时或清零成交。
    pub async fn stale_unknown_legs(&self, timeout: Duration) -> Result<Vec<ClosedLegRef>> {
        let secs = timeout.as_secs() as i64;
        let rows = sqlx::query_as::<_, ClosedLegRef>(
            "SELECT id, order_id, platform
             FROM legs
             WHERE status IN ('unknown','actived')
               AND COALESCE(submitted_at,created_at) < NOW() - make_interval(secs => $1)
             ORDER BY id",
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
             WHERE status IN ('unknown','actived')
               AND COALESCE(submitted_at,created_at) < NOW() - make_interval(secs => $1)",
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

fn leg_open(status: &str) -> bool {
    matches!(status, "pending" | "unknown" | "actived")
}

fn leg_pm_order_constraints(
    leg: &LegRow,
    poll: Option<&OrderPoll>,
) -> Result<Option<PmOrderConstraints>> {
    if leg.platform != POLYMARKET {
        return Ok(None);
    }
    let Some(oid) = leg
        .third_order_id
        .as_deref()
        .or(leg.client_order_id.as_deref())
    else {
        return Ok(None);
    };
    let funder = leg
        .funder_address
        .as_deref()
        .ok_or_else(|| Error::msg("missing polymarket funder"))?;
    pm_order_constraints(
        leg.last_order_info.as_ref(),
        oid,
        &leg.token_id,
        funder,
        poll,
    )
    .map(Some)
}

async fn lock_leg_parent(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    leg_id: i64,
) -> Result<(i64, bool)> {
    let row: (i64, String, bool) = sqlx::query_as(
        "SELECT o.id,o.position_status,o.settled_at IS NOT NULL FROM arb_orders o
         WHERE o.id=(SELECT order_id FROM legs WHERE id=$1) FOR UPDATE",
    )
    .bind(leg_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok((row.0, !row.2 && row.1 == "watching"))
}

async fn read_locked_leg(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    leg_id: i64,
) -> Result<LegRow> {
    Ok(sqlx::query_as::<_, LegRow>(
        "SELECT id,order_id,platform,token_id,label,side,intent,funder_address,wallet_address,
         service,req_price,req_shares,client_order_id,third_order_id,status,submitted_at,
         last_order_info,updated_at FROM legs WHERE id=$1 FOR UPDATE",
    )
    .bind(leg_id)
    .fetch_one(&mut **tx)
    .await?)
}

async fn refresh_parent_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_id: i64,
) -> Result<()> {
    let rows: Vec<TerminalLegRow> = sqlx::query_as(
        "SELECT status,side,platform,label,actual_shares,actual_price,actual_fee
         FROM legs WHERE order_id=$1 ORDER BY id FOR UPDATE",
    )
    .bind(order_id)
    .fetch_all(&mut **tx)
    .await?;
    if rows.is_empty() || rows.iter().any(|(status, ..)| leg_open(status)) {
        return Ok(());
    }
    let positive = rows.iter().any(|(status, _, _, _, shares, _, _)| {
        matches!(status.as_str(), "matched" | "completed")
            && shares.is_some_and(|qty| qty > Decimal::ZERO)
    });
    let actual_rows: Vec<_> = rows
        .into_iter()
        .filter(|(status, ..)| matches!(status.as_str(), "matched" | "completed"))
        .map(|(_, side, platform, label, shares, price, fee)| {
            (
                side,
                platform,
                label,
                shares.unwrap_or_default(),
                price.unwrap_or_default(),
                fee.unwrap_or_default(),
            )
        })
        .collect();
    let (cost, rev, profit) = compute_actuals(&actual_rows);
    sqlx::query(
        "UPDATE arb_orders SET actual_cost=$2,actual_rev=$3,actual_profit=$4,
             status=CASE WHEN status='actived' THEN $5 ELSE status END,
             completed_at=CASE WHEN status='actived' THEN NOW() ELSE completed_at END,
             rebalance_status=CASE WHEN status='actived' AND $5='cancelled' THEN 'completed' ELSE rebalance_status END,
             rebalanced_at=CASE WHEN status='actived' AND $5='cancelled' THEN NOW() ELSE rebalanced_at END,
             updated_at=NOW()
         WHERE id=$1 AND settled_at IS NULL AND position_status='watching'",
    ).bind(order_id).bind(cost).bind(rev).bind(profit)
        .bind(if positive { "completed" } else { "cancelled" })
        .execute(&mut **tx).await?;
    Ok(())
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
