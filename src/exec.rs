use crate::book::{BookStore, DirtyCoalescer};
use crate::calc::{best_plan, ArbLimits, ArbPlan, FeeContext};
use crate::config::{Config, OUTCOME, POLYMARKET};
use crate::discovery::load_active_topics;
use crate::domain::{Topic, TopicKey};
use crate::error::{Error, Result};
use crate::hedge::{needs_rebalance, plan_hedge, HedgeSide};
use crate::platforms::outcome::OutcomeVenue;
use crate::platforms::polymarket::PolymarketVenue;
use crate::platforms::{MarketOrderRequest, OrderSide, SubmitResult, TradeFill};
use crate::store::Store;
use rust_decimal::Decimal;
use serde_json::json;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex, RwLock};

pub struct Engine {
    pub cfg: Config,
    pub store: Store,
    pub common: PgPool,
    pub books: Arc<Mutex<BookStore>>,
    pub dirty: Arc<Mutex<DirtyCoalescer>>,
    pub topics: Arc<RwLock<HashMap<TopicKey, Topic>>>,
    pub pm: PolymarketVenue,
    pub outcome: OutcomeVenue,
    pub pm_sub_tx: mpsc::Sender<Vec<String>>,
    pub out_sub_tx: mpsc::Sender<Vec<String>>,
}

impl Engine {
    pub async fn refresh_discovery(&self) -> Result<()> {
        let topics = load_active_topics(&self.common, &self.cfg.enabled_platforms).await?;
        let mut map = HashMap::new();
        {
            let mut books = self.books.lock().await;
            books.clear_topic_index();
            for topic in &topics {
                for token in &topic.tokens {
                    books.index_token(&token.platform, &token.token_id, topic.key);
                }
                map.insert(topic.key, topic.clone());
            }
        }
        let (pm_tokens, out_tokens) = self.books.lock().await.desired_tokens();
        let _ = self.pm_sub_tx.send(pm_tokens.clone()).await;
        let _ = self.out_sub_tx.send(out_tokens.clone()).await;
        *self.topics.write().await = map;
        tracing::info!(topics = topics.len(), pm = pm_tokens.len(), outcome = out_tokens.len(), "discovery refreshed");
        Ok(())
    }

    pub async fn handle_topic(&self, topic_key: TopicKey) -> Result<()> {
        let maybe_again = {
            let mut dirty = self.dirty.lock().await;
            dirty.mark(topic_key)
        };
        if maybe_again.is_none() {
            return Ok(());
        }
        let result = self.evaluate_topic(topic_key).await;
        if let Some(again) = self.dirty.lock().await.finish(topic_key) {
            let _ = again;
            return Box::pin(self.handle_topic(topic_key)).await;
        }
        result
    }

    async fn evaluate_topic(&self, topic_key: TopicKey) -> Result<()> {
        let topic = {
            let topics = self.topics.read().await;
            topics.get(&topic_key).cloned()
        };
        let Some(topic) = topic else {
            return Ok(());
        };
        if self.store.has_active_topic(topic_key).await? {
            return Ok(());
        }
        let pm_bps = self
            .store
            .polymarket_prior_bps("", self.cfg.polymarket_fee_bps_prior)
            .await?;
        let fees = FeeContext {
            polymarket_fee_bps: pm_bps,
            outcome_taker_rate: self.cfg.outcome_taker_fee_rate,
            extra_cost_multiplier: self.cfg.extra_cost_multiplier,
        };
        let limits = ArbLimits {
            cost_limit: self.cfg.arb_cost_limit,
            min_profit: self.cfg.arb_min_profit,
            min_apr: self.cfg.arb_min_apr,
            days: crate::calc::days_until(topic.end_date),
        };
        let plan = {
            let books = self.books.lock().await;
            best_plan(&topic, &books, &fees, &limits, Instant::now(), self.cfg.book_stale)
        };
        let Some(plan) = plan else {
            return Ok(());
        };
        tracing::info!(
            topic = %topic.key.as_str(),
            profit = %plan.profit,
            cost = %plan.total_cost,
            roi = %plan.roi,
            apr = %plan.apr,
            "arb opportunity"
        );
        if !self.cfg.enable_buy {
            return Ok(());
        }
        self.execute_plan(&topic, plan).await
    }

    async fn execute_plan(&self, topic: &Topic, plan: ArbPlan) -> Result<()> {
        let required = plan.pm.cost + plan.pm.fee;
        let funder = self.select_funder(required).await?;
        let fills = json!([
            {"platform": POLYMARKET, "token": plan.pm.token_id, "label": plan.pm.label, "shares": plan.pm.shares, "price": plan.pm.cap_price},
            {"platform": OUTCOME, "token": plan.outcome.token_id, "label": plan.outcome.label, "shares": plan.outcome.shares, "price": plan.outcome.cap_price}
        ]);
        let order_id = self
            .store
            .insert_order(
                topic.key,
                &topic.title,
                &topic.market_title,
                topic.end_date,
                plan.net_shares,
                plan.profit,
                plan.total_cost,
                &fills,
            )
            .await?;
        self.store.mark_order_status(order_id, "actived").await?;
        let pm_token = topic.token(POLYMARKET, &plan.pm.label);
        let out_token = topic.token(OUTCOME, &plan.outcome.label);
        let pm_req = MarketOrderRequest {
            token_id: plan.pm.token_id.clone(),
            shares: plan.pm.shares,
            cap_price: plan.pm.cap_price,
            side: OrderSide::Buy,
            neg_risk: pm_token.and_then(|t| t.neg_risk),
            tick_size: None,
            asset_id: None,
            funder_address: Some(funder.clone()),
        };
        let out_req = MarketOrderRequest {
            token_id: plan.outcome.token_id.clone(),
            shares: plan.outcome.shares,
            cap_price: plan.outcome.cap_price,
            side: OrderSide::Buy,
            neg_risk: None,
            tick_size: None,
            asset_id: out_token.and_then(|t| t.asset_id),
            funder_address: None,
        };
        let pm_leg = self
            .store
            .insert_leg(
                order_id,
                POLYMARKET,
                &plan.pm.token_id,
                &plan.pm.label,
                "BUY",
                "arb_buy",
                Some(&funder),
                Some(&funder),
                plan.pm.cap_price,
                plan.pm.shares,
                plan.pm.fee,
                None,
            )
            .await?;
        let out_leg = self
            .store
            .insert_leg(
                order_id,
                OUTCOME,
                &plan.outcome.token_id,
                &plan.outcome.label,
                "BUY",
                "arb_buy",
                None,
                self.outcome.account_address(),
                plan.outcome.cap_price,
                plan.outcome.shares,
                plan.outcome.fee,
                None,
            )
            .await?;
        let (pm_res, out_res) = tokio::join!(
            self.submit_pm(pm_leg, &funder, &pm_req),
            self.submit_outcome(out_leg, &out_req)
        );
        if let Err(err) = pm_res {
            tracing::error!(error = %err, "polymarket submit failed");
        }
        if let Err(err) = out_res {
            tracing::error!(error = %err, "outcome submit failed");
        }
        Ok(())
    }

    async fn select_funder(&self, required: Decimal) -> Result<String> {
        let mut current = self
            .pm
            .next_funder()
            .await
            .ok_or_else(|| Error::msg("no polymarket funder configured"))?;
        for _ in 0..3 {
            match self.pm.balance(&current).await {
                Ok(bal) if bal >= required => return Ok(current),
                Ok(bal) => tracing::warn!(funder = %current, %bal, %required, "polymarket balance low"),
                Err(err) => tracing::warn!(funder = %current, error = %err, "balance check failed"),
            }
            current = self
                .pm
                .rotate_from(&current)
                .await
                .ok_or_else(|| Error::msg("unable to rotate polymarket funder"))?;
        }
        Err(Error::msg("no polymarket funder with sufficient balance"))
    }

    async fn submit_pm(
        &self,
        leg_id: i64,
        funder: &str,
        req: &MarketOrderRequest,
    ) -> Result<()> {
        let prepared = self.pm.prepare_market_order(funder, req).await?;
        self.store
            .insert_envelope(leg_id, &prepared.order_hash, &prepared.envelope)
            .await?;
        let result = self.pm.post_prepared(&prepared).await?;
        persist_submit(&self.store, leg_id, result).await
    }

    async fn submit_outcome(&self, leg_id: i64, req: &MarketOrderRequest) -> Result<()> {
        let prepared = self.outcome.prepare_market_order(req)?;
        self.store
            .insert_envelope(leg_id, &prepared.order_hash, &prepared.envelope)
            .await?;
        let result = self.outcome.post_prepared(prepared).await?;
        persist_submit(&self.store, leg_id, result).await
    }

    pub async fn reconcile(&self) -> Result<()> {
        let legs = self.store.open_legs().await?;
        for leg in legs {
            if let Err(err) = self.reconcile_leg(&leg).await {
                tracing::warn!(leg_id = leg.id, error = %err, "reconcile failed");
            }
        }
        mark_orders_complete(&self.store).await?;
        Ok(())
    }

    async fn reconcile_leg(&self, leg: &crate::store::LegRow) -> Result<()> {
        if leg.platform == POLYMARKET {
            self.reconcile_pm(leg).await
        } else {
            self.reconcile_outcome(leg).await
        }
    }

    async fn reconcile_pm(&self, leg: &crate::store::LegRow) -> Result<()> {
        let funder = leg
            .funder_address
            .as_deref()
            .ok_or_else(|| Error::msg("missing funder"))?;
        let mut order_found = false;
        if let Some(oid) = &leg.third_order_id {
            let poll = self.pm.poll_order(funder, oid).await?;
            order_found = poll.found;
            if poll.found {
                let _ = self.store.update_leg_submitted(leg.id, "actived", Some(oid), &poll.raw).await;
            }
        }
        let trades = self.pm.poll_trades(funder, &leg.token_id).await?;
        let matched = filter_trades(&trades, leg.third_order_id.as_deref(), leg.client_order_id.as_deref());
        if matched.is_empty() {
            if !order_found && matches!(leg.status.as_str(), "unknown" | "pending") {
                // FAK 404 is not proof of no fill; wait until a later pass still has no trades.
                return Ok(());
            }
            return Ok(());
        }
        apply_fills(&self.store, &self.cfg, POLYMARKET, &leg.token_id, leg.id, &matched).await
    }

    async fn reconcile_outcome(&self, leg: &crate::store::LegRow) -> Result<()> {
        if let Some(oid) = &leg.third_order_id {
            let _ = self.outcome.poll_order(oid, &leg.token_id).await;
        }
        let fills = self.outcome.poll_fills(Some(&leg.token_id)).await?;
        let matched = filter_trades(&fills, leg.third_order_id.as_deref(), leg.client_order_id.as_deref());
        if matched.is_empty() {
            return Ok(());
        }
        apply_fills(&self.store, &self.cfg, OUTCOME, &leg.token_id, leg.id, &matched).await
    }

    pub async fn hedge_once(&self) -> Result<()> {
        let orders = self.store.completed_unbalanced_orders().await?;
        for order in orders {
            let key = TopicKey::new(order.event_id, order.unified_index);
            let topic = self.topics.read().await.get(&key).cloned();
            let Some(topic) = topic else {
                continue;
            };
            let positions = self.store.positions_for_order(order.id).await?;
            if !needs_rebalance(&positions, &topic.labels(), self.cfg.min_rebalance_qty) {
                self.store.mark_rebalance(order.id, "completed").await?;
                continue;
            }
            let mut balances = HashMap::new();
            if let Some(funder) = self.pm.next_funder().await {
                if let Ok(bal) = self.pm.balance(&funder).await {
                    balances.insert(POLYMARKET.to_string(), bal);
                }
            }
            if let Ok(bal) = self.outcome.user_state().await {
                balances.insert(OUTCOME.to_string(), bal);
            }
            let pm_bps = self
                .store
                .polymarket_prior_bps("", self.cfg.polymarket_fee_bps_prior)
                .await?;
            let fees = FeeContext {
                polymarket_fee_bps: pm_bps,
                outcome_taker_rate: self.cfg.outcome_taker_fee_rate,
                extra_cost_multiplier: self.cfg.extra_cost_multiplier,
            };
            let actions = {
                let books = self.books.lock().await;
                plan_hedge(
                    &topic,
                    &positions,
                    &books,
                    &balances,
                    &fees,
                    self.cfg.min_rebalance_qty,
                    Instant::now(),
                    self.cfg.book_stale,
                )
            };
            if actions.is_empty() {
                continue;
            }
            self.store.mark_rebalance(order.id, "actived").await?;
            for action in actions {
                if let Err(err) = self.execute_hedge(order.id, &action).await {
                    tracing::error!(error = %err, "hedge submit failed");
                }
            }
        }
        Ok(())
    }

    async fn execute_hedge(&self, order_id: i64, action: &crate::hedge::HedgeAction) -> Result<()> {
        let side = match action.side {
            HedgeSide::Buy => OrderSide::Buy,
            HedgeSide::Sell => OrderSide::Sell,
        };
        if action.platform == POLYMARKET {
            let funder = self
                .select_funder(action.cap_price * action.shares)
                .await?;
            let req = MarketOrderRequest {
                token_id: action.token_id.clone(),
                shares: action.shares,
                cap_price: action.cap_price,
                side,
                neg_risk: None,
                tick_size: None,
                asset_id: None,
                funder_address: Some(funder.clone()),
            };
            let leg_id = self
                .store
                .insert_leg(
                    order_id,
                    POLYMARKET,
                    &action.token_id,
                    &action.label,
                    side.as_str(),
                    "rebalance",
                    Some(&funder),
                    Some(&funder),
                    action.cap_price,
                    action.shares,
                    Decimal::ZERO,
                    None,
                )
                .await?;
            self.submit_pm(leg_id, &funder, &req).await
        } else {
            let req = MarketOrderRequest {
                token_id: action.token_id.clone(),
                shares: action.shares,
                cap_price: action.cap_price,
                side,
                neg_risk: None,
                tick_size: None,
                asset_id: crate::domain::parse_side_coin(&action.token_id)
                    .map(|(id, side)| crate::domain::side_asset_id(id, side)),
                funder_address: None,
            };
            let leg_id = self
                .store
                .insert_leg(
                    order_id,
                    OUTCOME,
                    &action.token_id,
                    &action.label,
                    side.as_str(),
                    "rebalance",
                    None,
                    self.outcome.account_address(),
                    action.cap_price,
                    action.shares,
                    Decimal::ZERO,
                    None,
                )
                .await?;
            self.submit_outcome(leg_id, &req).await
        }
    }
}

async fn persist_submit(store: &Store, leg_id: i64, result: SubmitResult) -> Result<()> {
    match result {
        SubmitResult::Ack {
            order_id,
            order_hash: _,
            envelope,
            ..
        } => {
            store
                .update_leg_submitted(leg_id, "actived", Some(&order_id), &envelope)
                .await?;
        }
        SubmitResult::NoMatch {
            order_hash: _,
            envelope,
            message,
        } => {
            store
                .update_leg_submitted(leg_id, "cancelled", None, &json!({"message": message, "envelope": envelope}))
                .await?;
        }
        SubmitResult::Unknown {
            order_id,
            order_hash: _,
            envelope,
            message,
        } => {
            store
                .update_leg_submitted(
                    leg_id,
                    "unknown",
                    order_id.as_deref(),
                    &json!({"message": message, "envelope": envelope}),
                )
                .await?;
        }
    }
    Ok(())
}

fn filter_trades<'a>(
    trades: &'a [TradeFill],
    order_id: Option<&str>,
    client_id: Option<&str>,
) -> Vec<&'a TradeFill> {
    trades
        .iter()
        .filter(|t| {
            if let Some(oid) = order_id {
                if t.order_id.as_deref() == Some(oid) || t.raw.to_string().contains(oid) {
                    return true;
                }
            }
            if let Some(cid) = client_id {
                if t.raw.to_string().contains(cid) {
                    return true;
                }
            }
            false
        })
        .collect()
}

async fn apply_fills(
    store: &Store,
    cfg: &Config,
    platform: &str,
    token_id: &str,
    leg_id: i64,
    fills: &[&TradeFill],
) -> Result<()> {
    let mut shares = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    let mut last_oid: Option<String> = None;
    for fill in fills {
        store
            .upsert_fill(
                leg_id,
                fill.order_id.as_deref(),
                if fill.trade_id.is_empty() {
                    None
                } else {
                    Some(&fill.trade_id)
                },
                fill.shares,
                fill.price,
                fill.fee,
                fill.fee_rate_bps,
                &fill.raw,
            )
            .await?;
        shares += fill.shares;
        notional += fill.shares * fill.price;
        fee += fill.fee;
        last_oid = fill.order_id.clone();
        if notional > Decimal::ZERO && fill.fee > Decimal::ZERO {
            let sample = (fill.fee / notional) * Decimal::from(10_000);
            let _ = store
                .update_fee_ema(platform, token_id, sample, cfg.fee_ema_alpha)
                .await;
        }
    }
    let avg = if shares > Decimal::ZERO {
        notional / shares
    } else {
        Decimal::ZERO
    };
    store
        .update_leg_fill(
            leg_id,
            "matched",
            last_oid.as_deref(),
            avg,
            shares,
            fee,
            &json!({"fills": fills.len()}),
        )
        .await?;
    Ok(())
}

pub async fn mark_orders_complete(store: &Store) -> Result<()> {
    sqlx::query(
        "UPDATE arb_orders o SET status = 'completed', updated_at = NOW(), completed_at = NOW()
         WHERE o.status = 'actived'
           AND NOT EXISTS (
             SELECT 1 FROM legs l
             WHERE l.order_id = o.id AND l.status IN ('pending','unknown','actived')
           )",
    )
    .execute(&store.pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::FromStr;
    use serde_json::json;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn matches_trade_by_order_id_only() {
        let trades = vec![
            TradeFill {
                trade_id: "t1".into(),
                order_id: Some("oid-1".into()),
                shares: d("3"),
                price: d("0.4"),
                fee: d("0.01"),
                fee_rate_bps: Some(d("25")),
                raw: json!({"oid":"oid-1"}),
            },
            TradeFill {
                trade_id: "t2".into(),
                order_id: Some("oid-2".into()),
                shares: d("9"),
                price: d("0.5"),
                fee: d("0.02"),
                fee_rate_bps: None,
                raw: json!({"oid":"oid-2"}),
            },
        ];
        let matched = filter_trades(&trades, Some("oid-1"), None);
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].trade_id, "t1");
        assert!(filter_trades(&trades, None, None).is_empty());
    }
}
