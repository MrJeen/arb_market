use crate::book::{BookStore, DirtyCoalescer};
use crate::calc::{best_plan, ArbLimits, ArbPlan, FeeContext};
use crate::config::{Config, OUTCOME, POLYMARKET};
use crate::discovery::load_active_topics;
use crate::domain::{Topic, TopicKey};
use crate::error::{Error, Result};
use crate::hedge::{needs_rebalance, plan_hedge, HedgeSide};
use crate::notify::{self, NatsNotifier, PlaceNotice, PlaceResult};
use crate::platforms::outcome::OutcomeVenue;
use crate::platforms::polymarket::PolymarketVenue;
use crate::platforms::{ioc_fill, pm_fak_fill, MarketOrderRequest, OrderSide, SubmitResult, TradeFill};
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
    pub notify: Option<NatsNotifier>,
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

    fn fee_context(&self, topic: &Topic) -> FeeContext {
        let rate = topic.polymarket_fee_rate().unwrap_or_else(|| {
            self.cfg.polymarket_fee_bps_prior / Decimal::from(10_000)
        });
        FeeContext {
            polymarket_fee_rate: rate,
            outcome_taker_rate: self.cfg.outcome_taker_fee_rate,
            extra_cost_multiplier: self.cfg.extra_cost_multiplier,
        }
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
        if self.store.count_stale_unknown_legs(self.cfg.unknown_leg_timeout).await? > 0 {
            tracing::error!("stale unknown legs present; skip new arb");
            return Ok(());
        }
        if self.store.count_active_orders().await? >= self.cfg.max_active_orders as i64 {
            tracing::warn!(limit = self.cfg.max_active_orders, "max active orders reached");
            return Ok(());
        }
        self.ensure_topic_pm_ticks(&topic).await;
        let fees = self.fee_context(&topic);
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
        let order_id = match self
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
            .await
        {
            Ok(id) => id,
            Err(Error::Sqlx(sqlx::Error::Database(db))) if db.code().as_deref() == Some("23505") => {
                tracing::info!(topic = %topic.key.as_str(), "active topic already claimed");
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        self.store.mark_order_status(order_id, "actived").await?;
        let pm_token = topic.token(POLYMARKET, &plan.pm.label);
        let out_token = topic.token(OUTCOME, &plan.outcome.label);
        let pm_tick = self.ensure_pm_tick(&plan.pm.token_id).await;
        let pm_req = MarketOrderRequest {
            token_id: plan.pm.token_id.clone(),
            shares: plan.pm.shares,
            cap_price: plan.pm.cap_price,
            side: OrderSide::Buy,
            neg_risk: pm_token.and_then(|t| t.neg_risk),
            tick_size: pm_tick,
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
                self.polymarket_service(&funder),
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
                None,
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
        if let Err(err) = &pm_res {
            tracing::error!(error = %err, "polymarket submit failed");
        }
        if let Err(err) = &out_res {
            tracing::error!(error = %err, "outcome submit failed");
        }
        self.notify_place(order_id, topic, &funder, &plan, pm_res, out_res);
        Ok(())
    }

    fn notify_place(
        &self,
        order_id: i64,
        topic: &Topic,
        funder: &str,
        plan: &ArbPlan,
        pm_res: Result<SubmitResult>,
        out_res: Result<SubmitResult>,
    ) {
        let Some(notify) = &self.notify else {
            return;
        };
        let pm_platform = self.polymarket_platform_label(funder);
        let pm = place_result(
            pm_platform.clone(),
            plan.pm.label.clone(),
            plan.pm.token_id.clone(),
            pm_res,
        );
        let outcome = place_result(
            OUTCOME.to_string(),
            plan.outcome.label.clone(),
            plan.outcome.token_id.clone(),
            out_res,
        );
        notify.publish_place(PlaceNotice {
            order_id,
            title: topic.title.clone(),
            platforms: vec![pm_platform, OUTCOME.to_string()],
            results: vec![pm, outcome],
        });
    }

    fn polymarket_service(&self, funder: &str) -> Option<&str> {
        self.cfg
            .polymarket_funders
            .iter()
            .find(|item| item.funder_address.eq_ignore_ascii_case(funder))
            .and_then(|item| item.service.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    fn polymarket_platform_label(&self, funder: &str) -> String {
        notify::format_platform_label(POLYMARKET, self.polymarket_service(funder))
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
    ) -> Result<SubmitResult> {
        let prepared = self.pm.prepare_market_order(funder, req).await?;
        self.store
            .insert_envelope(leg_id, &prepared.order_hash, &prepared.envelope)
            .await?;
        let result = self.pm.post_prepared(&prepared).await?;
        persist_submit(&self.store, leg_id, POLYMARKET, req.side, &result).await?;
        Ok(result)
    }

    async fn submit_outcome(&self, leg_id: i64, req: &MarketOrderRequest) -> Result<SubmitResult> {
        let prepared = self.outcome.prepare_market_order(req)?;
        self.store
            .insert_envelope(leg_id, &prepared.order_hash, &prepared.envelope)
            .await?;
        let result = self.outcome.post_prepared(prepared).await?;
        persist_submit(&self.store, leg_id, OUTCOME, req.side, &result).await?;
        Ok(result)
    }

    pub async fn reconcile(&self) -> Result<()> {
        let expired = self
            .store
            .fail_stale_pending_without_envelope(self.cfg.pending_leg_timeout)
            .await?;
        if expired > 0 {
            tracing::warn!(expired, "failed stale pending legs without envelope");
        }
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
        let mut size_matched = None;
        let mut poll_price = None;
        if let Some(oid) = &leg.third_order_id {
            let poll = self.pm.poll_order(funder, oid).await?;
            order_found = poll.found;
            size_matched = poll.shares.filter(|v| *v > Decimal::ZERO);
            poll_price = poll.price.filter(|v| *v > Decimal::ZERO);
            if poll.found && size_matched.is_none() {
                let _ = self.store.update_leg_submitted(leg.id, "actived", Some(oid), &poll.raw).await;
            }
        }
        let trades = self.pm.poll_trades(funder, &leg.token_id).await?;
        let matched = filter_trades(&trades, leg.third_order_id.as_deref(), leg.client_order_id.as_deref());
        upsert_fill_rows(&self.store, leg.id, &matched).await?;
        if let Some(shares) = size_matched {
            let (price, fee) = fill_price_and_fee(&matched, poll_price);
            return close_leg_matched(&self.store, leg.id, leg.third_order_id.as_deref(), shares, price, fee).await;
        }
        if !matched.is_empty() {
            return apply_fills(&self.store, leg.id, &matched).await;
        }
        if !order_found && matches!(leg.status.as_str(), "unknown" | "pending") {
            // FAK 404 is not proof of no fill; wait until a later pass still has no trades.
            return Ok(());
        }
        Ok(())
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
        apply_fills(&self.store, leg.id, &matched).await
    }

    pub async fn hedge_once(&self) -> Result<()> {
        if self.store.count_stale_unknown_legs(self.cfg.unknown_leg_timeout).await? > 0 {
            tracing::error!("stale unknown legs present; skip hedge");
            return Ok(());
        }
        let orders = self.store.completed_unbalanced_orders().await?;
        for order in orders {
            let key = TopicKey::new(order.event_id, order.unified_index);
            let topic = self.topics.read().await.get(&key).cloned();
            let Some(topic) = topic else {
                continue;
            };
            if self.store.has_open_rebalance_legs(order.id).await? {
                tracing::debug!(order_id = order.id, "skip hedge, rebalance legs in flight");
                continue;
            }
            let positions = self.store.positions_for_order(order.id).await?;
            match needs_rebalance(&positions, &topic.labels(), self.cfg.min_rebalance_qty) {
                None => {
                    tracing::warn!(order_id = order.id, "incomplete positions, skip rebalance complete");
                    continue;
                }
                Some(false) => {
                    self.store.mark_rebalance(order.id, "completed").await?;
                    let _ = self.store.refresh_order_actuals(order.id).await;
                    continue;
                }
                Some(true) => {}
            }
            if !self.cfg.enable_buy {
                tracing::info!(order_id = order.id, "hedge skipped, ENABLE_BUY=false");
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
            let fees = self.fee_context(&topic);
            self.ensure_topic_pm_ticks(&topic).await;
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
            if side == OrderSide::Sell {
                let funder = self
                    .store
                    .buy_funder_for_token(order_id, POLYMARKET, &action.token_id)
                    .await?
                    .or(self.pm.next_funder().await)
                    .ok_or_else(|| Error::msg("no polymarket funder for sell"))?;
                match self.pm.token_balance(&funder, &action.token_id).await {
                    Ok(bal) if bal >= action.shares => {}
                    Ok(bal) => {
                        return Err(Error::msg(format!(
                            "polymarket sell skipped, token balance {bal} < {}",
                            action.shares
                        )));
                    }
                    Err(err) => {
                        return Err(Error::msg(format!(
                            "polymarket sell skipped, token balance unavailable: {err}"
                        )));
                    }
                }
                let req = MarketOrderRequest {
                    token_id: action.token_id.clone(),
                    shares: action.shares,
                    cap_price: action.cap_price,
                    side,
                    neg_risk: None,
                    tick_size: self.ensure_pm_tick(&action.token_id).await,
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
                        self.polymarket_service(&funder),
                        action.cap_price,
                        action.shares,
                        Decimal::ZERO,
                        None,
                    )
                    .await?;
                self.submit_pm(leg_id, &funder, &req).await?;
                return Ok(());
            }
            let funder = self
                .select_funder(action.cap_price * action.shares)
                .await?;
            let req = MarketOrderRequest {
                token_id: action.token_id.clone(),
                shares: action.shares,
                cap_price: action.cap_price,
                side,
                neg_risk: None,
                tick_size: self.ensure_pm_tick(&action.token_id).await,
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
                    self.polymarket_service(&funder),
                    action.cap_price,
                    action.shares,
                    Decimal::ZERO,
                    None,
                )
                .await?;
            self.submit_pm(leg_id, &funder, &req).await?;
            Ok(())
        } else {
            if side == OrderSide::Sell {
                match self.outcome.token_balance(&action.token_id).await {
                    Ok(bal) if bal >= action.shares => {}
                    Ok(bal) => {
                        return Err(Error::msg(format!(
                            "outcome sell skipped, token balance {bal} < {}",
                            action.shares
                        )));
                    }
                    Err(err) => {
                        return Err(Error::msg(format!(
                            "outcome sell skipped, token balance unavailable: {err}"
                        )));
                    }
                }
            }
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
                    None,
                    action.cap_price,
                    action.shares,
                    Decimal::ZERO,
                    None,
                )
                .await?;
            self.submit_outcome(leg_id, &req).await?;
            Ok(())
        }
    }

    pub async fn resync_stale_pm_books(&self) -> Result<Vec<TopicKey>> {
        let limit = self.cfg.book_resync_batch.min(500);
        let stale = {
            let books = self.books.lock().await;
            books.stale_pm_tokens(self.cfg.book_stale, Instant::now(), limit)
        };
        if stale.is_empty() {
            tracing::debug!(stale = 0, "polymarket book resync skipped");
            return Ok(Vec::new());
        }
        let started = Instant::now();
        let payloads = match self.pm.rest_books(&stale).await {
            Ok(payloads) => payloads,
            Err(err) => {
                tracing::warn!(
                    stale = stale.len(),
                    error = %err,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "polymarket book resync failed"
                );
                return Err(err);
            }
        };
        let now = Instant::now();
        let (applied, skipped_old, topics) = {
            let mut books = self.books.lock().await;
            let (applied, skipped_old) =
                crate::platforms::polymarket::apply_rest_books(&mut books, &payloads, now);
            let mut topics = Vec::new();
            for token in &applied {
                topics.extend(books.topics_for(POLYMARKET, token));
            }
            (applied, skipped_old, topics)
        };
        tracing::info!(
            stale = stale.len(),
            requested = stale.len(),
            applied = applied.len(),
            skipped_old,
            topics = topics.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "polymarket book resync"
        );
        Ok(topics)
    }

    async fn ensure_topic_pm_ticks(&self, topic: &Topic) {
        for token in &topic.tokens {
            if token.platform == POLYMARKET {
                let _ = self.ensure_pm_tick(&token.token_id).await;
            }
        }
    }

    /// 盘口已有 tick 则直接用；没有才请求一次 `/tick-size` 并写回订单簿。
    async fn ensure_pm_tick(&self, token_id: &str) -> Option<Decimal> {
        {
            let books = self.books.lock().await;
            if let Some(tick) = books.get(POLYMARKET, token_id).and_then(|book| book.tick_size) {
                return Some(tick);
            }
        }
        match self.pm.fetch_tick_size(token_id).await {
            Ok(tick) => {
                self.books.lock().await.set_tick_size(POLYMARKET, token_id, tick);
                Some(tick)
            }
            Err(err) => {
                tracing::warn!(token_id, error = %err, "polymarket tick_size fetch failed");
                None
            }
        }
    }
}

async fn persist_submit(
    store: &Store,
    leg_id: i64,
    platform: &str,
    side: OrderSide,
    result: &SubmitResult,
) -> Result<()> {
    match result {
        SubmitResult::Ack {
            order_id,
            envelope,
            making,
            taking,
            ..
        } => {
            let fill = if platform == POLYMARKET {
                pm_fak_fill(side, *making, *taking)
            } else {
                let price = envelope
                    .get("price")
                    .and_then(crate::platforms::parse_decimal);
                ioc_fill(*taking, price)
            };
            if let Some((shares, price)) = fill {
                close_leg_matched(store, leg_id, Some(order_id), shares, price, Decimal::ZERO).await?;
            } else {
                store
                    .update_leg_submitted(leg_id, "actived", Some(order_id), envelope)
                    .await?;
            }
        }
        SubmitResult::NoMatch {
            envelope,
            message,
            ..
        } => {
            store
                .update_leg_fill(
                    leg_id,
                    "cancelled",
                    None,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    &json!({"message": message, "envelope": envelope}),
                )
                .await?;
        }
        SubmitResult::Unknown {
            order_id,
            envelope,
            message,
            ..
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

fn place_result(
    platform: String,
    label: String,
    market: String,
    result: Result<SubmitResult>,
) -> PlaceResult {
    let error = match result {
        Ok(SubmitResult::Ack { .. }) => None,
        Ok(SubmitResult::NoMatch { message, .. }) | Ok(SubmitResult::Unknown { message, .. }) => {
            Some(message)
        }
        Err(err) => Some(format_place_error(&err)),
    };
    PlaceResult {
        platform,
        label,
        market,
        error,
    }
}

fn format_place_error(err: &Error) -> String {
    match err {
        Error::Http { status, message } => format!("HTTP {status} {message}"),
        Error::Rejected { code, message } => format!("{code} {message}"),
        other => other.to_string(),
    }
}

fn filter_trades<'a>(
    trades: &'a [TradeFill],
    order_id: Option<&str>,
    client_id: Option<&str>,
) -> Vec<&'a TradeFill> {
    trades
        .iter()
        .filter(|t| t.matches(order_id, client_id))
        .collect()
}

async fn upsert_fill_rows(store: &Store, leg_id: i64, fills: &[&TradeFill]) -> Result<()> {
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
    }
    Ok(())
}

fn fill_price_and_fee(fills: &[&TradeFill], fallback_price: Option<Decimal>) -> (Decimal, Decimal) {
    let mut shares = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    for fill in fills {
        shares += fill.shares;
        notional += fill.shares * fill.price;
        fee += fill.fee;
    }
    let price = if shares > Decimal::ZERO {
        notional / shares
    } else {
        fallback_price.unwrap_or(Decimal::ZERO)
    };
    (price, fee)
}

async fn close_leg_matched(
    store: &Store,
    leg_id: i64,
    third_order_id: Option<&str>,
    shares: Decimal,
    price: Decimal,
    fee: Decimal,
) -> Result<()> {
    store
        .update_leg_fill(
            leg_id,
            "matched",
            third_order_id,
            price,
            shares,
            fee,
            &json!({"source": "fak_terminal"}),
        )
        .await
}

async fn apply_fills(store: &Store, leg_id: i64, fills: &[&TradeFill]) -> Result<()> {
    upsert_fill_rows(store, leg_id, fills).await?;
    let (price, fee) = fill_price_and_fee(fills, None);
    let shares: Decimal = fills.iter().map(|f| f.shares).sum();
    if shares <= Decimal::ZERO {
        return Ok(());
    }
    close_leg_matched(
        store,
        leg_id,
        fills.last().and_then(|f| f.order_id.as_deref()),
        shares,
        price,
        fee,
    )
    .await
}

pub fn parent_terminal_status(has_open_legs: bool, positive_matched: bool) -> Option<&'static str> {
    if has_open_legs {
        None
    } else if positive_matched {
        Some("completed")
    } else {
        Some("cancelled")
    }
}

pub async fn mark_orders_complete(store: &Store) -> Result<()> {
    let completed: Vec<(i64,)> = sqlx::query_as(
        "UPDATE arb_orders o SET status = 'completed', updated_at = NOW(), completed_at = NOW()
         WHERE o.status = 'actived'
           AND NOT EXISTS (
             SELECT 1 FROM legs l
             WHERE l.order_id = o.id AND l.status IN ('pending','unknown','actived')
           )
           AND EXISTS (
             SELECT 1 FROM legs l
             WHERE l.order_id = o.id AND l.status = 'matched' AND COALESCE(l.actual_shares, 0) > 0
           )
         RETURNING o.id",
    )
    .fetch_all(&store.pool)
    .await?;
    sqlx::query(
        "UPDATE arb_orders o SET status = 'cancelled', rebalance_status = 'completed',
                updated_at = NOW(), completed_at = NOW(), rebalanced_at = NOW()
         WHERE o.status = 'actived'
           AND NOT EXISTS (
             SELECT 1 FROM legs l
             WHERE l.order_id = o.id AND l.status IN ('pending','unknown','actived')
           )
           AND NOT EXISTS (
             SELECT 1 FROM legs l
             WHERE l.order_id = o.id AND l.status = 'matched' AND COALESCE(l.actual_shares, 0) > 0
           )",
    )
    .execute(&store.pool)
    .await?;
    for (id,) in completed {
        if let Err(err) = store.refresh_order_actuals(id).await {
            tracing::warn!(order_id = id, error = %err, "refresh actuals failed");
        }
    }
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

    fn fill(trade_id: &str, order_id: &str, shares: &str, extra_ids: &[&str]) -> TradeFill {
        let mut order_ids = vec![order_id.to_string()];
        order_ids.extend(extra_ids.iter().map(|id| (*id).to_string()));
        TradeFill {
            trade_id: trade_id.into(),
            order_id: Some(order_id.into()),
            order_ids,
            coin: None,
            shares: d(shares),
            price: d("0.4"),
            fee: d("0.01"),
            fee_rate_bps: None,
            raw: json!({"oid": order_id}),
        }
    }

    #[test]
    fn matches_trade_by_order_id_only() {
        let trades = vec![
            fill("t1", "oid-1", "3", &[]),
            fill("t2", "oid-2", "9", &[]),
        ];
        let matched = filter_trades(&trades, Some("oid-1"), None);
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].trade_id, "t1");
        assert!(filter_trades(&trades, None, None).is_empty());
        assert!(filter_trades(&trades, Some("oid-1"), None)[0]
            .matches(Some("oid-1"), None));
        assert!(!fill("t3", "oid-3", "1", &[]).matches(Some("oid-1"), Some("oid-3-substring")));
    }

    #[test]
    fn matches_maker_order_id_without_json_substring() {
        let trade = fill("t1", "taker-1", "3", &["maker-9"]);
        assert!(trade.matches(Some("maker-9"), None));
        assert!(!trade.matches(Some("taker"), None));
    }

    #[test]
    fn parent_status_requires_positive_matched() {
        assert_eq!(parent_terminal_status(true, true), None);
        assert_eq!(parent_terminal_status(false, true), Some("completed"));
        assert_eq!(parent_terminal_status(false, false), Some("cancelled"));
    }
}
