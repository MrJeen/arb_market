use crate::book::{BookStore, DirtyCoalescer, OrderBook};
use crate::calc::{
    below_venue_mins, best_plan, confirm_plan, confirm_plan_reason, diagnose_books,
    first_usable_ask, inspect_calc, min_trade_amount, min_trade_cost, ArbLimits, ArbPlan,
    CalcMissSnapshot, FeeContext,
};
use crate::config::{Config, OUTCOME, POLYMARKET};
use crate::discovery::{load_active_topics, load_topic};
use crate::domain::{MarketIdentity, Topic, TopicKey};
use crate::error::{Error, Result};
use crate::hedge::{
    hedge_order_tokens, leftover_untradeable, needs_rebalance, plan_hedge, HedgeSide,
};
use crate::notify::{
    self, NatsNotifier, PlaceNotice, PlaceResult, SettlementNotice, TakeProfitCompletedNotice,
    TakeProfitTriggerNotice,
};
use crate::platforms::outcome::OutcomeVenue;
use crate::platforms::polymarket::PolymarketVenue;
use crate::platforms::{
    ioc_fill, pm_fak_fill, FillPage, MarketOrderRequest, OrderPoll, OrderSide, SubmitResult,
    TradeFill,
};
use crate::reconcile::{FillEvidence, LegResolution, PmTradeScan};
use crate::settlement::{OutcomeSettlement, SettlementStatus};
use crate::stats::MinuteStats;
use crate::store::{ArbOrderRow, NewLeg, Store};
use crate::take_profit::{plan_take_profit, TakeProfitAction, TakeProfitPlan};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
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
    pub stats: Arc<MinuteStats>,
    pub position_scan_cursor: Mutex<i64>,
    pub settlement_scan_cursor: Mutex<i64>,
    pub last_settlement_sweep: Mutex<Option<Instant>>,
    /// 已告警过的超时 unknown 腿；超时腿留在库里持续重试，告警只推一次。
    pub reported_stale_unknown: Mutex<HashSet<i64>>,
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
        tracing::info!(
            topics = topics.len(),
            pm = pm_tokens.len(),
            outcome = out_tokens.len(),
            "discovery refreshed"
        );
        Ok(())
    }

    pub async fn handle_topic(&self, topic_key: TopicKey) -> Result<()> {
        loop {
            self.stats.wakeup();
            let claimed = {
                let mut dirty = self.dirty.lock().await;
                dirty.mark(topic_key).is_some()
            };
            if !claimed {
                self.stats.coalesced();
                return Ok(());
            }
            let result = self.evaluate_topic(topic_key).await;
            let again = {
                let mut dirty = self.dirty.lock().await;
                dirty.finish(topic_key).is_some()
            };
            if !again {
                return result;
            }
        }
    }

    async fn block_new_arb(&self, topic: &str) -> Result<bool> {
        if self.cfg.max_active_orders > 0
            && self.store.count_active_orders().await? >= self.cfg.max_active_orders as i64
        {
            tracing::warn!(
                topic,
                limit = self.cfg.max_active_orders,
                "max orders reached"
            );
            self.stats.max_orders();
            return Ok(true);
        }
        if self.cfg.max_realized_loss > Decimal::ZERO {
            let pnl = self.store.sum_actual_profit().await?;
            if pnl < Decimal::ZERO && -pnl >= self.cfg.max_realized_loss {
                tracing::warn!(
                    topic,
                    loss = %(-pnl),
                    limit = %self.cfg.max_realized_loss,
                    "max realized loss reached"
                );
                self.stats.max_loss();
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn fee_context(&self, topic: &Topic) -> FeeContext {
        self.fee_context_from(Some(topic))
    }

    fn fee_context_from(&self, topic: Option<&Topic>) -> FeeContext {
        let rate = topic
            .and_then(Topic::polymarket_fee_rate)
            .unwrap_or_else(|| self.cfg.polymarket_fee_bps_prior / Decimal::from(10_000));
        FeeContext {
            polymarket_fee_rate: rate,
            outcome_taker_rate: self.cfg.outcome_taker_fee_rate,
        }
    }

    async fn evaluate_topic(&self, topic_key: TopicKey) -> Result<()> {
        let topic = {
            let topics = self.topics.read().await;
            topics.get(&topic_key).cloned()
        };
        let Some(topic) = topic else {
            self.stats.no_topic();
            return Ok(());
        };
        if self.store.has_active_topic(topic_key).await? {
            self.stats.active_topic();
            return Ok(());
        }
        if self
            .store
            .count_stale_unknown_legs(self.cfg.unknown_leg_timeout)
            .await?
            > 0
        {
            tracing::error!("stale unconfirmed legs present; skip new arb");
            self.stats.stale_unknown();
            return Ok(());
        }
        if self.block_new_arb(&topic_key.as_str()).await? {
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
        let (plan, pm_book_ts, out_book_ts, pm_ask, pm_sz, out_ask, out_sz) = {
            let books = self.books.lock().await;
            let now = Instant::now();
            let plan = best_plan(&topic, &books, &fees, &limits, now, self.cfg.book_stale);
            let (skips, pairs) =
                inspect_calc(&topic, &books, &fees, &limits, now, self.cfg.book_stale);
            self.stats.add_missing_book(skips.missing_book);
            self.stats.add_stale_book(skips.stale_book);
            self.stats.add_unit_cost(skips.unit_cost);
            self.stats.add_unprofitable(skips.unprofitable);
            if plan.is_none() && !pairs.is_empty() {
                self.stats.record_calc_miss(CalcMissSnapshot {
                    topic: topic.key.as_str(),
                    pairs,
                });
            }
            let (pm_book_ts, out_book_ts, pm_ask, pm_sz, out_ask, out_sz) = plan
                .as_ref()
                .map(|p| {
                    let pm = books.get(POLYMARKET, &p.pm.token_id);
                    let out = books.get(OUTCOME, &p.outcome.token_id);
                    let (pm_ask, pm_sz) = pm
                        .and_then(|b| first_usable_ask(&b.asks, false))
                        .map(|(px, sz)| (Some(px), Some(sz)))
                        .unwrap_or((None, None));
                    let (out_ask, out_sz) = out
                        .and_then(|b| first_usable_ask(&b.asks, true))
                        .map(|(px, sz)| (Some(px), Some(sz)))
                        .unwrap_or((None, None));
                    (
                        pm.map(|b| b.exchange_ts_ms).unwrap_or(0),
                        out.map(|b| b.exchange_ts_ms).unwrap_or(0),
                        pm_ask,
                        pm_sz,
                        out_ask,
                        out_sz,
                    )
                })
                .unwrap_or((0, 0, None, None, None, None));
            (
                plan,
                pm_book_ts,
                out_book_ts,
                pm_ask,
                pm_sz,
                out_ask,
                out_sz,
            )
        };
        self.stats.calc();
        let Some(plan) = plan else {
            return Ok(());
        };
        self.stats.found();
        tracing::info!(
            topic = %topic.key.as_str(),
            profit = %plan.profit,
            cost = %plan.total_cost,
            roi = %plan.roi,
            apr = %plan.apr,
            pm_ts = pm_book_ts,
            out_ts = out_book_ts,
            pm_ask = %fmt_px(pm_ask),
            pm_sz = %fmt_px(pm_sz),
            out_ask = %fmt_px(out_ask),
            out_sz = %fmt_px(out_sz),
            "arb opportunity"
        );
        if !self.cfg.enable_arb {
            self.stats.arb_disabled();
            return Ok(());
        }
        self.execute_plan(&topic, plan).await
    }

    async fn execute_plan(&self, topic: &Topic, plan: ArbPlan) -> Result<()> {
        self.ensure_trading_enabled(TradingIntent::Arbitrage)?;
        let fees = self.fee_context(topic);
        let pm_need = plan.pm.cost + plan.pm.fee;
        let out_need = plan.outcome.cost + plan.outcome.fee;
        let (pm_bal, out_bal) =
            tokio::join!(self.select_funder(pm_need), self.outcome.user_state());
        let (funder, pm_balance) = match pm_bal {
            Ok(pair) => pair,
            Err(err) => {
                tracing::warn!(
                    topic = %topic.key.as_str(),
                    error = %err,
                    "polymarket balance check skipped arb"
                );
                self.stats.pm_bal();
                return Ok(());
            }
        };
        let out_balance = match out_bal {
            Ok(bal) if bal >= out_need => bal,
            Ok(bal) => {
                tracing::info!(
                    topic = %topic.key.as_str(),
                    %bal,
                    required = %out_need,
                    "outcome buy skipped, usdc insufficient"
                );
                self.notify_balance_insufficient(
                    OUTCOME,
                    bal,
                    out_need,
                    &format!("arb topic={}", topic.key.as_str()),
                );
                self.stats.out_bal();
                return Ok(());
            }
            Err(err) => {
                tracing::warn!(
                    topic = %topic.key.as_str(),
                    error = %err,
                    "outcome usdc balance unavailable"
                );
                self.stats.out_bal();
                return Ok(());
            }
        };
        tracing::info!(
            topic = %topic.key.as_str(),
            funder = %funder,
            pm_balance = %pm_balance,
            out_balance = %out_balance,
            "balances confirmed"
        );

        let Some(plan) = self.confirm_http_plan(topic, &plan).await? else {
            return Ok(());
        };
        let pm_need = plan.pm.cost + plan.pm.fee;
        let out_need = plan.outcome.cost + plan.outcome.fee;
        if pm_need > pm_balance {
            tracing::info!(
                topic = %topic.key.as_str(),
                required = %pm_need,
                %pm_balance,
                "http plan exceeds polymarket balance"
            );
            self.stats.exceed_bal();
            return Ok(());
        }
        if out_need > out_balance {
            tracing::info!(
                topic = %topic.key.as_str(),
                required = %out_need,
                %out_balance,
                "http plan exceeds outcome balance"
            );
            self.notify_balance_insufficient(
                OUTCOME,
                out_balance,
                out_need,
                &format!("http-confirm topic={}", topic.key.as_str()),
            );
            self.stats.exceed_bal();
            return Ok(());
        }
        if self.block_new_arb(&topic.key.as_str()).await? {
            return Ok(());
        }

        self.ensure_trading_enabled(TradingIntent::Arbitrage)?;
        let fills = json!([
            {"platform": POLYMARKET, "token": plan.pm.token_id, "label": plan.pm.label, "shares": plan.pm.shares, "price": plan.pm.cap_price},
            {"platform": OUTCOME, "token": plan.outcome.token_id, "label": plan.outcome.label, "shares": plan.outcome.shares, "price": plan.outcome.cap_price}
        ]);
        let pm_token = topic.token(POLYMARKET, &plan.pm.label);
        let out_token = topic.token(OUTCOME, &plan.outcome.label);
        // 建档必须原子：`actived` 且没有腿的父单会被回填判为无成交并取消。
        let initial_legs = [
            NewLeg {
                platform: POLYMARKET,
                token_id: &plan.pm.token_id,
                label: &plan.pm.label,
                side: "BUY",
                intent: "arb_buy",
                funder: Some(funder.as_str()),
                wallet: Some(funder.as_str()),
                service: self.polymarket_service(&funder),
                req_price: plan.pm.cap_price,
                req_shares: plan.pm.shares,
                req_fee: plan.pm.fee,
                client_order_id: None,
            },
            NewLeg {
                platform: OUTCOME,
                token_id: &plan.outcome.token_id,
                label: &plan.outcome.label,
                side: "BUY",
                intent: "arb_buy",
                funder: None,
                wallet: self.outcome.account_address(),
                service: None,
                req_price: plan.outcome.cap_price,
                req_shares: plan.outcome.shares,
                req_fee: plan.outcome.fee,
                client_order_id: None,
            },
        ];
        let (order_id, leg_ids) = match self
            .store
            .insert_actived_order_with_legs(
                topic.key,
                &topic.market_identity()?,
                &topic.title,
                &topic.market_title,
                topic.end_date,
                plan.net_shares,
                plan.profit,
                plan.total_cost,
                &fills,
                &initial_legs,
            )
            .await
        {
            Ok(created) => created,
            Err(Error::Sqlx(sqlx::Error::Database(db)))
                if db.code().as_deref() == Some("23505") =>
            {
                tracing::info!(topic = %topic.key.as_str(), "active topic already claimed");
                self.stats.claimed();
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        let [pm_leg, out_leg] = <[i64; 2]>::try_from(leg_ids)
            .map_err(|_| Error::msg("arb order must be created with both initial legs"))?;
        self.stats.orders();
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
        let (pm_res, out_res) = tokio::join!(
            self.submit_pm(pm_leg, &funder, &pm_req, &fees, TradingIntent::Arbitrage),
            self.submit_outcome(out_leg, &out_req, &fees, TradingIntent::Arbitrage)
        );
        if let Err(err) = &pm_res {
            tracing::error!(error = %err, "polymarket submit failed");
            self.stats.pm_fail();
        } else {
            self.stats.pm_ok();
        }
        if let Err(err) = &out_res {
            tracing::error!(error = %err, "outcome submit failed");
            self.stats.out_fail();
        } else {
            self.stats.out_ok();
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

    async fn confirm_http_plan(&self, topic: &Topic, plan: &ArbPlan) -> Result<Option<ArbPlan>> {
        let (pm_res, out_res) = tokio::join!(
            async {
                let started = Instant::now();
                let r = self.pm.rest_book(&plan.pm.token_id).await;
                (r, Instant::now(), started.elapsed())
            },
            async {
                let started = Instant::now();
                let r = self.outcome.rest_book(&plan.outcome.token_id).await;
                (r, Instant::now(), started.elapsed())
            }
        );
        let (pm_snap, pm_at, pm_elapsed) = pm_res;
        let (out_snap, out_at, out_elapsed) = out_res;
        let skew = book_recv_skew(pm_at, out_at);
        let (pm_bids, pm_asks, pm_ts) = match pm_snap {
            Ok(snap) => {
                tracing::info!(
                    platform = POLYMARKET,
                    token = %plan.pm.token_id,
                    elapsed_ms = pm_elapsed.as_millis() as u64,
                    "polymarket rest book fetched"
                );
                snap
            }
            Err(err) => {
                tracing::warn!(
                    platform = POLYMARKET,
                    token = %plan.pm.token_id,
                    elapsed_ms = pm_elapsed.as_millis() as u64,
                    error = %err,
                    "polymarket rest book failed"
                );
                self.stats.http_fail();
                return Ok(None);
            }
        };
        let (out_bids, out_asks, out_ts) = match out_snap {
            Ok(snap) => {
                tracing::info!(
                    platform = OUTCOME,
                    token = %plan.outcome.token_id,
                    elapsed_ms = out_elapsed.as_millis() as u64,
                    "outcome rest book fetched"
                );
                snap
            }
            Err(err) => {
                tracing::warn!(
                    platform = OUTCOME,
                    token = %plan.outcome.token_id,
                    elapsed_ms = out_elapsed.as_millis() as u64,
                    error = %err,
                    "outcome rest book failed"
                );
                self.stats.http_fail();
                return Ok(None);
            }
        };
        let (pm_ask, pm_sz) = first_usable_ask(&pm_asks, false)
            .map(|(px, sz)| (Some(px), Some(sz)))
            .unwrap_or((None, None));
        let (out_ask, out_sz) = first_usable_ask(&out_asks, true)
            .map(|(px, sz)| (Some(px), Some(sz)))
            .unwrap_or((None, None));
        // REST 盘口写回内存：exchange_ts 更旧或相同则丢弃。两边都拉到就写，skew 失败也写。
        let (
            pm_prev_ts,
            out_prev_ts,
            pm_prev_ask,
            pm_prev_sz,
            out_prev_ask,
            out_prev_sz,
            pm_applied,
            out_applied,
        ) = {
            let mut books = self.books.lock().await;
            let pm_prev = books.get(POLYMARKET, &plan.pm.token_id);
            let out_prev = books.get(OUTCOME, &plan.outcome.token_id);
            let pm_prev_ts = pm_prev.map(|b| b.exchange_ts_ms).unwrap_or(0);
            let out_prev_ts = out_prev.map(|b| b.exchange_ts_ms).unwrap_or(0);
            let (pm_prev_ask, pm_prev_sz) = pm_prev
                .and_then(|b| first_usable_ask(&b.asks, false))
                .map(|(px, sz)| (Some(px), Some(sz)))
                .unwrap_or((None, None));
            let (out_prev_ask, out_prev_sz) = out_prev
                .and_then(|b| first_usable_ask(&b.asks, true))
                .map(|(px, sz)| (Some(px), Some(sz)))
                .unwrap_or((None, None));
            let pm_applied = books.replace_snapshot(
                POLYMARKET,
                &plan.pm.token_id,
                pm_bids.clone(),
                pm_asks.clone(),
                pm_ts,
                pm_at,
            );
            let out_applied = books.replace_snapshot(
                OUTCOME,
                &plan.outcome.token_id,
                out_bids.clone(),
                out_asks.clone(),
                out_ts,
                out_at,
            );
            (
                pm_prev_ts,
                out_prev_ts,
                pm_prev_ask,
                pm_prev_sz,
                out_prev_ask,
                out_prev_sz,
                pm_applied,
                out_applied,
            )
        };
        if !book_recv_skew_ok(pm_at, out_at, HTTP_BOOK_SKEW_MAX) {
            tracing::warn!(
                topic = %topic.key.as_str(),
                pm_token = %plan.pm.token_id,
                out_token = %plan.outcome.token_id,
                skew_ms = skew.as_millis() as u64,
                pm_elapsed_ms = pm_elapsed.as_millis() as u64,
                out_elapsed_ms = out_elapsed.as_millis() as u64,
                pm_ts,
                out_ts,
                pm_prev_ts,
                out_prev_ts,
                pm_ask = %fmt_px(pm_ask),
                pm_sz = %fmt_px(pm_sz),
                out_ask = %fmt_px(out_ask),
                out_sz = %fmt_px(out_sz),
                pm_prev_ask = %fmt_px(pm_prev_ask),
                pm_prev_sz = %fmt_px(pm_prev_sz),
                out_prev_ask = %fmt_px(out_prev_ask),
                out_prev_sz = %fmt_px(out_prev_sz),
                pm_applied,
                out_applied,
                "http book receive skew exceeded 1s"
            );
            self.stats.skew();
            return Ok(None);
        }
        tracing::info!(
            topic = %topic.key.as_str(),
            pm_token = %plan.pm.token_id,
            out_token = %plan.outcome.token_id,
            skew_ms = skew.as_millis() as u64,
            pm_elapsed_ms = pm_elapsed.as_millis() as u64,
            out_elapsed_ms = out_elapsed.as_millis() as u64,
            pm_ts,
            out_ts,
            pm_prev_ts,
            out_prev_ts,
            pm_ask = %fmt_px(pm_ask),
            pm_sz = %fmt_px(pm_sz),
            out_ask = %fmt_px(out_ask),
            out_sz = %fmt_px(out_sz),
            pm_prev_ask = %fmt_px(pm_prev_ask),
            pm_prev_sz = %fmt_px(pm_prev_sz),
            out_prev_ask = %fmt_px(out_prev_ask),
            out_prev_sz = %fmt_px(out_prev_sz),
            pm_applied,
            out_applied,
            "http books received"
        );
        let pm_tick = self.ensure_pm_tick(&plan.pm.token_id).await;
        let pm_book = OrderBook {
            platform: POLYMARKET.to_string(),
            token_id: plan.pm.token_id.clone(),
            bids: pm_bids,
            asks: pm_asks,
            exchange_ts_ms: pm_ts,
            received_at: pm_at,
            stale: false,
            tick_size: pm_tick,
        };
        let out_book = OrderBook {
            platform: OUTCOME.to_string(),
            token_id: plan.outcome.token_id.clone(),
            bids: out_bids,
            asks: out_asks,
            exchange_ts_ms: out_ts,
            received_at: out_at,
            stale: false,
            tick_size: None,
        };
        let fees = self.fee_context(topic);
        let limits = ArbLimits {
            cost_limit: self.cfg.arb_cost_limit,
            min_profit: self.cfg.arb_min_profit,
            min_apr: self.cfg.arb_min_apr,
            days: crate::calc::days_until(topic.end_date),
        };
        match confirm_plan(topic, plan, &pm_book, &out_book, &fees, &limits) {
            Some(confirmed) => {
                tracing::info!(
                    topic = %topic.key.as_str(),
                    profit = %confirmed.profit,
                    cost = %confirmed.total_cost,
                    roi = %confirmed.roi,
                    apr = %confirmed.apr,
                    pm_ts,
                    out_ts,
                    pm_ask = %fmt_px(pm_ask),
                    pm_sz = %fmt_px(pm_sz),
                    out_ask = %fmt_px(out_ask),
                    out_sz = %fmt_px(out_sz),
                    "http arb confirmed"
                );
                Ok(Some(confirmed))
            }
            None => {
                let miss = diagnose_books(
                    topic,
                    &pm_book,
                    &out_book,
                    &fees,
                    &limits,
                    &plan.pm.label,
                    &plan.outcome.label,
                );
                let reason = confirm_plan_reason(topic, plan, &pm_book, &out_book, &fees, &limits);
                tracing::info!(
                    topic = %topic.key.as_str(),
                    pm_ts,
                    out_ts,
                    pm_prev_ts,
                    out_prev_ts,
                    pm_ask = %fmt_px(pm_ask),
                    pm_sz = %fmt_px(pm_sz),
                    out_ask = %fmt_px(out_ask),
                    out_sz = %fmt_px(out_sz),
                    pm_prev_ask = %fmt_px(pm_prev_ask),
                    pm_prev_sz = %fmt_px(pm_prev_sz),
                    out_prev_ask = %fmt_px(out_prev_ask),
                    out_prev_sz = %fmt_px(out_prev_sz),
                    unit_cost = %fmt_px(miss.unit_cost),
                    reason,
                    pm_applied,
                    out_applied,
                    "http plan not fillable"
                );
                self.stats.no_longer();
                Ok(None)
            }
        }
    }

    async fn select_funder(&self, required: Decimal) -> Result<(String, Decimal)> {
        let mut current = self
            .pm
            .next_funder()
            .await
            .ok_or_else(|| Error::msg("no polymarket funder configured"))?;
        for _ in 0..3 {
            match self.pm.balance(&current).await {
                Ok(bal) if bal >= required => return Ok((current, bal)),
                Ok(bal) => {
                    tracing::warn!(funder = %current, %bal, %required, "polymarket balance low")
                }
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

    async fn require_outcome_usdc(&self, required: Decimal, context: &str) -> Result<()> {
        match self.outcome.user_state().await {
            Ok(bal) if bal >= required => Ok(()),
            Ok(bal) => {
                self.notify_balance_insufficient(OUTCOME, bal, required, context);
                Err(Error::msg(format!(
                    "outcome buy skipped, usdc {bal} < {required}"
                )))
            }
            Err(err) => Err(Error::msg(format!(
                "outcome buy skipped, usdc balance unavailable: {err}"
            ))),
        }
    }

    fn notify_balance_insufficient(
        &self,
        platform: &str,
        balance: Decimal,
        required: Decimal,
        context: &str,
    ) {
        if let Some(notify) = &self.notify {
            notify.publish_balance_insufficient(platform, balance, required, context);
        }
    }

    async fn hedge_pm_funder(
        &self,
        order_id: i64,
        token_id: &str,
        side: OrderSide,
    ) -> Result<String> {
        let token_buy = if side == OrderSide::Sell {
            self.store
                .buy_funder_for_token(order_id, POLYMARKET, token_id)
                .await?
        } else {
            None
        };
        let order_funder = self.store.order_pm_funder(order_id).await?;
        resolve_hedge_pm_funder(side == OrderSide::Sell, token_buy, order_funder)
    }

    async fn require_pm_usdc(&self, funder: &str, required: Decimal) -> Result<()> {
        match self.pm.balance(funder).await {
            Ok(bal) if bal >= required => Ok(()),
            Ok(bal) => Err(Error::msg(format!(
                "polymarket buy skipped, usdc {bal} < {required}"
            ))),
            Err(err) => Err(Error::msg(format!(
                "polymarket buy skipped, usdc balance unavailable: {err}"
            ))),
        }
    }

    async fn require_pm_token(&self, funder: &str, token_id: &str, shares: Decimal) -> Result<()> {
        match self.pm.token_balance(funder, token_id).await {
            Ok(bal) if bal >= shares => Ok(()),
            Ok(bal) => Err(Error::msg(format!(
                "polymarket sell skipped, token balance {bal} < {shares}"
            ))),
            Err(err) => Err(Error::msg(format!(
                "polymarket sell skipped, token balance unavailable: {err}"
            ))),
        }
    }

    async fn require_outcome_token(&self, token_id: &str, shares: Decimal) -> Result<()> {
        match self.outcome.token_balance(token_id).await {
            Ok(bal) if bal >= shares => Ok(()),
            Ok(bal) => Err(Error::msg(format!(
                "outcome sell skipped, token balance {bal} < {shares}"
            ))),
            Err(err) => Err(Error::msg(format!(
                "outcome sell skipped, token balance unavailable: {err}"
            ))),
        }
    }

    async fn token_book_snapshot(&self, platform: &str, token_id: &str) -> Option<Value> {
        let books = self.books.lock().await;
        books.get(platform, token_id).map(OrderBook::snapshot_json)
    }

    fn ensure_trading_enabled(&self, intent: TradingIntent) -> Result<()> {
        ensure_trading_submission_enabled(intent.enabled(&self.cfg), intent)
    }

    async fn submit_pm(
        &self,
        leg_id: i64,
        funder: &str,
        req: &MarketOrderRequest,
        fees: &FeeContext,
        intent: TradingIntent,
    ) -> Result<SubmitResult> {
        self.ensure_trading_enabled(intent)?;
        let prepared = self.pm.prepare_market_order(funder, req).await?;
        let book_snapshot = self.token_book_snapshot(POLYMARKET, &req.token_id).await;
        self.store
            .insert_envelope(
                leg_id,
                &prepared.order_hash,
                &prepared.envelope,
                &prepared.payload,
                book_snapshot.as_ref(),
            )
            .await?;
        let (result, response) = self.pm.post_prepared(&prepared).await?;
        persist_submit(
            &self.store,
            leg_id,
            POLYMARKET,
            req.side,
            &result,
            fees,
            &response,
        )
        .await?;
        Ok(result)
    }

    async fn submit_outcome(
        &self,
        leg_id: i64,
        req: &MarketOrderRequest,
        fees: &FeeContext,
        intent: TradingIntent,
    ) -> Result<SubmitResult> {
        self.ensure_trading_enabled(intent)?;
        let prepared = self.outcome.prepare_market_order(req)?;
        let book_snapshot = self.token_book_snapshot(OUTCOME, &req.token_id).await;
        self.store
            .insert_envelope(
                leg_id,
                &prepared.order_hash,
                &prepared.envelope,
                &prepared.payload,
                book_snapshot.as_ref(),
            )
            .await?;
        let (result, response) = self.outcome.post_prepared(prepared).await?;
        persist_submit(
            &self.store,
            leg_id,
            OUTCOME,
            req.side,
            &result,
            fees,
            &response,
        )
        .await?;
        Ok(result)
    }

    pub async fn reconcile(&self) -> Result<()> {
        let expired = self
            .store
            .fail_stale_pending_unsubmitted(self.cfg.pending_leg_timeout)
            .await?;
        if expired > 0 {
            tracing::warn!(expired, "failed stale pending legs never submitted");
        }
        let promoted = self.store.promote_submitted_pending_to_unknown().await?;
        if promoted > 0 {
            tracing::warn!(
                promoted,
                "pending legs with submit timestamp marked unknown"
            );
        }
        let timed_out = self
            .store
            .stale_unknown_legs(self.cfg.unknown_leg_timeout)
            .await?;
        // 超时不是零成交证据，这些腿留在 unknown 继续回填；告警按腿去重，避免每轮重复推送。
        let newly_stale = self.take_unreported_stale_unknown(&timed_out).await;
        if !newly_stale.is_empty() {
            tracing::error!(
                count = newly_stale.len(),
                still_stale = timed_out.len(),
                timeout_secs = self.cfg.unknown_leg_timeout.as_secs(),
                "stale unconfirmed legs retained for reconciliation; manual verification required"
            );
            if let Some(notify) = &self.notify {
                notify.publish_alert(notify::format_unknown_timeout_notice(
                    &notify::format_notify_tag(&self.cfg.cat),
                    &newly_stale,
                ));
            }
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

    /// 只返回本进程尚未告警过的超时腿，并回收已经不再超时（已回填或人工处理）的记录，
    /// 使同一条腿再次超时时能重新告警。
    async fn take_unreported_stale_unknown(
        &self,
        stale: &[crate::store::ClosedLegRef],
    ) -> Vec<crate::store::ClosedLegRef> {
        let current: HashSet<i64> = stale.iter().map(|leg| leg.id).collect();
        let mut reported = self.reported_stale_unknown.lock().await;
        reported.retain(|id| current.contains(id));
        stale
            .iter()
            .filter(|leg| reported.insert(leg.id))
            .cloned()
            .collect()
    }

    async fn reconcile_leg(&self, leg: &crate::store::LegRow) -> Result<()> {
        if leg.platform == POLYMARKET {
            self.reconcile_pm(leg).await
        } else {
            self.reconcile_outcome(leg).await
        }
    }

    async fn reconcile_pm(&self, leg: &crate::store::LegRow) -> Result<()> {
        let Some((current, poll, page)) = reconcile_pm_page(&self.pm, &self.store, leg).await?
        else {
            return Ok(());
        };
        self.apply_fill_page(&current, poll, page).await
    }

    async fn reconcile_outcome(&self, leg: &crate::store::LegRow) -> Result<()> {
        let Some(selector) = leg
            .third_order_id
            .as_deref()
            .or(leg.client_order_id.as_deref())
        else {
            return Ok(());
        };
        let poll = self.outcome.poll_order(selector, &leg.token_id).await?;
        if !poll.found {
            return Ok(());
        }
        let Some(current) = self.store.record_order_poll(leg, &poll).await? else {
            return Ok(());
        };
        let submitted = current
            .submitted_at
            .ok_or_else(|| Error::msg("missing submission time for fill history"))?;
        let progress = current
            .last_order_info
            .as_ref()
            .and_then(|info| info.get("fill_progress"))
            .cloned()
            .unwrap_or(Value::Null);
        // 与平台时钟留重叠；精确 oid 匹配会排除窗口中其他订单的成交。
        let page = self
            .outcome
            .poll_fill_page(
                &current.token_id,
                submitted.timestamp_millis().saturating_sub(30_000).max(0),
                &progress,
            )
            .await?;
        self.apply_fill_page(&current, poll, page).await
    }

    async fn apply_fill_page(
        &self,
        leg: &crate::store::LegRow,
        poll: OrderPoll,
        page: FillPage,
    ) -> Result<()> {
        let mut matched: Vec<TradeFill> =
            filter_trades(&page.fills, poll.order_id.as_deref(), None)
                .into_iter()
                .map(Clone::clone)
                .collect();
        if matched.iter().any(|fill| {
            fill.coin
                .as_deref()
                .is_some_and(|coin| coin != leg.token_id)
        }) {
            return Err(Error::msg("matched trade token does not match leg"));
        }
        if leg.platform == POLYMARKET && matched.iter().any(needs_pm_fee_snapshot) {
            let identities = self.store.market_identities_for_order(leg.order_id).await?;
            let market_id = identities.require(POLYMARKET)?;
            // 估算政策经用户明确选择；快照保存在每条成交中，不以策略先验冒充实收。
            let schedule = self.pm.fee_schedule(market_id).await?;
            for fill in matched
                .iter_mut()
                .filter(|fill| needs_pm_fee_snapshot(fill))
            {
                fill.raw["fee_calculation"] = schedule.clone();
            }
        }
        let expected_shares = leg
            .last_order_info
            .as_ref()
            .and_then(|info| info.pointer("/submission/expected_shares"))
            .and_then(crate::platforms::parse_decimal);
        let pm_scan = if leg.platform == POLYMARKET {
            Some(serde_json::from_value::<PmTradeScan>(
                page.progress.clone(),
            )?)
        } else {
            None
        };
        let trades_only = leg.platform == POLYMARKET && !poll.found;
        let evidence = FillEvidence {
            poll,
            page_complete: page.complete,
            history_complete: page.history_complete,
            expected_shares,
            pm_scan,
        };
        let started = Instant::now();
        let resolution = self
            .store
            .record_reconciliation(leg, &matched, &evidence, &page.progress)
            .await?;
        match resolution {
            LegResolution::Pending(reason) => tracing::debug!(
                platform=%leg.platform,leg_id=leg.id,order_id=leg.order_id,reason,
                elapsed_ms=started.elapsed().as_millis() as u64,"trade reconciliation pending"
            ),
            LegResolution::Terminal {
                status,
                shares,
                fee,
                fee_sources,
                ..
            } => tracing::info!(
                platform=%leg.platform,leg_id=leg.id,order_id=leg.order_id,status,%shares,%fee,
                trades_only,?fee_sources,elapsed_ms=started.elapsed().as_millis() as u64,"trade reconciliation finalized"
            ),
        }
        Ok(())
    }

    pub async fn hedge_once(&self) -> Result<()> {
        let after_id = *self.position_scan_cursor.lock().await;
        let watching = self
            .store
            .completed_unbalanced_orders(after_id, self.cfg.position_scan_batch)
            .await?;
        advance_scan_cursor(&self.position_scan_cursor, &watching, after_id).await;
        let watching_batch = watching.len();
        self.process_scan_batch(watching).await;
        // 活跃批次已处理完才取 pending，避免 pending 查询失败时连带丢掉本轮的活跃订单。
        let pending = self.due_settlement_pending_batch().await?;
        if !pending.is_empty() {
            tracing::info!(
                watching_batch,
                pending_batch = pending.len(),
                interval_secs = self.cfg.settlement_pending_scan_interval.as_secs(),
                "settlement pending sweep"
            );
            self.process_scan_batch(pending).await;
        }
        Ok(())
    }

    async fn process_scan_batch(&self, orders: Vec<ArbOrderRow>) {
        for order in orders {
            if let Err(err) = self.hedge_order_once(&order).await {
                self.stats.exec_err();
                tracing::warn!(order_id = order.id, error = %err, "position order scan failed");
            }
        }
    }

    /// `settlement_pending` 只等对手方 payout，结算不会回退，因此按独立间隔清扫。
    /// 未到期返回空集合，既不查库也不推进游标。
    async fn due_settlement_pending_batch(&self) -> Result<Vec<ArbOrderRow>> {
        let now = Instant::now();
        let last_sweep = *self.last_settlement_sweep.lock().await;
        if !settlement_sweep_due(last_sweep, now, self.cfg.settlement_pending_scan_interval) {
            return Ok(Vec::new());
        }
        let after_id = *self.settlement_scan_cursor.lock().await;
        let orders = self
            .store
            .settlement_pending_orders(after_id, self.cfg.settlement_pending_scan_batch)
            .await?;
        advance_scan_cursor(&self.settlement_scan_cursor, &orders, after_id).await;
        *self.last_settlement_sweep.lock().await = Some(now);
        Ok(orders)
    }

    async fn hedge_order_once(&self, order: &ArbOrderRow) -> Result<()> {
        if settlement_only_scan(&order.position_status) {
            self.stats.settlement_pending_scan();
            let Some(identity) = self.settlement_identity_for_order(order).await? else {
                return Ok(());
            };
            self.settlement_gate(order.id, &order.title, &identity)
                .await?;
            return Ok(());
        }
        if self.finish_terminal_lifecycle_action(order).await? {
            return Ok(());
        }
        if self.store.has_open_lifecycle_legs(order.id).await? {
            tracing::debug!(order_id = order.id, "position action legs in flight");
            self.stats.lifecycle_busy();
            return Ok(());
        }
        let positions = self.store.positions_for_order(order.id).await?;
        if !has_position(&positions) {
            match self.store.finalize_closed_position(order.id).await? {
                Some((actual_cost, actual_rev, actual_profit)) => {
                    tracing::info!(
                        order_id = order.id,
                        %actual_cost,
                        %actual_rev,
                        %actual_profit,
                        "position actuals refreshed and position closed"
                    );
                    if let Some(notify) = &self.notify {
                        notify.publish_order_actuals(order.id, actual_profit, actual_cost);
                    }
                }
                None => tracing::debug!(
                    order_id = order.id,
                    "position close deferred because state or exposure changed"
                ),
            }
            return Ok(());
        }

        let key = TopicKey::new(order.event_id, order.unified_index);
        let topic = match self.topics.read().await.get(&key).cloned() {
            Some(topic) => Some(topic),
            None => self.restore_historical_topic(order.id, key).await?,
        };
        let Some(topic) = topic else {
            self.stats.unavailable();
            tracing::warn!(order_id = order.id, topic = %key.as_str(), "position topic unavailable; fail closed");
            return Ok(());
        };
        let identity = match self.market_identity_for_order(order.id, &topic).await {
            Ok(identity) => identity,
            Err(err) => {
                self.stats.unavailable();
                tracing::warn!(order_id = order.id, topic = %key.as_str(), error = %err, "position market identity unavailable; fail closed");
                return Ok(());
            }
        };
        let settlement_access = self
            .settlement_gate(order.id, &order.title, &identity)
            .await?;
        if settlement_access == SettlementAccess::Stop {
            return Ok(());
        }

        let fees = self.fee_context(&topic);

        if settlement_access == SettlementAccess::All {
            let take_profit_tokens = take_profit_book_tokens(&topic, &positions);
            self.refresh_hedge_books(order.id, &take_profit_tokens)
                .await;
            for (platform, token_id) in &take_profit_tokens {
                if platform == POLYMARKET {
                    let _ = self.ensure_pm_tick(token_id).await;
                }
            }
            self.stats.take_profit_scan();
            let cached_plan = {
                let books = self.books.lock().await;
                plan_take_profit(
                    &topic,
                    &positions,
                    &books,
                    &fees,
                    self.cfg.take_profit_min_gain,
                    Instant::now(),
                    self.cfg.book_stale,
                )
            };
            if let Some(plan) = cached_plan {
                self.stats.take_profit_candidate();
                if let Some(confirmed) = self
                    .confirm_take_profit(&topic, &positions, &fees, &plan)
                    .await?
                {
                    if self.cfg.enable_take_profit {
                        let Some(claim_id) = self
                            .store
                            .try_claim_lifecycle(order.id, "take_profit")
                            .await?
                        else {
                            self.stats.lifecycle_busy();
                            return Ok(());
                        };
                        // Final gate after claiming and immediately before creating either leg.
                        if self
                            .settlement_gate(order.id, &order.title, &identity)
                            .await?
                            != SettlementAccess::All
                        {
                            self.store
                                .release_lifecycle(order.id, "take_profit", claim_id)
                                .await?;
                            return Ok(());
                        }
                        self.stats.take_profit_confirmed();
                        if let Some(notify) = &self.notify {
                            notify.publish_take_profit_trigger(TakeProfitTriggerNotice {
                                order_id: order.id,
                                title: topic.title.clone(),
                                expected_gain: confirmed.gain,
                            });
                        }
                        let result = self
                            .execute_take_profit(order.id, claim_id, &topic, &fees, &confirmed)
                            .await;
                        if let Err(err) = result {
                            self.release_failed_zero_leg_claim(order.id, "take_profit", claim_id)
                                .await?;
                            return Err(err);
                        }
                        return Ok(());
                    }
                    record_take_profit_not_submitted(&self.stats, true);
                    tracing::info!(
                        order_id = order.id,
                        "take profit calculated; submission disabled"
                    );
                } else {
                    record_take_profit_not_submitted(&self.stats, false);
                }
            }
        }

        match needs_rebalance(&positions, &topic.labels(), self.cfg.min_rebalance_qty) {
            None => {
                tracing::warn!(order_id = order.id, "non-binary position cannot be managed");
                return Ok(());
            }
            Some(false) => {
                if order.rebalance_status != "completed" {
                    self.complete_rebalance(order.id).await?;
                }
                return Ok(());
            }
            Some(true) => {}
        }
        // Full-access planning reads balances regardless of the trading switch so dry-run
        // calculations match live trading. Reduce-only deliberately leaves balances empty.
        let order_funder = self.store.order_pm_funder(order.id).await?;
        let mut balances = HashMap::new();
        if settlement_access == SettlementAccess::All {
            if let Some(funder) = order_funder.as_deref() {
                match self.pm.balance(funder).await {
                    Ok(bal) => {
                        balances.insert(POLYMARKET.to_string(), bal);
                    }
                    Err(err) => {
                        tracing::warn!(order_id = order.id, funder, error = %err, "hedge polymarket balance unavailable")
                    }
                }
            }
            if let Ok(bal) = self.outcome.user_state().await {
                balances.insert(OUTCOME.to_string(), bal);
            }
        }
        let order_tokens = hedge_order_tokens(&topic, &positions, self.cfg.min_rebalance_qty);
        self.refresh_hedge_books(order.id, &order_tokens).await;
        for (platform, token_id) in &order_tokens {
            if platform == POLYMARKET {
                let _ = self.ensure_pm_tick(token_id).await;
            }
        }
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
            let untradeable = {
                let books = self.books.lock().await;
                leftover_untradeable(
                    &topic,
                    &positions,
                    &books,
                    self.cfg.min_rebalance_qty,
                    Instant::now(),
                    self.cfg.book_stale,
                )
            };
            if untradeable {
                self.complete_rebalance(order.id).await?;
            }
            return Ok(());
        }
        let actions: Vec<_> = actions
            .into_iter()
            .filter(|action| action_allowed_for_rebalance(action, settlement_access))
            .collect();
        if actions.is_empty() {
            tracing::info!(
                order_id = order.id,
                "rebalance has no permitted reduce-only actions"
            );
            return Ok(());
        }
        if !self.cfg.enable_rebalance {
            self.stats.rebalance_disabled();
            tracing::info!(
                order_id = order.id,
                "rebalance calculated; submission disabled"
            );
            return Ok(());
        }
        let Some(claim_id) = self
            .store
            .try_claim_lifecycle(order.id, "rebalance")
            .await?
        else {
            self.stats.lifecycle_busy();
            return Ok(());
        };
        // Balances, REST books and planning are complete; gate once more before the first leg.
        let final_access = self
            .settlement_gate(order.id, &order.title, &identity)
            .await?;
        if final_access == SettlementAccess::Stop
            || actions
                .iter()
                .any(|action| !action_allowed_for_rebalance(action, final_access))
        {
            self.store
                .release_lifecycle(order.id, "rebalance", claim_id)
                .await?;
            return Ok(());
        }
        let result = async {
            self.store.mark_rebalance(order.id, "actived").await?;
            let mut first_error = None;
            for action in actions {
                if let Err(err) = self.execute_hedge(order.id, claim_id, &action, &fees).await {
                    tracing::error!(order_id = order.id, error = %err, "hedge submit failed");
                    first_error.get_or_insert(err);
                }
            }
            match first_error {
                Some(err) => Err(err),
                None => Ok(()),
            }
        }
        .await;
        if let Err(err) = result {
            self.release_failed_zero_leg_claim(order.id, "rebalance", claim_id)
                .await?;
            return Err(err);
        }
        Ok(())
    }

    async fn restore_historical_topic(
        &self,
        order_id: i64,
        key: TopicKey,
    ) -> Result<Option<Topic>> {
        let Some(topic) = load_topic(&self.common, key, &self.cfg.enabled_platforms).await? else {
            return Ok(None);
        };
        // Historical topics are used for this lifecycle pass only. Mutating the active topic map
        // would let the next discovery refresh race with subscription ownership.
        tracing::info!(order_id, topic = %key.as_str(), "historical position topic restored");
        Ok(Some(topic))
    }

    async fn finish_terminal_lifecycle_action(&self, order: &ArbOrderRow) -> Result<bool> {
        let (Some(action), Some(claim_id), Some(claimed_at)) = (
            order.lifecycle_action.as_deref(),
            order.lifecycle_claim_id,
            order.lifecycle_claimed_at,
        ) else {
            return Ok(false);
        };
        let (total, open) = self
            .store
            .lifecycle_leg_counts(order.id, action, claim_id)
            .await?;
        if total == 0 {
            let timeout = chrono::Duration::from_std(self.cfg.pending_leg_timeout)
                .unwrap_or(chrono::Duration::MAX);
            if chrono::Utc::now().signed_duration_since(claimed_at) > timeout {
                let released = self
                    .store
                    .release_lifecycle(order.id, action, claim_id)
                    .await?;
                tracing::warn!(
                    order_id = order.id,
                    action,
                    claim_id = %claim_id,
                    released,
                    "stale zero-leg lifecycle claim released"
                );
            }
            // A fresh owner may still be preparing its first leg; either way this row is busy now.
            return Ok(true);
        }
        if open > 0 {
            return Ok(false);
        }
        let has_positive_fill = if action == "take_profit" {
            self.store
                .lifecycle_claim_has_positive_fill(order.id, action, claim_id)
                .await?
        } else {
            false
        };
        // Keep the claim until actuals are durable: a transient refresh failure must remain
        // retryable on the next scan and must not lose the completion notification.
        let take_profit_actuals = if action == "take_profit" && has_positive_fill {
            Some(self.store.refresh_order_actuals(order.id).await?)
        } else {
            None
        };
        let released = self
            .store
            .release_lifecycle(order.id, action, claim_id)
            .await?;
        if !released {
            tracing::debug!(
                order_id = order.id,
                action,
                claim_id = %claim_id,
                "terminal lifecycle claim ownership changed"
            );
            return Ok(true);
        }
        if action == "take_profit" {
            if let Some((actual_cost, _, actual_profit)) = take_profit_actuals {
                if let Some(notify) = &self.notify {
                    notify.publish_take_profit_completed(TakeProfitCompletedNotice {
                        order_id: order.id,
                        title: order.title.clone(),
                        actual_profit,
                        actual_cost,
                    });
                }
            } else {
                tracing::info!(order_id = order.id, claim_id = %claim_id, "take profit completed without fills; notification skipped");
            }
        }
        tracing::info!(order_id = order.id, action, claim_id = %claim_id, "position action released");
        Ok(true)
    }

    async fn release_failed_zero_leg_claim(
        &self,
        order_id: i64,
        intent: &str,
        claim_id: uuid::Uuid,
    ) -> Result<()> {
        let (total, _) = self
            .store
            .lifecycle_leg_counts(order_id, intent, claim_id)
            .await?;
        if total == 0 {
            self.store
                .release_lifecycle(order_id, intent, claim_id)
                .await?;
        }
        Ok(())
    }

    async fn market_identity_for_order(
        &self,
        order_id: i64,
        topic: &Topic,
    ) -> Result<MarketIdentity> {
        let stored = self.store.market_identities_for_order(order_id).await?;
        if stored.get(POLYMARKET).is_some() && stored.get(OUTCOME).is_some() {
            return Ok(stored);
        }

        let topic_identity = topic.market_identity()?;
        let missing_required_identity = [POLYMARKET, OUTCOME]
            .iter()
            .any(|platform| stored.get(platform).is_none());
        if missing_required_identity {
            // Passing the complete topic identity lets the store detect a conflicting existing
            // platform while adding only missing rows.
            let updated = self
                .store
                .backfill_market_identity(order_id, &topic_identity)
                .await?;
            tracing::info!(order_id, updated, "historical market identity backfilled");
        }
        let identity = self.store.market_identities_for_order(order_id).await?;
        identity.require(POLYMARKET)?;
        identity.require(OUTCOME)?;
        Ok(identity)
    }

    /// `settlement_pending` 只需要两平台的市场标识。按构造进入 pending 前必然已回填过，
    /// 但 `order_market_identities` 一旦缺行（人工清理、部分回滚），必须能像活跃分支那样
    /// 恢复历史 topic 并回填，否则该订单会每轮报错且永远无法推进。
    /// 无法解析时 fail-closed：计入 `unavailable` 并返回 `None`，不阻断整轮扫描。
    async fn settlement_identity_for_order(
        &self,
        order: &ArbOrderRow,
    ) -> Result<Option<MarketIdentity>> {
        let stored = self.store.market_identities_for_order(order.id).await?;
        if stored.get(POLYMARKET).is_some() && stored.get(OUTCOME).is_some() {
            return Ok(Some(stored));
        }
        let key = TopicKey::new(order.event_id, order.unified_index);
        let topic = match self.topics.read().await.get(&key).cloned() {
            Some(topic) => Some(topic),
            None => self.restore_historical_topic(order.id, key).await?,
        };
        let Some(topic) = topic else {
            self.stats.unavailable();
            tracing::warn!(order_id = order.id, topic = %key.as_str(), "settlement pending topic unavailable; fail closed");
            return Ok(None);
        };
        match self.market_identity_for_order(order.id, &topic).await {
            Ok(identity) => Ok(Some(identity)),
            Err(err) => {
                self.stats.unavailable();
                tracing::warn!(order_id = order.id, topic = %key.as_str(), error = %err, "settlement pending market identity unavailable; fail closed");
                Ok(None)
            }
        }
    }

    async fn settlement_gate(
        &self,
        order_id: i64,
        title: &str,
        identity: &MarketIdentity,
    ) -> Result<SettlementAccess> {
        self.stats.settlement_scan();
        let polymarket_market_id = identity.require(POLYMARKET)?;
        let outcome_market_id = identity.require(OUTCOME)?;
        let (pm, outcome) = tokio::join!(
            self.pm.settlement(polymarket_market_id),
            self.outcome.settlement(outcome_market_id)
        );
        // Polymarket 侧按 winner 构造，恒为 0/1；只有 Outcome 的 HIP-4 分数会打破 $1 假设。
        // 计数口径是观测次数而非去重订单数：结算确认后本轮扫描只会走到这里一次，
        // 但订单最终化前的后续轮次会重复计数。
        if let Ok(status @ OutcomeSettlement::Settled { payouts }) = &outcome {
            if status.is_fractional() {
                self.stats.outcome_fractional_settlement();
                tracing::warn!(
                    order_id,
                    outcome_market_id,
                    payouts = ?payouts,
                    "outcome settled with fractional payout; per-pair $1 valuation does not hold"
                );
            }
        }
        let (access, settled_source) = settlement_decision(pm.as_ref(), outcome.as_ref());
        if let Some(source) = settled_source {
            let evidence = json!({
                "polymarket": settlement_result_evidence(&pm),
                "outcome": settlement_result_evidence(&outcome),
            });
            // One confirmed venue is enough to stop trading, but both payouts are required before
            // terminal state and actuals are made durable. Until then the order is retried.
            if let (
                Ok(SettlementStatus::Settled {
                    payouts: pm_payouts,
                }),
                Ok(OutcomeSettlement::Settled {
                    payouts: outcome_payouts,
                }),
            ) = (&pm, &outcome)
            {
                let mut payouts = HashMap::new();
                for payout in pm_payouts {
                    payouts.insert(
                        (POLYMARKET.to_string(), payout.token_id.clone()),
                        payout.payout,
                    );
                }
                for payout in outcome_payouts {
                    payouts.insert(
                        (OUTCOME.to_string(), payout.token_id.clone()),
                        payout.payout,
                    );
                }
                let started = Instant::now();
                let result = self
                    .store
                    .finalize_position_settlement(
                        order_id,
                        "polymarket+outcome",
                        &evidence,
                        &payouts,
                    )
                    .await;
                if let Some((_, _, actual_profit)) = handle_settlement_finalization_result(
                    result,
                    &self.stats,
                    order_id,
                    polymarket_market_id,
                    outcome_market_id,
                    started.elapsed(),
                )? {
                    if let Some(notify) = &self.notify {
                        notify.publish_settlement(SettlementNotice {
                            order_id,
                            title: title.to_string(),
                            status: "settled (polymarket+outcome)".to_string(),
                            actual_profit: Some(actual_profit),
                        });
                    }
                }
            } else {
                match self
                    .store
                    .mark_settlement_pending(order_id, source, &evidence)
                    .await?
                {
                    Some((pending_since, true)) => {
                        self.stats.settlement_pending_entered();
                        tracing::warn!(
                            order_id,
                            source,
                            pending_since = %pending_since,
                            polymarket_error = ?pm.as_ref().err().map(ToString::to_string),
                            outcome_error = ?outcome.as_ref().err().map(ToString::to_string),
                            pm = pm.as_ref().ok().map(|status| status.kind()),
                            outcome = outcome.as_ref().ok().map(|status| status.kind()),
                            "settlement pending entered; all position trading stopped"
                        );
                    }
                    Some((pending_since, false)) => {
                        let pending_secs = chrono::Utc::now()
                            .signed_duration_since(pending_since)
                            .num_seconds()
                            .max(0);
                        tracing::warn!(
                            order_id,
                            source,
                            pending_since = %pending_since,
                            pending_secs,
                            polymarket_error = ?pm.as_ref().err().map(ToString::to_string),
                            outcome_error = ?outcome.as_ref().err().map(ToString::to_string),
                            pm = pm.as_ref().ok().map(|status| status.kind()),
                            outcome = outcome.as_ref().ok().map(|status| status.kind()),
                            "settlement pending; waiting for both venue payouts"
                        );
                    }
                    None => {
                        // UPDATE 未命中且不在 pending：可能是活动 claim、被并发置为终态，
                        // 也可能行已不存在。查一次真实状态，避免统一归因误导排查。
                        let state = self.store.position_state(order_id).await?;
                        tracing::warn!(
                            order_id,
                            source,
                            current_status = ?state.as_ref().map(|(status, _, _)| status.as_str()),
                            already_settled = ?state.as_ref().map(|(_, settled, _)| *settled),
                            lifecycle_action = ?state.as_ref().and_then(|(_, _, action)| action.as_deref()),
                            polymarket_error = ?pm.as_ref().err().map(ToString::to_string),
                            outcome_error = ?outcome.as_ref().err().map(ToString::to_string),
                            pm = pm.as_ref().ok().map(|status| status.kind()),
                            outcome = outcome.as_ref().ok().map(|status| status.kind()),
                            "settlement pending transition not applied; current order state is not eligible"
                        );
                    }
                }
            }
            return Ok(SettlementAccess::Stop);
        }
        if access == SettlementAccess::ReduceOnly {
            self.stats.unavailable();
            tracing::warn!(
                order_id,
                polymarket_error = ?pm.as_ref().err().map(ToString::to_string),
                outcome_error = ?outcome.as_ref().err().map(ToString::to_string),
                pm = pm.as_ref().ok().map(|status| status.kind()),
                outcome = outcome.as_ref().ok().map(|status| status.kind()),
                "settlement status unavailable; reduce-only access selected"
            );
        }
        Ok(access)
    }

    async fn confirm_take_profit(
        &self,
        topic: &Topic,
        positions: &crate::hedge::Positions,
        fees: &FeeContext,
        cached: &TakeProfitPlan,
    ) -> Result<Option<TakeProfitPlan>> {
        let pm_action = cached
            .actions
            .iter()
            .find(|a| a.platform == POLYMARKET)
            .ok_or_else(|| Error::msg("take profit plan missing polymarket action"))?;
        let out_action = cached
            .actions
            .iter()
            .find(|a| a.platform == OUTCOME)
            .ok_or_else(|| Error::msg("take profit plan missing outcome action"))?;
        let (pm, outcome) = tokio::join!(
            async {
                let started = Instant::now();
                let result = self.pm.rest_book(&pm_action.token_id).await;
                (result, Instant::now(), started.elapsed())
            },
            async {
                let started = Instant::now();
                let result = self.outcome.rest_book(&out_action.token_id).await;
                (result, Instant::now(), started.elapsed())
            }
        );
        let (pm_result, pm_at, pm_elapsed) = pm;
        let (out_result, out_at, out_elapsed) = outcome;
        let skew = book_recv_skew(pm_at, out_at);
        let (pm_bids, pm_asks, pm_ts) = match pm_result {
            Ok(book) => book,
            Err(err) => {
                tracing::warn!(
                    topic = %topic.key.as_str(),
                    token = %pm_action.token_id,
                    elapsed_ms = pm_elapsed.as_millis() as u64,
                    error = %err,
                    "take profit polymarket book failed"
                );
                self.stats.http_fail();
                return Ok(None);
            }
        };
        let (out_bids, out_asks, out_ts) = match out_result {
            Ok(book) => book,
            Err(err) => {
                tracing::warn!(
                    topic = %topic.key.as_str(),
                    token = %out_action.token_id,
                    elapsed_ms = out_elapsed.as_millis() as u64,
                    error = %err,
                    "take profit outcome book failed"
                );
                self.stats.http_fail();
                return Ok(None);
            }
        };
        if !book_recv_skew_ok(pm_at, out_at, HTTP_BOOK_SKEW_MAX) {
            tracing::warn!(
                topic = %topic.key.as_str(),
                pm_token = %pm_action.token_id,
                out_token = %out_action.token_id,
                pm_elapsed_ms = pm_elapsed.as_millis() as u64,
                out_elapsed_ms = out_elapsed.as_millis() as u64,
                skew_ms = skew.as_millis() as u64,
                "take profit http book receive skew exceeded 1s"
            );
            self.stats.skew();
            return Ok(None);
        }
        tracing::info!(
            topic = %topic.key.as_str(),
            pm_token = %pm_action.token_id,
            out_token = %out_action.token_id,
            pm_elapsed_ms = pm_elapsed.as_millis() as u64,
            out_elapsed_ms = out_elapsed.as_millis() as u64,
            skew_ms = skew.as_millis() as u64,
            "take profit rest books fetched"
        );
        let mut books = BookStore::default();
        books.replace_snapshot(
            POLYMARKET,
            &pm_action.token_id,
            pm_bids,
            pm_asks,
            pm_ts,
            pm_at,
        );
        books.replace_snapshot(
            OUTCOME,
            &out_action.token_id,
            out_bids,
            out_asks,
            out_ts,
            out_at,
        );
        if let Some(tick) = self.ensure_pm_tick(&pm_action.token_id).await {
            books.set_tick_size(POLYMARKET, &pm_action.token_id, tick);
        }
        Ok(plan_take_profit(
            topic,
            positions,
            &books,
            fees,
            self.cfg.take_profit_min_gain,
            Instant::now(),
            self.cfg.book_stale,
        ))
    }

    async fn execute_take_profit(
        &self,
        order_id: i64,
        claim_id: uuid::Uuid,
        topic: &Topic,
        fees: &FeeContext,
        plan: &TakeProfitPlan,
    ) -> Result<()> {
        self.ensure_trading_enabled(TradingIntent::TakeProfit)?;
        let pm_action = plan
            .actions
            .iter()
            .find(|a| a.platform == POLYMARKET)
            .ok_or_else(|| Error::msg("take profit plan missing polymarket action"))?;
        let out_action = plan
            .actions
            .iter()
            .find(|a| a.platform == OUTCOME)
            .ok_or_else(|| Error::msg("take profit plan missing outcome action"))?;
        let funder = self
            .hedge_pm_funder(order_id, &pm_action.token_id, OrderSide::Sell)
            .await?;
        let (pm_balance, out_balance) = tokio::join!(
            self.require_pm_token(&funder, &pm_action.token_id, pm_action.shares),
            self.require_outcome_token(&out_action.token_id, out_action.shares),
        );
        pm_balance?;
        out_balance?;

        let pm_req = self
            .take_profit_request(topic, pm_action, Some(&funder))
            .await;
        let out_req = self.take_profit_request(topic, out_action, None).await;
        let leg_ids = self
            .store
            .insert_legs_atomic(
                order_id,
                "take_profit",
                claim_id,
                &[
                    NewLeg {
                        platform: POLYMARKET,
                        token_id: &pm_action.token_id,
                        label: &pm_action.label,
                        side: "SELL",
                        intent: "take_profit",
                        funder: Some(&funder),
                        wallet: Some(&funder),
                        service: self.polymarket_service(&funder),
                        req_price: pm_action.cap_price,
                        req_shares: pm_action.shares,
                        req_fee: pm_action.fee,
                        client_order_id: None,
                    },
                    NewLeg {
                        platform: OUTCOME,
                        token_id: &out_action.token_id,
                        label: &out_action.label,
                        side: "SELL",
                        intent: "take_profit",
                        funder: None,
                        wallet: self.outcome.account_address(),
                        service: None,
                        req_price: out_action.cap_price,
                        req_shares: out_action.shares,
                        req_fee: out_action.fee,
                        client_order_id: None,
                    },
                ],
            )
            .await?;
        let [pm_leg, out_leg]: [i64; 2] = leg_ids
            .try_into()
            .map_err(|_| Error::msg("take profit must create exactly two legs"))?;
        let (pm_result, out_result) = tokio::join!(
            self.submit_pm(pm_leg, &funder, &pm_req, fees, TradingIntent::TakeProfit),
            self.submit_outcome(out_leg, &out_req, fees, TradingIntent::TakeProfit),
        );
        if submit_confirmed(&pm_result) {
            self.stats.take_profit_pm_ok();
        } else {
            self.stats.take_profit_pm_fail();
        }
        if submit_confirmed(&out_result) {
            self.stats.take_profit_out_ok();
        } else {
            self.stats.take_profit_out_fail();
        }
        pm_result?;
        out_result?;
        Ok(())
    }

    async fn take_profit_request(
        &self,
        topic: &Topic,
        action: &TakeProfitAction,
        funder: Option<&str>,
    ) -> MarketOrderRequest {
        let token = topic.token(&action.platform, &action.label);
        MarketOrderRequest {
            token_id: action.token_id.clone(),
            shares: action.shares,
            cap_price: action.cap_price,
            side: OrderSide::Sell,
            neg_risk: token.and_then(|t| t.neg_risk),
            tick_size: if action.platform == POLYMARKET {
                self.ensure_pm_tick(&action.token_id).await
            } else {
                None
            },
            asset_id: token.and_then(|t| t.asset_id),
            funder_address: funder.map(str::to_string),
        }
    }

    async fn complete_rebalance(&self, order_id: i64) -> Result<()> {
        // 先落最终实际值；失败时保留 pending/actived，下一轮仍可重试并避免漏通知。
        let (actual_cost, _actual_rev, actual_profit) =
            self.store.refresh_order_actuals(order_id).await?;
        self.store.mark_rebalance(order_id, "completed").await?;
        tracing::info!(
            order_id,
            actual_profit = %actual_profit,
            actual_cost = %actual_cost,
            "rebalance completed"
        );
        if let Some(notify) = &self.notify {
            notify.publish_order_actuals(order_id, actual_profit, actual_cost);
        }
        Ok(())
    }

    async fn execute_hedge(
        &self,
        order_id: i64,
        claim_id: uuid::Uuid,
        action: &crate::hedge::HedgeAction,
        fees: &FeeContext,
    ) -> Result<()> {
        self.ensure_trading_enabled(TradingIntent::Rebalance)?;
        let side = match action.side {
            HedgeSide::Buy => OrderSide::Buy,
            HedgeSide::Sell => OrderSide::Sell,
        };
        let buy = side == OrderSide::Buy;
        if below_venue_mins(
            &action.platform,
            buy,
            action.shares,
            action.shares * action.cap_price,
        ) {
            return Err(Error::msg(format!(
                "{} {} skipped, {} shares @ {} below min {} shares / {} notional",
                action.platform,
                side.as_str(),
                action.shares,
                action.cap_price,
                min_trade_amount(&action.platform, buy),
                min_trade_cost(&action.platform, buy)
            )));
        }
        if action.platform == POLYMARKET {
            let funder = self
                .hedge_pm_funder(order_id, &action.token_id, side)
                .await?;
            tracing::info!(
                order_id,
                funder = %funder,
                side = side.as_str(),
                token = %action.token_id,
                "hedge pinned to original polymarket funder"
            );
            if side == OrderSide::Sell {
                self.require_pm_token(&funder, &action.token_id, action.shares)
                    .await?;
            } else {
                self.require_pm_usdc(&funder, action.shares * action.cap_price)
                    .await?;
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
                .insert_leg_for_claim(
                    order_id,
                    "rebalance",
                    claim_id,
                    &NewLeg {
                        platform: POLYMARKET,
                        token_id: &action.token_id,
                        label: &action.label,
                        side: side.as_str(),
                        intent: "rebalance",
                        funder: Some(&funder),
                        wallet: Some(&funder),
                        service: self.polymarket_service(&funder),
                        req_price: action.cap_price,
                        req_shares: action.shares,
                        req_fee: action.fee,
                        client_order_id: None,
                    },
                )
                .await?;
            self.submit_pm(leg_id, &funder, &req, fees, TradingIntent::Rebalance)
                .await?;
            Ok(())
        } else {
            if side == OrderSide::Sell {
                self.require_outcome_token(&action.token_id, action.shares)
                    .await?;
            } else {
                self.require_outcome_usdc(
                    action.shares * action.cap_price,
                    &format!("rebalance orderId={order_id}"),
                )
                .await?;
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
                .insert_leg_for_claim(
                    order_id,
                    "rebalance",
                    claim_id,
                    &NewLeg {
                        platform: OUTCOME,
                        token_id: &action.token_id,
                        label: &action.label,
                        side: side.as_str(),
                        intent: "rebalance",
                        funder: None,
                        wallet: self.outcome.account_address(),
                        service: None,
                        req_price: action.cap_price,
                        req_shares: action.shares,
                        req_fee: action.fee,
                        client_order_id: None,
                    },
                )
                .await?;
            self.submit_outcome(leg_id, &req, fees, TradingIntent::Rebalance)
                .await?;
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

    async fn refresh_hedge_books(&self, order_id: i64, tokens: &[(String, String)]) {
        let now = Instant::now();
        let stale = self.cfg.book_stale;
        let need: Vec<(String, String)> = {
            let books = self.books.lock().await;
            tokens
                .iter()
                .filter(|(platform, token_id)| {
                    books
                        .get(platform, token_id)
                        .map(|book| !book.is_fresh(stale, now))
                        .unwrap_or(true)
                })
                .cloned()
                .collect()
        };
        if need.is_empty() {
            return;
        }
        let fetches = need.into_iter().map(|(platform, token_id)| async move {
            let started = Instant::now();
            let result = if platform == POLYMARKET {
                self.pm.rest_book(&token_id).await
            } else {
                self.outcome.rest_book(&token_id).await
            };
            (
                platform,
                token_id,
                result,
                Instant::now(),
                started.elapsed(),
            )
        });
        for (platform, token_id, result, received_at, elapsed) in
            futures_util::future::join_all(fetches).await
        {
            match result {
                Ok((bids, asks, exchange_ts_ms)) => {
                    let applied = self.books.lock().await.replace_snapshot(
                        &platform,
                        &token_id,
                        bids,
                        asks,
                        exchange_ts_ms,
                        received_at,
                    );
                    tracing::info!(
                        order_id,
                        %platform,
                        token = %token_id,
                        exchange_ts_ms,
                        applied,
                        elapsed_ms = elapsed.as_millis() as u64,
                        "hedge rest book fetched"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        order_id,
                        %platform,
                        token = %token_id,
                        elapsed_ms = elapsed.as_millis() as u64,
                        error = %err,
                        "hedge rest book failed"
                    );
                }
            }
        }
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
            if let Some(tick) = books
                .get(POLYMARKET, token_id)
                .and_then(|book| book.tick_size)
            {
                return Some(tick);
            }
        }
        match self.pm.fetch_tick_size(token_id).await {
            Ok(tick) => {
                self.books
                    .lock()
                    .await
                    .set_tick_size(POLYMARKET, token_id, tick);
                Some(tick)
            }
            Err(err) => {
                tracing::warn!(token_id, error = %err, "polymarket tick_size fetch failed");
                None
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettlementAccess {
    All,
    ReduceOnly,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TradingIntent {
    Arbitrage,
    Rebalance,
    TakeProfit,
}

impl TradingIntent {
    fn enabled(self, cfg: &Config) -> bool {
        workflow_switch_enabled(
            cfg.enable_arb,
            cfg.enable_rebalance,
            cfg.enable_take_profit,
            self,
        )
    }

    fn env_name(self) -> &'static str {
        match self {
            Self::Arbitrage => "ENABLE_ARB",
            Self::Rebalance => "ENABLE_REBALANCE",
            Self::TakeProfit => "ENABLE_TAKE_PROFIT",
        }
    }
}

fn workflow_switch_enabled(
    enable_arb: bool,
    enable_rebalance: bool,
    enable_take_profit: bool,
    intent: TradingIntent,
) -> bool {
    match intent {
        TradingIntent::Arbitrage => enable_arb,
        TradingIntent::Rebalance => enable_rebalance,
        TradingIntent::TakeProfit => enable_take_profit,
    }
}

fn ensure_trading_submission_enabled(enabled: bool, intent: TradingIntent) -> Result<()> {
    if enabled {
        Ok(())
    } else {
        Err(Error::msg(format!("{}=false", intent.env_name())))
    }
}

fn action_allowed_for_rebalance(
    action: &crate::hedge::HedgeAction,
    access: SettlementAccess,
) -> bool {
    match action.side {
        HedgeSide::Sell => access != SettlementAccess::Stop,
        HedgeSide::Buy => access == SettlementAccess::All,
    }
}

fn has_position(positions: &crate::hedge::Positions) -> bool {
    positions
        .values()
        .flat_map(|labels| labels.values())
        .any(|qty| !qty.is_zero())
}

fn take_profit_book_tokens(
    topic: &Topic,
    positions: &crate::hedge::Positions,
) -> Vec<(String, String)> {
    let labels = topic.labels();
    if labels.len() != 2 {
        return Vec::new();
    }
    let mut seen = HashSet::new();
    let mut tokens = Vec::new();
    for (pm_label, out_label) in [(&labels[0], &labels[1]), (&labels[1], &labels[0])] {
        if position_qty(positions, POLYMARKET, pm_label) <= Decimal::ZERO
            || position_qty(positions, OUTCOME, out_label) <= Decimal::ZERO
        {
            continue;
        }
        for (platform, label) in [(POLYMARKET, pm_label), (OUTCOME, out_label)] {
            let Some(token) = topic.token(platform, label) else {
                continue;
            };
            let key = (token.platform.clone(), token.token_id.clone());
            if seen.insert(key.clone()) {
                tokens.push(key);
            }
        }
    }
    tokens
}

fn position_qty(positions: &crate::hedge::Positions, platform: &str, label: &str) -> Decimal {
    positions
        .get(platform)
        .and_then(|by_label| {
            by_label.get(label).copied().or_else(|| {
                by_label
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case(label))
                    .map(|(_, qty)| *qty)
            })
        })
        .unwrap_or(Decimal::ZERO)
}

fn settlement_only_scan(position_status: &str) -> bool {
    position_status == "settlement_pending"
}

fn settlement_sweep_due(last_sweep: Option<Instant>, now: Instant, interval: Duration) -> bool {
    match last_sweep {
        // 首轮立即清扫，避免重启后 pending 订单要等满一个间隔才被看到。
        None => true,
        Some(last) => now.saturating_duration_since(last) >= interval,
    }
}

/// 批次非空时游标推进到最后一行；整个扫描集合为空时归零，让后续新增订单从头被扫到。
async fn advance_scan_cursor(cursor: &Mutex<i64>, rows: &[ArbOrderRow], after_id: i64) {
    if let Some(last) = rows.last() {
        *cursor.lock().await = last.id;
    } else if after_id > 0 {
        // The store already attempted a wraparound query; reset the persisted cursor state
        // when the entire scan set is empty so later inserts start from the beginning.
        *cursor.lock().await = 0;
    }
}

fn record_take_profit_not_submitted(stats: &MinuteStats, disabled: bool) {
    if disabled {
        stats.take_profit_disabled();
    } else {
        stats.take_profit_cancelled();
    }
}

fn handle_settlement_finalization_result(
    result: Result<Option<(Decimal, Decimal, Decimal)>>,
    stats: &MinuteStats,
    order_id: i64,
    polymarket_market_id: &str,
    outcome_market_id: &str,
    elapsed: Duration,
) -> Result<Option<(Decimal, Decimal, Decimal)>> {
    match &result {
        Ok(Some(_)) => stats.settled(),
        Ok(None) => tracing::warn!(
            order_id,
            polymarket_market_id,
            outcome_market_id,
            "settlement finalization not applied; current order state is not finalizable"
        ),
        Err(err) => {
            stats.settlement_finalize_fail();
            tracing::error!(
                service = "postgres",
                operation = "finalize_position_settlement",
                order_id,
                polymarket_market_id,
                outcome_market_id,
                elapsed_ms = elapsed.as_millis() as u64,
                error = %err,
                "settlement finalization failed"
            );
        }
    }
    result
}

fn settlement_decision<E1, E2>(
    polymarket: std::result::Result<&SettlementStatus, E1>,
    outcome: std::result::Result<&OutcomeSettlement, E2>,
) -> (SettlementAccess, Option<&'static str>) {
    if matches!(polymarket, Ok(SettlementStatus::Settled { .. })) {
        return (SettlementAccess::Stop, Some(POLYMARKET));
    }
    if matches!(outcome, Ok(OutcomeSettlement::Settled { .. })) {
        return (SettlementAccess::Stop, Some(OUTCOME));
    }
    if matches!(polymarket, Ok(SettlementStatus::TradableUnsettled))
        && matches!(outcome, Ok(OutcomeSettlement::Unsettled))
    {
        (SettlementAccess::All, None)
    } else {
        (SettlementAccess::ReduceOnly, None)
    }
}

fn settlement_result_evidence<T, E>(result: &std::result::Result<T, E>) -> Value
where
    T: serde::Serialize,
    E: std::fmt::Display,
{
    match result {
        Ok(status) => json!({"status": status}),
        Err(err) => json!({"error": err.to_string()}),
    }
}

const HTTP_BOOK_SKEW_MAX: Duration = Duration::from_secs(1);

fn fmt_px(price: Option<Decimal>) -> String {
    price
        .map(|px| px.normalize().to_string())
        .unwrap_or_else(|| "-".into())
}

pub fn book_recv_skew(a: Instant, b: Instant) -> Duration {
    a.saturating_duration_since(b)
        .max(b.saturating_duration_since(a))
}

pub fn book_recv_skew_ok(a: Instant, b: Instant, max: Duration) -> bool {
    book_recv_skew(a, b) <= max
}

async fn reconcile_pm_page(
    pm: &PolymarketVenue,
    store: &Store,
    leg: &crate::store::LegRow,
) -> Result<Option<(crate::store::LegRow, OrderPoll, FillPage)>> {
    let funder = leg
        .funder_address
        .as_deref()
        .ok_or_else(|| Error::msg("missing funder"))?;
    let Some(selector) = leg
        .third_order_id
        .as_deref()
        .or(leg.client_order_id.as_deref())
    else {
        return Ok(None);
    };
    let poll = pm.poll_order(funder, selector).await?;
    let Some(current) = store.record_order_poll(leg, &poll).await? else {
        return Ok(None);
    };
    let Some(submitted) = current.submitted_at else {
        store
            .record_reconciliation_wait(&current, "submission_time_missing")
            .await?;
        tracing::debug!(
            service = "polymarket",
            operation = "reconcile",
            leg_id = leg.id,
            "missing submission time for fixed trade window"
        );
        return Ok(None);
    };
    let after = submitted.timestamp();
    let before = after
        .checked_add(300)
        .ok_or_else(|| Error::msg("polymarket trade window overflow"))?;
    let oid = poll
        .order_id
        .as_deref()
        .ok_or_else(|| Error::msg("missing order selector"))?;
    let progress = current
        .last_order_info
        .as_ref()
        .and_then(|info| info.get("fill_progress"))
        .cloned()
        .unwrap_or(Value::Null);
    // 缺失 order 不等于零成交；固定五分钟只限定检索范围，不限制确认等待时长。
    let page = pm
        .poll_trade_page(funder, &leg.token_id, oid, after, before, &progress)
        .await?;
    Ok(Some((current, poll, page)))
}

async fn persist_submit(
    store: &Store,
    leg_id: i64,
    platform: &str,
    side: OrderSide,
    result: &SubmitResult,
    _fees: &FeeContext,
    response: &serde_json::Value,
) -> Result<()> {
    let (status, oid, evidence) = match result {
        SubmitResult::Ack {
            order_id,
            envelope,
            making,
            taking,
            avg_px,
            ..
        } => {
            let expected = ack_fill(platform, side, *making, *taking, *avg_px, envelope)
                .map(|(shares, _)| shares);
            (
                "actived",
                Some(order_id.as_str()),
                json!({"kind":"ack","expected_shares":expected}),
            )
        }
        SubmitResult::NoMatch { message, .. } => (
            "cancelled",
            None,
            json!({"kind":"no_match","message":message}),
        ),
        SubmitResult::Unknown {
            order_id, message, ..
        } => (
            "unknown",
            order_id.as_deref(),
            json!({"kind":"unknown","message":message}),
        ),
        SubmitResult::Failed {
            status, message, ..
        } => (
            "failed",
            None,
            json!({"kind":"rejected","http_status":status,"message":message}),
        ),
    };
    store
        .record_submission(leg_id, status, oid, &evidence, response)
        .await
}

pub fn ack_fill(
    platform: &str,
    side: OrderSide,
    making: Option<Decimal>,
    taking: Option<Decimal>,
    avg_px: Option<Decimal>,
    envelope: &serde_json::Value,
) -> Option<(Decimal, Decimal)> {
    if platform == POLYMARKET {
        pm_fak_fill(side, making, taking)
    } else {
        let price = avg_px.or_else(|| {
            envelope
                .get("price")
                .and_then(crate::platforms::parse_decimal)
        });
        ioc_fill(taking, price)
    }
}

fn submit_confirmed(result: &Result<SubmitResult>) -> bool {
    matches!(result, Ok(SubmitResult::Ack { .. }))
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
        Ok(SubmitResult::Failed {
            status, message, ..
        }) => Some(format!("HTTP {status} {message}")),
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

fn needs_pm_fee_snapshot(fill: &TradeFill) -> bool {
    fill.finality == crate::platforms::FillFinality::Confirmed
        && fill.fee.is_none()
        && fill.raw.get("role").and_then(Value::as_str) != Some("maker")
        && fill.raw.get("fee_calculation").is_none()
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
    store.complete_orders().await
}

fn resolve_hedge_pm_funder(
    sell: bool,
    token_buy_funder: Option<String>,
    order_funder: Option<String>,
) -> Result<String> {
    if sell {
        if let Some(funder) = token_buy_funder {
            return Ok(funder);
        }
    }
    order_funder.ok_or_else(|| Error::msg("hedge requires original polymarket funder"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calc::estimate_taker_fee;
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
            fee: Some(d("0.01")),
            fee_rate_bps: None,
            fee_token: Some("USDC".into()),
            finality: crate::platforms::FillFinality::Confirmed,
            raw: json!({"oid": order_id}),
        }
    }

    #[tokio::test]
    #[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
    async fn pm_missing_order_execution_fetches_window_and_finalizes_trades() {
        use crate::platforms::polymarket::tests::poll_stub;
        use sqlx::postgres::PgPoolOptions;
        let uri = std::env::var("APP_POSTGRES_URI").expect("requires a test database");
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&uri)
            .await
            .unwrap();
        let schema = format!("pm_window_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let search_path = schema.clone();
        let exercised: anyhow::Result<()> = async {
            let pool = PgPoolOptions::new().max_connections(2).after_connect(move |conn, _| {
                let path = search_path.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('search_path', $1, false)").bind(path).execute(conn).await?;
                    Ok(())
                })
            }).connect(&uri).await?;
            let store = Store { pool };
            store.migrate().await?;
            for (http_status, body, missing_time) in [(404, json!({}), false), (200, Value::Null, false), (404, json!({}), true)] {
                let identity = MarketIdentity::new(POLYMARKET, "test-condition")?;
                let (_, ids) = store.insert_actived_order_with_legs(
                    TopicKey::new(uuid::Uuid::new_v4(), 0), &identity, "PM window test", "PM window test", None,
                    d("10"), d("1"), d("9"), &json!([]), &[NewLeg {
                        platform: POLYMARKET, token_id: "yes", label: "yes", side: "BUY", intent: "arb_buy",
                        funder: Some("test-funder"), wallet: None, service: None,
                        req_price: d("0.5"), req_shares: d("10"), req_fee: Decimal::ZERO, client_order_id: None,
                    }],
                ).await?;
                let oid = format!("taker-{}", ids[0]);
                store.insert_envelope(ids[0], &oid, &json!({}), &json!({"test":true}), None).await?;
                if missing_time {
                    sqlx::query("UPDATE legs SET submitted_at=NULL WHERE id=$1").bind(ids[0]).execute(&store.pool).await?;
                }
                let leg = store.open_legs().await?.into_iter().find(|leg| leg.id == ids[0]).unwrap();
                let mut responses = vec![(http_status, body)];
                if !missing_time {
                    responses.push((200, json!({"data":[{
                        "id":"confirmed-trade","taker_order_id":oid,"asset_id":"yes",
                        "size":"6","price":"0.5","status":"CONFIRMED",
                        "fee_amount":"0.01","fee_token":"USDC","maker_orders":[]
                    }], "next_cursor":"LTE="})));
                }
                let (pm, server) = poll_stub(responses).await;
                let page_result = reconcile_pm_page(&pm, &store, &leg).await?;
                let requests = tokio::time::timeout(Duration::from_secs(5), server).await??;
                if missing_time {
                    assert!(page_result.is_none());
                    assert_eq!(requests.len(), 1);
                    let info: Value = sqlx::query_scalar("SELECT last_order_info FROM legs WHERE id=$1")
                        .bind(leg.id).fetch_one(&store.pool).await?;
                    assert_eq!(info["waiting_reason"], "submission_time_missing");
                    continue;
                }
                assert_eq!(requests.len(), 2);
                assert!(requests[0].starts_with(&format!("GET /data/order/{oid} ")));
                let after = leg.submitted_at.unwrap().timestamp();
                let url = url::Url::parse(&format!("http://localhost{}", requests[1].split_whitespace().nth(1).unwrap()))?;
                let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
                assert_eq!(query.get("after"), Some(&after.to_string()));
                assert_eq!(query.get("before"), Some(&(after + 300).to_string()));
                let (current, poll, page) = page_result.unwrap();
                assert!(!poll.found);
                assert_eq!(current.submitted_at, leg.submitted_at);
                assert_eq!(current.third_order_id.as_deref(), Some(oid.as_str()));
                let evidence = FillEvidence {
                    poll, page_complete: page.complete, history_complete: page.history_complete,
                    expected_shares: None,
                    pm_scan: Some(serde_json::from_value(page.progress.clone())?),
                };
                let matched: Vec<_> = filter_trades(&page.fills, Some(&oid), None).into_iter().cloned().collect();
                assert!(matches!(store.record_reconciliation(&current, &matched, &evidence, &page.progress).await?,
                    LegResolution::Terminal { status:"matched", shares, .. } if shares == d("6")));
            }
            store.pool.close().await;
            Ok(())
        }.await;
        let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&admin)
            .await;
        admin.close().await;
        cleanup.unwrap();
        exercised.unwrap();
    }

    #[test]
    fn workflow_switch_matrix_is_independent() {
        for mask in 0_u8..8 {
            let arb = mask & 1 != 0;
            let rebalance = mask & 2 != 0;
            let take_profit = mask & 4 != 0;
            assert_eq!(
                workflow_switch_enabled(arb, rebalance, take_profit, TradingIntent::Arbitrage),
                arb
            );
            assert_eq!(
                workflow_switch_enabled(arb, rebalance, take_profit, TradingIntent::Rebalance),
                rebalance
            );
            assert_eq!(
                workflow_switch_enabled(arb, rebalance, take_profit, TradingIntent::TakeProfit),
                take_profit
            );
        }
    }

    #[test]
    fn workflow_switches_block_their_own_submission_kind() {
        for intent in [
            TradingIntent::Arbitrage,
            TradingIntent::Rebalance,
            TradingIntent::TakeProfit,
        ] {
            let err = ensure_trading_submission_enabled(false, intent).unwrap_err();
            assert!(err.to_string().contains(intent.env_name()));
            assert!(ensure_trading_submission_enabled(true, intent).is_ok());
        }
    }

    #[test]
    fn rebalance_permissions_follow_settlement_access() {
        let action = |side| crate::hedge::HedgeAction {
            platform: POLYMARKET.into(),
            token_id: "token".into(),
            label: "yes".into(),
            side,
            shares: Decimal::ONE,
            cap_price: Decimal::ONE,
            fee: Decimal::ZERO,
            marginal_value: Decimal::ZERO,
        };
        assert!(action_allowed_for_rebalance(
            &action(HedgeSide::Sell),
            SettlementAccess::ReduceOnly,
        ));
        assert!(!action_allowed_for_rebalance(
            &action(HedgeSide::Buy),
            SettlementAccess::ReduceOnly,
        ));
        assert!(action_allowed_for_rebalance(
            &action(HedgeSide::Buy),
            SettlementAccess::All,
        ));
        assert!(action_allowed_for_rebalance(
            &action(HedgeSide::Sell),
            SettlementAccess::All,
        ));
        assert!(!action_allowed_for_rebalance(
            &action(HedgeSide::Sell),
            SettlementAccess::Stop,
        ));
    }

    #[test]
    fn matches_trade_by_order_id_only() {
        let trades = vec![fill("t1", "oid-1", "3", &[]), fill("t2", "oid-2", "9", &[])];
        let matched = filter_trades(&trades, Some("oid-1"), None);
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].trade_id, "t1");
        assert!(filter_trades(&trades, None, None).is_empty());
        assert!(filter_trades(&trades, Some("oid-1"), None)[0].matches(Some("oid-1"), None));
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
        // 单腿 matched + 另一腿 cancelled：无 open 腿且有正成交 → completed
        assert_eq!(parent_terminal_status(false, true), Some("completed"));
    }

    #[test]
    fn ack_fill_uses_avg_px_not_cap() {
        let (shares, price) = ack_fill(
            OUTCOME,
            OrderSide::Buy,
            None,
            Some(d("5")),
            Some(d("0.55")),
            &json!({"price": "0.60"}),
        )
        .unwrap();
        assert_eq!(shares.to_string(), "5");
        assert_eq!(price.to_string(), "0.55");
        let cap_only = ack_fill(
            OUTCOME,
            OrderSide::Buy,
            None,
            Some(d("5")),
            None,
            &json!({"price": "0.60"}),
        )
        .unwrap();
        assert_eq!(cap_only.1.to_string(), "0.60");
    }

    #[test]
    fn fill_fee_uses_actual_shares_and_price() {
        let fees = FeeContext {
            polymarket_fee_rate: d("0.07"),
            outcome_taker_rate: d("0.00035"),
        };
        let (shares, price) = ack_fill(
            OUTCOME,
            OrderSide::Buy,
            None,
            Some(d("30")),
            Some(d("0.949")),
            &json!({}),
        )
        .unwrap();
        let fee = estimate_taker_fee(OUTCOME, shares, price, &fees);
        assert_eq!(fee, d("30") * d("0.949") * d("0.00035"));
        let pm_fee = estimate_taker_fee(POLYMARKET, d("30"), d("0.40"), &fees);
        assert_eq!(pm_fee, d("30") * d("0.07") * d("0.40") * d("0.60"));
    }

    #[test]
    fn book_recv_skew_allows_exact_one_second() {
        let a = Instant::now();
        let b = a + Duration::from_secs(1);
        assert!(book_recv_skew_ok(a, b, HTTP_BOOK_SKEW_MAX));
        assert!(book_recv_skew_ok(b, a, HTTP_BOOK_SKEW_MAX));
    }

    #[test]
    fn book_recv_skew_rejects_over_one_second() {
        let a = Instant::now();
        let b = a + Duration::from_secs(1) + Duration::from_millis(1);
        assert!(!book_recv_skew_ok(a, b, HTTP_BOOK_SKEW_MAX));
        assert!(!book_recv_skew_ok(b, a, HTTP_BOOK_SKEW_MAX));
    }

    #[test]
    fn hedge_buy_uses_order_funder_not_token_buy() {
        let funder =
            resolve_hedge_pm_funder(false, Some("0xtoken".into()), Some("0xorder".into())).unwrap();
        assert_eq!(funder, "0xorder");
    }

    #[test]
    fn hedge_sell_prefers_token_buy_funder() {
        let funder =
            resolve_hedge_pm_funder(true, Some("0xtoken".into()), Some("0xorder".into())).unwrap();
        assert_eq!(funder, "0xtoken");
    }

    #[test]
    fn hedge_sell_falls_back_to_order_funder() {
        let funder = resolve_hedge_pm_funder(true, None, Some("0xorder".into())).unwrap();
        assert_eq!(funder, "0xorder");
    }

    #[test]
    fn hedge_rejects_without_original_funder() {
        assert!(resolve_hedge_pm_funder(false, Some("0xtoken".into()), None).is_err());
        assert!(resolve_hedge_pm_funder(true, None, None).is_err());
    }

    #[test]
    fn take_profit_submit_success_requires_ack() {
        let envelope = json!({});
        let ack = Ok(SubmitResult::Ack {
            order_id: "order-1".into(),
            order_hash: "h".into(),
            envelope: envelope.clone(),
            making: Some(d("1")),
            taking: Some(d("0.5")),
            avg_px: Some(d("0.5")),
        });
        let no_match = Ok(SubmitResult::NoMatch {
            order_hash: "h".into(),
            envelope: envelope.clone(),
            message: "no fill".into(),
        });
        let unknown = Ok(SubmitResult::Unknown {
            order_id: None,
            order_hash: "h".into(),
            envelope: envelope.clone(),
            message: "transport uncertain".into(),
        });
        let failed = Ok(SubmitResult::Failed {
            order_hash: "h".into(),
            envelope,
            status: 400,
            message: "rejected".into(),
        });
        assert!(submit_confirmed(&ack));
        assert!(!submit_confirmed(&no_match));
        assert!(!submit_confirmed(&unknown));
        assert!(!submit_confirmed(&failed));
        assert!(!submit_confirmed(&Err(Error::msg("submit error"))));
    }

    #[test]
    fn settlement_pending_uses_settlement_only_scan() {
        assert!(settlement_only_scan("settlement_pending"));
        for status in ["watching", "closed", "settled"] {
            assert!(!settlement_only_scan(status));
        }
    }

    #[test]
    fn settlement_sweep_runs_first_then_only_after_the_full_interval() {
        let interval = Duration::from_secs(60);
        let start = Instant::now();
        assert!(settlement_sweep_due(None, start, interval));
        assert!(!settlement_sweep_due(Some(start), start, interval));
        assert!(!settlement_sweep_due(
            Some(start),
            start + Duration::from_secs(59),
            interval
        ));
        assert!(settlement_sweep_due(
            Some(start),
            start + interval,
            interval
        ));
        // 时钟回拨不应把等待态订单变成每轮全速轮询。
        assert!(!settlement_sweep_due(
            Some(start + interval),
            start,
            interval
        ));
    }

    #[tokio::test]
    async fn scan_cursor_advances_to_last_row_and_resets_on_empty_set() {
        let cursor = Mutex::new(0);
        let rows = vec![scan_row(7), scan_row(19)];
        advance_scan_cursor(&cursor, &rows, 0).await;
        assert_eq!(*cursor.lock().await, 19);

        // 空批次意味着 store 的回绕重查也没命中，游标必须归零。
        advance_scan_cursor(&cursor, &[], 19).await;
        assert_eq!(*cursor.lock().await, 0);

        // 已经在起点时空批次不做无谓写入，也不会把游标推成负数。
        advance_scan_cursor(&cursor, &[], 0).await;
        assert_eq!(*cursor.lock().await, 0);
    }

    fn scan_row(id: i64) -> ArbOrderRow {
        ArbOrderRow {
            id,
            title: "t".into(),
            event_id: uuid::Uuid::nil(),
            unified_index: 0,
            status: "completed".into(),
            rebalance_status: "completed".into(),
            position_status: "watching".into(),
            lifecycle_action: None,
            lifecycle_claim_id: None,
            lifecycle_claimed_at: None,
            settlement_source: None,
            settlement_result: None,
            settled_at: None,
            settlement_pending_since: None,
            settlement_pending_source: None,
            settlement_pending_result: None,
        }
    }

    #[test]
    fn take_profit_disabled_and_cancelled_metrics_are_exclusive() {
        let stats = MinuteStats::new();
        record_take_profit_not_submitted(&stats, true);
        let disabled = stats.snapshot_and_reset();
        assert_eq!(disabled.take_profit_disabled, 1);
        assert_eq!(disabled.take_profit_cancelled, 0);

        record_take_profit_not_submitted(&stats, false);
        let cancelled = stats.snapshot_and_reset();
        assert_eq!(cancelled.take_profit_disabled, 0);
        assert_eq!(cancelled.take_profit_cancelled, 1);
    }

    #[test]
    fn settlement_finalization_results_preserve_values_and_count_once() {
        let stats = MinuteStats::new();
        let actuals = (Decimal::new(95, 1), Decimal::from(10), Decimal::new(5, 1));
        let observed = |result| {
            handle_settlement_finalization_result(
                result,
                &stats,
                42,
                "pm-market",
                "95",
                Duration::from_millis(12),
            )
        };

        assert_eq!(observed(Ok(Some(actuals))).unwrap(), Some(actuals));
        let success = stats.snapshot_and_reset();
        assert_eq!(success.settled, 1);
        assert_eq!(success.settlement_finalize_fail, 0);
        assert_eq!(success.exec_err, 0);

        assert_eq!(observed(Ok(None)).unwrap(), None);
        assert_eq!(stats.snapshot_and_reset(), Default::default());

        let err = observed(Err(Error::Sqlx(sqlx::Error::PoolClosed))).unwrap_err();
        assert!(matches!(err, Error::Sqlx(sqlx::Error::PoolClosed)));
        let failure = stats.snapshot_and_reset();
        assert_eq!(failure.settled, 0);
        assert_eq!(failure.settlement_finalize_fail, 1);
        assert_eq!(failure.exec_err, 0);

        let err = observed(Err(Error::msg(
            "missing settlement payout for outcome:#5160",
        )))
        .unwrap_err();
        assert!(matches!(err, Error::Msg(ref message)
            if message == "missing settlement payout for outcome:#5160"));
        assert_eq!(stats.snapshot_and_reset().settlement_finalize_fail, 1);
        assert_eq!(stats.snapshot_and_reset(), Default::default());
    }

    #[test]
    fn settlement_decision_requires_both_known_unsettled_for_full_access() {
        let pm = SettlementStatus::TradableUnsettled;
        let outcome = OutcomeSettlement::Unsettled;
        assert_eq!(
            settlement_decision::<(), ()>(Ok(&pm), Ok(&outcome)),
            (SettlementAccess::All, None)
        );
        let unavailable = SettlementStatus::Unavailable;
        assert_eq!(
            settlement_decision::<(), ()>(Ok(&unavailable), Ok(&outcome)),
            (SettlementAccess::ReduceOnly, None)
        );
    }

    #[test]
    fn settlement_decision_stops_on_one_successful_settlement_despite_other_error() {
        let pm_settled = SettlementStatus::Settled { payouts: vec![] };
        let outcome_settled = OutcomeSettlement::Settled { payouts: vec![] };
        assert_eq!(
            settlement_decision::<(), &str>(Ok(&pm_settled), Err("outcome timeout")),
            (SettlementAccess::Stop, Some(POLYMARKET))
        );
        assert_eq!(
            settlement_decision::<&str, ()>(Err("pm timeout"), Ok(&outcome_settled)),
            (SettlementAccess::Stop, Some(OUTCOME))
        );
        assert_eq!(
            settlement_decision::<&str, &str>(Err("pm timeout"), Err("outcome timeout")),
            (SettlementAccess::ReduceOnly, None)
        );
    }

    #[test]
    fn settlement_decision_prefers_polymarket_when_both_settled() {
        let pm = SettlementStatus::Settled { payouts: vec![] };
        let outcome = OutcomeSettlement::Settled { payouts: vec![] };
        assert_eq!(
            settlement_decision::<(), ()>(Ok(&pm), Ok(&outcome)),
            (SettlementAccess::Stop, Some(POLYMARKET))
        );
    }

    fn take_profit_topic() -> Topic {
        use crate::domain::TokenRef;
        use uuid::Uuid;

        let token = |platform: &str, token_id: &str, label: &str| TokenRef {
            platform: platform.into(),
            token_id: token_id.into(),
            label: label.into(),
            option_id: "1".into(),
            condition_id: None,
            asset_id: None,
            side_index: None,
            neg_risk: None,
            fees_enabled: None,
            fee_rate: None,
        };
        Topic {
            key: TopicKey::new(Uuid::nil(), 0),
            title: "topic".into(),
            market_title: "market".into(),
            end_date: None,
            tokens: vec![
                token(POLYMARKET, "pm-yes", "yes"),
                token(POLYMARKET, "pm-no", "no"),
                token(OUTCOME, "out-yes", "yes"),
                token(OUTCOME, "out-no", "no"),
                token(POLYMARKET, "pm-yes", "YES"),
            ],
        }
    }

    #[test]
    fn take_profit_tokens_only_include_positive_complementary_pair_and_deduplicate() {
        let positions = crate::hedge::Positions::from([
            (
                POLYMARKET.into(),
                HashMap::from([("YES".into(), d("3")), ("no".into(), Decimal::ZERO)]),
            ),
            (
                OUTCOME.into(),
                HashMap::from([("yes".into(), d("4")), ("no".into(), d("2"))]),
            ),
        ]);

        assert_eq!(
            take_profit_book_tokens(&take_profit_topic(), &positions),
            vec![
                (POLYMARKET.into(), "pm-yes".into()),
                (OUTCOME.into(), "out-no".into()),
            ]
        );
    }

    #[test]
    fn take_profit_tokens_include_both_positive_complementary_directions_once() {
        let positions = crate::hedge::Positions::from([
            (
                POLYMARKET.into(),
                HashMap::from([("yes".into(), d("3")), ("no".into(), d("1"))]),
            ),
            (
                OUTCOME.into(),
                HashMap::from([("yes".into(), d("4")), ("no".into(), d("2"))]),
            ),
        ]);
        let tokens = take_profit_book_tokens(&take_profit_topic(), &positions);

        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.iter().collect::<HashSet<_>>().len(), 4);
        assert!(tokens.contains(&(POLYMARKET.into(), "pm-yes".into())));
        assert!(tokens.contains(&(OUTCOME.into(), "out-no".into())));
        assert!(tokens.contains(&(POLYMARKET.into(), "pm-no".into())));
        assert!(tokens.contains(&(OUTCOME.into(), "out-yes".into())));
    }

    #[test]
    fn take_profit_tokens_deduplicate_shared_token_ids() {
        let mut topic = take_profit_topic();
        topic
            .tokens
            .iter_mut()
            .filter(|token| token.platform == POLYMARKET)
            .for_each(|token| token.token_id = "pm-shared".into());
        let positions = crate::hedge::Positions::from([
            (
                POLYMARKET.into(),
                HashMap::from([("yes".into(), d("3")), ("no".into(), d("1"))]),
            ),
            (
                OUTCOME.into(),
                HashMap::from([("yes".into(), d("4")), ("no".into(), d("2"))]),
            ),
        ]);

        let tokens = take_profit_book_tokens(&topic, &positions);
        assert_eq!(tokens.len(), 3);
        assert_eq!(
            tokens
                .iter()
                .filter(|(platform, token_id)| platform == POLYMARKET && token_id == "pm-shared")
                .count(),
            1
        );
    }

    #[test]
    fn position_detection_includes_negative_exposure() {
        let mut positions = crate::hedge::Positions::new();
        positions
            .entry(POLYMARKET.into())
            .or_default()
            .insert("yes".into(), Decimal::ZERO);
        positions
            .entry(OUTCOME.into())
            .or_default()
            .insert("no".into(), -Decimal::ONE);
        assert!(has_position(&positions));
        positions
            .get_mut(OUTCOME)
            .unwrap()
            .insert("no".into(), Decimal::ZERO);
        assert!(!has_position(&positions));
        positions
            .get_mut(POLYMARKET)
            .unwrap()
            .insert("yes".into(), Decimal::ONE);
        assert!(has_position(&positions));
    }
}
