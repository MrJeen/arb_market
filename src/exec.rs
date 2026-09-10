use crate::book::{BookStore, DirtyCoalescer, OrderBook};
use crate::calc::{
    best_plan, confirm_plan, confirm_plan_reason, diagnose_books, first_usable_ask, inspect_calc,
    ArbLimits, ArbPlan, FeeContext,
};
use crate::config::{Config, OUTCOME, POLYMARKET};
use crate::discovery::{load_active_topics, load_topic};
use crate::domain::{MarketIdentity, Topic, TopicKey};
use crate::error::{Error, Result};
use crate::hedge::{
    hedge_buy_required, hedge_candidates, hedge_order_tokens, leftover_untradeable,
    needs_rebalance, plan_hedge, HedgeSide,
};
use crate::notify::{
    self, NatsNotifier, PlaceNotice, PlaceResult, SettlementNotice, TakeProfitCompletedNotice,
    TakeProfitTriggerNotice,
};
use crate::platforms::outcome::{OutcomeFeeSnapshot, OutcomeVenue};
use crate::platforms::polymarket::PolymarketVenue;
use crate::platforms::{
    ioc_fill, pm_fak_fill, FillPage, MarketOrderRequest, OrderPoll, OrderSide, PreparedOrder,
    SubmitResult, TradeFill,
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

/// 一个动作的估值依据；最后准入后所有腿沿用它，不追逐后台刷新。
#[derive(Clone)]
struct ActionFees {
    context: FeeContext,
    outcome: OutcomeFeeSnapshot,
    outcome_id: u64,
}

struct ConfirmedArb {
    plan: ArbPlan,
    fees: ActionFees,
    deadline: Instant,
}

struct ConfirmedTakeProfit {
    plan: TakeProfitPlan,
    fees: ActionFees,
    deadline: Instant,
    notionals: [Decimal; 2],
}

#[allow(clippy::too_many_arguments)]
fn action_fee_estimate(
    fees: &ActionFees,
    platform: &str,
    token_id: &str,
    side: OrderSide,
    shares: Decimal,
    notional: Decimal,
    fee: Decimal,
    reserve: Decimal,
    funding: Decimal,
) -> Value {
    let mut estimate = fees.outcome.estimate_json();
    estimate["action"] = json!({
        "platform": platform, "token": token_id, "side": side.as_str(),
        "shares": shares.to_string(), "notional": notional.to_string(),
        "fee": fee.to_string(), "settlement_reserve": reserve.to_string(),
        "funding": funding.to_string(),
        "reserve_scope": "action_paired_outcome_exposure_not_additive_across_legs",
    });
    estimate
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

    fn fee_context(&self, topic: &Topic) -> Result<ActionFees> {
        let outcome_id: u64 = topic
            .market_identity()?
            .require(OUTCOME)?
            .parse()
            .map_err(|_| Error::msg("invalid outcome fee market identity"))?;
        if topic
            .tokens
            .iter()
            .filter(|token| token.platform == OUTCOME)
            .any(|token| {
                crate::domain::parse_side_coin(&token.token_id).map(|(id, _)| id)
                    != Some(outcome_id)
            })
        {
            return Err(Error::msg("outcome fee token/market identity mismatch"));
        }
        let outcome = self.outcome.fee_snapshot(outcome_id)?;
        Ok(ActionFees {
            context: FeeContext {
                polymarket_fee_rate: topic
                    .polymarket_fee_rate()
                    .unwrap_or_else(|| self.cfg.polymarket_fee_bps_prior / Decimal::from(10_000)),
                outcome_taker_rate: outcome.taker_rate,
                outcome_builder_rate: outcome.builder_rate,
            },
            outcome,
            outcome_id,
        })
    }

    fn available_fees(&self, topic: &Topic) -> Option<ActionFees> {
        match self.fee_context(topic) {
            Ok(fees) => Some(fees),
            Err(err) => {
                self.stats.outcome_fee_unavailable();
                tracing::debug!(topic = %topic.key.as_str(), error = %err,
                    "outcome fee unavailable; skip new trading action");
                None
            }
        }
    }

    fn fees_admitted(&self, fees: &ActionFees, deadline: Instant) -> bool {
        Instant::now() < deadline
            && fees.outcome.is_fresh()
            && self
                .outcome
                .fee_snapshot(fees.outcome_id)
                .is_ok_and(|current| current.is_fresh() && fees.outcome.same_rules(&current))
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
        let Some(selected_fees) = self.available_fees(&topic) else {
            return Ok(());
        };
        let fees = selected_fees.context.clone();
        let limits = ArbLimits {
            cost_limit: self.cfg.arb_cost_limit,
            min_profit: self.cfg.arb_min_profit,
            min_apr: self.cfg.arb_min_apr,
            days: crate::calc::days_until(topic.end_date),
        };
        let (plan, pm_book_ts, out_book_ts, pm_ask, pm_sz, out_ask, out_sz, skips, pairs) = {
            let books = self.books.lock().await;
            let now = Instant::now();
            let plan = best_plan(&topic, &books, &fees, &limits);
            let (skips, pairs) =
                inspect_calc(&topic, &books, &fees, &limits, now, self.cfg.book_stale);
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
                skips,
                pairs,
            )
        };
        self.stats.record_calc_skips(&skips);
        self.stats
            .record_calc_samples(topic.key, pairs, plan.is_none());
        self.stats.calc();
        let Some(plan) = plan else {
            return Ok(());
        };
        self.stats.found();
        tracing::debug!(
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
        let Some(out_need) = plan.outcome_required() else {
            tracing::warn!(topic = %topic.key.as_str(), "arb exact valuation unavailable");
            return Ok(());
        };
        let (pm_bal, out_bal) = tokio::join!(self.select_funder(&plan), self.outcome.user_state());
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
            Ok(bal) if plan.outcome_balance_sufficient(bal) => bal,
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

        let Some(ConfirmedArb {
            plan,
            fees,
            deadline: confirmation_deadline,
        }) = self.confirm_http_plan(topic, &plan).await?
        else {
            return Ok(());
        };
        let (Some(pm_need), Some(out_need)) = (plan.pm_required(), plan.outcome_required()) else {
            tracing::warn!(topic = %topic.key.as_str(), "confirmed exact valuation unavailable");
            return Ok(());
        };
        if !plan.pm_balance_sufficient(pm_balance) {
            tracing::info!(
                topic = %topic.key.as_str(),
                required = %pm_need,
                %pm_balance,
                "http plan exceeds polymarket balance"
            );
            self.stats.exceed_bal();
            return Ok(());
        }
        if !plan.outcome_balance_sufficient(out_balance) {
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

        self.execute_confirmed_plan(topic, &plan, &funder, &fees, confirmation_deadline)
            .await
    }

    async fn execute_confirmed_plan(
        &self,
        topic: &Topic,
        plan: &ArbPlan,
        funder: &str,
        selected_fees: &ActionFees,
        confirmation_deadline: Instant,
    ) -> Result<()> {
        self.ensure_trading_enabled(TradingIntent::Arbitrage)?;
        let fees = &selected_fees.context;
        let pm_tick = self.ensure_pm_tick(&plan.pm.token_id).await;
        if !pm_tick.is_some_and(|tick| {
            tick > Decimal::ZERO
                && tick <= Decimal::ONE
                && crate::calc::align_polymarket_price(plan.pm.cap_price, tick) == plan.pm.cap_price
        }) {
            tracing::info!(topic = %topic.key.as_str(), "polymarket tick unavailable or cap changed before admission");
            return Ok(());
        }
        if !self.fees_admitted(selected_fees, confirmation_deadline) {
            tracing::debug!(topic = %topic.key.as_str(), "arb fee confirmation expired or changed");
            return Ok(());
        }
        let pm_estimate = action_fee_estimate(
            selected_fees,
            POLYMARKET,
            &plan.pm.token_id,
            OrderSide::Buy,
            plan.pm.shares,
            plan.pm.cost,
            plan.pm.fee,
            plan.settlement_reserve,
            plan.pm_required()
                .ok_or_else(|| Error::msg("missing PM funding"))?,
        );
        let out_estimate = action_fee_estimate(
            selected_fees,
            OUTCOME,
            &plan.outcome.token_id,
            OrderSide::Buy,
            plan.outcome.shares,
            plan.outcome.cost,
            plan.outcome.fee,
            plan.settlement_reserve,
            plan.outcome_required()
                .ok_or_else(|| Error::msg("missing Outcome funding"))?,
        );
        let fills = json!([
            {"platform": POLYMARKET, "token": plan.pm.token_id, "label": plan.pm.label, "shares": plan.pm.shares, "price": plan.pm.cap_price},
            {"platform": OUTCOME, "token": plan.outcome.token_id, "label": plan.outcome.label, "shares": plan.outcome.shares, "price": plan.outcome.cap_price, "estimate": out_estimate}
        ]);
        let pm_token = topic.token(POLYMARKET, &plan.pm.label);
        let out_token = topic.token(OUTCOME, &plan.outcome.label);
        let pm_req = MarketOrderRequest {
            token_id: plan.pm.token_id.clone(),
            shares: plan.pm.shares,
            cap_price: plan.pm.cap_price,
            side: OrderSide::Buy,
            neg_risk: pm_token.and_then(|t| t.neg_risk),
            tick_size: pm_tick,
            asset_id: None,
            funder_address: Some(funder.to_owned()),
        };
        validate_pm_request_tick(&pm_req)?;
        if let Err(err) =
            crate::platforms::polymarket::market_buy_base_units(pm_req.shares, pm_req.cap_price)
        {
            tracing::info!(topic = %topic.key.as_str(), reason = %err,
                "arb skipped before admission: polymarket buy amounts unrepresentable");
            return Ok(());
        }
        let prepared_pm = self.pm.prepare_market_order(funder, &pm_req).await?;
        if !self.fees_admitted(selected_fees, confirmation_deadline) {
            return Ok(());
        }
        // 建档必须原子：`actived` 且没有腿的父单会被回填判为无成交并取消。
        let initial_legs = [
            NewLeg {
                platform: POLYMARKET,
                token_id: &plan.pm.token_id,
                label: &plan.pm.label,
                side: "BUY",
                intent: "arb_buy",
                funder: Some(funder),
                wallet: Some(funder),
                service: self.polymarket_service(funder),
                req_price: plan.pm.cap_price,
                req_shares: plan.pm.shares,
                req_fee: plan.pm.fee,
                client_order_id: None,
                fee_estimate: Some(pm_estimate),
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
                fee_estimate: Some(out_estimate),
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
                plan.expected_revenue,
                plan.profit,
                plan.total_cost,
                &fills,
                &initial_legs,
                self.cfg.max_active_orders,
                confirmation_deadline,
            )
            .await
        {
            Ok(created) => created,
            Err(Error::OrderCapacityReached) => {
                self.stats.max_orders();
                tracing::info!(topic = %topic.key.as_str(), limit = self.cfg.max_active_orders, "order admission capacity reached");
                return Ok(());
            }
            Err(Error::OrderConfirmationExpired) => {
                tracing::info!(topic = %topic.key.as_str(), "order confirmation expired while awaiting admission");
                return Ok(());
            }
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
        // 建档等待也可能跨越刷新/过期；尚未签名时整组取消，不能留下单边提交。
        if !self.fees_admitted(selected_fees, confirmation_deadline) {
            self.store
                .abort_unsubmitted_legs(&[pm_leg, out_leg], "fee_confirmation_changed_or_expired")
                .await?;
            mark_orders_complete(&self.store).await?;
            return Ok(());
        }
        self.stats.orders();
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
            self.submit_prepared_pm(
                pm_leg,
                &prepared_pm,
                &pm_req,
                fees,
                TradingIntent::Arbitrage
            ),
            self.submit_outcome(out_leg, &out_req, fees, TradingIntent::Arbitrage)
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
        self.notify_place(order_id, topic, funder, plan, pm_res, out_res);
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

    async fn confirm_http_plan(
        &self,
        topic: &Topic,
        plan: &ArbPlan,
    ) -> Result<Option<ConfirmedArb>> {
        self.ensure_pm_tick(&plan.pm.token_id).await;
        let (pm_ticket, out_ticket) = {
            let books = self.books.lock().await;
            (
                books.begin_rest(POLYMARKET, &plan.pm.token_id),
                books.begin_rest(OUTCOME, &plan.outcome.token_id),
            )
        };
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
        let (pm_bids, pm_asks, pm_ts, pm_tick) = match pm_snap {
            Ok(snap) => {
                tracing::info!(
                    platform = POLYMARKET,
                    token = %plan.pm.token_id,
                    elapsed_ms = pm_elapsed.as_millis() as u64,
                    "polymarket rest book fetched"
                );
                (snap.bids, snap.asks, snap.exchange_ts_ms, snap.tick_size)
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
        // 两边校验及返回本次请求副本在同一锁内完成；拒绝不可回退为 WS 确认。
        let accepted = {
            let mut books = self.books.lock().await;
            accept_confirmation_books(
                &mut books,
                &pm_ticket,
                (pm_bids, pm_asks, pm_ts, pm_at, pm_tick),
                &out_ticket,
                (out_bids, out_asks, out_ts, out_at),
                self.cfg.book_stale,
                Instant::now(),
            )
        };
        let Some((pm_book, out_book)) = accepted else {
            tracing::debug!(topic = %topic.key.as_str(), pm_epoch = pm_ticket.epoch,
                pm_revision = pm_ticket.revision, out_epoch = out_ticket.epoch,
                out_revision = out_ticket.revision, "http confirmation book rejected or expired");
            return Ok(None);
        };
        if !book_recv_skew_ok(pm_at, out_at, HTTP_BOOK_SKEW_MAX) {
            tracing::warn!(topic = %topic.key.as_str(), skew_ms = skew.as_millis() as u64,
                "http book receive skew exceeded 1s");
            self.stats.skew();
            return Ok(None);
        }
        let confirmation_deadline = pm_at.min(out_at) + self.cfg.book_stale;
        let Some(selected_fees) = self.available_fees(topic) else {
            return Ok(None);
        };
        let fees = selected_fees.context.clone();
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
                Ok(Some(ConfirmedArb {
                    plan: confirmed,
                    fees: selected_fees,
                    deadline: confirmation_deadline,
                }))
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
                    pm_ask = %fmt_px(pm_ask),
                    pm_sz = %fmt_px(pm_sz),
                    out_ask = %fmt_px(out_ask),
                    out_sz = %fmt_px(out_sz),
                    unit_cost = %fmt_px(miss.unit_cost),
                    reason,
                    "http plan not fillable"
                );
                self.stats.no_longer();
                Ok(None)
            }
        }
    }

    async fn select_funder(&self, plan: &ArbPlan) -> Result<(String, Decimal)> {
        let required = plan
            .pm_required()
            .ok_or_else(|| Error::msg("arb exact valuation unavailable"))?;
        let mut current = self
            .pm
            .next_funder()
            .await
            .ok_or_else(|| Error::msg("no polymarket funder configured"))?;
        for _ in 0..3 {
            match self.pm.balance(&current).await {
                Ok(bal) if plan.pm_balance_sufficient(bal) => return Ok((current, bal)),
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

    async fn submit_prepared_pm(
        &self,
        leg_id: i64,
        prepared: &PreparedOrder,
        req: &MarketOrderRequest,
        fees: &FeeContext,
        intent: TradingIntent,
    ) -> Result<SubmitResult> {
        self.ensure_trading_enabled(intent)?;
        validate_pm_request_tick(req)?;
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
        let (result, response) = self.pm.post_prepared(prepared).await?;
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
            .poll_fill_page_after_terminal(
                &current.token_id,
                submitted.timestamp_millis().saturating_sub(30_000).max(0),
                &progress,
                current
                    .last_order_info
                    .as_ref()
                    .and_then(|info| info.get("outcome_terminal_observed_at_ms"))
                    .and_then(Value::as_u64),
            )
            .await?;
        self.apply_fill_page(&current, poll, page).await
    }

    async fn pm_reconciliation_fee_snapshot(&self, leg: &crate::store::LegRow) -> Result<Value> {
        let started = Instant::now();
        let result: Result<Value> = async {
            let identities = self.store.market_identities_for_order(leg.order_id).await?;
            let condition_id = identities.require(POLYMARKET)?;
            let key = self.store.order_topic_key(leg.order_id).await?;
            let topic = self.topics.read().await.get(&key).cloned();
            let rate = if let Some(topic) = topic {
                let tokens: Vec<_> = topic
                    .tokens
                    .iter()
                    .filter(|token| token.platform == POLYMARKET && token.token_id == leg.token_id)
                    .collect();
                if tokens.len() != 1
                    || !tokens[0]
                        .condition_id
                        .as_deref()
                        .is_some_and(|id| id.eq_ignore_ascii_case(condition_id))
                {
                    return Err(Error::msg("cached COMMON polymarket fee identity mismatch"));
                }
                let token = tokens[0];
                if token.fees_enabled == Some(false) {
                    Some(Decimal::ZERO)
                } else {
                    token
                        .fee_rate
                        .map(crate::discovery::validate_pm_fee_rate)
                        .transpose()?
                }
            } else {
                crate::discovery::load_pm_fee_rate(&self.common, key, condition_id, &leg.token_id)
                    .await?
            };
            let (rate, source) = match rate {
                Some(rate) => (rate, "common"),
                None => (
                    self.cfg.polymarket_fee_bps_prior / Decimal::from(10_000),
                    "env",
                ),
            };
            crate::discovery::validate_pm_fee_rate(rate)?;
            let bps = rate * Decimal::from(10_000);
            let mut snapshot = json!({
                "version": 1, "source": source, "rate": rate.to_string(), "bps": bps.to_string(),
                "event_id": key.event_id, "unified_index": key.unified_index,
                "condition_id": condition_id, "token_id": leg.token_id,
                "fetched_at_ms": chrono::Utc::now().timestamp_millis(),
                "fee_currency": "USD", "rounding": "five_decimal_half_up"
            });
            if source == "env" {
                snapshot["config_key"] = json!("POLYMARKET_FEE_BPS_PRIOR");
                snapshot["fallback_reason"] = json!("catalog_or_fee_missing");
            }
            tracing::debug!(
                order_id = leg.order_id, leg_id = leg.id, %condition_id,
                token_id = %leg.token_id, %rate, %bps, source,
                fallback_reason = ?snapshot.get("fallback_reason").and_then(|value| value.as_str()),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "polymarket reconciliation fee source selected"
            );
            Ok(snapshot)
        }
        .await;
        if let Err(err) = &result {
            tracing::warn!(service = "common", operation = "reconciliation_fee",
                order_id = leg.order_id, leg_id = leg.id, token_id = %leg.token_id,
                elapsed_ms = started.elapsed().as_millis() as u64, error = %err,
                "polymarket reconciliation fee source unavailable");
        }
        result
    }

    async fn apply_fill_page(
        &self,
        leg: &crate::store::LegRow,
        poll: OrderPoll,
        page: FillPage,
    ) -> Result<()> {
        let trades_only = leg.platform == POLYMARKET && !poll.found;
        let started = Instant::now();
        let resolution = apply_reconciliation_page(&self.store, leg, poll, page, || {
            self.pm_reconciliation_fee_snapshot(leg)
        })
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
            .settlement_gate_after_end(order.id, &order.title, &identity, topic.end_date)
            .await?;
        if settlement_access == SettlementAccess::Stop {
            return Ok(());
        }

        let Some(selected_fees) = self.available_fees(&topic) else {
            return Ok(());
        };
        let fees = selected_fees.context.clone();

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
                if let Some(confirmed) = self.confirm_take_profit(&topic, &positions, &plan).await?
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
                            .settlement_gate_after_end(
                                order.id,
                                &order.title,
                                &identity,
                                topic.end_date,
                            )
                            .await?
                            != SettlementAccess::All
                        {
                            self.store
                                .release_lifecycle(order.id, "take_profit", claim_id)
                                .await?;
                            return Ok(());
                        }
                        let result = self
                            .execute_take_profit(
                                order.id,
                                claim_id,
                                &topic,
                                &positions,
                                &confirmed.plan,
                            )
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
        let order_funder = self.store.order_pm_funder(order.id).await?;
        let order_tokens = hedge_order_tokens(&topic, &positions, self.cfg.min_rebalance_qty);
        self.refresh_hedge_books(order.id, &order_tokens).await;
        for (platform, token_id) in &order_tokens {
            if platform == POLYMARKET {
                let _ = self.ensure_pm_tick(token_id).await;
            }
        }
        let Some(actions) = self
            .funded_hedge_plan(
                order.id,
                &topic,
                &positions,
                order_funder.as_deref(),
                settlement_access,
            )
            .await
        else {
            return Ok(());
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
            .settlement_gate_after_end(order.id, &order.title, &identity, topic.end_date)
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
        let result = self
            .execute_hedges(order.id, claim_id, &topic, &positions, &actions)
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

    async fn settlement_gate_after_end(
        &self,
        order_id: i64,
        title: &str,
        identity: &MarketIdentity,
        end_date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<SettlementAccess> {
        // 普通持仓到期前不查结算；已确认结算的 pending 路径直接调用原 gate。
        let checked_at = chrono::Utc::now();
        let due = settlement_check_due(end_date, checked_at);
        let decision = if !due {
            self.stats.settlement_skipped_before_end();
            "skip_before_end"
        } else if end_date.is_none() {
            self.stats.settlement_end_date_missing();
            "query_missing_end_date"
        } else {
            "query_due"
        };
        tracing::debug!(
            order_id,
            ?end_date,
            %checked_at,
            decision,
            "settlement time gate evaluated"
        );
        if !due {
            return Ok(SettlementAccess::All);
        }
        self.settlement_gate(order_id, title, identity).await
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
                "settlement_fee_status": "unknown",
                "profit_basis": "gross_payout_less_trade_costs",
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
                            status: "settled (polymarket+outcome); 未扣未核实的 Outcome 结算费"
                                .to_string(),
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
        cached: &TakeProfitPlan,
    ) -> Result<Option<ConfirmedTakeProfit>> {
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
        self.ensure_pm_tick(&pm_action.token_id).await;
        let (pm_ticket, out_ticket) = {
            let books = self.books.lock().await;
            (
                books.begin_rest(POLYMARKET, &pm_action.token_id),
                books.begin_rest(OUTCOME, &out_action.token_id),
            )
        };
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
        let (pm_bids, pm_asks, pm_ts, pm_tick) = match pm_result {
            Ok(book) => (book.bids, book.asks, book.exchange_ts_ms, book.tick_size),
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
        let accepted = {
            let mut books = self.books.lock().await;
            accept_confirmation_books(
                &mut books,
                &pm_ticket,
                (pm_bids, pm_asks, pm_ts, pm_at, pm_tick),
                &out_ticket,
                (out_bids, out_asks, out_ts, out_at),
                self.cfg.book_stale,
                Instant::now(),
            )
        };
        let Some((pm_book, out_book)) = accepted else {
            tracing::debug!(topic = %topic.key.as_str(), "take profit confirmation book rejected or expired");
            return Ok(None);
        };
        // 只放接纳后的请求副本，不允许独立缓存接纳原始被拒回包。
        let mut confirmed_books = BookStore::new(self.cfg.book_stale);
        for book in [pm_book, out_book] {
            let ticket = confirmed_books.begin_rest(&book.platform, &book.token_id);
            if confirmed_books
                .accept_rest(
                    &ticket,
                    book.bids,
                    book.asks,
                    book.exchange_ts_ms,
                    book.received_at,
                    book.tick_size,
                )
                .is_err()
            {
                return Ok(None);
            }
        }
        let Some(fees) = self.available_fees(topic) else {
            return Ok(None);
        };
        // 仅确认原方向原数量；不在 claim 内缩量或换方向。
        let mut fixed_positions = crate::hedge::Positions::new();
        for action in &cached.actions {
            if position_qty(positions, &action.platform, &action.label) < action.shares {
                return Ok(None);
            }
            fixed_positions
                .entry(action.platform.clone())
                .or_default()
                .insert(action.label.clone(), action.shares);
        }
        let Some(plan) = plan_take_profit(
            topic,
            &fixed_positions,
            &confirmed_books,
            &fees.context,
            self.cfg.take_profit_min_gain,
            Instant::now(),
            self.cfg.book_stale,
        )
        .filter(|plan| same_take_profit_quantity(cached, plan)) else {
            return Ok(None);
        };
        let mut notionals = [Decimal::ZERO; 2];
        for (index, action) in plan.actions.iter().enumerate() {
            let Some(notional) = action_notional(
                &confirmed_books,
                &action.platform,
                &action.token_id,
                OrderSide::Sell,
                action.shares,
            ) else {
                return Ok(None);
            };
            notionals[index] = notional;
        }
        Ok(Some(ConfirmedTakeProfit {
            plan,
            fees,
            deadline: pm_at.min(out_at) + self.cfg.book_stale,
            notionals,
        }))
    }

    async fn execute_take_profit(
        &self,
        order_id: i64,
        claim_id: uuid::Uuid,
        topic: &Topic,
        positions: &crate::hedge::Positions,
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

        // claim 后余额等待完成，再按 HTTP 盘口与当前费用确认原数量。
        let Some(confirmed) = self.confirm_take_profit(topic, positions, plan).await? else {
            return Err(Error::msg(
                "take profit original quantity no longer confirmed",
            ));
        };
        let plan = &confirmed.plan;
        let pm_action = plan
            .actions
            .iter()
            .find(|a| a.platform == POLYMARKET)
            .unwrap();
        let out_action = plan.actions.iter().find(|a| a.platform == OUTCOME).unwrap();
        let fees = &confirmed.fees.context;
        let estimates: Vec<_> = plan
            .actions
            .iter()
            .enumerate()
            .map(|(index, action)| {
                action_fee_estimate(
                    &confirmed.fees,
                    &action.platform,
                    &action.token_id,
                    OrderSide::Sell,
                    action.shares,
                    confirmed.notionals[index],
                    action.fee,
                    Decimal::ZERO,
                    Decimal::ZERO,
                )
            })
            .collect();
        let pm_estimate = estimates[plan
            .actions
            .iter()
            .position(|a| a.platform == POLYMARKET)
            .unwrap()]
        .clone();
        let out_estimate = estimates[plan
            .actions
            .iter()
            .position(|a| a.platform == OUTCOME)
            .unwrap()]
        .clone();
        let pm_req = self
            .take_profit_request(topic, pm_action, Some(&funder))
            .await;
        let out_req = self.take_profit_request(topic, out_action, None).await;
        validate_pm_request_tick(&pm_req)?;
        if !self.fees_admitted(&confirmed.fees, confirmed.deadline) {
            return Err(Error::msg(
                "take profit fee confirmation changed or expired",
            ));
        }
        let prepared_pm = self.pm.prepare_market_order(&funder, &pm_req).await?;
        let identity = self.market_identity_for_order(order_id, topic).await?;
        if self
            .settlement_gate_after_end(order_id, &topic.title, &identity, topic.end_date)
            .await?
            != SettlementAccess::All
        {
            return Err(Error::msg("take profit settlement gate changed"));
        }
        if !self.fees_admitted(&confirmed.fees, confirmed.deadline) {
            return Err(Error::msg(
                "take profit fee confirmation changed or expired",
            ));
        }
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
                        fee_estimate: Some(pm_estimate),
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
                        fee_estimate: Some(out_estimate),
                    },
                ],
            )
            .await?;
        let [pm_leg, out_leg]: [i64; 2] = leg_ids
            .try_into()
            .map_err(|_| Error::msg("take profit must create exactly two legs"))?;
        if !self.fees_admitted(&confirmed.fees, confirmed.deadline) {
            self.store
                .abort_unsubmitted_legs(&[pm_leg, out_leg], "fee_confirmation_changed_or_expired")
                .await?;
            self.store
                .release_lifecycle(order_id, "take_profit", claim_id)
                .await?;
            return Ok(());
        }
        self.stats.take_profit_confirmed();
        if let Some(notify) = &self.notify {
            notify.publish_take_profit_trigger(TakeProfitTriggerNotice {
                order_id,
                title: topic.title.clone(),
                expected_gain: plan.gain,
            });
        }
        let (pm_result, out_result) = tokio::join!(
            self.submit_prepared_pm(
                pm_leg,
                &prepared_pm,
                &pm_req,
                fees,
                TradingIntent::TakeProfit
            ),
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

    async fn funded_hedge_plan(
        &self,
        order_id: i64,
        topic: &Topic,
        positions: &crate::hedge::Positions,
        funder: Option<&str>,
        access: SettlementAccess,
    ) -> Option<Vec<crate::hedge::HedgeAction>> {
        let selected = self.available_fees(topic)?;
        let buy_platforms = {
            let books = self.books.lock().await;
            hedge_candidates(
                topic,
                positions,
                &books,
                &selected.context,
                self.cfg.min_rebalance_qty,
                Instant::now(),
                self.cfg.book_stale,
            )
            .buy_platforms()
        };
        // 关闭提交仍按真实资金试算；仅减仓模式不查询买入资金。
        let balances = self
            .hedge_candidate_balances(order_id, funder, access, &buy_platforms)
            .await;
        // 余额等待可能跨过盘口或费用有效期，不能沿用预筛时的候选。
        // 新出现但本轮未查询的平台按缺失余额过滤，留待下一轮重新评估。
        let books = self.books.lock().await;
        let selected = self.available_fees(topic)?;
        Some(plan_hedge(
            topic,
            positions,
            &books,
            &balances,
            &selected.context,
            self.cfg.min_rebalance_qty,
            Instant::now(),
            self.cfg.book_stale,
        ))
    }

    async fn hedge_candidate_balances(
        &self,
        order_id: i64,
        funder: Option<&str>,
        access: SettlementAccess,
        platforms: &[String],
    ) -> HashMap<String, Decimal> {
        let mut balances = HashMap::new();
        if access != SettlementAccess::All {
            return balances;
        }
        if platforms.iter().any(|platform| platform == POLYMARKET) {
            if let Some(funder) = funder {
                match self.pm.balance(funder).await {
                    Ok(bal) => {
                        balances.insert(POLYMARKET.to_string(), bal);
                    }
                    Err(err) => {
                        tracing::warn!(order_id, funder, error = %err, "hedge polymarket balance unavailable")
                    }
                }
            }
        }
        if platforms.iter().any(|platform| platform == OUTCOME) {
            if let Ok(bal) = self.outcome.user_state().await {
                balances.insert(OUTCOME.to_string(), bal);
            }
        }
        balances
    }

    async fn load_outcome_buy_balance(
        &self,
        balances: &mut HashMap<String, Decimal>,
    ) -> Result<()> {
        if !balances.contains_key(OUTCOME) {
            balances.insert(OUTCOME.to_string(), self.outcome.user_state().await?);
        }
        Ok(())
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

    async fn execute_hedges(
        &self,
        order_id: i64,
        claim_id: uuid::Uuid,
        topic: &Topic,
        positions: &crate::hedge::Positions,
        cached: &[crate::hedge::HedgeAction],
    ) -> Result<()> {
        self.ensure_trading_enabled(TradingIntent::Rebalance)?;
        let mut funders = HashMap::new();
        let mut balances = HashMap::new();
        // 所有外部余额/tick等待均在整组最终重算前，避免第一腿发送后再追逐费率。
        for action in cached {
            let side = hedge_order_side(&action.side);
            if action.platform == POLYMARKET {
                let funder = self
                    .hedge_pm_funder(order_id, &action.token_id, side)
                    .await?;
                if side == OrderSide::Sell {
                    self.require_pm_token(&funder, &action.token_id, action.shares)
                        .await?;
                } else {
                    balances.insert(POLYMARKET.to_string(), self.pm.balance(&funder).await?);
                }
                self.ensure_pm_tick(&action.token_id).await;
                funders.insert(action.token_id.clone(), funder);
            } else if side == OrderSide::Sell {
                self.require_outcome_token(&action.token_id, action.shares)
                    .await?;
            } else {
                self.load_outcome_buy_balance(&mut balances).await?;
            }
        }
        let tokens = hedge_order_tokens(topic, positions, self.cfg.min_rebalance_qty);
        self.refresh_hedge_books(order_id, &tokens).await;
        let selected = self
            .available_fees(topic)
            .ok_or_else(|| Error::msg("rebalance fee unavailable"))?;
        let fees = &selected.context;
        let (actions, notionals, deadline) = {
            let books = self.books.lock().await;
            let replanned = plan_hedge(
                topic,
                positions,
                &books,
                &balances,
                fees,
                self.cfg.min_rebalance_qty,
                Instant::now(),
                self.cfg.book_stale,
            );
            // 不自动缩量、改方向或重复搜索；下一轮可以用新条件重新择优。
            let mut actions = Vec::new();
            let mut notionals = Vec::new();
            let mut deadline = Instant::now() + self.cfg.book_stale;
            for old in cached {
                let action = replanned
                    .iter()
                    .find(|new| same_hedge_quantity(old, new))
                    .ok_or_else(|| Error::msg("rebalance original quantity no longer confirmed"))?
                    .clone();
                let book = books
                    .get(&action.platform, &action.token_id)
                    .ok_or_else(|| Error::msg("rebalance confirmation book unavailable"))?;
                deadline = deadline.min(book.received_at + self.cfg.book_stale);
                // HedgeAction 没有保存均价。审计沿用其估值现金流，避免把重新走整数
                // 深度的金额冒充 planner 用过的名义额；派生值不参与任何准入判断。
                notionals.push(match action.side {
                    HedgeSide::Sell => action.marginal_value + action.fee,
                    HedgeSide::Buy => {
                        action.shares * (Decimal::ONE - fees.outcome_taker_rate)
                            - action.marginal_value
                            - action.fee
                    }
                });
                actions.push(action);
            }
            (actions, notionals, deadline)
        };
        let mut required = HashMap::<String, Decimal>::new();
        let mut requests = Vec::new();
        let mut estimates = Vec::new();
        for (index, action) in actions.iter().enumerate() {
            let side = hedge_order_side(&action.side);
            let funding = if side == OrderSide::Buy {
                let funding = hedge_buy_required(
                    &action.platform,
                    action.shares,
                    action.cap_price,
                    action.fee,
                    fees,
                )?;
                *required.entry(action.platform.clone()).or_default() += funding;
                funding
            } else {
                Decimal::ZERO
            };
            let tick = if action.platform == POLYMARKET {
                self.books
                    .lock()
                    .await
                    .get(POLYMARKET, &action.token_id)
                    .and_then(|book| book.tick_size)
            } else {
                None
            };
            let request = MarketOrderRequest {
                token_id: action.token_id.clone(),
                shares: action.shares,
                cap_price: action.cap_price,
                side,
                neg_risk: None,
                tick_size: tick,
                asset_id: crate::domain::parse_side_coin(&action.token_id)
                    .map(|(id, side)| crate::domain::side_asset_id(id, side)),
                funder_address: funders.get(&action.token_id).cloned(),
            };
            if action.platform == POLYMARKET {
                validate_pm_request_tick(&request)?;
            }
            let mut estimate = action_fee_estimate(
                &selected,
                &action.platform,
                &action.token_id,
                side,
                action.shares,
                notionals[index],
                action.fee,
                if side == OrderSide::Buy {
                    action.shares * fees.outcome_taker_rate
                } else {
                    Decimal::ZERO
                },
                funding,
            );
            estimate["action"]["notional_source"] = json!("derived_from_planner_cashflow");
            estimates.push(estimate);
            requests.push(request);
        }
        for (platform, amount) in required {
            if balances.get(&platform).copied().unwrap_or(Decimal::ZERO) < amount {
                if platform == OUTCOME {
                    self.notify_balance_insufficient(
                        OUTCOME,
                        balances.get(&platform).copied().unwrap_or(Decimal::ZERO),
                        amount,
                        &format!("rebalance orderId={order_id}"),
                    );
                }
                return Err(Error::msg(
                    "rebalance confirmed aggregate funding insufficient",
                ));
            }
        }
        let mut prepared_pm = Vec::new();
        for (action, request) in actions.iter().zip(&requests) {
            prepared_pm.push(if action.platform == POLYMARKET {
                Some(
                    self.pm
                        .prepare_market_order(request.funder_address.as_deref().unwrap(), request)
                        .await?,
                )
            } else {
                None
            });
        }
        let legs: Vec<_> = actions
            .iter()
            .zip(&requests)
            .zip(estimates)
            .map(|((action, request), estimate)| {
                let funder = request.funder_address.as_deref();
                NewLeg {
                    platform: &action.platform,
                    token_id: &action.token_id,
                    label: &action.label,
                    side: request.side.as_str(),
                    intent: "rebalance",
                    funder,
                    wallet: if action.platform == POLYMARKET {
                        funder
                    } else {
                        self.outcome.account_address()
                    },
                    service: funder.and_then(|f| self.polymarket_service(f)),
                    req_price: action.cap_price,
                    req_shares: action.shares,
                    req_fee: action.fee,
                    client_order_id: None,
                    fee_estimate: Some(estimate),
                }
            })
            .collect();
        let identity = self.market_identity_for_order(order_id, topic).await?;
        let access = self
            .settlement_gate_after_end(order_id, &topic.title, &identity, topic.end_date)
            .await?;
        if access == SettlementAccess::Stop
            || actions
                .iter()
                .any(|action| !action_allowed_for_rebalance(action, access))
        {
            return Err(Error::msg("rebalance settlement gate changed"));
        }
        self.store.mark_rebalance(order_id, "actived").await?;
        if !self.fees_admitted(&selected, deadline) {
            return Err(Error::msg("rebalance fee confirmation changed or expired"));
        }
        let ids = self
            .store
            .insert_legs_atomic(order_id, "rebalance", claim_id, &legs)
            .await?;
        if !self.fees_admitted(&selected, deadline) {
            self.store
                .abort_unsubmitted_legs(&ids, "fee_confirmation_changed_or_expired")
                .await?;
            self.store
                .release_lifecycle(order_id, "rebalance", claim_id)
                .await?;
            return Ok(());
        }
        // 此后整组冻结。已开始提交则保持现有部分失败/未知状态对账，不重签或重发。
        let mut first_error = None;
        for (((leg_id, request), action), prepared) in ids
            .into_iter()
            .zip(&requests)
            .zip(&actions)
            .zip(&prepared_pm)
        {
            let result = if action.platform == POLYMARKET {
                self.submit_prepared_pm(
                    leg_id,
                    prepared.as_ref().unwrap(),
                    request,
                    fees,
                    TradingIntent::Rebalance,
                )
                .await
            } else {
                self.submit_outcome(leg_id, request, fees, TradingIntent::Rebalance)
                    .await
            };
            if let Err(err) = result {
                tracing::error!(order_id, error = %err, "hedge submit failed");
                first_error.get_or_insert(err);
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub async fn resync_stale_pm_books(&self) -> Result<Vec<TopicKey>> {
        let limit = self.cfg.book_resync_batch.min(500);
        let (stale, tickets) = {
            let mut books = self.books.lock().await;
            let stale = books.stale_pm_tokens(self.cfg.book_stale, Instant::now(), limit);
            let tickets: Vec<_> = stale
                .iter()
                .map(|token| books.begin_rest(POLYMARKET, token))
                .collect();
            (stale, tickets)
        };
        if stale.is_empty() {
            tracing::debug!(stale = 0, "polymarket book resync skipped");
            return Ok(Vec::new());
        }
        let started = Instant::now();
        let payloads = match self.pm.rest_books(&stale).await {
            Ok(payloads) => payloads,
            Err(err) => {
                let elapsed_ms = started.elapsed().as_millis() as u64;
                self.stats
                    .record_pm_book_resync(stale.len(), None, elapsed_ms);
                tracing::warn!(
                    stale = stale.len(),
                    error = %err,
                    elapsed_ms,
                    "polymarket book resync failed"
                );
                return Err(err);
            }
        };
        let now = Instant::now();
        let (applied, skipped, topics) = {
            let mut books = self.books.lock().await;
            let (applied, skipped) = crate::platforms::polymarket::apply_rest_books(
                &mut books, &payloads, &tickets, now,
            );
            let mut topics = Vec::new();
            for token in &applied {
                topics.extend(books.topics_for(POLYMARKET, token));
            }
            (applied, skipped, topics)
        };
        let elapsed_ms = started.elapsed().as_millis() as u64;
        self.stats.record_pm_book_resync(
            stale.len(),
            Some((payloads.len(), applied.len(), skipped)),
            elapsed_ms,
        );
        tracing::debug!(
            stale = stale.len(),
            requested = stale.len(),
            applied = applied.len(),
            skipped = skipped.total(),
            topics = topics.len(),
            elapsed_ms,
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
            if platform == POLYMARKET {
                self.ensure_pm_tick(&token_id).await;
            }
            let ticket = self.books.lock().await.begin_rest(&platform, &token_id);
            let started = Instant::now();
            let result = if platform == POLYMARKET {
                self.pm
                    .rest_book(&token_id)
                    .await
                    .map(|book| (book.bids, book.asks, book.exchange_ts_ms, book.tick_size))
            } else {
                self.outcome
                    .rest_book(&token_id)
                    .await
                    .map(|(bids, asks, ts)| (bids, asks, ts, None))
            };
            (
                platform,
                token_id,
                ticket,
                result,
                Instant::now(),
                started.elapsed(),
            )
        });
        for (platform, token_id, ticket, result, received_at, elapsed) in
            futures_util::future::join_all(fetches).await
        {
            match result {
                Ok((bids, asks, exchange_ts_ms, tick)) => {
                    let accepted = self.books.lock().await.accept_rest(
                        &ticket,
                        bids,
                        asks,
                        exchange_ts_ms,
                        received_at,
                        tick,
                    );
                    self.stats.record_hedge_book(
                        &platform,
                        Some(accepted.is_ok()),
                        elapsed.as_millis() as u64,
                    );
                    tracing::debug!(
                        order_id,
                        %platform,
                        token = %token_id,
                        exchange_ts_ms,
                        accepted = accepted.is_ok(),
                        elapsed_ms = elapsed.as_millis() as u64,
                        "hedge rest book fetched"
                    );
                }
                Err(err) => {
                    self.stats
                        .record_hedge_book(&platform, None, elapsed.as_millis() as u64);
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
        let ticket = {
            let books = self.books.lock().await;
            if let Some(tick) = books.tick_size(POLYMARKET, token_id) {
                return Some(tick);
            }
            books.begin_rest(POLYMARKET, token_id)
        };
        match self.pm.fetch_tick_size(token_id).await {
            Ok(tick) => self.books.lock().await.seed_tick_size(&ticket, tick),
            Err(err) => {
                tracing::warn!(token_id, error = %err, "polymarket tick_size fetch failed");
                None
            }
        }
    }
}

// 自动交易不能在 tick 缺失/冲突时借 venue 的人工入口 fallback 重新取值下单。
fn validate_pm_request_tick(req: &MarketOrderRequest) -> Result<()> {
    let tick = req
        .tick_size
        .ok_or_else(|| Error::msg("polymarket tick unavailable"))?;
    if tick <= Decimal::ZERO || tick > Decimal::ONE {
        return Err(Error::msg("invalid accepted polymarket tick"));
    }
    let aligned = match req.side {
        OrderSide::Buy => crate::calc::align_polymarket_price(req.cap_price, tick),
        OrderSide::Sell => crate::calc::align_polymarket_sell_price(req.cap_price, tick),
    };
    if aligned != req.cap_price {
        return Err(Error::msg("polymarket cap incompatible with accepted tick"));
    }
    Ok(())
}

/// 不使用全局 get 的 WS 优先策略：HTTP 硬确认只能用这次通过票据验证的完整副本。
fn accept_confirmation_books(
    books: &mut BookStore,
    pm_ticket: &crate::book::RestTicket,
    pm: (
        Vec<crate::book::Level>,
        Vec<crate::book::Level>,
        i64,
        Instant,
        Option<Decimal>,
    ),
    out_ticket: &crate::book::RestTicket,
    out: (
        Vec<crate::book::Level>,
        Vec<crate::book::Level>,
        i64,
        Instant,
    ),
    max_age: Duration,
    now: Instant,
) -> Option<(OrderBook, OrderBook)> {
    let pm_book = books.accept_rest(pm_ticket, pm.0, pm.1, pm.2, pm.3, pm.4);
    let out_book = books.accept_rest(out_ticket, out.0, out.1, out.2, out.3, None);
    let (Ok(pm_book), Ok(out_book)) = (pm_book, out_book) else {
        return None;
    };
    if !pm_book.is_fresh(max_age, now)
        || !out_book.is_fresh(max_age, now)
        || pm_book.tick_size.is_none()
    {
        return None;
    }
    Some((pm_book, out_book))
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

fn same_take_profit_quantity(old: &TakeProfitPlan, new: &TakeProfitPlan) -> bool {
    old.shares == new.shares
        && old.actions.iter().all(|action| {
            new.actions.iter().any(|other| {
                action.platform == other.platform
                    && action.token_id == other.token_id
                    && action.shares == other.shares
            })
        })
}

fn hedge_order_side(side: &HedgeSide) -> OrderSide {
    match side {
        HedgeSide::Buy => OrderSide::Buy,
        HedgeSide::Sell => OrderSide::Sell,
    }
}

fn same_hedge_quantity(old: &crate::hedge::HedgeAction, new: &crate::hedge::HedgeAction) -> bool {
    old.platform == new.platform
        && old.token_id == new.token_id
        && old.side == new.side
        && old.shares == new.shares
}

/// 用同一份确认盘口记录名义额，不从费用倒推，也不把限价金额冒充均价成交额。
fn action_notional(
    books: &BookStore,
    platform: &str,
    token: &str,
    side: OrderSide,
    shares: Decimal,
) -> Option<Decimal> {
    let book = books.get(platform, token)?;
    let levels = match side {
        OrderSide::Buy => &book.asks,
        OrderSide::Sell => &book.bids,
    };
    let mut left = shares;
    let mut notional = Decimal::ZERO;
    for level in levels
        .iter()
        .filter(|level| level.price > Decimal::ZERO && level.size > Decimal::ZERO)
    {
        let quantity = left.min(level.size);
        notional += quantity * level.price;
        left -= quantity;
        if left <= Decimal::ZERO {
            return Some(notional);
        }
    }
    None
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

fn settlement_check_due(
    end_date: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    end_date.is_none_or(|end| now >= end)
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

async fn apply_reconciliation_page<F, Fut>(
    store: &Store,
    leg: &crate::store::LegRow,
    poll: OrderPoll,
    page: FillPage,
    fee_snapshot: F,
) -> Result<LegResolution>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Value>>,
{
    let mut matched: Vec<TradeFill> = filter_trades(&page.fills, poll.order_id.as_deref(), None)
        .into_iter()
        .cloned()
        .collect();
    if matched.iter().any(|fill| {
        fill.coin
            .as_deref()
            .is_some_and(|coin| coin != leg.token_id)
    }) {
        return Err(Error::msg("matched trade token does not match leg"));
    }
    if leg.platform == POLYMARKET && !matched.is_empty() {
        let oid = poll
            .order_id
            .as_deref()
            .ok_or_else(|| Error::msg("missing reconciliation order id"))?;
        let ids: Vec<_> = matched
            .iter()
            .map(|fill| fill.trade_id.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let stored = store.reconciliation_observations(leg, oid, &ids).await?;
        matched = merge_page_observations(&matched, stored)?;
    }
    if leg.platform == POLYMARKET && matched.iter().any(needs_pm_fee_snapshot) {
        // 仅真正缺少可靠证据者才读取来源；同页共享，IO不持锁，事务内仍重新合并。
        let schedule = fee_snapshot().await?;
        for fill in matched
            .iter_mut()
            .filter(|fill| needs_pm_fee_snapshot(fill))
        {
            fill.raw["fee_calculation"] = schedule.clone();
        }
    }
    if leg.platform == POLYMARKET {
        for fill in &mut matched {
            crate::reconcile::restore_pm_fee_rate(fill)?;
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
    let evidence = FillEvidence {
        poll,
        page_complete: page.complete,
        history_complete: page.history_complete,
        expected_shares,
        outcome_scan: if leg.platform == OUTCOME {
            Some(serde_json::from_value(page.progress.clone())?)
        } else {
            None
        },
        pm_scan,
        pm_order_constraints: None,
    };
    store
        .record_reconciliation(leg, &matched, &evidence, &page.progress)
        .await
}

fn merge_page_observations(page: &[TradeFill], stored: Vec<TradeFill>) -> Result<Vec<TradeFill>> {
    let stored: HashMap<_, _> = stored
        .into_iter()
        .map(|fill| (fill.trade_id.clone(), fill))
        .collect();
    let mut merged = std::collections::BTreeMap::new();
    for incoming in page {
        let previous = merged
            .get(&incoming.trade_id)
            .or_else(|| stored.get(&incoming.trade_id));
        let fill = match previous {
            Some(previous) => crate::reconcile::merge_observation(previous, incoming)?,
            None => incoming.clone(),
        };
        merged.insert(fill.trade_id.clone(), fill);
    }
    Ok(merged.into_values().collect())
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

    // 不加载环境配置或实盘账户；签名只使用公开测试私钥，所有 HTTP 地址均由 loopback stub 提供。
    fn admission_test_config(base: &str) -> Config {
        Config {
            common_postgres_uri: String::new(),
            app_postgres_uri: String::new(),
            enabled_platforms: [POLYMARKET.to_string(), OUTCOME.to_string()].into(),
            enable_arb: true,
            enable_rebalance: false,
            enable_take_profit: false,
            take_profit_min_gain: Decimal::ZERO,
            discovery_interval: Duration::from_secs(30),
            reconcile_interval: Duration::from_secs(2),
            hedge_interval: Duration::from_secs(5),
            book_stale: Duration::from_secs(5),
            book_resync: Duration::from_secs(10),
            book_resync_batch: 1,
            position_scan_batch: 1,
            settlement_pending_scan_interval: Duration::from_secs(60),
            settlement_pending_scan_batch: 1,
            arb_min_profit: Decimal::ZERO,
            arb_min_apr: Decimal::ZERO,
            arb_cost_limit: d("100"),
            min_rebalance_qty: Decimal::ONE,
            polymarket_fee_bps_prior: Decimal::ZERO,
            pending_leg_timeout: Duration::from_secs(300),
            unknown_leg_timeout: Duration::from_secs(300),
            max_active_orders: 10,
            max_realized_loss: Decimal::ZERO,
            polymarket_clob_url: base.into(),
            polymarket_ws_url: base.into(),
            polymarket_funders: vec![],
            polymarket_auth_ttl: Duration::ZERO,
            hyperliquid_info_url: format!("{base}/info"),
            hyperliquid_exchange_url: format!("{base}/exchange"),
            hyperliquid_ws_url: base.into(),
            hyperliquid_mainnet: false,
            outcome_agent_private_key: Some(format!("{:064x}", 1)),
            outcome_account_address: Some("0x7e5f4552091a69125d5dfcb7b8c2659029395bdf".into()),
            outcome_builder_address: None,
            outcome_builder_fee: 0,
            nats_url: None,
            nats_token: None,
            nats_subject: String::new(),
            nats_channel: String::new(),
            cat: "admission-test".into(),
        }
    }

    async fn fee_test_engine() -> Engine {
        let base = "http://127.0.0.1:1";
        let cfg = admission_test_config(base);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let outcome = OutcomeVenue::connect(&cfg).unwrap();
        let (pm, _) = crate::platforms::polymarket::tests::execution_test_venue(base.into()).await;
        let (pm_sub_tx, _) = mpsc::channel(1);
        let (out_sub_tx, _) = mpsc::channel(1);
        Engine {
            cfg,
            store: Store { pool: pool.clone() },
            common: pool,
            books: Arc::new(Mutex::new(BookStore::default())),
            dirty: Arc::new(Mutex::new(DirtyCoalescer::default())),
            topics: Arc::new(RwLock::new(HashMap::new())),
            pm,
            outcome,
            pm_sub_tx,
            out_sub_tx,
            notify: None,
            stats: Arc::new(MinuteStats::new()),
            position_scan_cursor: Mutex::new(0),
            settlement_scan_cursor: Mutex::new(0),
            last_settlement_sweep: Mutex::new(None),
            reported_stale_unknown: Mutex::new(HashSet::new()),
        }
    }

    fn fee_test_topic() -> Topic {
        let mut topic = take_profit_topic();
        for token in &mut topic.tokens {
            if token.platform == POLYMARKET {
                token.condition_id = Some("test-condition".into());
                token.fees_enabled = Some(false);
            } else {
                token.token_id = if token.label == "yes" { "#10" } else { "#11" }.into();
            }
        }
        topic
    }

    // 只接受余额接口，不接受下单；可在首个响应前暂停以验证等待后的重算。
    async fn balance_test_server(
        replies: Vec<(u16, Value)>,
        pause: Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    ) -> (
        String,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Vec<String>>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut replies = replies.into_iter();
            let mut pause = pause;
            let mut requests = Vec::new();
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = &mut stop_rx => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                let mut bytes = Vec::new();
                let (body_start, length) = loop {
                    let mut buffer = [0u8; 4096];
                    let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buffer[..n]);
                    assert!(bytes.len() < 65_536);
                    if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&bytes[..index]).unwrap();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        assert!(length < 65_536);
                        break (index + 4, length);
                    }
                };
                while bytes.len() < body_start + length {
                    let mut buffer = [0u8; 4096];
                    let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buffer[..n]);
                }
                let request = std::str::from_utf8(&bytes[..body_start])
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap()
                    .to_string();
                if request.starts_with("POST /info ") {
                    let body: Value =
                        serde_json::from_slice(&bytes[body_start..body_start + length]).unwrap();
                    assert_eq!(body["type"], "spotClearinghouseState");
                } else {
                    assert!(request.starts_with("GET /balance-allowance?"));
                    assert!(request.contains("asset_type=COLLATERAL"));
                }
                requests.push(request);
                if let Some((arrived, release)) = pause.take() {
                    arrived.send(()).unwrap();
                    tokio::time::timeout(Duration::from_secs(3), release)
                        .await
                        .unwrap()
                        .unwrap();
                }
                let (status, body) = replies.next().expect("unexpected extra balance request");
                let body = body.to_string();
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            assert!(
                replies.next().is_none(),
                "expected balance request was not made"
            );
            requests
        });
        (base, stop_tx, server)
    }

    fn usdc_reply(balance: &str) -> (u16, Value) {
        (
            200,
            json!({"balances": [{"coin": "USDC", "total": balance, "hold": "0"}]}),
        )
    }

    async fn balance_test_engine(base: &str) -> (Engine, String) {
        let mut engine = fee_test_engine().await;
        engine.cfg = admission_test_config(base);
        engine.outcome = OutcomeVenue::connect(&engine.cfg).unwrap();
        let (pm, funder) =
            crate::platforms::polymarket::tests::execution_test_venue(base.into()).await;
        engine.pm = pm;
        engine
            .outcome
            .install_test_fee_snapshot(1, Decimal::ZERO, Decimal::ZERO);
        (engine, funder)
    }

    async fn hedge_balance_books(engine: &Engine, platform: &str, buy: bool) {
        let tokens = if platform == POLYMARKET {
            ["pm-no", "pm-yes"]
        } else {
            ["#11", "#10"]
        };
        let mut books = engine.books.lock().await;
        for token in tokens {
            books.set_tick_size(platform, token, d("0.01"));
            books.replace_snapshot(
                platform,
                token,
                vec![crate::book::Level {
                    price: d("0.4"),
                    size: d("30"),
                }],
                if buy {
                    vec![crate::book::Level {
                        price: d("0.4"),
                        size: d("30"),
                    }]
                } else {
                    vec![]
                },
                1,
                Instant::now(),
            );
        }
    }

    #[tokio::test]
    async fn hedge_balances_follow_candidates_and_permissions() {
        for case in [
            "empty",
            "sell",
            "stale",
            "reduce",
            "missing-funder",
            "outcome",
            "pm",
            "both",
            "insufficient",
            "error",
        ] {
            let replies = match case {
                "pm" => vec![(200, json!({"balance": "100000000"}))],
                "both" => vec![(200, json!({"balance": "100000000"})), usdc_reply("100")],
                "outcome" => vec![usdc_reply("100")],
                "insufficient" => vec![usdc_reply("0")],
                "error" => vec![(503, json!({"error": "test"}))],
                _ => vec![],
            };
            let expected_requests = replies.len();
            let (base, stop, server) = balance_test_server(replies, None).await;
            let (engine, funder) = balance_test_engine(&base).await;
            let topic = fee_test_topic();
            let mut positions = crate::hedge::Positions::new();
            let excess = if matches!(case, "pm" | "missing-funder") {
                OUTCOME
            } else {
                POLYMARKET
            };
            for label in ["no", "yes"] {
                positions
                    .entry(excess.into())
                    .or_default()
                    .insert(label.into(), d("30"));
            }
            if case == "both" {
                positions.clear();
                positions.insert(POLYMARKET.into(), HashMap::from([("no".into(), d("30"))]));
                positions.insert(OUTCOME.into(), HashMap::from([("no".into(), d("30"))]));
            }
            if case != "empty" {
                hedge_balance_books(&engine, POLYMARKET, !matches!(case, "sell")).await;
                hedge_balance_books(&engine, OUTCOME, !matches!(case, "sell")).await;
            }
            if case == "stale" {
                engine.books.lock().await.mark_platform_stale(OUTCOME);
            }
            let access = if case == "reduce" {
                SettlementAccess::ReduceOnly
            } else {
                SettlementAccess::All
            };
            let funder = if case == "missing-funder" {
                None
            } else {
                Some(funder.as_str())
            };
            // 默认关闭提交，但规划仍按有效买入候选查询真实资金。
            assert!(!engine.cfg.enable_rebalance);
            let actions = engine
                .funded_hedge_plan(1, &topic, &positions, funder, access)
                .await
                .unwrap();
            if matches!(case, "outcome" | "pm" | "both") {
                assert_eq!(actions.len(), 2, "{case}");
                assert!(
                    actions.iter().all(|action| action.side == HedgeSide::Buy),
                    "{case}"
                );
            } else if case == "empty" {
                assert!(actions.is_empty());
            } else {
                assert_eq!(actions.len(), 2, "{case}");
                assert!(
                    actions.iter().all(|action| action.side == HedgeSide::Sell),
                    "{case}"
                );
            }
            stop.send(()).unwrap();
            let requests = server.await.unwrap();
            assert_eq!(requests.len(), expected_requests, "{case}");
            let pm_count = requests
                .iter()
                .filter(|request| request.starts_with("GET "))
                .count();
            assert_eq!(
                pm_count,
                usize::from(matches!(case, "pm" | "both")),
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn hedge_rechecks_books_and_fees_after_balance_wait() {
        for mutation in ["stale", "fee-expired", "fee-changed", "new-platform"] {
            let (arrived_tx, arrived_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let (base, stop, server) =
                balance_test_server(vec![usdc_reply("100")], Some((arrived_tx, release_rx))).await;
            let (engine, funder) = balance_test_engine(&base).await;
            hedge_balance_books(&engine, POLYMARKET, false).await;
            hedge_balance_books(&engine, OUTCOME, true).await;
            let topic = fee_test_topic();
            let positions = crate::hedge::Positions::from([
                (POLYMARKET.into(), HashMap::from([("yes".into(), d("30"))])),
                (OUTCOME.into(), HashMap::from([("yes".into(), d("30"))])),
            ]);
            let planning = engine.funded_hedge_plan(
                1,
                &topic,
                &positions,
                Some(&funder),
                SettlementAccess::All,
            );
            let mutate = async {
                tokio::time::timeout(Duration::from_secs(3), arrived_rx)
                    .await
                    .unwrap()
                    .unwrap();
                match mutation {
                    "stale" => engine.books.lock().await.mark_platform_stale(OUTCOME),
                    "fee-expired" => engine.outcome.expire_test_fee_snapshot(1),
                    "fee-changed" => {
                        engine
                            .outcome
                            .install_test_fee_snapshot(1, d("0.9"), Decimal::ZERO)
                    }
                    "new-platform" => hedge_balance_books(&engine, POLYMARKET, true).await,
                    _ => unreachable!(),
                }
                release_tx.send(()).unwrap();
            };
            let (actions, ()) = tokio::join!(planning, mutate);
            if mutation == "fee-expired" {
                assert!(actions.is_none());
            } else {
                let actions = actions.unwrap();
                if mutation == "new-platform" {
                    assert!(actions
                        .iter()
                        .any(|a| a.platform == OUTCOME && a.side == HedgeSide::Buy));
                    assert!(!actions
                        .iter()
                        .any(|a| a.platform == POLYMARKET && a.side == HedgeSide::Buy));
                } else {
                    assert!(actions.iter().all(|a| a.side == HedgeSide::Sell));
                }
            }
            stop.send(()).unwrap();
            assert_eq!(server.await.unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn outcome_execution_balance_is_local_to_each_stage() {
        let (base, stop, server) =
            balance_test_server(vec![usdc_reply("100"), usdc_reply("0")], None).await;
        let (engine, _) = balance_test_engine(&base).await;
        let planning = engine
            .hedge_candidate_balances(
                1,
                None,
                SettlementAccess::All,
                &[OUTCOME.into(), OUTCOME.into()],
            )
            .await;
        assert_eq!(planning[OUTCOME], d("100"));
        let mut execution = HashMap::new();
        engine
            .load_outcome_buy_balance(&mut execution)
            .await
            .unwrap();
        engine
            .load_outcome_buy_balance(&mut execution)
            .await
            .unwrap();
        assert_eq!(execution[OUTCOME], Decimal::ZERO);
        stop.send(()).unwrap();
        assert_eq!(server.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn hedge_reconfirmation_requires_original_action_to_remain_selected() {
        let engine = fee_test_engine().await;
        engine
            .outcome
            .install_test_fee_snapshot(1, Decimal::ZERO, Decimal::ZERO);
        hedge_balance_books(&engine, POLYMARKET, true).await;
        hedge_balance_books(&engine, OUTCOME, true).await;
        let topic = fee_test_topic();
        let positions = crate::hedge::Positions::from([(
            POLYMARKET.into(),
            HashMap::from([("yes".into(), d("30"))]),
        )]);
        let fees = engine.available_fees(&topic).unwrap();
        let funded = HashMap::from([(OUTCOME.into(), d("100"))]);
        let mut books = engine.books.lock().await;
        let plan = |books: &BookStore, balances: &HashMap<String, Decimal>| {
            plan_hedge(
                &topic,
                &positions,
                books,
                balances,
                &fees.context,
                engine.cfg.min_rebalance_qty,
                Instant::now(),
                engine.cfg.book_stale,
            )
        };
        let original = plan(&books, &funded).remove(0);
        assert_eq!(original.side, HedgeSide::Buy);
        // 第二阶段余额下降：原买入不可确认，不能直接换成卖单提交。
        let no_cash = plan(&books, &HashMap::from([(OUTCOME.into(), Decimal::ZERO)]));
        assert_eq!(no_cash[0].side, HedgeSide::Sell);
        assert!(!no_cash
            .iter()
            .any(|action| same_hedge_quantity(&original, action)));
        // 买候选仍有效且资金足够，但卖出已更优，也必须拒绝原买入。
        books.replace_snapshot(
            POLYMARKET,
            "pm-yes",
            vec![crate::book::Level {
                price: d("0.8"),
                size: d("30"),
            }],
            vec![],
            2,
            Instant::now(),
        );
        assert_eq!(
            hedge_candidates(
                &topic,
                &positions,
                &books,
                &fees.context,
                engine.cfg.min_rebalance_qty,
                Instant::now(),
                engine.cfg.book_stale,
            )
            .buy_platforms(),
            vec![OUTCOME.to_string()]
        );
        let better_sell = plan(&books, &funded);
        assert_eq!(better_sell[0].side, HedgeSide::Sell);
        assert!(!better_sell
            .iter()
            .any(|action| same_hedge_quantity(&original, action)));
    }

    #[tokio::test]
    async fn outcome_fee_admission_rejects_changed_expired_and_missing_rules_without_io() {
        let engine = fee_test_engine().await;
        let topic = fee_test_topic();
        assert!(engine.available_fees(&topic).is_none());
        engine
            .outcome
            .install_test_fee_snapshot(1, d("0.001344"), d("0.0001"));
        let fees = engine.fee_context(&topic).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        assert!(engine.fees_admitted(&fees, deadline));
        assert!(!engine.fees_admitted(&fees, Instant::now()));
        engine
            .outcome
            .install_test_fee_snapshot(1, d("0.001344"), d("0.0001"));
        assert!(
            engine.fees_admitted(&fees, deadline),
            "same rules renewal must remain valid"
        );
        engine
            .outcome
            .install_test_fee_snapshot(1, d("0.002"), d("0.0001"));
        assert!(!engine.fees_admitted(&fees, deadline));
        let current = engine.fee_context(&topic).unwrap();
        engine.outcome.expire_test_fee_snapshot(1);
        assert!(!engine.fees_admitted(&current, deadline));
        assert!(engine.available_fees(&topic).is_none());
        assert_eq!(engine.stats.snapshot_and_reset().outcome_fee_unavailable, 2);
    }

    #[tokio::test]
    async fn confirmed_arb_changed_or_stale_fee_never_reaches_db_or_submission() {
        let engine = fee_test_engine().await;
        let topic = fee_test_topic();
        engine
            .outcome
            .install_test_fee_snapshot(1, d("0.001344"), Decimal::ZERO);
        let selected = engine.fee_context(&topic).unwrap();
        let limits = ArbLimits {
            cost_limit: d("100"),
            min_profit: Decimal::ZERO,
            min_apr: Decimal::ZERO,
            days: 1,
        };
        let plan = {
            let mut books = engine.books.lock().await;
            books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
            for (platform, token) in [(POLYMARKET, "pm-yes"), (OUTCOME, "#11")] {
                books.replace_snapshot(
                    platform,
                    token,
                    vec![],
                    vec![crate::book::Level {
                        price: d("0.4"),
                        size: d("30"),
                    }],
                    1,
                    Instant::now(),
                );
            }
            crate::calc::plan_arbitrage(
                &topic,
                books.get(POLYMARKET, "pm-yes").unwrap(),
                books.get(OUTCOME, "#11").unwrap(),
                topic.token(POLYMARKET, "yes").unwrap(),
                topic.token(OUTCOME, "no").unwrap(),
                &selected.context,
                &limits,
            )
            .unwrap()
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        engine
            .outcome
            .install_test_fee_snapshot(1, d("0.002"), Decimal::ZERO);
        engine
            .execute_confirmed_plan(&topic, &plan, "unused", &selected, deadline)
            .await
            .unwrap();
        engine
            .outcome
            .install_test_fee_snapshot(1, d("0.001344"), Decimal::ZERO);
        let fresh = engine.fee_context(&topic).unwrap();
        engine.outcome.expire_test_fee_snapshot(1);
        engine
            .execute_confirmed_plan(&topic, &plan, "unused", &fresh, deadline)
            .await
            .unwrap();
        assert_eq!(engine.stats.snapshot_and_reset().orders, 0);
    }

    #[tokio::test]
    async fn frozen_action_estimates_do_not_follow_refresh_and_include_pm_pair_reserve() {
        let engine = fee_test_engine().await;
        engine
            .outcome
            .install_test_fee_snapshot(1, d("0.001344"), d("0.0001"));
        let frozen = engine.fee_context(&fee_test_topic()).unwrap();
        let estimate = |platform| {
            action_fee_estimate(
                &frozen,
                platform,
                "test",
                OrderSide::Buy,
                d("30"),
                d("12"),
                d("0.0012"),
                d("0.04032"),
                d("12.0012"),
            )
        };
        let before = estimate(POLYMARKET);
        engine
            .outcome
            .install_test_fee_snapshot(1, d("0.003"), d("0.002"));
        let after = estimate(POLYMARKET);
        assert_eq!(before, after);
        assert_eq!(after["action"]["settlement_reserve"], "0.04032");
        assert_eq!(estimate(OUTCOME)["action"]["fee"], "0.0012");
        assert_eq!(frozen.context.outcome_taker_rate, d("0.001344"));
    }

    #[test]
    fn lifecycle_confirmation_rejects_resizing_or_switching_original_actions() {
        let action = crate::hedge::HedgeAction {
            platform: OUTCOME.into(),
            token_id: "#11".into(),
            label: "no".into(),
            side: HedgeSide::Buy,
            shares: d("30"),
            cap_price: d("0.4"),
            fee: Decimal::ZERO,
            marginal_value: d("17.95968"),
        };
        let mut changed = action.clone();
        changed.shares = d("29");
        assert!(!same_hedge_quantity(&action, &changed));
        changed = action.clone();
        changed.side = HedgeSide::Sell;
        assert!(!same_hedge_quantity(&action, &changed));
        changed = action.clone();
        changed.cap_price = d("0.41");
        assert!(
            same_hedge_quantity(&action, &changed),
            "recomputed cap is separately funded"
        );
        let sell = |platform: &str, token: &str| TakeProfitAction {
            platform: platform.into(),
            token_id: token.into(),
            label: "yes".into(),
            shares: d("30"),
            cap_price: d("0.6"),
            fee: Decimal::ZERO,
        };
        let old = TakeProfitPlan {
            actions: [sell(POLYMARKET, "pm"), sell(OUTCOME, "#11")],
            shares: d("30"),
            gross_revenue: d("36"),
            total_fee: Decimal::ZERO,
            gain: d("6"),
        };
        let mut resized = old.clone();
        resized.shares = d("29");
        resized
            .actions
            .iter_mut()
            .for_each(|action| action.shares = d("29"));
        assert!(!same_take_profit_quantity(&old, &resized));
        let mut switched = old.clone();
        switched.actions[1].token_id = "#10".into();
        assert!(!same_take_profit_quantity(&old, &switched));
        assert!(same_take_profit_quantity(&old, &old));
    }

    #[test]
    fn settlement_end_gate_uses_exact_time_and_queries_unknown_dates() {
        let now = chrono::DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        assert!(!settlement_check_due(
            Some(now + chrono::Duration::nanoseconds(1)),
            now
        ));
        assert!(settlement_check_due(Some(now), now));
        assert!(settlement_check_due(
            Some(now - chrono::Duration::nanoseconds(1)),
            now
        ));
        assert!(settlement_check_due(None, now));
    }

    #[tokio::test]
    async fn settlement_end_gate_skips_future_and_queries_due_or_unknown() {
        use crate::platforms::polymarket::tests::execution_test_venue;
        use sqlx::postgres::PgPoolOptions;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let observed = requests.clone();
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = &mut stop_rx => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                let mut bytes = Vec::new();
                let (body_start, content_length) = loop {
                    let mut buffer = [0u8; 4096];
                    let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(n > 0, "stub request ended before headers");
                    bytes.extend_from_slice(&buffer[..n]);
                    assert!(bytes.len() < 65_536);
                    if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&bytes[..index]).unwrap();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        assert!(length < 65_536);
                        break (index + 4, length);
                    }
                };
                while bytes.len() < body_start + content_length {
                    let mut buffer = [0u8; 4096];
                    let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(n > 0, "stub request ended before body");
                    bytes.extend_from_slice(&buffer[..n]);
                }
                let request = std::str::from_utf8(&bytes[..body_start])
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap()
                    .to_string();
                let body = match request.as_str() {
                    "GET /markets/test-condition HTTP/1.1" => json!({
                        "tokens": [{"token_id": "123", "winner": false}],
                        "closed": false, "accepting_orders": true, "enable_order_book": true
                    }),
                    "POST /info HTTP/1.1" => {
                        let body: Value =
                            serde_json::from_slice(&bytes[body_start..body_start + content_length])
                                .unwrap();
                        assert_eq!(body, json!({"type": "settledOutcome", "outcome": 1211}));
                        Value::Null
                    }
                    _ => panic!("unexpected stub request: {request}"),
                }
                .to_string();
                observed.lock().await.push(request);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });

        // 本测试不访问数据库；未结算响应无需任何持仓状态写入。
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let cfg = admission_test_config(&base);
        let outcome = OutcomeVenue::connect(&cfg).unwrap();
        let (pm, _) = execution_test_venue(base).await;
        let (pm_sub_tx, _pm_sub_rx) = mpsc::channel(1);
        let (out_sub_tx, _out_sub_rx) = mpsc::channel(1);
        let engine = Engine {
            cfg,
            store: Store { pool: pool.clone() },
            common: pool,
            books: Arc::new(Mutex::new(BookStore::default())),
            dirty: Arc::new(Mutex::new(DirtyCoalescer::default())),
            topics: Arc::new(RwLock::new(HashMap::new())),
            pm,
            outcome,
            pm_sub_tx,
            out_sub_tx,
            notify: None,
            stats: Arc::new(MinuteStats::new()),
            position_scan_cursor: Mutex::new(0),
            settlement_scan_cursor: Mutex::new(0),
            last_settlement_sweep: Mutex::new(None),
            reported_stale_unknown: Mutex::new(HashSet::new()),
        };
        let mut identity = MarketIdentity::new(POLYMARKET, "test-condition").unwrap();
        identity.insert(OUTCOME, "1211").unwrap();
        let future = chrono::Utc::now() + chrono::Duration::hours(1);
        assert_eq!(
            engine
                .settlement_gate_after_end(1, "test", &identity, Some(future))
                .await
                .unwrap(),
            SettlementAccess::All
        );
        assert!(requests.lock().await.is_empty());
        let skipped = engine.stats.snapshot_and_reset();
        assert_eq!(skipped.settlement_scan, 0);
        assert_eq!(skipped.settlement_skipped_before_end, 1);
        assert_eq!(skipped.settlement_end_date_missing, 0);

        // 同一入口每次重判时间；未来检查不能缓存成提交前永久放行。
        let past = chrono::Utc::now() - chrono::Duration::hours(1);
        for end_date in [Some(past), None] {
            assert_eq!(
                engine
                    .settlement_gate_after_end(1, "test", &identity, end_date)
                    .await
                    .unwrap(),
                SettlementAccess::All
            );
            let queried = engine.stats.snapshot_and_reset();
            assert_eq!(queried.settlement_scan, 1);
            assert_eq!(queried.settlement_skipped_before_end, 0);
            assert_eq!(
                queried.settlement_end_date_missing,
                u64::from(end_date.is_none())
            );
        }
        // pending 使用的原 gate 不经过时间门禁，仍查询两个平台。
        assert_eq!(
            engine.settlement_gate(1, "test", &identity).await.unwrap(),
            SettlementAccess::All
        );
        let pending = engine.stats.snapshot_and_reset();
        assert_eq!(pending.settlement_scan, 1);
        assert_eq!(pending.settlement_skipped_before_end, 0);
        assert_eq!(pending.settlement_end_date_missing, 0);
        stop_tx.send(()).unwrap();
        server.await.unwrap();
        let requests = requests.lock().await;
        assert_eq!(requests.len(), 6);
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.starts_with("GET /markets/"))
                .count(),
            3
        );
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.starts_with("POST /info "))
                .count(),
            3
        );
    }

    #[tokio::test]
    #[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
    async fn confirmed_pm_amount_admission_blocks_all_side_effects() {
        use crate::platforms::polymarket::tests::execution_test_venue;
        use sqlx::postgres::PgPoolOptions;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let uri = std::env::var("APP_POSTGRES_URI").expect("requires a test database");
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&uri)
            .await
            .unwrap();
        let schema = format!("exec_admission_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let observed = requests.clone();
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = &mut stop_rx => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                let mut bytes = Vec::new();
                let (body_start, content_length) = loop {
                    let mut buffer = [0u8; 4096];
                    let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(n > 0, "stub request ended before headers");
                    bytes.extend_from_slice(&buffer[..n]);
                    assert!(bytes.len() < 65_536, "unexpectedly large stub request");
                    if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&bytes[..index]).unwrap();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        assert!(length < 65_536, "unexpectedly large stub body");
                        break (index + 4, length);
                    }
                };
                while bytes.len() < body_start + content_length {
                    let mut buffer = [0u8; 4096];
                    let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(n > 0, "stub request ended before body");
                    bytes.extend_from_slice(&buffer[..n]);
                }
                // 只记录请求行；签名、认证头和请求体既不保留也不打印。
                let request = std::str::from_utf8(&bytes[..body_start])
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap()
                    .to_string();
                let body = match request.as_str() {
                    "POST /order HTTP/1.1" => json!({
                        "success": true, "status": "matched", "orderID": "pm-admission",
                        "makingAmount": "3.33", "takingAmount": "10"
                    }),
                    "POST /exchange HTTP/1.1" => json!({
                        "status": "ok", "response": {"type": "order", "data": {"statuses": [
                            {"filled": {"totalSz": "10", "avgPx": "0.4", "oid": 777}}
                        ]}}
                    }),
                    _ => panic!("unexpected stub request: {request}"),
                }
                .to_string();
                observed.lock().await.push(request);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let search_path = schema.clone();
        let exercised: anyhow::Result<()> = async {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .after_connect(move |conn, _| {
                    let path = search_path.clone();
                    Box::pin(async move {
                        sqlx::query("SELECT set_config('search_path', $1, false)")
                            .bind(path)
                            .execute(conn)
                            .await?;
                        Ok(())
                    })
                })
                .connect(&uri)
                .await?;
            let store = Store { pool: pool.clone() };
            store.migrate().await?;
            let cfg = admission_test_config(&base);
            let outcome = OutcomeVenue::connect(&cfg)?;
            outcome.install_test_fee_snapshot(0, Decimal::ZERO, Decimal::ZERO);
            let (pm, funder) = execution_test_venue(base).await;
            let (pm_sub_tx, _pm_sub_rx) = mpsc::channel(1);
            let (out_sub_tx, _out_sub_rx) = mpsc::channel(1);
            let engine = Engine {
                cfg,
                store,
                common: pool,
                books: Arc::new(Mutex::new(BookStore::default())),
                dirty: Arc::new(Mutex::new(DirtyCoalescer::default())),
                topics: Arc::new(RwLock::new(HashMap::new())),
                pm,
                outcome,
                pm_sub_tx,
                out_sub_tx,
                notify: None,
                stats: Arc::new(MinuteStats::new()),
                position_scan_cursor: Mutex::new(0),
                settlement_scan_cursor: Mutex::new(0),
                last_settlement_sweep: Mutex::new(None),
                reported_stale_unknown: Mutex::new(HashSet::new()),
            };
            let token = |platform: &str, id: &str, label: &str| crate::domain::TokenRef {
                platform: platform.into(),
                token_id: id.into(),
                label: label.into(),
                option_id: if platform == OUTCOME {
                    "0".into()
                } else {
                    "admission-market".into()
                },
                condition_id: Some("admission-condition".into()),
                asset_id: (platform == OUTCOME).then_some(100_000_001),
                side_index: None,
                neg_risk: Some(false),
                fees_enabled: Some(false),
                fee_rate: Some(Decimal::ZERO),
            };
            let pm_token = token(POLYMARKET, "123", "yes");
            let out_token = token(OUTCOME, "#01", "no");
            let topic = Topic {
                key: TopicKey::new(uuid::Uuid::new_v4(), 0),
                title: "admission test".into(),
                market_title: "admission test".into(),
                end_date: None,
                tokens: vec![pm_token.clone(), out_token.clone()],
            };
            let fees = FeeContext {
                polymarket_fee_rate: Decimal::ZERO,
                outcome_taker_rate: Decimal::ZERO,
                outcome_builder_rate: Decimal::ZERO,
            };
            let limits = ArbLimits {
                cost_limit: d("100"),
                min_profit: Decimal::ZERO,
                min_apr: Decimal::ZERO,
                days: 1,
            };
            // 负例与正例均由真实盘口生成并通过 HTTP 确认所用的 confirm_plan；不伪造 ArbPlan。
            for (shares, expected_rows) in [("7", (0_i64, 0_i64, 0_i64)), ("10", (1, 2, 2))] {
                let confirmed = {
                    let now = Instant::now();
                    let mut books = engine.books.lock().await;
                    books.set_tick_size(POLYMARKET, "123", d("0.001"));
                    for (platform, id, price) in
                        [(POLYMARKET, "123", "0.333"), (OUTCOME, "#01", "0.4")]
                    {
                        books.replace_snapshot(
                            platform,
                            id,
                            vec![],
                            vec![crate::book::Level {
                                price: d(price),
                                size: d(shares),
                            }],
                            if shares == "7" { 100 } else { 101 },
                            now,
                        );
                    }
                    let pm_book = books.get(POLYMARKET, "123").unwrap();
                    let out_book = books.get(OUTCOME, "#01").unwrap();
                    let plan = crate::calc::plan_arbitrage(
                        &topic, pm_book, out_book, &pm_token, &out_token, &fees, &limits,
                    )
                    .ok_or_else(|| anyhow::anyhow!("calculator rejected {shares}-share fixture"))?;
                    anyhow::ensure!(plan.pm.shares == d(shares));
                    anyhow::ensure!(plan.pm.cap_price == d("0.333"));
                    confirm_plan(&topic, &plan, pm_book, out_book, &fees, &limits).ok_or_else(
                        || anyhow::anyhow!("confirmation rejected {shares}-share fixture"),
                    )?
                };
                anyhow::ensure!(
                    crate::platforms::polymarket::market_buy_base_units(
                        confirmed.pm.shares,
                        confirmed.pm.cap_price,
                    )
                    .is_ok()
                        == (shares == "10")
                );
                tokio::time::timeout(
                    Duration::from_secs(10),
                    engine.execute_confirmed_plan(
                        &topic,
                        &confirmed,
                        &funder,
                        &engine.fee_context(&topic)?,
                        Instant::now() + Duration::from_secs(30),
                    ),
                )
                .await??;
                let counts: (i64, i64, i64) = sqlx::query_as(
                    "SELECT (SELECT COUNT(*) FROM arb_orders), (SELECT COUNT(*) FROM legs), \
                     (SELECT COUNT(*) FROM signed_envelopes)",
                )
                .fetch_one(&engine.store.pool)
                .await?;
                anyhow::ensure!(
                    counts == expected_rows,
                    "{shares}-share row counts: {counts:?}"
                );
                let mut calls = requests.lock().await.clone();
                calls.sort();
                if shares == "7" {
                    anyhow::ensure!(
                        calls.is_empty(),
                        "invalid plan submitted HTTP requests: {calls:?}"
                    );
                } else {
                    anyhow::ensure!(calls == ["POST /exchange HTTP/1.1", "POST /order HTTP/1.1"]);
                    let submitted: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM legs WHERE submitted_at IS NOT NULL \
                         AND third_order_id IS NOT NULL AND status = 'actived'",
                    )
                    .fetch_one(&engine.store.pool)
                    .await?;
                    anyhow::ensure!(
                        submitted == 2,
                        "positive control must persist both successful submissions"
                    );
                }
            }
            engine.store.pool.close().await;
            Ok(())
        }
        .await;
        let _ = stop_tx.send(());
        let server_result = server.await;
        let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&admin)
            .await;
        admin.close().await;
        cleanup.unwrap();
        server_result.unwrap();
        exercised.unwrap();
    }

    #[test]
    fn arb_http_confirmation_refreshes_exact_funding_before_admission() {
        let token = |platform: &str, id: &str, label: &str| crate::domain::TokenRef {
            platform: platform.into(),
            token_id: id.into(),
            label: label.into(),
            option_id: "market".into(),
            condition_id: None,
            asset_id: None,
            side_index: None,
            neg_risk: None,
            fees_enabled: None,
            fee_rate: None,
        };
        let pm = token(POLYMARKET, "pm", "yes");
        let out = token(OUTCOME, "out", "no");
        let topic = Topic {
            key: TopicKey::new(uuid::Uuid::nil(), 0),
            title: "test".into(),
            market_title: "test".into(),
            end_date: None,
            tokens: vec![pm.clone(), out.clone()],
        };
        let levels = |rows: &[(&str, &str)]| {
            rows.iter()
                .map(|(price, size)| crate::book::Level {
                    price: d(price),
                    size: d(size),
                })
                .collect()
        };
        let now = Instant::now();
        let mut books = BookStore::default();
        books.set_tick_size(POLYMARKET, "pm", d("0.01"));
        books.replace_snapshot(
            POLYMARKET,
            "pm",
            vec![],
            levels(&[("0.30", "4.5"), ("0.40", "95.5")]),
            100,
            now,
        );
        books.replace_snapshot(OUTCOME, "out", vec![], levels(&[("0.40", "200")]), 100, now);
        let fees = FeeContext {
            polymarket_fee_rate: Decimal::ZERO,
            outcome_taker_rate: Decimal::ZERO,
            outcome_builder_rate: Decimal::ZERO,
        };
        let limits = ArbLimits {
            cost_limit: d("100"),
            min_profit: d("1"),
            min_apr: Decimal::ZERO,
            days: 1,
        };
        let first = crate::calc::plan_arbitrage(
            &topic,
            books.get(POLYMARKET, "pm").unwrap(),
            books.get(OUTCOME, "out").unwrap(),
            &pm,
            &out,
            &fees,
            &limits,
        )
        .unwrap();
        assert!(first.pm_balance_sufficient(d("39.70")));
        let pm_ticket = books.begin_rest(POLYMARKET, "pm");
        let out_ticket = books.begin_rest(OUTCOME, "out");
        let (pm_book, out_book) = accept_confirmation_books(
            &mut books,
            &pm_ticket,
            (
                vec![],
                levels(&[("0.40", "100")]),
                101,
                now,
                Some(d("0.01")),
            ),
            &out_ticket,
            (vec![], levels(&[("0.40", "200")]), 101, now),
            Duration::from_secs(5),
            now,
        )
        .unwrap();
        let confirmed = confirm_plan(&topic, &first, &pm_book, &out_book, &fees, &limits).unwrap();
        assert_eq!(confirmed.pm.shares, first.pm.shares);
        assert_eq!(confirmed.pm.cap_price, first.pm.cap_price);
        assert_eq!(confirmed.pm_required(), Some(d("40")));
        assert!(!confirmed.pm_balance_sufficient(d("39.70")));
        assert!(confirmed.pm_balance_sufficient(d("40")));
        assert!(confirmed.outcome_balance_sufficient(d("40")));
    }

    #[test]
    fn automatic_pm_submission_requires_accepted_tick_and_unchanged_cap() {
        let mut req = MarketOrderRequest {
            token_id: "pm".into(),
            shares: d("10"),
            cap_price: d("0.451"),
            side: OrderSide::Buy,
            neg_risk: None,
            tick_size: None,
            asset_id: None,
            funder_address: None,
        };
        assert!(validate_pm_request_tick(&req).is_err());
        for tick in ["0", "-0.01", "1.1", "0.01"] {
            req.tick_size = Some(d(tick));
            assert!(validate_pm_request_tick(&req).is_err());
        }
        req.tick_size = Some(d("0.001"));
        assert!(validate_pm_request_tick(&req).is_ok());
        req.side = OrderSide::Sell;
        assert!(validate_pm_request_tick(&req).is_ok());
        req.tick_size = Some(d("0.01"));
        assert!(validate_pm_request_tick(&req).is_err());
    }

    #[test]
    fn hard_http_confirmation_carries_single_rest_tick() {
        let mut books = BookStore::default();
        let now = Instant::now();
        let pm = books.begin_rest(POLYMARKET, "pm");
        let out = books.begin_rest(OUTCOME, "out");
        let (pm_book, _) = accept_confirmation_books(
            &mut books,
            &pm,
            (vec![], vec![], 100, now, Some(d("0.001"))),
            &out,
            (vec![], vec![], 100, now),
            Duration::from_secs(5),
            now,
        )
        .unwrap();
        assert_eq!(pm_book.tick_size, Some(d("0.001")));
        assert_eq!(books.tick_size(POLYMARKET, "pm"), Some(d("0.001")));
    }

    #[test]
    fn hard_http_confirmation_rejects_conflict_without_ws_fallback_then_recovers() {
        let now = Instant::now();
        let level = || {
            vec![crate::book::Level {
                price: d("0.5"),
                size: d("3"),
            }]
        };
        for conflict_on_pm in [true, false] {
            let mut books = BookStore::default();
            books.set_tick_size(POLYMARKET, "pm", d("0.01"));
            books.replace_snapshot(POLYMARKET, "pm", vec![], level(), 100, now);
            books.replace_snapshot(OUTCOME, "out", vec![], level(), 100, now);
            // 两条生产确认路径共用此入口；持续冲突每轮只尝试一次。
            for _ in 0..3 {
                let pm = books.begin_rest(POLYMARKET, "pm");
                let out = books.begin_rest(OUTCOME, "out");
                if conflict_on_pm {
                    let ticket = books.begin_rest(POLYMARKET, "pm");
                    books
                        .accept_rest(&ticket, vec![], level(), 100, now, None)
                        .unwrap();
                } else {
                    let ticket = books.begin_rest(OUTCOME, "out");
                    books
                        .accept_rest(&ticket, vec![], level(), 100, now, None)
                        .unwrap();
                }
                assert!(accept_confirmation_books(
                    &mut books,
                    &pm,
                    (vec![], level(), 100, now, None),
                    &out,
                    (vec![], level(), 100, now),
                    Duration::from_secs(5),
                    now
                )
                .is_none());
                assert!(books
                    .get_at(POLYMARKET, "pm", now)
                    .unwrap()
                    .is_fresh(Duration::from_secs(5), now));
                assert!(books
                    .get_at(OUTCOME, "out", now)
                    .unwrap()
                    .is_fresh(Duration::from_secs(5), now));
            }
            let pm = books.begin_rest(POLYMARKET, "pm");
            let out = books.begin_rest(OUTCOME, "out");
            assert!(accept_confirmation_books(
                &mut books,
                &pm,
                (vec![], level(), 100, now, None),
                &out,
                (vec![], level(), 100, now),
                Duration::from_secs(5),
                now
            )
            .is_some());
        }
    }

    #[test]
    fn hard_http_confirmation_rejects_expired_missing_tick_and_delete_races() {
        let now = Instant::now();
        for case in 0..3 {
            let mut books = BookStore::default();
            if case != 0 {
                books.set_tick_size(POLYMARKET, "pm", d("0.01"));
            }
            let level = || {
                vec![crate::book::Level {
                    price: d("0.5"),
                    size: d("3"),
                }]
            };
            books.replace_snapshot(POLYMARKET, "pm", vec![], level(), 100, now);
            let pm = books.begin_rest(POLYMARKET, "pm");
            let out = books.begin_rest(OUTCOME, "out");
            if case == 2 {
                books.apply_levels(POLYMARKET, "pm", &[(false, d("0.5"), d("0"))], 100, now);
            }
            let check_at = if case == 1 {
                now + Duration::from_secs(6)
            } else {
                now
            };
            assert!(accept_confirmation_books(
                &mut books,
                &pm,
                (vec![], level(), 101, now, None),
                &out,
                (vec![], level(), 101, now),
                Duration::from_secs(5),
                check_at
            )
            .is_none());
            if case == 2 {
                assert!(books.get_at(POLYMARKET, "pm", now).unwrap().asks.is_empty());
            }
        }
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
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .after_connect(move |conn, _| {
                    let path = search_path.clone();
                    Box::pin(async move {
                        sqlx::query("SELECT set_config('search_path', $1, false)")
                            .bind(path)
                            .execute(conn)
                            .await?;
                        Ok(())
                    })
                })
                .connect(&uri)
                .await?;
            let store = Store { pool };
            store.migrate().await?;
            for (http_status, body, missing_time) in [
                (404, json!({}), false),
                (200, Value::Null, false),
                (404, json!({}), true),
            ] {
                let identity = MarketIdentity::new(POLYMARKET, "test-condition")?;
                let (_, ids) = store
                    .insert_actived_order_with_legs(
                        TopicKey::new(uuid::Uuid::new_v4(), 0),
                        &identity,
                        "PM window test",
                        "PM window test",
                        None,
                        d("10"),
                        d("1"),
                        d("9"),
                        &json!([]),
                        &[NewLeg {
                            platform: POLYMARKET,
                            token_id: "yes",
                            label: "yes",
                            side: "BUY",
                            intent: "arb_buy",
                            funder: Some("test-funder"),
                            wallet: None,
                            service: None,
                            req_price: d("0.5"),
                            req_shares: d("10"),
                            req_fee: Decimal::ZERO,
                            client_order_id: None,
                            fee_estimate: None,
                        }],
                        0,
                        Instant::now() + Duration::from_secs(30),
                    )
                    .await?;
                let oid = format!("taker-{}", ids[0]);
                store
                    .insert_envelope(ids[0], &oid, &json!({}), &json!({"test":true}), None)
                    .await?;
                if missing_time {
                    sqlx::query("UPDATE legs SET submitted_at=NULL WHERE id=$1")
                        .bind(ids[0])
                        .execute(&store.pool)
                        .await?;
                }
                let leg = store
                    .open_legs()
                    .await?
                    .into_iter()
                    .find(|leg| leg.id == ids[0])
                    .unwrap();
                let mut responses = vec![(http_status, body)];
                if !missing_time {
                    responses.push((
                        200,
                        json!({"data":[{
                        "id":"confirmed-trade","taker_order_id":oid,"asset_id":"yes",
                        "size":"6","price":"0.5","status":"CONFIRMED",
                        "fee_amount":"0.01","fee_token":"USDC","maker_orders":[]
                    }], "next_cursor":"LTE="}),
                    ));
                }
                let (pm, server) = poll_stub(responses).await;
                let page_result = reconcile_pm_page(&pm, &store, &leg).await?;
                let requests = tokio::time::timeout(Duration::from_secs(5), server).await??;
                if missing_time {
                    assert!(page_result.is_none());
                    assert_eq!(requests.len(), 1);
                    let info: Value =
                        sqlx::query_scalar("SELECT last_order_info FROM legs WHERE id=$1")
                            .bind(leg.id)
                            .fetch_one(&store.pool)
                            .await?;
                    assert_eq!(info["waiting_reason"], "submission_time_missing");
                    continue;
                }
                assert_eq!(requests.len(), 2);
                assert!(requests[0].starts_with(&format!("GET /data/order/{oid} ")));
                let after = leg.submitted_at.unwrap().timestamp();
                let url = url::Url::parse(&format!(
                    "http://localhost{}",
                    requests[1].split_whitespace().nth(1).unwrap()
                ))?;
                let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
                assert_eq!(query.get("after"), Some(&after.to_string()));
                assert_eq!(query.get("before"), Some(&(after + 300).to_string()));
                let (current, poll, page) = page_result.unwrap();
                assert!(!poll.found);
                assert_eq!(current.submitted_at, leg.submitted_at);
                assert_eq!(current.third_order_id.as_deref(), Some(oid.as_str()));
                assert!(
                    matches!(apply_reconciliation_page(&store, &current, poll, page, || async {
                        panic!("actual fee must not load a fee source")
                    }).await?,
                    LegResolution::Terminal { status:"matched", shares, .. } if shares == d("6"))
                );
            }
            store.pool.close().await;
            Ok(())
        }
        .await;
        let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&admin)
            .await;
        admin.close().await;
        cleanup.unwrap();
        exercised.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
    async fn pm_fee_source_common_env_cache_and_errors() {
        use crate::platforms::polymarket::tests::execution_test_venue;
        use sqlx::postgres::PgPoolOptions;
        let uri = std::env::var("APP_POSTGRES_URI").expect("requires a test database");
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&uri)
            .await
            .unwrap();
        let schema = format!("pm_fee_source_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let path = schema.clone();
        let exercised: anyhow::Result<()> = async {
            let pool = PgPoolOptions::new().max_connections(2).after_connect(move |conn, _| {
                let path = path.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('search_path', $1, false)").bind(path).execute(conn).await?;
                    Ok(())
                })
            }).connect(&uri).await?;
            let store = Store { pool: pool.clone() };
            store.migrate().await?;
            sqlx::query("CREATE TABLE events (id UUID PRIMARY KEY, unified_options JSONB)").execute(&pool).await?;
            let key = TopicKey::new(uuid::Uuid::new_v4(), 7);
            let identity = MarketIdentity::new(POLYMARKET, "test-condition")?;
            let (_, ids) = store.insert_actived_order_with_legs(
                key, &identity, "fee source", "fee source", None,
                d("10"), d("1"), d("9"), &json!([]), &[NewLeg {
                    platform: POLYMARKET, token_id: "yes", label: "yes", side: "BUY", intent: "arb_buy",
                    funder: Some("test-funder"), wallet: None, service: None,
                    req_price: d("0.5"), req_shares: d("10"), req_fee: Decimal::ZERO, client_order_id: None,
                        fee_estimate: None,
                }], 0, Instant::now() + Duration::from_secs(30)
            ).await?;
            let leg = store.open_legs().await?.into_iter().find(|leg| leg.id == ids[0]).unwrap();
            // 地址不监听：费率来源不能意外依赖 PM HTTP。
            let base = "http://127.0.0.1:1";
            let mut cfg = admission_test_config(base);
            cfg.polymarket_fee_bps_prior = d("700");
            let outcome = OutcomeVenue::connect(&cfg)?;
            let (pm, _) = execution_test_venue(base.into()).await;
            let (pm_sub_tx, _) = mpsc::channel(1);
            let (out_sub_tx, _) = mpsc::channel(1);
            let engine = Engine {
                cfg, store, common: pool.clone(),
                books: Arc::new(Mutex::new(BookStore::default())),
                dirty: Arc::new(Mutex::new(DirtyCoalescer::default())),
                topics: Arc::new(RwLock::new(HashMap::new())), pm, outcome,
                pm_sub_tx, out_sub_tx, notify: None, stats: Arc::new(MinuteStats::new()),
                position_scan_cursor: Mutex::new(0), settlement_scan_cursor: Mutex::new(0),
                last_settlement_sweep: Mutex::new(None), reported_stale_unknown: Mutex::new(HashSet::new()),
            };
            let missing = engine.pm_reconciliation_fee_snapshot(&leg).await?;
            assert_eq!(missing["source"], "env");
            assert_eq!(crate::platforms::parse_decimal(&missing["bps"]), Some(d("700")));
            let catalog = |rate: Value, enabled: bool| json!([{"index":7,"platformOptions":[{
                "platform":"polymarket","conditionId":"test-condition","feesEnabled":enabled,
                "feeSchedule":{"rate":rate},"outcomes":[{"tokenId":"yes"}]
            }]}]);
            for (rate, enabled, source, expected) in [
                (json!("0.05"), true, "common", d("500")),
                (Value::Null, true, "env", d("700")),
                (json!("bad"), false, "common", Decimal::ZERO),
                (json!("0"), true, "common", Decimal::ZERO),
            ] {
                sqlx::query("INSERT INTO events VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET unified_options=EXCLUDED.unified_options")
                    .bind(key.event_id).bind(catalog(rate, enabled)).execute(&pool).await?;
                let snapshot = engine.pm_reconciliation_fee_snapshot(&leg).await?;
                assert_eq!(snapshot["source"], source);
                assert_eq!(crate::platforms::parse_decimal(&snapshot["bps"]), Some(expected));
                assert_eq!(snapshot["condition_id"], "test-condition");
                assert_eq!(snapshot["token_id"], "yes");
            }
            for raw in [catalog(json!("bad"), true), catalog(json!("1.01"), true),
                json!([{"index":7,"platformOptions":[{"platform":"polymarket","conditionId":"wrong"}]}])] {
                sqlx::query("UPDATE events SET unified_options=$1").bind(raw).execute(&pool).await?;
                assert!(engine.pm_reconciliation_fee_snapshot(&leg).await.is_err());
            }
            sqlx::query("DROP TABLE events").execute(&pool).await?;
            assert!(engine.pm_reconciliation_fee_snapshot(&leg).await.is_err());
            let topic = Topic {
                key, title: String::new(), market_title: String::new(), end_date: None,
                tokens: vec![crate::domain::TokenRef {
                    platform: POLYMARKET.into(), token_id: "yes".into(), label: "yes".into(),
                    option_id: "market".into(), condition_id: Some("test-condition".into()),
                    asset_id: None, side_index: None, neg_risk: Some(false),
                    fees_enabled: Some(true), fee_rate: Some(d("0.04")),
                }],
            };
            engine.topics.write().await.insert(key, topic.clone());
            let cached = engine.pm_reconciliation_fee_snapshot(&leg).await?;
            assert_eq!(cached["source"], "common");
            assert_eq!(crate::platforms::parse_decimal(&cached["bps"]), Some(d("400")));
            let mut wrong = topic;
            wrong.tokens[0].condition_id = Some("wrong".into());
            engine.topics.write().await.insert(key, wrong);
            assert!(engine.pm_reconciliation_fee_snapshot(&leg).await.is_err());
            pool.close().await;
            Ok(())
        }.await;
        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
        exercised.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
    async fn pm_fee_recovery_uses_persisted_observations_before_source_lookup() {
        use crate::platforms::polymarket::tests::poll_stub;
        use sqlx::postgres::PgPoolOptions;
        let uri = std::env::var("APP_POSTGRES_URI").expect("requires a test database");
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&uri)
            .await
            .unwrap();
        let schema = format!("pm_fee_recovery_{}", uuid::Uuid::new_v4().simple());
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
            // 复用、混合新旧快照、真正缺费失败、以及预读后的并发更新。
            for mode in ["reuse", "mixed", "fee_failure", "stale"] {
                let identity = MarketIdentity::new(POLYMARKET, "test-condition")?;
                let (parent_id, ids) = store.insert_actived_order_with_legs(
                    TopicKey::new(uuid::Uuid::new_v4(),0), &identity, "PM fee recovery", "PM fee recovery", None,
                    d("10"), d("1"), d("9"), &json!([]), &[NewLeg {
                        platform:POLYMARKET,token_id:"yes",label:"yes",side:"BUY",intent:"arb_buy",
                        funder:Some("test-funder"),wallet:None,service:None,req_price:d("0.5"),req_shares:d("10"),
                        req_fee:Decimal::ZERO,client_order_id:None,fee_estimate:None,
                    }],
                    0, Instant::now() + Duration::from_secs(30),
                ).await?;
                let id=ids[0];
                let oid=format!("fee-oid-{id}");
                store.insert_envelope(id,&oid,&json!({}),&json!({"test":true}),None).await?;
                let trade = |trade_id:&str, size:&str, status:&str| json!({
                    "id":trade_id,"taker_order_id":oid,"asset_id":"yes","size":size,"price":"0.5",
                    "status":status,"maker_orders":[]
                });
                let a=trade("a","6","TRADE_STATUS_CONFIRMED");
                let b=trade("b","4","TRADE_STATUS_MATCHED_NOT_BROADCASTED");
                let order=json!({"id":oid,"status":"ORDER_STATUS_MATCHED","asset_id":"yes","original_size":"10",
                    "size_matched":"10","associate_trades":["a","b"]});
                let (pm,server)=poll_stub(vec![(200,order),(200,json!({"data":[a.clone(),b],"next_cursor":"LTE="}))]).await;
                let source_calls = std::sync::atomic::AtomicUsize::new(0);
                let snapshot_for = |rate: &str| json!({"version":1,"source":"common",
                    "rate":rate,"bps":(d(rate)*d("10000")).to_string(),
                    "condition_id":"test-condition","token_id":"yes"});
                let leg=store.open_legs().await?.into_iter().find(|leg|leg.id==id).unwrap();
                let (current,poll,page)=reconcile_pm_page(&pm,&store,&leg).await?.unwrap();
                assert!(matches!(apply_reconciliation_page(&store,&current,poll,page,|| async {
                    source_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(snapshot_for("0.07"))
                }).await?,LegResolution::Pending(_)));
                let requests=tokio::time::timeout(Duration::from_secs(5),server).await??;
                assert_eq!(requests.len(),2);
                assert_eq!(source_calls.load(std::sync::atomic::Ordering::Relaxed),1);
                let saved:Value=sqlx::query_scalar("SELECT raw FROM fills WHERE leg_id=$1 AND trade_id='a'").bind(id).fetch_one(&store.pool).await?;
                let snapshot=saved["reconciliation_v1"]["raw"]["fee_calculation"].clone();
                assert_eq!(snapshot["rate"],"0.07");
                let next_b=trade("b","4",if mode=="reuse" || mode=="stale" {"TRADE_STATUS_FAILED"}else{"TRADE_STATUS_CONFIRMED"});
                let responses=vec![(200,Value::Null),(200,json!({"data":[a.clone(),a.clone(),next_b],"next_cursor":"LTE="}))];
                let (pm,server)=poll_stub(responses).await;
                let leg=store.open_legs().await?.into_iter().find(|leg|leg.id==id).unwrap();
                let (current,poll,page)=reconcile_pm_page(&pm,&store,&leg).await?.unwrap();
                let before:Value=sqlx::query_scalar("SELECT to_jsonb(l) FROM legs l WHERE id=$1").bind(id).fetch_one(&store.pool).await?;
                if mode=="stale" {store.record_reconciliation_wait(&current,"concurrent_update").await?;}
                let resolution=apply_reconciliation_page(&store,&current,poll,page,|| async {
                    assert!(mode=="mixed" || mode=="fee_failure", "saved fee must skip lookup");
                    source_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if mode=="fee_failure" { return Err(Error::msg("COMMON lookup failed")); }
                    Ok(snapshot_for("0.05"))
                }).await;
                let requests=tokio::time::timeout(Duration::from_secs(5),server).await??;
                assert_eq!(requests.len(),2);
                assert_eq!(source_calls.load(std::sync::atomic::Ordering::Relaxed),
                    if mode=="mixed" || mode=="fee_failure" {2}else{1});
                let after:Value=sqlx::query_scalar("SELECT to_jsonb(l) FROM legs l WHERE id=$1").bind(id).fetch_one(&store.pool).await?;
                if mode=="fee_failure" {
                    assert!(resolution.is_err());
                    assert_eq!(after,before);
                } else if mode=="stale" {
                    assert_eq!(resolution?,LegResolution::Pending("stale_leg_snapshot"));
                    assert_eq!(after["last_order_info"]["waiting_reason"],"concurrent_update");
                    assert_eq!(after["last_order_info"]["fill_progress"],before["last_order_info"]["fill_progress"]);
                } else {
                    let (shares,fee)=if mode=="reuse" {(d("6"),d("0.105"))}else{(d("10"),d("0.155"))};
                    assert!(matches!(resolution?,LegResolution::Terminal{status:"matched",shares:s,fee:f,..} if s==shares && f==fee));
                    let parent:Value=sqlx::query_scalar("SELECT to_jsonb(o) FROM arb_orders o WHERE id=$1").bind(parent_id).fetch_one(&store.pool).await?;
                    assert_eq!(parent["status"],"completed");
                    let cost:Decimal=sqlx::query_scalar("SELECT actual_cost FROM arb_orders WHERE id=$1").bind(parent_id).fetch_one(&store.pool).await?;
                    assert_eq!(cost,shares*d("0.5")+fee);
                }
                let latest:Value=sqlx::query_scalar("SELECT raw FROM fills WHERE leg_id=$1 AND trade_id='a'").bind(id).fetch_one(&store.pool).await?;
                assert_eq!(latest["reconciliation_v1"]["raw"]["fee_calculation"],snapshot);
                let bps:Decimal=sqlx::query_scalar("SELECT fee_rate_bps FROM fills WHERE leg_id=$1 AND trade_id='a'").bind(id).fetch_one(&store.pool).await?;
                assert_eq!(bps,d("700"));
                assert_eq!(crate::platforms::parse_decimal(&latest["reconciliation_v1"]["fee_rate_bps"]),Some(d("700")));
                let b_value:Value=sqlx::query_scalar("SELECT raw FROM fills WHERE leg_id=$1 AND trade_id='b'").bind(id).fetch_one(&store.pool).await?;
                if mode=="mixed" {assert_eq!(b_value["reconciliation_v1"]["raw"]["fee_calculation"]["rate"],"0.05");}
                if mode=="fee_failure" || mode=="stale" {assert_eq!(b_value["reconciliation_v1"]["finality"],"pending");}
                let count:i64=sqlx::query_scalar("SELECT count(*) FROM fills WHERE leg_id=$1").bind(id).fetch_one(&store.pool).await?;
                assert_eq!(count,2);
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
    fn page_fee_merge_preserves_evidence_without_inventing_scan_members() {
        let mut stored = fill("a", "oid", "6", &[]);
        stored.fee = None;
        stored.raw =
            json!({"role":"taker","fee_calculation":{"source":"clob-markets","rate":"0.07"}});
        let mut incoming = stored.clone();
        incoming.raw = json!({"role":"taker"});
        assert!(needs_pm_fee_snapshot(&incoming));
        let extra = fill("not_in_page", "oid", "4", &[]);
        let merged = merge_page_observations(
            &[incoming.clone(), incoming.clone()],
            vec![stored.clone(), extra],
        )
        .unwrap();
        assert_eq!(merged.len(), 1);
        assert!(!needs_pm_fee_snapshot(&merged[0]));
        assert_eq!(
            merged[0].raw["fee_calculation"],
            stored.raw["fee_calculation"]
        );
        incoming.fee = Some(Decimal::ZERO);
        assert!(!needs_pm_fee_snapshot(
            &merge_page_observations(&[incoming.clone()], vec![stored.clone()]).unwrap()[0]
        ));
        incoming.fee_token = Some("OTHER".into());
        let other = merge_page_observations(&[incoming.clone()], vec![stored.clone()]).unwrap();
        assert!(crate::reconcile::accounting_fee(POLYMARKET, &other[0])
            .unwrap()
            .is_none());
        incoming.finality = crate::platforms::FillFinality::Failed;
        assert!(merge_page_observations(&[incoming], vec![stored]).is_err());
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
            outcome_builder_rate: Decimal::ZERO,
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
        let fee = estimate_taker_fee(OUTCOME, OrderSide::Sell, shares, price, &fees);
        assert_eq!(fee, d("30") * d("0.949") * d("0.00035"));
        let pm_fee = estimate_taker_fee(POLYMARKET, OrderSide::Buy, d("30"), d("0.40"), &fees);
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
