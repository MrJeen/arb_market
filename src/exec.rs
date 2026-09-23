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
    self, NatsNotifier, PlaceNotice, PlaceResult, PlaceStatus, SettlementNotice,
    TakeProfitCompletedNotice, TakeProfitTriggerNotice,
};
use crate::platforms::outcome::fees::FeeLookupError;
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
    /// 再平衡实际收益为负的 topic 冷却期截止时间（5分钟内禁止套利计算）
    pub rebalance_loss_cooldown: Mutex<HashMap<TopicKey, Instant>>,
}

pub const REBALANCE_LOSS_COOLDOWN: Duration = Duration::from_secs(300);
pub const SUBMITTED_PENDING_GRACE_PERIOD: Duration = Duration::from_secs(20);

const ACTUALS_GATE_WARN_INTERVAL: Duration = Duration::from_secs(60);
static ACTUALS_GATE_LOG: std::sync::LazyLock<ActualsGateLog> =
    std::sync::LazyLock::new(ActualsGateLog::default);

/// 全局查询共用固定大小的进程状态，不按 topic 分配限频条目。
#[derive(Default)]
struct ActualsGateLog {
    next_query: std::sync::atomic::AtomicU64,
    observation: std::sync::Mutex<ActualsGateObservation>,
}

#[derive(Default)]
struct ActualsGateObservation {
    query: u64,
    last_report: Option<Instant>,
    blocked_since: Option<Instant>,
    blocked_checks: u64,
}

struct ActualsGateReport {
    event: ActualsGateEvent,
    observed_blocked_ms: u128,
    blocked_checks: u64,
}

#[derive(Debug, PartialEq, Eq)]
enum ActualsGateEvent {
    Blocked,
    StillBlocked,
    Recovered,
}

impl ActualsGateLog {
    fn begin_query(&self) -> u64 {
        self.next_query
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    #[cfg(test)]
    fn observe(&self, query: u64, unknown: i64, now: Instant) -> Option<ActualsGateEvent> {
        self.observe_report(query, unknown, now)
            .map(|report| report.event)
    }

    fn observe_report(&self, query: u64, unknown: i64, now: Instant) -> Option<ActualsGateReport> {
        let mut state = self
            .observation
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        // 序号在 SQL 开始前分配；旧查询晚返回不能覆盖较新的已完成观察。
        // 这里只决定日志，调用方始终用本次 SQL 结果执行风控。
        if query <= state.query {
            return None;
        }
        state.query = query;
        let event = if unknown > 0 {
            // episode 只累计已接受的有序观察；所有拒绝仍由分钟 counter 统计。
            state.blocked_checks += 1;
            if state.blocked_since.is_none() {
                state.blocked_since = Some(now);
                state.last_report = Some(now);
                ActualsGateEvent::Blocked
            } else if state.last_report.is_some_and(|last| {
                now.saturating_duration_since(last) >= ACTUALS_GATE_WARN_INTERVAL
            }) {
                state.last_report = Some(now);
                ActualsGateEvent::StillBlocked
            } else {
                return None;
            }
        } else if unknown == 0 && state.blocked_since.is_some() {
            ActualsGateEvent::Recovered
        } else {
            return None;
        };
        let report = ActualsGateReport {
            observed_blocked_ms: now
                .saturating_duration_since(state.blocked_since.unwrap())
                .as_millis(),
            blocked_checks: state.blocked_checks,
            event,
        };
        if report.event == ActualsGateEvent::Recovered {
            state.blocked_since = None;
            state.last_report = None;
            state.blocked_checks = 0;
        }
        Some(report)
    }

    fn record(&self, stats: &MinuteStats, query: u64, unknown: i64, now: Instant) {
        if unknown > 0 {
            stats.actuals_gate_blocked();
        }
        // observe_report 返回时已释放锁；锁内没有 await 或日志 I/O。
        let Some(report) = self.observe_report(query, unknown, now) else {
            return;
        };
        let observed_blocked_ms = report.observed_blocked_ms;
        let blocked_checks = report.blocked_checks;
        match report.event {
            ActualsGateEvent::Blocked => tracing::info!(
                scope = "global",
                reason = "incomplete_actuals_projection",
                unknown_orders = unknown,
                observed_blocked_ms,
                blocked_checks,
                "loss gate blocked by incomplete actuals projection"
            ),
            ActualsGateEvent::StillBlocked => tracing::warn!(
                scope = "global",
                reason = "incomplete_actuals_projection",
                unknown_orders = unknown,
                observed_blocked_ms,
                blocked_checks,
                "loss gate still blocked by incomplete actuals projection"
            ),
            ActualsGateEvent::Recovered => tracing::info!(
                scope = "global",
                reason = "incomplete_actuals_projection",
                unknown_orders = unknown,
                observed_blocked_ms,
                blocked_checks,
                "loss gate actuals projection completeness recovered"
            ),
        }
    }
}

fn record_submitted_pending_promoted(stats: &MinuteStats, promoted: u64) {
    stats.add_submitted_pending_promoted(promoted);
    if promoted > 0 {
        tracing::debug!(
            promoted, reason = "pending_after_submit",
            "submitted pending legs marked unknown for reconciliation; submission does not confirm fill"
        );
    }
}

/// 一个动作的估值依据；最后准入后所有腿沿用它，不追逐后台刷新。
#[derive(Clone)]
struct ActionFees {
    context: FeeContext,
    outcome: OutcomeFeeSnapshot,
    outcome_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FeeAdmissionError {
    DeadlineExpired,
    SelectedExpired,
    Current(FeeLookupError),
    RulesChanged,
}
impl FeeAdmissionError {
    fn reason(self) -> &'static str {
        match self {
            Self::DeadlineExpired => "confirmation_deadline_expired",
            Self::SelectedExpired => "selected_fee_snapshot_expired",
            Self::Current(err) => err.reason(),
            Self::RulesChanged => "fee_rules_changed",
        }
    }
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
    if platform == POLYMARKET {
        return json!({
            "version": 1, "fee_model": "pm_taker_v1",
            "polymarket_fee_rate": fees.context.polymarket_fee_rate.to_string(),
            "action": {
                "platform": platform, "token": token_id, "side": side.as_str(),
                "shares": shares.to_string(), "notional": notional.to_string(),
                "fee": fee.to_string(), "funding": funding.to_string(),
            }
        });
    }
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
            let query = ACTUALS_GATE_LOG.begin_query();
            let (pnl, unknown) = self
                .store
                .sum_actual_profit_with_timeouts(
                    self.cfg.pending_leg_timeout,
                    self.cfg.unknown_leg_timeout,
                )
                .await?;
            ACTUALS_GATE_LOG.record(&self.stats, query, unknown, Instant::now());
            if unknown > 0 {
                return Ok(true);
            }
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

    fn fee_context(&self, topic: &Topic) -> std::result::Result<ActionFees, FeeLookupError> {
        let outcome_id: u64 = topic
            .market_identity()
            .map_err(|_| FeeLookupError::InvalidIdentity)?
            .require(OUTCOME)
            .map_err(|_| FeeLookupError::InvalidIdentity)?
            .parse()
            .map_err(|_| FeeLookupError::InvalidIdentity)?;
        if topic
            .tokens
            .iter()
            .filter(|token| token.platform == OUTCOME)
            .any(|token| {
                crate::domain::parse_side_coin(&token.token_id).map(|(id, _)| id)
                    != Some(outcome_id)
            })
        {
            return Err(FeeLookupError::InvalidIdentity);
        }
        let outcome = self.outcome.lookup_fees(outcome_id)?;
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

    fn fees_admitted(
        &self,
        fees: &ActionFees,
        deadline: Instant,
    ) -> std::result::Result<(), FeeAdmissionError> {
        if Instant::now() >= deadline {
            return Err(FeeAdmissionError::DeadlineExpired);
        }
        if !fees.outcome.is_fresh() {
            return Err(FeeAdmissionError::SelectedExpired);
        }
        let current = self
            .outcome
            .lookup_fees(fees.outcome_id)
            .map_err(FeeAdmissionError::Current)?;
        if !fees.outcome.same_rules(&current) {
            return Err(FeeAdmissionError::RulesChanged);
        }
        Ok(())
    }

    // 只能在 books/事务锁外调用。即使刷新成功，本轮仍退出，下一轮重新确认。
    async fn prepare_fees(&self, topic: &Topic, action: &str, stage: &str) -> bool {
        match self.fee_context(topic) {
            Ok(_) => true,
            Err(err) => {
                self.stats.outcome_fee_unavailable();
                tracing::debug!(topic = %topic.key.as_str(), action, stage, reason = err.reason(), detail = err.detail(), "fee preparation rejected");
                self.refresh_missing_fees(err, action, stage).await;
                false
            }
        }
    }

    async fn refresh_missing_fees(&self, err: FeeLookupError, action: &str, stage: &str) {
        if matches!(err, FeeLookupError::Missing | FeeLookupError::Expired) {
            match self.outcome.refresh_fees_coordinated().await {
                Ok(outcome) => {
                    if outcome == crate::platforms::outcome::FeeRefreshOutcome::Refreshed {
                        self.stats.outcome_fee_refresh_ok();
                    }
                    tracing::debug!(
                        action,
                        stage,
                        cause = err.reason(),
                        ?outcome,
                        "fee preparation refresh completed; defer to next round"
                    );
                }
                Err(error) => {
                    self.stats.outcome_fee_refresh_failed();
                    tracing::warn!(action, stage, cause = err.reason(), %error, "fee preparation refresh failed; skip round");
                }
            }
        }
    }

    fn admit_fees(
        &self,
        fees: &ActionFees,
        deadline: Instant,
        action: &str,
        stage: &str,
        scope: &str,
    ) -> std::result::Result<(), FeeAdmissionError> {
        let result = self.fees_admitted(fees, deadline);
        if let Err(err) = result {
            let now = Instant::now();
            let (account_age_ms, market_age_ms) = fees.outcome.source_ages_ms();
            tracing::info!(
                action,
                stage,
                scope,
                outcome_id = fees.outcome_id,
                account_age_ms,
                market_age_ms,
                reason = err.reason(),
                remaining_ms = deadline.saturating_duration_since(now).as_millis() as u64,
                expired_ms = now.saturating_duration_since(deadline).as_millis() as u64,
                "fee confirmation rejected"
            );
            // 最终准入同步拒绝，先及时 abort/release；下轮锁外准备才允许刷新。
        }
        result
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
        if self.cfg.stop_arb_before_end {
            let checked_at = chrono::Utc::now();
            if arb_entry_closed(topic.end_date, checked_at) {
                self.stats.arb_skipped_before_end();
                tracing::debug!(
                    topic = %topic_key.as_str(),
                    end_date = ?topic.end_date,
                    %checked_at,
                    "arb skipped before market end"
                );
                return Ok(());
            }
        }
        {
            let mut cooldowns = self.rebalance_loss_cooldown.lock().await;
            let now = Instant::now();
            if let Some(&until) = cooldowns.get(&topic_key) {
                if now < until {
                    self.stats.rebalance_loss_cooldown();
                    tracing::debug!(
                        topic = %topic_key.as_str(),
                        remaining_secs = (until - now).as_secs(),
                        "arb evaluation skipped due to rebalance loss cooldown"
                    );
                    return Ok(());
                } else {
                    cooldowns.remove(&topic_key);
                }
            }
        }
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
        if !self.prepare_fees(&topic, "trading", "initial_screen").await {
            return Ok(());
        }
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
        if let Err(_reason) = self.admit_fees(
            selected_fees,
            confirmation_deadline,
            "arbitrage",
            "before_prepare",
            &topic.key.as_str(),
        ) {
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
        if let Err(_reason) = self.admit_fees(
            selected_fees,
            confirmation_deadline,
            "arbitrage",
            "after_prepare_before_archive",
            &topic.key.as_str(),
        ) {
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
                &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
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
            Err(Error::OrderTopicBlocked) => {
                self.stats.active_topic();
                tracing::info!(topic = %topic.key.as_str(), "order admission blocked by executing order or unfinished rebalance");
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
        if let Err(reason) = self.admit_fees(
            selected_fees,
            confirmation_deadline,
            "arbitrage",
            "after_archive",
            &format!(
                "topic={} order={order_id} legs={pm_leg},{out_leg}",
                topic.key.as_str()
            ),
        ) {
            self.store
                .abort_unsubmitted_legs(
                    &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
                    &[pm_leg, out_leg],
                    reason.reason(),
                )
                .await?;
            mark_orders_complete(
                &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
                &self.store,
            )
            .await?;
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
        if !self
            .prepare_fees(topic, "arbitrage", "final_book_confirmation")
            .await
        {
            return Ok(None);
        }
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
                    worst_profit = %confirmed.worst_profit,
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

    #[tracing::instrument(skip_all, fields(leg_id = leg_id, order_hash = %prepared.order_hash))]
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
                &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
                leg_id,
                &prepared.order_hash,
                &prepared.envelope,
                &prepared.payload,
                book_snapshot.as_ref(),
            )
            .await?;
        let (result, response) = self.pm.post_prepared(prepared).await?;
        persist_submit(
            &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
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
                &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
                leg_id,
                &prepared.order_hash,
                &prepared.envelope,
                &prepared.payload,
                book_snapshot.as_ref(),
            )
            .await?;
        let (result, response) = self.outcome.post_prepared(prepared).await?;
        persist_submit(
            &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
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
            .fail_stale_pending_unsubmitted(
                &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
                self.cfg.pending_leg_timeout,
            )
            .await?;
        if expired > 0 {
            tracing::warn!(expired, "failed stale pending legs never submitted");
        }
        let promoted = self
            .store
            .promote_submitted_pending_to_unknown(
                &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
                SUBMITTED_PENDING_GRACE_PERIOD,
            )
            .await?;
        record_submitted_pending_promoted(&self.stats, promoted);
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
            if leg.status == "pending" {
                continue;
            }
            if let Err(err) = self.reconcile_leg(&leg).await {
                tracing::warn!(leg_id = leg.id, error = %err, "reconcile failed");
            }
        }
        mark_orders_complete(
            &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
            &self.store,
        )
        .await?;
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
        let Some((current, poll, page)) = reconcile_pm_page(
            &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
            &self.pm,
            &self.store,
            leg,
        )
        .await?
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
        let Some(current) = self
            .store
            .record_order_poll(
                &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
                leg,
                &poll,
            )
            .await?
        else {
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
        let empty_missing_scan = trades_only
            && page
                .progress
                .get("trade_ids")
                .and_then(Value::as_array)
                .is_some_and(Vec::is_empty);
        let started = Instant::now();
        let resolution = apply_reconciliation_page(
            &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
            &self.store,
            leg,
            poll,
            page,
            || self.pm_reconciliation_fee_snapshot(leg),
        )
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
                trades_only,?fee_sources,
                reason = if empty_missing_scan && status == "failed" && shares.is_zero() {
                    "order_missing_empty_trade_scan"
                } else { "terminal_trade_evidence" },
                elapsed_ms=started.elapsed().as_millis() as u64,"trade reconciliation finalized"
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
            self.finish_terminal_lifecycle_action(order).await?;
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

        if !self.prepare_fees(&topic, "trading", "initial_screen").await {
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
                    self.complete_rebalance(order.id, key).await?;
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
                self.complete_rebalance(order.id, key).await?;
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
        let (released, take_profit_actuals) = self
            .store
            .release_lifecycle_with_actuals(order.id, action, claim_id)
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
        if action == "take_profit" && has_positive_fill {
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
                tracing::info!(order_id = order.id, claim_id = %claim_id, "take profit completed without priced fills; notification skipped");
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
        // Each branch commits before joining: a slow peer cannot delay durable stop or fee collection.
        let pm_future = async {
            let endpoint = self.pm.settlement_endpoint();
            if let Some(payouts) = self
                .store
                .platform_settlement_result(order_id, POLYMARKET, polymarket_market_id, endpoint)
                .await?
            {
                return Ok::<_, Error>(SettlementStatus::Settled { payouts });
            }
            let (status, response) = self
                .pm
                .settlement_with_evidence(polymarket_market_id)
                .await?;
            if let SettlementStatus::Settled { payouts } = &status {
                self.store
                    .save_platform_settlement_result(
                        order_id,
                        POLYMARKET,
                        polymarket_market_id,
                        endpoint,
                        payouts,
                        &response,
                    )
                    .await?;
            }
            Ok(status)
        };
        let outcome_future = async {
            let endpoint = self.outcome.info_endpoint();
            let status = if let Some(payouts) = self
                .store
                .platform_settlement_result(order_id, OUTCOME, outcome_market_id, endpoint)
                .await?
            {
                OutcomeSettlement::Settled { payouts }
            } else {
                let (status, response) = self
                    .outcome
                    .settlement_with_evidence(outcome_market_id)
                    .await?;
                if let OutcomeSettlement::Settled { payouts } = &status {
                    self.store
                        .save_platform_settlement_result(
                            order_id,
                            OUTCOME,
                            outcome_market_id,
                            endpoint,
                            payouts,
                            &response,
                        )
                        .await?;
                }
                status
            };
            let ready = if let OutcomeSettlement::Settled { payouts } = &status {
                let map = payouts
                    .iter()
                    .map(|p| ((OUTCOME.to_string(), p.token_id.clone()), p.payout))
                    .collect();
                self.prepare_outcome_settlement_fees(order_id, &map).await
            } else {
                Ok(false)
            };
            Ok::<_, Error>((status, ready))
        };
        let (pm, outcome_result) = tokio::join!(pm_future, outcome_future);
        let (outcome, fee_ready) = match outcome_result {
            Ok((status, ready)) => (Ok(status), ready),
            Err(error) => (Err(error), Ok(false)),
        };
        // PM 也可按 price 分数兑付；本指标只观测 Outcome，不覆盖所有跨平台估值偏差。
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
        let (mut access, settled_source) = settlement_decision(pm.as_ref(), outcome.as_ref());
        // Durable state unavailable is not permission to trade, including reduce-only.
        if pm.is_err() || outcome.is_err() {
            access = SettlementAccess::Stop;
        }
        if let Some(source) = settled_source {
            let evidence = json!({
                "polymarket": settlement_result_evidence(&pm),
                "outcome": settlement_result_evidence(&outcome),
                "settlement_fee_status": "unknown",
                "profit_basis": "gross_payout_less_trade_costs",
            });
            // One confirmed venue is enough to stop trading, but both payouts are required before
            // terminal state and actuals are made durable. Until then the order is retried.
            if let (Ok(SettlementStatus::Settled { .. }), Ok(OutcomeSettlement::Settled { .. })) =
                (&pm, &outcome)
            {
                // 市场结果不是到账证据；实扣费用未核实前仍保持停止交易。
                if !matches!(fee_ready, Ok(true)) {
                    let reason = match fee_ready {
                        Ok(false) => "settlement fee evidence scanning".to_string(),
                        Err(err) => err.to_string(),
                        Ok(true) => unreachable!(),
                    };
                    let pending = self
                        .store
                        .mark_settlement_pending(order_id, source, &evidence)
                        .await?;
                    if matches!(pending, Some((_, true))) {
                        self.stats.settlement_pending_entered();
                        tracing::warn!(
                            service = "outcome",
                            order_id,
                            reason,
                            "settlement fee pending; all trading stopped"
                        );
                    } else {
                        tracing::debug!(
                            service = "outcome",
                            order_id,
                            reason,
                            "settlement fee verification pending"
                        );
                    }
                    return Ok(SettlementAccess::Stop);
                }
                let started = Instant::now();
                let result = self
                    .store
                    .finalize_position_settlement(order_id, "polymarket+outcome", &evidence)
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
                            status: "settled (polymarket+outcome); 已核实 Outcome 结算费，收益已扣实费（无剩余仓位则无需结算费）"
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

    async fn prepare_outcome_settlement_fees(
        &self,
        order_id: i64,
        payouts: &HashMap<(String, String), Decimal>,
    ) -> Result<bool> {
        let endpoint = self.outcome.info_endpoint();
        let keys = match self
            .store
            .settlement_fee_keys_for_order(order_id, endpoint)
            .await
        {
            Ok(keys) => keys,
            Err(_) => {
                self.store
                    .settlement_collection_keys(order_id, endpoint)
                    .await?
            }
        };
        let mut ready = true;
        for key in keys {
            let payout = payouts
                .get(&(OUTCOME.to_string(), key.token.clone()))
                .copied()
                .ok_or_else(|| Error::msg("missing Outcome settlement payout"))?;
            match crate::settlement_fees::prepare_online(
                &self.store,
                self.outcome.http_client(),
                endpoint,
                &key,
                payout,
            )
            .await
            {
                Ok(done) => ready &= done,
                Err(error) => {
                    ready = false;
                    tracing::warn!(service="outcome",order_id,token_id=%key.token,error=%error,"settlement fee group remains pending");
                }
            }
        }
        Ok(ready)
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
        if !self
            .prepare_fees(topic, "take_profit", "final_book_confirmation")
            .await
        {
            return Ok(None);
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
        if let Err(reason) = self.admit_fees(
            &confirmed.fees,
            confirmed.deadline,
            "take_profit",
            "before_prepare",
            &format!("order={order_id} claim={claim_id}"),
        ) {
            return Err(Error::msg(reason.reason()));
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
        if let Err(reason) = self.admit_fees(
            &confirmed.fees,
            confirmed.deadline,
            "take_profit",
            "after_prepare_before_archive",
            &format!("order={order_id} claim={claim_id}"),
        ) {
            return Err(Error::msg(reason.reason()));
        }
        let leg_ids = self
            .store
            .insert_legs_atomic(
                &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
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
        if let Err(reason) = self.admit_fees(
            &confirmed.fees,
            confirmed.deadline,
            "take_profit",
            "after_archive",
            &format!("order={order_id} claim={claim_id}"),
        ) {
            self.store
                .abort_unsubmitted_legs(
                    &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
                    &[pm_leg, out_leg],
                    reason.reason(),
                )
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

    async fn complete_rebalance(&self, order_id: i64, key: TopicKey) -> Result<()> {
        let Some(projection) = self.store.mark_rebalance(order_id, "completed").await? else {
            return Ok(());
        };
        let Some((actual_cost, _actual_rev, actual_profit)) = projection.actuals() else {
            tracing::warn!(
                order_id,
                "rebalance completed with unknown actuals; notification skipped"
            );
            return Ok(());
        };
        tracing::info!(
            order_id,
            actual_profit = %actual_profit,
            actual_cost = %actual_cost,
            "rebalance completed"
        );
        if actual_profit < Decimal::ZERO {
            let until = Instant::now() + REBALANCE_LOSS_COOLDOWN;
            self.rebalance_loss_cooldown.lock().await.insert(key, until);
            tracing::warn!(
                order_id,
                topic = %key.as_str(),
                actual_profit = %actual_profit,
                cooldown_secs = REBALANCE_LOSS_COOLDOWN.as_secs(),
                "rebalance completed with negative profit; arb evaluation paused for 5m"
            );
        }
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
        if !self
            .prepare_fees(topic, "rebalance", "final_book_confirmation")
            .await
        {
            // 本轮尚未创建订单腿，退出时立即释放占用，不等待零腿超时回收。
            self.store
                .release_lifecycle(order_id, "rebalance", claim_id)
                .await?;
            return Ok(());
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
        if let Err(reason) = self.admit_fees(
            &selected,
            deadline,
            "rebalance",
            "before_prepare",
            &format!("order={order_id} claim={claim_id}"),
        ) {
            return Err(Error::msg(reason.reason()));
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
        if let Err(reason) = self.admit_fees(
            &selected,
            deadline,
            "rebalance",
            "after_prepare_before_archive",
            &format!("order={order_id} claim={claim_id}"),
        ) {
            return Err(Error::msg(reason.reason()));
        }
        let ids = self
            .store
            .insert_legs_atomic(
                &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
                order_id,
                "rebalance",
                claim_id,
                &legs,
            )
            .await?;
        if let Err(reason) = self.admit_fees(
            &selected,
            deadline,
            "rebalance",
            "after_archive",
            &format!("order={order_id} claim={claim_id}"),
        ) {
            self.store
                .abort_unsubmitted_legs(
                    &|wallet, token| self.outcome.latest_actuals_fee(wallet, token),
                    &ids,
                    reason.reason(),
                )
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

/// 不使用全局 get：HTTP 硬确认只能用这次通过票据验证的完整副本。
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

/// 已知结束时间且当前已进入结束前 48 小时（含边界与已结束）时关闭新套利。
fn arb_entry_closed(
    end_date: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    end_date.is_some_and(|end| now >= end - chrono::TimeDelta::days(2))
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
    resolver: &crate::store::actuals::FeeResolver<'_>,
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
        .record_reconciliation(resolver, leg, &matched, &evidence, &page.progress)
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
    resolver: &crate::store::actuals::FeeResolver<'_>,
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
    let Some(current) = store.record_order_poll(resolver, leg, &poll).await? else {
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
    resolver: &crate::store::actuals::FeeResolver<'_>,
    store: &Store,
    leg_id: i64,
    platform: &str,
    side: OrderSide,
    result: &SubmitResult,
    fees: &FeeContext,
    response: &crate::platforms::SubmissionResponse,
) -> Result<()> {
    let matched_fill = pm_matched_submit_fill(platform, side, result, fees, response);
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
            let mut evidence = json!({"kind":"ack","expected_shares":expected});
            if let Some((shares, price, fee)) = matched_fill {
                evidence["fill"] = json!({
                    "source":"submit_response", "shares":shares, "price":price,
                    "fee":fee, "fee_source":"estimated", "fee_rate":fees.polymarket_fee_rate,
                    "fee_formula":"shares * rate * price * (1 - price)",
                });
            } else if platform == POLYMARKET
                && response.get("status").and_then(Value::as_str) == Some("matched")
            {
                tracing::warn!(
                    service = "polymarket",
                    operation = "submit_fill",
                    endpoint = "/order",
                    leg_id,
                    order_id,
                    response_status = "matched",
                    reason = "invalid_matched_amounts",
                    "matched submission retained for reconciliation"
                );
            }
            (
                if matched_fill.is_some() {
                    "matched"
                } else {
                    "actived"
                },
                Some(order_id.as_str()),
                evidence,
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
        .record_submission_with_fill(
            resolver,
            leg_id,
            status,
            oid,
            &evidence,
            response,
            matched_fill,
        )
        .await
}

fn pm_matched_submit_fill(
    platform: &str,
    side: OrderSide,
    result: &SubmitResult,
    fees: &FeeContext,
    response: &Value,
) -> Option<(Decimal, Decimal, Decimal)> {
    if platform != POLYMARKET
        || response.get("success").and_then(Value::as_bool) != Some(true)
        || response.get("status").and_then(Value::as_str) != Some("matched")
    {
        return None;
    }
    let SubmitResult::Ack { making, taking, .. } = result else {
        return None;
    };
    // BUY/SELL 的 making、taking 含义相反；只使用响应金额，不回退到请求量或限价。
    let (shares, price) = pm_fak_fill(side, *making, *taking)?;
    if price > Decimal::ONE || fees.polymarket_fee_rate < Decimal::ZERO {
        return None;
    }
    let fee = crate::calc::estimate_polymarket_fee(shares, price, fees);
    Some((shares, price, fee))
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
    let (status, message) = match result {
        Ok(SubmitResult::Ack { .. }) => (PlaceStatus::Accepted, String::new()),
        Ok(SubmitResult::NoMatch { message, .. }) => (PlaceStatus::NoMatch, message),
        Ok(SubmitResult::Unknown { message, .. }) => (PlaceStatus::Pending, message),
        Ok(SubmitResult::Failed {
            status, message, ..
        }) => {
            let message = if message.trim().is_empty() {
                "提交被明确拒绝"
            } else {
                &message
            };
            (PlaceStatus::Rejected, format!("HTTP {status} {message}"))
        }
        // 外层错误也可能发生在提交后的持久化阶段，不能据此断言未成交。
        Err(_) => (
            PlaceStatus::ExecutionError,
            "执行异常，提交结果需核实".into(),
        ),
    };
    PlaceResult {
        platform,
        label,
        market,
        status,
        message,
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

pub async fn mark_orders_complete(
    resolver: &crate::store::actuals::FeeResolver<'_>,
    store: &Store,
) -> Result<()> {
    store.complete_orders(resolver).await
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
#[path = "../tests/unit/exec.rs"]
mod tests;
