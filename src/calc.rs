use crate::book::{BookSource, Level, OrderBook};
use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::{TokenRef, Topic};
use crate::platforms::OrderSide;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;

mod exact;

#[derive(Debug, Clone)]
pub struct FeeContext {
    /// Polymarket `feeSchedule.rate` (0.07 crypto, not 700 bps).
    pub polymarket_fee_rate: Decimal,
    /// 协议 taker 平仓费率，同时用于未来结算准备估计。
    pub outcome_taker_rate: Decimal,
    /// 实际随订单发送的 builder 比例；买卖均按名义额计提。
    pub outcome_builder_rate: Decimal,
}

#[derive(Debug, Clone)]
pub struct ArbLimits {
    pub cost_limit: Decimal,
    pub min_profit: Decimal,
    pub min_apr: Decimal,
    pub days: i64,
}

#[derive(Debug, Clone)]
pub struct LegPlan {
    pub platform: String,
    pub token_id: String,
    pub label: String,
    pub shares: Decimal,
    pub avg_price: Decimal,
    pub cap_price: Decimal,
    pub cost: Decimal,
    pub fee: Decimal,
}

#[derive(Debug, Clone)]
pub struct ArbPlan {
    pub pm: LegPlan,
    pub outcome: LegPlan,
    pub net_shares: Decimal,
    /// 所选结算估算政策下的互补组合最低情景净兑付，不是实扣保证。
    pub expected_revenue: Decimal,
    /// 未来结算准备，不计入即时现金成本或买腿费用。
    pub settlement_reserve: Decimal,
    pub total_cost: Decimal,
    pub profit: Decimal,
    /// 最坏成交情况下的保底净利润（两腿均按 cap 成交）。
    pub worst_profit: Decimal,
    /// 最坏成交情况下的总现金支出（两腿均按 cap 成交）。
    pub worst_cost: Decimal,
    pub roi: Decimal,
    pub apr: Decimal,
    // 精确快照仅 calc 持有；公开金额只是展示/存储投影。
    exact: exact::ExactMetrics,
}

impl ArbPlan {
    pub(crate) fn pm_balance_sufficient(&self, balance: Decimal) -> bool {
        self.exact.balance_sufficient(self, balance, true)
    }

    pub(crate) fn outcome_balance_sufficient(&self, balance: Decimal) -> bool {
        self.exact.balance_sufficient(self, balance, false)
    }

    pub(crate) fn pm_required(&self) -> Option<Decimal> {
        self.exact.required(self, true)
    }

    pub(crate) fn outcome_required(&self) -> Option<Decimal> {
        self.exact.required(self, false)
    }
}

#[cfg(test)]
#[derive(Clone, Default)]
struct LegacyAcc {
    pm_shares: Decimal,
    out_shares: Decimal,
    pm_cost: Decimal,
    out_cost: Decimal,
    pm_cap: Decimal,
    out_cap: Decimal,
}

#[cfg(test)]
impl LegacyAcc {
    fn plus(&self, net: Decimal, pm: &LegacyPmQuote, out_px: Decimal) -> Self {
        debug_assert_eq!(net, floor_shares(net));
        debug_assert!(net > Decimal::ZERO && net <= pm.max_net);
        Self {
            pm_shares: self.pm_shares + net,
            out_shares: self.out_shares + net,
            pm_cost: self.pm_cost + pm.cost(net),
            out_cost: self.out_cost + out_px * net,
            pm_cap: self.pm_cap.max(pm.cap),
            out_cap: self.out_cap.max(out_px),
        }
    }

    fn is_empty(&self) -> bool {
        self.pm_shares <= Decimal::ZERO || self.out_shares <= Decimal::ZERO
    }
}

pub fn complementary_pairs(labels: &[String]) -> Vec<(String, String)> {
    if labels.len() != 2 {
        return Vec::new();
    }
    vec![
        (labels[0].clone(), labels[1].clone()),
        (labels[1].clone(), labels[0].clone()),
    ]
}

/// FAK 买入：PM 至少 5 股且 1U；Outcome 买卖都至少 1U。PM FAK 卖出无股数/名义下限。
pub fn min_trade_cost(platform: &str, buy: bool) -> Decimal {
    if platform == POLYMARKET && !buy {
        Decimal::ZERO
    } else {
        Decimal::ONE
    }
}

pub fn min_trade_amount(platform: &str, buy: bool) -> Decimal {
    if platform == POLYMARKET && buy {
        Decimal::from(5)
    } else {
        Decimal::ZERO
    }
}

pub fn below_venue_mins(platform: &str, buy: bool, shares: Decimal, notional: Decimal) -> bool {
    shares < min_trade_amount(platform, buy) || notional < min_trade_cost(platform, buy)
}

pub fn days_until(end_date: Option<DateTime<Utc>>) -> i64 {
    days_until_from(end_date, Utc::now())
}

pub fn days_until_from(end_date: Option<DateTime<Utc>>, now: DateTime<Utc>) -> i64 {
    let Some(end) = end_date else {
        return 1;
    };
    if end <= now {
        return 1;
    }
    let secs = (end - now).num_seconds();
    if secs <= 0 {
        return 1;
    }
    let days = (secs as f64 / 86_400.0).ceil() as i64;
    days.max(1)
}

pub fn plan_arbitrage(
    _topic: &Topic,
    pm_yes_or_a: &OrderBook,
    out_comp: &OrderBook,
    pm_token: &TokenRef,
    out_token: &TokenRef,
    fees: &FeeContext,
    limits: &ArbLimits,
) -> Option<ArbPlan> {
    search_pair(
        pm_token,
        out_token,
        &pm_yes_or_a.asks,
        &out_comp.asks,
        fees,
        limits,
        pm_yes_or_a.tick_size?,
    )
}

/// 用最新盘口验证已有 plan：原 shares 能在 cap 内吃满，且按实际均价仍过门槛。
/// 通过后刷新本次实际财务估算，保持原身份、方向、数量和限价。
pub fn confirm_plan(
    topic: &Topic,
    plan: &ArbPlan,
    pm_book: &OrderBook,
    out_book: &OrderBook,
    fees: &FeeContext,
    limits: &ArbLimits,
) -> Option<ArbPlan> {
    validate_plan_on_books(topic, plan, pm_book, out_book, fees, limits).ok()
}

pub fn confirm_plan_reason(
    topic: &Topic,
    plan: &ArbPlan,
    pm_book: &OrderBook,
    out_book: &OrderBook,
    fees: &FeeContext,
    limits: &ArbLimits,
) -> &'static str {
    validate_plan_on_books(topic, plan, pm_book, out_book, fees, limits)
        .err()
        .unwrap_or("ok")
}

fn validate_plan_on_books(
    topic: &Topic,
    plan: &ArbPlan,
    pm_book: &OrderBook,
    out_book: &OrderBook,
    fees: &FeeContext,
    limits: &ArbLimits,
) -> Result<ArbPlan, &'static str> {
    let pm_token = topic
        .token(POLYMARKET, &plan.pm.label)
        .ok_or("missing_book")?;
    let out_token = topic
        .token(OUTCOME, &plan.outcome.label)
        .ok_or("missing_book")?;
    if pm_token.token_id != plan.pm.token_id
        || out_token.token_id != plan.outcome.token_id
        || pm_book.token_id != plan.pm.token_id
        || out_book.token_id != plan.outcome.token_id
    {
        return Err("token_mismatch");
    }
    let tick = pm_book.tick_size.ok_or("no_tick")?;
    // 新 tick 只能验证原 cap，不能重对齐并扩大已批准的限价。
    if tick <= Decimal::ZERO
        || tick > Decimal::ONE
        || plan.pm.cap_price < tick
        || plan.pm.cap_price > Decimal::ONE - tick
        || plan.pm.cap_price % tick != Decimal::ZERO
    {
        return Err("pm_unfillable");
    }
    exact::confirm(
        plan,
        &pm_book.asks,
        &out_book.asks,
        pm_token,
        out_token,
        fees,
        limits,
    )
}

#[cfg(test)]
fn take_asks_cost(
    asks: &[Level],
    shares: Decimal,
    cap: Decimal,
    floor_out: bool,
) -> Option<Decimal> {
    exact::project(&exact::take_asks_cost(asks, shares, cap, floor_out)?, false)
}

#[cfg(test)]
struct LegacyPmQuote {
    max_net: Decimal,
    first_cost: Decimal,
    cap: Decimal,
}

#[cfg(test)]
impl LegacyPmQuote {
    fn cost(&self, net: Decimal) -> Decimal {
        self.first_cost + (net - Decimal::ONE) * self.cap
    }
}

fn search_pair(
    pm_token: &TokenRef,
    out_token: &TokenRef,
    pm_asks: &[Level],
    out_asks: &[Level],
    fees: &FeeContext,
    limits: &ArbLimits,
    pm_tick: Decimal,
) -> Option<ArbPlan> {
    match exact::search(
        pm_token, out_token, pm_asks, out_asks, fees, limits, pm_tick,
    ) {
        Ok(plan) => Some(plan),
        Err(reason) => {
            if reason == "invalid_parameters" || reason == "unrepresentable" {
                tracing::warn!(
                    reason,
                    "new arbitrage calculation rejected unsupported input or projection"
                );
            }
            None
        }
    }
}

/// 与 `search_pair` 相同的首档：先按卖价升序，再丢掉数量不可用的档。
pub fn first_usable_ask(asks: &[Level], floor_out: bool) -> Option<(Decimal, Decimal)> {
    let mut asks = asks.to_vec();
    asks.sort_by(|a, b| a.price.cmp(&b.price));
    drop_unusable(&mut asks, floor_out);
    let level = asks.first()?;
    let size = if floor_out {
        floor_shares(level.size)
    } else {
        level.size
    };
    if level.price <= Decimal::ZERO || size <= Decimal::ZERO {
        return None;
    }
    Some((level.price, size))
}

fn drop_unusable(asks: &mut Vec<Level>, floor_out: bool) {
    while let Some(level) = asks.first() {
        let usable = if floor_out {
            floor_shares(level.size)
        } else {
            level.size
        };
        if level.price <= Decimal::ZERO || usable <= Decimal::ZERO {
            asks.remove(0);
        } else {
            break;
        }
    }
}

#[cfg(test)]
impl LegacyAcc {
    fn metrics(&self, fees: &FeeContext, limits: &ArbLimits) -> PlanMetrics {
        if self.is_empty() {
            return PlanMetrics::default();
        }
        let net = self.pm_shares.min(self.out_shares);
        let pm_avg = self.pm_cost / self.pm_shares;
        let out_avg = self.out_cost / self.out_shares;
        let pm_fee = estimate_polymarket_fee(self.pm_shares, pm_avg, fees);
        let out_fee = estimate_outcome_fee(self.out_cost, OrderSide::Buy, fees);
        let total_cost = self.pm_cost + self.out_cost + pm_fee + out_fee;
        let profit = net * (Decimal::ONE - fees.outcome_taker_rate) - total_cost;
        let roi = if total_cost > Decimal::ZERO {
            profit / total_cost
        } else {
            Decimal::ZERO
        };
        let days = limits.days.max(1);
        let apr = roi * Decimal::from(365) / Decimal::from(days);
        PlanMetrics {
            net,
            pm_avg,
            out_avg,
            pm_fee,
            out_fee,
            total_cost,
            profit,
            roi,
            apr,
        }
    }

    fn passes_mins(&self) -> bool {
        if self.is_empty() {
            return false;
        }
        self.pm_shares >= min_trade_amount(POLYMARKET, true)
            && self.pm_cost >= min_trade_cost(POLYMARKET, true)
            && self.out_shares >= min_trade_amount(OUTCOME, true)
            && self.out_cap * self.out_shares >= min_trade_cost(OUTCOME, true)
    }

    fn passes_profit_and_mins(&self, fees: &FeeContext, limits: &ArbLimits) -> bool {
        let m = self.metrics(fees, limits);
        self.passes_mins() && m.profit >= limits.min_profit && m.apr >= limits.min_apr
    }

    fn passes_all(&self, fees: &FeeContext, limits: &ArbLimits) -> bool {
        let m = self.metrics(fees, limits);
        self.passes_profit_and_mins(fees, limits) && m.total_cost <= limits.cost_limit
    }
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Default)]
struct PlanMetrics {
    net: Decimal,
    pm_avg: Decimal,
    out_avg: Decimal,
    pm_fee: Decimal,
    out_fee: Decimal,
    total_cost: Decimal,
    profit: Decimal,
    roi: Decimal,
    apr: Decimal,
}

pub fn best_plan(
    topic: &Topic,
    books: &crate::book::BookStore,
    fees: &FeeContext,
    limits: &ArbLimits,
) -> Option<ArbPlan> {
    let labels = topic.labels();
    let mut best: Option<ArbPlan> = None;
    for (pm_label, out_label) in complementary_pairs(&labels) {
        let Some(pm_token) = topic.token(POLYMARKET, &pm_label) else {
            continue;
        };
        let Some(out_token) = topic.token(OUTCOME, &out_label) else {
            continue;
        };
        let Some(pm_book) = books.get(POLYMARKET, &pm_token.token_id) else {
            continue;
        };
        let Some(out_book) = books.get(OUTCOME, &out_token.token_id) else {
            continue;
        };
        // 静止盘口可用于发现候选；实际下单仍须通过新鲜的双边 REST 确认。
        if pm_book.stale || out_book.stale {
            continue;
        }
        if let Some(plan) =
            plan_arbitrage(topic, pm_book, out_book, pm_token, out_token, fees, limits)
        {
            let better = match &best {
                None => true,
                Some(cur) => plan.exact.compare(&cur.exact).is_gt(),
            };
            if better {
                best = Some(plan);
            }
        }
    }
    best
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CalcSkipCounts {
    pub missing_book: u64,
    pub stale_book: u64,
    pub stale_pm_only: u64,
    pub stale_out_only: u64,
    pub stale_both: u64,
    pub stale_pm_invalid: u64,
    pub stale_pm_expired: u64,
    pub stale_out_invalid: u64,
    pub stale_out_expired: u64,
    pub unit_cost: u64,
    pub unprofitable: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CalcBookState {
    pub source: BookSource,
    pub age_ms: u64,
    pub invalid: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CalcStaleDetail {
    pub pm: CalcBookState,
    pub out: CalcBookState,
    /// 仅作盘口年龄的诊断背景，不参与普通套利候选准入。
    pub threshold_ms: u64,
    pub kind: StaleKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaleKind {
    PmOnly,
    OutOnly,
    Both,
}

impl StaleKind {
    pub fn index(self) -> usize {
        match self {
            Self::PmOnly => 0,
            Self::OutOnly => 1,
            Self::Both => 2,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PmOnly => "pm_only",
            Self::OutOnly => "out_only",
            Self::Both => "both",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CalcPairSample {
    pub pm_label: String,
    pub out_label: String,
    pub pm_ask: Option<Decimal>,
    pub pm_sz: Option<Decimal>,
    pub out_ask: Option<Decimal>,
    pub out_sz: Option<Decimal>,
    pub unit_cost: Option<Decimal>,
    pub unit_expected_revenue: Option<Decimal>,
    pub reason: &'static str,
    pub stale: Option<CalcStaleDetail>,
}

impl CalcPairSample {
    pub fn compact(&self) -> String {
        format!(
            "{}/{} pm={}x{} out={}x{} unit={} revenue={} reason={}",
            self.pm_label,
            self.out_label,
            fmt_dec(self.pm_ask),
            fmt_dec(self.pm_sz),
            fmt_dec(self.out_ask),
            fmt_dec(self.out_sz),
            fmt_dec(self.unit_cost),
            fmt_dec(self.unit_expected_revenue),
            self.reason
        )
    }
}

#[derive(Debug, Clone)]
pub struct CalcMissSnapshot {
    pub topic: String,
    pub pairs: Vec<CalcPairSample>,
}

impl CalcMissSnapshot {
    pub fn log(&self) {
        let pairs = self
            .pairs
            .iter()
            .map(CalcPairSample::compact)
            .collect::<Vec<_>>()
            .join(" | ");
        tracing::info!(topic = %self.topic, pairs = %pairs, "calc miss sample");
    }
}

fn fmt_dec(value: Option<Decimal>) -> String {
    value
        .map(|v| v.normalize().to_string())
        .unwrap_or_else(|| "-".into())
}

/// 按互补 pair 分类未成对原因，并带上首档价格，供分钟样本日志使用。
pub fn inspect_calc(
    topic: &Topic,
    books: &crate::book::BookStore,
    fees: &FeeContext,
    limits: &ArbLimits,
    now: std::time::Instant,
    stale: std::time::Duration,
) -> (CalcSkipCounts, Vec<CalcPairSample>) {
    let mut counts = CalcSkipCounts::default();
    let mut pairs = Vec::new();
    for (pm_label, out_label) in complementary_pairs(&topic.labels()) {
        let sample = diagnose_pair(
            topic, books, fees, limits, now, stale, &pm_label, &out_label,
        );
        if sample.reason == "ok" {
            continue;
        }
        match sample.reason {
            "missing_book" => counts.missing_book += 1,
            "stale_book" => {
                counts.stale_book += 1;
                if let Some(detail) = sample.stale {
                    match detail.kind {
                        StaleKind::PmOnly => counts.stale_pm_only += 1,
                        StaleKind::OutOnly => counts.stale_out_only += 1,
                        StaleKind::Both => counts.stale_both += 1,
                    }
                    if detail.pm.invalid {
                        counts.stale_pm_invalid += 1;
                    }
                    if detail.out.invalid {
                        counts.stale_out_invalid += 1;
                    }
                }
            }
            "unit_cost_ge_revenue" => counts.unit_cost += 1,
            _ => counts.unprofitable += 1,
        }
        pairs.push(sample);
    }
    (counts, pairs)
}

fn diagnose_pair(
    topic: &Topic,
    books: &crate::book::BookStore,
    fees: &FeeContext,
    limits: &ArbLimits,
    now: std::time::Instant,
    stale: std::time::Duration,
    pm_label: &str,
    out_label: &str,
) -> CalcPairSample {
    let mut sample = CalcPairSample {
        pm_label: pm_label.to_string(),
        out_label: out_label.to_string(),
        pm_ask: None,
        pm_sz: None,
        out_ask: None,
        out_sz: None,
        unit_cost: None,
        unit_expected_revenue: None,
        reason: "missing_book",
        stale: None,
    };
    let Some(pm_token) = topic.token(POLYMARKET, pm_label) else {
        return sample;
    };
    let Some(out_token) = topic.token(OUTCOME, out_label) else {
        return sample;
    };
    let Some((pm_book, pm_source)) = books.get_with_source(POLYMARKET, &pm_token.token_id) else {
        return sample;
    };
    let Some((out_book, out_source)) = books.get_with_source(OUTCOME, &out_token.token_id) else {
        return sample;
    };
    if pm_book.stale || out_book.stale {
        sample.reason = "stale_book";
        sample.stale = Some(CalcStaleDetail {
            pm: CalcBookState {
                source: pm_source,
                age_ms: now.duration_since(pm_book.received_at).as_millis() as u64,
                invalid: pm_book.stale,
            },
            out: CalcBookState {
                source: out_source,
                age_ms: now.duration_since(out_book.received_at).as_millis() as u64,
                invalid: out_book.stale,
            },
            threshold_ms: stale.as_millis() as u64,
            kind: match (pm_book.stale, out_book.stale) {
                (true, false) => StaleKind::PmOnly,
                (false, true) => StaleKind::OutOnly,
                _ => StaleKind::Both,
            },
        });
        return sample;
    }
    diagnose_loaded(
        topic, pm_book, out_book, pm_token, out_token, fees, limits, sample,
    )
}

/// 按与 `plan_arbitrage` 相同的输入诊断一对盘口，不检查新鲜度。
pub fn diagnose_books(
    topic: &Topic,
    pm_book: &OrderBook,
    out_book: &OrderBook,
    fees: &FeeContext,
    limits: &ArbLimits,
    pm_label: &str,
    out_label: &str,
) -> CalcPairSample {
    let sample = CalcPairSample {
        pm_label: pm_label.to_string(),
        out_label: out_label.to_string(),
        pm_ask: None,
        pm_sz: None,
        out_ask: None,
        out_sz: None,
        unit_cost: None,
        unit_expected_revenue: None,
        reason: "missing_book",
        stale: None,
    };
    let Some(pm_token) = topic.token(POLYMARKET, pm_label) else {
        return sample;
    };
    let Some(out_token) = topic.token(OUTCOME, out_label) else {
        return sample;
    };
    diagnose_loaded(
        topic, pm_book, out_book, pm_token, out_token, fees, limits, sample,
    )
}

fn diagnose_loaded(
    _topic: &Topic,
    pm_book: &OrderBook,
    out_book: &OrderBook,
    pm_token: &TokenRef,
    out_token: &TokenRef,
    fees: &FeeContext,
    limits: &ArbLimits,
    mut sample: CalcPairSample,
) -> CalcPairSample {
    if let Some((pm_px, pm_sz)) = first_usable_ask(&pm_book.asks, false) {
        if let Some((out_px, out_sz)) = first_usable_ask(&out_book.asks, true) {
            sample.pm_ask = Some(pm_px);
            sample.pm_sz = Some(pm_sz);
            sample.out_ask = Some(out_px);
            sample.out_sz = Some(out_sz);
            sample.unit_cost = exact::unit_display(pm_px, out_px, fees);
            sample.unit_expected_revenue = Some(Decimal::ONE - fees.outcome_taker_rate);
        }
    }
    sample.reason = match pm_book.tick_size {
        None => "no_tick",
        Some(tick) => exact::search(
            pm_token,
            out_token,
            &pm_book.asks,
            &out_book.asks,
            fees,
            limits,
            tick,
        )
        .err()
        .unwrap_or("ok"),
    };
    sample
}

pub fn estimate_polymarket_fee(shares: Decimal, price: Decimal, fees: &FeeContext) -> Decimal {
    if fees.polymarket_fee_rate.is_zero() {
        return Decimal::ZERO;
    }
    let one_minus = Decimal::ONE - price;
    // Official: fee = C × feeRate × p × (1 - p)
    shares * fees.polymarket_fee_rate * price * one_minus
}

/// 仅用于增加正仓的买入、减少可用正仓的卖出；不推广到带符号仓位。
pub fn estimate_outcome_fee(notional: Decimal, side: OrderSide, fees: &FeeContext) -> Decimal {
    let rate = match side {
        OrderSide::Buy => fees.outcome_builder_rate,
        OrderSide::Sell => fees.outcome_taker_rate + fees.outcome_builder_rate,
    };
    notional * rate
}

pub fn estimate_taker_fee(
    platform: &str,
    side: OrderSide,
    shares: Decimal,
    price: Decimal,
    fees: &FeeContext,
) -> Decimal {
    if platform == POLYMARKET {
        estimate_polymarket_fee(shares, price, fees)
    } else {
        estimate_outcome_fee(shares * price, side, fees)
    }
}

pub fn floor_shares(value: Decimal) -> Decimal {
    value.trunc()
}

pub fn align_polymarket_price(price: Decimal, tick: Decimal) -> Decimal {
    align_polymarket_tick(price, tick, true)
}

pub fn align_polymarket_sell_price(price: Decimal, tick: Decimal) -> Decimal {
    align_polymarket_tick(price, tick, false)
}

fn align_polymarket_tick(price: Decimal, tick: Decimal, buy: bool) -> Decimal {
    let tick = if tick > Decimal::ZERO {
        tick
    } else {
        Decimal::from_str("0.01").unwrap()
    };
    let rounded = if buy {
        (price / tick).ceil() * tick
    } else {
        (price / tick).floor() * tick
    };
    let min = tick;
    let max = (Decimal::ONE - tick).max(min);
    rounded.clamp(min, max)
}

pub fn align_hedge_price(
    platform: &str,
    buy: bool,
    price: Decimal,
    pm_tick: Option<Decimal>,
) -> Option<Decimal> {
    if platform == POLYMARKET {
        let tick = pm_tick?;
        Some(if buy {
            align_polymarket_price(price, tick)
        } else {
            align_polymarket_sell_price(price, tick)
        })
    } else {
        Some(align_outcome_price(price))
    }
}

pub fn align_outcome_price(price: Decimal) -> Decimal {
    let aligned = round_sigfigs(price, 5);
    let min = Decimal::from_str("0.001").unwrap();
    let max = Decimal::from_str("0.999").unwrap();
    aligned.clamp(min, max)
}

pub fn round_sigfigs(value: Decimal, sig: u32) -> Decimal {
    if value.is_zero() {
        return value;
    }
    let f = value.abs().to_f64().unwrap_or(0.0);
    if f == 0.0 {
        return Decimal::ZERO;
    }
    let digits = sig as i32 - 1 - f.log10().floor() as i32;
    let factor = 10f64.powi(digits);
    let rounded = (f * factor).round() / factor;
    Decimal::from_str(&format!("{rounded}")).unwrap_or(value)
}

#[cfg(test)]
#[path = "../tests/unit/calc.rs"]
mod tests;
