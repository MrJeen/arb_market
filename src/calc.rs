use crate::book::{Level, OrderBook};
use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::{TokenRef, Topic};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;

mod exact;

#[derive(Debug, Clone)]
pub struct FeeContext {
    /// Polymarket `feeSchedule.rate` (0.07 crypto, not 700 bps).
    pub polymarket_fee_rate: Decimal,
    pub outcome_taker_rate: Decimal,
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
    pub total_cost: Decimal,
    pub profit: Decimal,
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
        let out_fee = estimate_outcome_fee(self.out_cost, fees);
        let total_cost = self.pm_cost + self.out_cost + pm_fee + out_fee;
        let profit = net - total_cost;
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
    now: std::time::Instant,
    stale: std::time::Duration,
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
        if !pm_book.is_fresh(stale, now) || !out_book.is_fresh(stale, now) {
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
    pub unit_cost: u64,
    pub unprofitable: u64,
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
    pub reason: &'static str,
}

impl CalcPairSample {
    pub fn compact(&self) -> String {
        format!(
            "{}/{} pm={}x{} out={}x{} unit={} reason={}",
            self.pm_label,
            self.out_label,
            fmt_dec(self.pm_ask),
            fmt_dec(self.pm_sz),
            fmt_dec(self.out_ask),
            fmt_dec(self.out_sz),
            fmt_dec(self.unit_cost),
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
            "stale_book" => counts.stale_book += 1,
            "unit_cost_ge_1" => counts.unit_cost += 1,
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
        reason: "missing_book",
    };
    let Some(pm_token) = topic.token(POLYMARKET, pm_label) else {
        return sample;
    };
    let Some(out_token) = topic.token(OUTCOME, out_label) else {
        return sample;
    };
    let Some(pm_book) = books.get(POLYMARKET, &pm_token.token_id) else {
        return sample;
    };
    let Some(out_book) = books.get(OUTCOME, &out_token.token_id) else {
        return sample;
    };
    if !pm_book.is_fresh(stale, now) || !out_book.is_fresh(stale, now) {
        sample.reason = "stale_book";
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
        reason: "missing_book",
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

pub fn estimate_outcome_fee(notional: Decimal, fees: &FeeContext) -> Decimal {
    notional * fees.outcome_taker_rate
}

pub fn estimate_taker_fee(
    platform: &str,
    shares: Decimal,
    price: Decimal,
    fees: &FeeContext,
) -> Decimal {
    if platform == POLYMARKET {
        estimate_polymarket_fee(shares, price, fees)
    } else {
        estimate_outcome_fee(shares * price, fees)
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
mod tests {
    use super::*;
    use crate::book::{BookStore, Level};
    use crate::domain::{TokenRef, Topic, TopicKey};
    use std::time::Instant;
    use uuid::Uuid;

    pub(super) fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn token(platform: &str, id: &str, label: &str) -> TokenRef {
        TokenRef {
            platform: platform.into(),
            token_id: id.into(),
            label: label.into(),
            option_id: "1".into(),
            condition_id: None,
            asset_id: None,
            side_index: None,
            neg_risk: None,
            fees_enabled: None,
            fee_rate: None,
        }
    }

    pub(super) fn sample_topic() -> Topic {
        Topic {
            key: TopicKey::new(Uuid::nil(), 0),
            title: "t".into(),
            market_title: "m".into(),
            end_date: None,
            tokens: vec![
                token(POLYMARKET, "pm-yes", "yes"),
                token(POLYMARKET, "pm-no", "no"),
                token(OUTCOME, "#10", "no"),
                token(OUTCOME, "#11", "yes"),
            ],
        }
    }

    pub(super) fn fees_zero() -> FeeContext {
        FeeContext {
            polymarket_fee_rate: Decimal::ZERO,
            outcome_taker_rate: Decimal::ZERO,
        }
    }

    pub(super) fn limits(min_profit: &str, cost_limit: &str) -> ArbLimits {
        ArbLimits {
            cost_limit: d(cost_limit),
            min_profit: d(min_profit),
            min_apr: Decimal::ZERO,
            days: 1,
        }
    }

    fn snapshot(
        books: &mut BookStore,
        platform: &str,
        token_id: &str,
        asks: Vec<(&str, &str)>,
        now: Instant,
    ) {
        books.replace_snapshot(
            platform,
            token_id,
            vec![],
            asks.into_iter()
                .map(|(p, s)| Level {
                    price: d(p),
                    size: d(s),
                })
                .collect(),
            1,
            now,
        );
        if platform == POLYMARKET {
            books.set_tick_size(platform, token_id, d("0.01"));
        }
    }

    fn plan_with(books: &BookStore, now: Instant, limits: &ArbLimits) -> ArbPlan {
        best_plan(
            &sample_topic(),
            books,
            &fees_zero(),
            limits,
            now,
            std::time::Duration::from_secs(5),
        )
        .expect("plan")
    }

    #[test]
    fn venue_mins_match_fak_and_outcome_rules() {
        assert!(below_venue_mins(POLYMARKET, true, d("4"), d("2")));
        assert!(below_venue_mins(POLYMARKET, true, d("5"), d("0.9")));
        assert!(!below_venue_mins(POLYMARKET, true, d("5"), d("1")));
        assert!(!below_venue_mins(POLYMARKET, false, d("1"), d("0.01")));
        assert!(below_venue_mins(OUTCOME, true, d("20"), d("0.99")));
        assert!(below_venue_mins(OUTCOME, false, d("1"), d("0.5")));
        assert!(!below_venue_mins(OUTCOME, true, d("2"), d("1")));
        assert!(!below_venue_mins(OUTCOME, false, d("1"), d("1")));
    }

    fn inspect_reasons(books: &BookStore, now: Instant, limits: &ArbLimits) -> Vec<&'static str> {
        inspect_calc(
            &sample_topic(),
            books,
            &fees_zero(),
            limits,
            now,
            std::time::Duration::from_secs(5),
        )
        .1
        .into_iter()
        .map(|p| p.reason)
        .collect()
    }

    #[test]
    fn inspect_reports_unit_cost_and_venue_min() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.60", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.50", "50")], now);
        snapshot(&mut books, POLYMARKET, "pm-no", vec![("0.40", "3")], now);
        snapshot(&mut books, OUTCOME, "#11", vec![("0.40", "3")], now);
        let reasons = inspect_reasons(&books, now, &limits("-1", "10"));
        assert!(reasons.contains(&"unit_cost_ge_1"));
        assert!(reasons.contains(&"venue_min"));
    }

    #[test]
    fn inspect_reports_no_tick() {
        let mut books = BookStore::default();
        let now = Instant::now();
        books.replace_snapshot(
            POLYMARKET,
            "pm-yes",
            vec![],
            vec![Level {
                price: d("0.40"),
                size: d("50"),
            }],
            1,
            now,
        );
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        snapshot(&mut books, POLYMARKET, "pm-no", vec![("0.60", "50")], now);
        snapshot(&mut books, OUTCOME, "#11", vec![("0.50", "50")], now);
        let reasons = inspect_reasons(&books, now, &limits("-1", "10"));
        assert!(reasons.contains(&"no_tick"));
    }

    #[test]
    fn inspect_reports_cost_limit() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        snapshot(&mut books, POLYMARKET, "pm-no", vec![("0.60", "50")], now);
        snapshot(&mut books, OUTCOME, "#11", vec![("0.50", "50")], now);
        let reasons = inspect_reasons(&books, now, &limits("-1", "2"));
        assert!(reasons.contains(&"cost_limit"));
    }

    fn assert_fractional_plan(
        pm_asks: Vec<Level>,
        out_asks: Vec<Level>,
        fees: &FeeContext,
        limits: &ArbLimits,
        shares: Decimal,
        pm_cost: Decimal,
        pm_cap: Decimal,
    ) -> ArbPlan {
        let now = Instant::now();
        let mut books = BookStore::default();
        books.replace_snapshot(POLYMARKET, "pm-yes", vec![], pm_asks, 1, now);
        books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
        books.replace_snapshot(OUTCOME, "#10", vec![], out_asks, 1, now);
        let topic = sample_topic();
        let pm = books.get(POLYMARKET, "pm-yes").unwrap();
        let out = books.get(OUTCOME, "#10").unwrap();
        let plan = plan_arbitrage(
            &topic,
            pm,
            out,
            &topic.tokens[0],
            &topic.tokens[2],
            fees,
            limits,
        )
        .expect("fractional depth is fillable");
        assert_eq!(plan.net_shares, shares);
        assert_eq!(plan.pm.shares, shares);
        assert_eq!(plan.outcome.shares, shares);
        assert_eq!(floor_shares(shares), shares);
        assert_eq!(plan.pm.cost, pm_cost);
        assert_eq!(plan.pm.cap_price, pm_cap);
        assert_eq!(
            take_asks_cost(&pm.asks, shares, plan.pm.cap_price, false),
            Some(plan.pm.cost)
        );
        assert_eq!(
            take_asks_cost(&out.asks, shares, plan.outcome.cap_price, true),
            Some(plan.outcome.cost)
        );
        assert_eq!(
            plan.total_cost,
            plan.pm.cost + plan.outcome.cost + plan.pm.fee + plan.outcome.fee
        );
        assert!(confirm_plan(&topic, &plan, pm, out, fees, limits).is_some());
        plan
    }

    pub(super) fn levels(rows: &[(&str, &str)]) -> Vec<Level> {
        rows.iter()
            .map(|(price, size)| Level {
                price: d(price),
                size: d(size),
            })
            .collect()
    }

    #[test]
    fn legacy_decimal_28_digit_flat_oracle_records_nonconvexity() {
        let mut counterexamples = 0;
        let mut checked_28_digit_case = false;
        // 实数模型的边际总成本恰为1，跨档折价让收益近乎平坦；
        // 仅保留旧 Decimal 舍入对照；不是新精确搜索的 oracle。
        for p in [d("0.4"), d("0.5"), d("0.6")] {
            for rate in [d("0.07"), d("0.1"), d("1")] {
                let fees = FeeContext {
                    polymarket_fee_rate: rate,
                    outcome_taker_rate: Decimal::ZERO,
                };
                let out_px = Decimal::ONE - p - rate * p * (Decimal::ONE - p);
                for scale in (13..=28).rev() {
                    checked_28_digit_case |= scale == 28;
                    let discount = Decimal::new(1, scale);
                    let quote = LegacyPmQuote {
                        max_net: d("40"),
                        first_cost: p - discount,
                        cap: p,
                    };
                    let acc = LegacyAcc::default();
                    let mut bounds = limits("0", "100");
                    bounds.days = 365;
                    let profits: Vec<_> = (5..=40)
                        .map(|n| {
                            acc.plus(Decimal::from(n), &quote, out_px)
                                .metrics(&fees, &bounds)
                                .profit
                        })
                        .collect();
                    for left in 0..profits.len() - 2 {
                        for right in left + 2..profits.len() {
                            for mid in left + 1..right {
                                if profits[mid] > profits[left].max(profits[right]) {
                                    bounds.min_profit = profits[mid];
                                    let passes = |i: usize| {
                                        acc.plus(Decimal::from(i + 5), &quote, out_px)
                                            .passes_all(&fees, &bounds)
                                    };
                                    assert!(!passes(left));
                                    assert!(passes(mid));
                                    assert!(!passes(right));
                                    counterexamples += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        assert!(checked_28_digit_case);
        assert!(counterexamples > 0, "旧 Decimal 舍入对照应保留非凸反例");
    }

    #[test]
    fn legacy_decimal_prefix_search_rounding_false_positive() {
        // 正常价格/[0,1]费率/非负门槛仍可出现两端失败而中间通过。
        // 旧 Decimal 的39股通过是舍入假阳性；新路径使用精确有理数。
        let quote = LegacyPmQuote {
            max_net: d("40"),
            first_cost: d("0.39999999999999999999999999"),
            cap: d("0.4"),
        };
        let fees = FeeContext {
            polymarket_fee_rate: Decimal::ONE,
            outcome_taker_rate: Decimal::ZERO,
        };
        let bounds = ArbLimits {
            days: 365,
            ..limits("0.000000000000000000000000013", "100")
        };
        let acc = LegacyAcc::default();
        for (n, profit, passes) in [
            (5, "0.0000000000000000000000000120", false),
            (39, "0.000000000000000000000000013", true),
            (40, "0.000000000000000000000000012", false),
        ] {
            let trial = acc.plus(Decimal::from(n), &quote, d("0.36"));
            assert!(trial.passes_mins());
            assert_eq!(trial.metrics(&fees, &bounds).profit, d(profit));
            assert_eq!(trial.passes_all(&fees, &bounds), passes);
        }
        let maximum = (5..=40).rev().find(|n| {
            acc.plus(Decimal::from(*n), &quote, d("0.36"))
                .passes_all(&fees, &bounds)
        });
        assert_eq!(maximum, Some(39));
    }

    #[test]
    fn known_regression_fractional_profit_boundary_original_example() {
        let plan = assert_fractional_plan(
            levels(&[("0.30", "4.5"), ("0.61", "100")]),
            levels(&[("0.40", "200")]),
            &fees_zero(),
            &limits("1", "100"),
            d("39"),
            d("22.395"),
            d("0.61"),
        );
        assert_eq!(plan.profit, d("1.005"));
        let cost_40 = take_asks_cost(
            &levels(&[("0.30", "4.5"), ("0.61", "100")]),
            d("40"),
            d("0.61"),
            false,
        )
        .unwrap();
        assert_eq!(d("40") - cost_40 - d("16"), d("0.995"));
    }

    #[test]
    fn fractional_first_level_fills_terminal_depth_not_just_venue_min() {
        assert_fractional_plan(
            levels(&[("0.30", "0.5"), ("0.40", "100")]),
            levels(&[("0.40", "1000")]),
            &fees_zero(),
            &limits("0", "1000"),
            d("100"),
            d("39.95"),
            d("0.40"),
        );
    }

    #[test]
    fn fractional_levels_cross_to_first_feasible_physical_interval() {
        assert_fractional_plan(
            levels(&[
                ("0.20", "0.2"),
                ("0.30", "0.3"),
                ("0.40", "0.5"),
                ("0.45", "20"),
            ]),
            levels(&[("0.40", "30")]),
            &fees_zero(),
            &limits("1", "100"),
            d("21"),
            d("9.33"),
            d("0.45"),
        );
    }

    #[test]
    fn fractional_normal_level_remainder_keeps_real_cost_and_worst_cap() {
        assert_fractional_plan(
            levels(&[("0.30", "4.5"), ("0.40", "100")]),
            levels(&[("0.40", "200")]),
            &fees_zero(),
            &limits("0", "1000"),
            d("104"),
            d("41.15"),
            d("0.40"),
        );
        let plan = assert_fractional_plan(
            levels(&[("0.20", "0.5"), ("0.21", "1.5"), ("0.40", "100")]),
            levels(&[("0.40", "1000")]),
            &fees_zero(),
            &limits("0", "3.615"),
            d("5"),
            d("1.615"),
            d("0.40"),
        );
        assert_eq!(plan.pm.avg_price, d("0.323"));
    }

    #[test]
    fn fractional_many_tiny_levels_do_not_hit_candidate_limit() {
        let pm = (0..1000)
            .map(|i| Level {
                price: d("0.20") + Decimal::from(i) * d("0.0001"),
                size: d("0.01"),
            })
            .collect();
        assert_fractional_plan(
            pm,
            levels(&[("0.40", "10")]),
            &fees_zero(),
            &limits("3", "100"),
            d("9"),
            d("2.20455"),
            d("0.29"),
        );
        let pm = (0..100)
            .map(|i| Level {
                price: d("0.20") + Decimal::from(i) * d("0.001"),
                size: d("0.1"),
            })
            .collect();
        assert_fractional_plan(
            pm,
            levels(&[("0.40", "10")]),
            &fees_zero(),
            &limits("3", "100"),
            d("9"),
            d("2.2005"),
            d("0.29"),
        );
    }

    #[test]
    fn fractional_search_can_visit_more_than_sixty_four_quotes() {
        let pm = (0..160)
            .map(|i| Level {
                price: d("0.20") + Decimal::from(i) * d("0.0001"),
                size: d("0.5"),
            })
            .collect();
        assert_fractional_plan(
            pm,
            levels(&[("0.40", "100")]),
            &fees_zero(),
            &limits("27", "100"),
            d("69"),
            d("14.27265"),
            d("0.22"),
        );
    }

    #[test]
    fn fractional_budget_refuses_unproven_fee_monotonicity() {
        for (pm_rate, out_rate) in [("-0.01", "0"), ("1.01", "0"), ("0", "-0.01"), ("0", "1.01")] {
            let fees = FeeContext {
                polymarket_fee_rate: d(pm_rate),
                outcome_taker_rate: d(out_rate),
            };
            assert!(search_pair(
                &sample_topic().tokens[0],
                &sample_topic().tokens[2],
                &levels(&[("0.3", "0.5"), ("0.4", "100")]),
                &levels(&[("0.4", "100")]),
                &fees,
                &limits("0", "100"),
                d("0.01")
            )
            .is_none());
        }
    }

    #[test]
    fn fractional_quotes_scale_with_levels_not_share_count() {
        assert_fractional_plan(
            levels(&[("0.30", "0.5"), ("0.40", "1000000000000")]),
            levels(&[("0.40", "1000000000000")]),
            &fees_zero(),
            &limits("0", "1000000000000"),
            d("1000000000000"),
            d("399999999999.95"),
            d("0.40"),
        );
    }

    #[test]
    fn fractional_budget_exact_and_just_below_requote_cost() {
        for (budget, shares, cost) in [("7.95", "10", "3.95"), ("7.949999", "9", "3.55")] {
            assert_fractional_plan(
                levels(&[("0.30", "0.5"), ("0.40", "100")]),
                levels(&[("0.40", "200")]),
                &fees_zero(),
                &limits("0", budget),
                d(shares),
                d(cost),
                d("0.40"),
            );
        }
    }

    #[test]
    fn fractional_budget_nonzero_fees_profit_and_apr() {
        let fees = FeeContext {
            polymarket_fee_rate: d("0.07"),
            outcome_taker_rate: d("0.00035"),
        };
        // 10 股 PM 均价0.395，费用0.1672825；Outcome费用0.0014。
        let mut exact = limits("1.8", "8.1186825");
        exact.days = 365;
        exact.min_apr = d("0.23");
        let plan = assert_fractional_plan(
            levels(&[("0.30", "0.5"), ("0.40", "100")]),
            levels(&[("0.40", "200")]),
            &fees,
            &exact,
            d("10"),
            d("3.95"),
            d("0.40"),
        );
        assert_eq!(plan.pm.fee, d("0.1672825"));
        assert_eq!(plan.total_cost, exact.cost_limit);
        exact.cost_limit -= d("0.0000001");
        exact.min_profit = d("0");
        assert_fractional_plan(
            levels(&[("0.30", "0.5"), ("0.40", "100")]),
            levels(&[("0.40", "200")]),
            &fees,
            &exact,
            d("9"),
            d("3.55"),
            d("0.40"),
        );
        // 已有整数累计后再桥接，预算必须包含整体均价手续费，而非仅新增报价手续费。
        let accumulated = limits("0", "7.7116825");
        assert_fractional_plan(
            levels(&[("0.30", "4.5"), ("0.40", "100")]),
            levels(&[("0.40", "200")]),
            &fees,
            &accumulated,
            d("10"),
            d("3.55"),
            d("0.40"),
        );
        exact.min_profit = d("1.8");
        assert!(search_pair(
            &token(POLYMARKET, "pm-yes", "yes"),
            &token(OUTCOME, "#10", "no"),
            &levels(&[("0.30", "0.5"), ("0.40", "100")]),
            &levels(&[("0.40", "200")]),
            &fees,
            &exact,
            d("0.01")
        )
        .is_none());
        exact.min_profit = Decimal::ZERO;
        exact.min_apr = d("0.25");
        assert!(search_pair(
            &token(POLYMARKET, "pm-yes", "yes"),
            &token(OUTCOME, "#10", "no"),
            &levels(&[("0.30", "0.5"), ("0.40", "100")]),
            &levels(&[("0.40", "200")]),
            &fees,
            &exact,
            d("0.01")
        )
        .is_none());
    }

    #[test]
    fn fractional_insufficient_total_and_outcome_per_level_floor() {
        for (pm, out) in [
            (
                levels(&[("0.30", "0.4"), ("0.40", "0.5")]),
                levels(&[("0.40", "100")]),
            ),
            (
                levels(&[("0.30", "4.5"), ("0.40", "0.4")]),
                levels(&[("0.40", "100")]),
            ),
            (
                levels(&[("0.30", "0.5"), ("0.40", "100")]),
                levels(&[("0.30", "2.9"), ("0.40", "2.9")]),
            ),
        ] {
            assert!(search_pair(
                &token(POLYMARKET, "pm-yes", "yes"),
                &token(OUTCOME, "#10", "no"),
                &pm,
                &out,
                &fees_zero(),
                &limits("0", "100"),
                d("0.01")
            )
            .is_none());
        }
        assert_fractional_plan(
            levels(&[("0.30", "0.5"), ("0.40", "100")]),
            levels(&[("0.20", "0.9"), ("0.30", "2.9"), ("0.40", "3.9")]),
            &fees_zero(),
            &limits("0", "100"),
            d("5"),
            d("1.95"),
            d("0.40"),
        );
    }

    #[test]
    fn accumulates_fractional_pm_first_level() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(
            &mut books,
            POLYMARKET,
            "pm-yes",
            vec![("0.30", "0.5"), ("0.40", "20")],
            now,
        );
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "20")], now);
        let plan = plan_with(&books, now, &limits("1", "100"));
        assert_eq!(plan.net_shares, d("20"));
        assert_eq!(plan.pm.cost, d("7.95"));
        assert_eq!(plan.pm.cap_price, d("0.40"));
    }

    #[test]
    fn fills_available_depth_when_first_level_passes() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let plan = plan_with(&books, now, &limits("3", "100"));
        // 过门槛后买满档深：50 股、成本 40，而不是刚过线的 15 股。
        assert_eq!(plan.net_shares, d("50"));
        assert_eq!(plan.total_cost, d("40"));
        assert_eq!(plan.profit, d("10"));
        assert_eq!(plan.pm.label, "yes");
        assert_eq!(plan.outcome.label, "no");
    }

    #[test]
    fn fills_budget_when_first_level_is_deep() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(
            &mut books,
            POLYMARKET,
            "pm-yes",
            vec![("0.40", "10000")],
            now,
        );
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "10000")], now);
        let plan = plan_with(&books, now, &limits("3", "100"));
        assert_eq!(plan.net_shares, d("125"));
        assert_eq!(plan.total_cost, d("100"));
        assert_eq!(plan.profit, d("25"));
    }

    #[test]
    fn accumulates_next_level_when_first_level_misses_profit() {
        // 直接保留旧计算样例；BookStore 现在拒绝快照中的重复价格。
        let asks = levels(&[("0.45", "10"), ("0.45", "30")]);
        let plan = search_pair(
            &token(POLYMARKET, "pm-yes", "yes"),
            &token(OUTCOME, "#10", "no"),
            &asks,
            &asks,
            &fees_zero(),
            &limits("3", "100"),
            d("0.01"),
        )
        .expect("same integer depth plan");
        // 单位利润 0.10；第一档 10 不够门槛，吃掉后再把第二档 30 买满 → 40 股。
        assert_eq!(plan.net_shares, d("40"));
        assert_eq!(plan.total_cost, d("36"));
        assert_eq!(plan.profit, d("4"));
    }

    #[test]
    fn picks_higher_roi_direction() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        snapshot(&mut books, POLYMARKET, "pm-no", vec![("0.20", "50")], now);
        snapshot(&mut books, OUTCOME, "#11", vec![("0.30", "50")], now);
        let plan = plan_with(&books, now, &limits("3", "100"));
        // yes+no 单位成本 0.80 ROI=0.25；no+yes 单位成本 0.50 ROI=1.00。两边都买满 50。
        assert_eq!(plan.pm.label, "no");
        assert_eq!(plan.outcome.label, "yes");
        assert_eq!(plan.net_shares, d("50"));
        assert_eq!(plan.total_cost, d("25"));
    }

    #[test]
    fn rejects_outcome_below_min_notional() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "20")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.09", "10")], now);
        let plan = best_plan(
            &sample_topic(),
            &books,
            &fees_zero(),
            &limits("0.1", "100"),
            now,
            std::time::Duration::from_secs(5),
        );
        // Outcome 10 * 0.09 = 0.90 < $1，整档吃完仍不够最小名义。
        assert!(plan.is_none());
    }

    #[test]
    fn floors_fractional_outcome_size() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(
            &mut books,
            POLYMARKET,
            "pm-yes",
            vec![("0.40", "40.9")],
            now,
        );
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "40.9")], now);
        let plan = plan_with(&books, now, &limits("3", "100"));
        assert_eq!(plan.outcome.shares, floor_shares(plan.outcome.shares));
        assert_eq!(plan.net_shares, d("40"));
    }

    #[test]
    fn rejects_when_apr_below_min() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let mut limits = limits("3", "100");
        limits.days = 365;
        limits.min_apr = d("0.30");
        // ROI=0.25，APR=0.25 < 0.30。
        let plan = best_plan(
            &sample_topic(),
            &books,
            &fees_zero(),
            &limits,
            now,
            std::time::Duration::from_secs(5),
        );
        assert!(plan.is_none());
    }

    #[test]
    fn days_until_ceils_fractional_day() {
        let now = DateTime::parse_from_rfc3339("2026-09-04T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2026-09-04T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(days_until_from(Some(end), now), 1);
        let end = DateTime::parse_from_rfc3339("2026-09-06T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(days_until_from(Some(end), now), 2);
        assert_eq!(days_until_from(None, now), 1);
    }

    #[test]
    fn polymarket_fee_uses_catalog_rate() {
        let fees = FeeContext {
            polymarket_fee_rate: d("0.07"),
            outcome_taker_rate: Decimal::ZERO,
        };
        // Official crypto table: 100 shares @ $0.50 → $1.75
        assert_eq!(
            estimate_polymarket_fee(d("100"), d("0.50"), &fees),
            d("1.75")
        );
        assert_eq!(
            estimate_polymarket_fee(d("100"), d("0.20"), &fees),
            d("1.12")
        );
        assert_eq!(
            estimate_polymarket_fee(d("100"), d("0.50"), &fees_zero()),
            Decimal::ZERO
        );
    }

    #[test]
    fn outcome_fee_uses_taker_rate() {
        let fees = FeeContext {
            polymarket_fee_rate: Decimal::ZERO,
            outcome_taker_rate: d("0.00035"),
        };
        assert_eq!(estimate_outcome_fee(d("100"), &fees), d("0.035"));
        assert_eq!(estimate_outcome_fee(Decimal::ZERO, &fees), Decimal::ZERO);
        assert_eq!(estimate_outcome_fee(d("100"), &fees_zero()), Decimal::ZERO);
    }

    #[test]
    fn skips_when_pm_tick_size_missing() {
        let mut books = BookStore::default();
        let now = Instant::now();
        books.replace_snapshot(
            POLYMARKET,
            "pm-yes",
            vec![],
            vec![Level {
                price: d("0.40"),
                size: d("50"),
            }],
            1,
            now,
        );
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let plan = best_plan(
            &sample_topic(),
            &books,
            &fees_zero(),
            &limits("3", "100"),
            now,
            std::time::Duration::from_secs(5),
        );
        assert!(plan.is_none());
    }

    #[test]
    fn aligns_buy_to_provided_tick() {
        assert_eq!(align_polymarket_price(d("0.451"), d("0.001")), d("0.451"));
        assert_eq!(align_polymarket_price(d("0.451"), d("0.01")), d("0.46"));
        assert_eq!(
            align_polymarket_sell_price(d("0.456"), d("0.01")),
            d("0.45")
        );
    }

    #[test]
    fn confirm_plan_rejects_when_http_books_no_longer_arb() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let first = plan_with(&books, now, &limits("3", "100"));
        let mut http = BookStore::default();
        snapshot(&mut http, POLYMARKET, "pm-yes", vec![("0.55", "50")], now);
        snapshot(&mut http, OUTCOME, "#10", vec![("0.55", "50")], now);
        assert!(confirm_plan(
            &sample_topic(),
            &first,
            http.get(POLYMARKET, "pm-yes").unwrap(),
            http.get(OUTCOME, "#10").unwrap(),
            &fees_zero(),
            &limits("3", "100"),
        )
        .is_none());
    }

    #[test]
    fn confirm_plan_keeps_original_size_and_cap() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let first = plan_with(&books, now, &limits("3", "100"));
        let mut http = BookStore::default();
        snapshot(&mut http, POLYMARKET, "pm-yes", vec![("0.39", "80")], now);
        snapshot(&mut http, OUTCOME, "#10", vec![("0.39", "80")], now);
        let confirmed = confirm_plan(
            &sample_topic(),
            &first,
            http.get(POLYMARKET, "pm-yes").unwrap(),
            http.get(OUTCOME, "#10").unwrap(),
            &fees_zero(),
            &limits("3", "100"),
        )
        .expect("still fillable");
        assert_eq!(confirmed.pm.shares, first.pm.shares);
        assert_eq!(confirmed.outcome.shares, first.outcome.shares);
        assert_eq!(confirmed.pm.cap_price, first.pm.cap_price);
        assert_eq!(confirmed.outcome.cap_price, first.outcome.cap_price);
        assert_eq!(confirmed.pm.avg_price, d("0.39"));
        assert_eq!(confirmed.outcome.avg_price, d("0.39"));
        assert_eq!(confirmed.pm.cost, d("19.5"));
        assert_eq!(confirmed.outcome.cost, d("19.5"));
    }

    #[test]
    fn confirm_plan_refreshes_cost_before_balance_check() {
        let now = Instant::now();
        let mut books = BookStore::default();
        snapshot(
            &mut books,
            POLYMARKET,
            "pm-yes",
            vec![("0.30", "4.5"), ("0.40", "95.5")],
            now,
        );
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "200")], now);
        let first = plan_with(&books, now, &limits("1", "100"));
        assert_eq!(first.pm.shares, d("100"));
        assert_eq!(first.pm.cost, d("39.55"));
        let pm_balance = d("39.70");
        assert!(first.pm.cost + first.pm.fee <= pm_balance);
        let mut http_pm = books.get(POLYMARKET, "pm-yes").unwrap().clone();
        http_pm.asks = levels(&[("0.40", "100")]);
        let confirmed = confirm_plan(
            &sample_topic(),
            &first,
            &http_pm,
            books.get(OUTCOME, "#10").unwrap(),
            &fees_zero(),
            &limits("1", "100"),
        )
        .expect("original shares and cap remain fillable");
        assert_eq!(confirmed.pm.cost, d("40"));
        // execute_plan 的 HTTP 确认后余额判定；不启动执行器或数据库。
        assert!(confirmed.pm.cost + confirmed.pm.fee > pm_balance);
        assert_eq!(confirmed.pm.shares, first.pm.shares);
        assert_eq!(confirmed.pm.cap_price, first.pm.cap_price);
    }

    #[test]
    fn confirm_plan_refreshes_all_financial_fields_with_fees() {
        let now = Instant::now();
        let mut books = BookStore::default();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let first = plan_with(&books, now, &limits("1", "100"));
        let fees = FeeContext {
            polymarket_fee_rate: d("0.07"),
            outcome_taker_rate: d("0.00035"),
        };
        for (pm_px, out_px) in [("0.39", "0.38"), ("0.40", "0.40")] {
            let mut pm = books.get(POLYMARKET, "pm-yes").unwrap().clone();
            let mut out = books.get(OUTCOME, "#10").unwrap().clone();
            pm.asks = levels(&[(pm_px, "80")]);
            out.asks = levels(&[(out_px, "80")]);
            // 模拟原均价低于原cap，确保上升及下降两个方向都刷新。
            let mut original = first.clone();
            original.pm.cost = d("19.75");
            original.outcome.cost = d("19.75");
            original.pm.avg_price = d("0.395");
            original.outcome.avg_price = d("0.395");
            let bounds = ArbLimits {
                days: 30,
                ..limits("1", "100")
            };
            let confirmed =
                confirm_plan(&sample_topic(), &original, &pm, &out, &fees, &bounds).unwrap();
            let pm_cost = d(pm_px) * d("50");
            let out_cost = d(out_px) * d("50");
            let pm_fee = estimate_polymarket_fee(d("50"), d(pm_px), &fees);
            let out_fee = estimate_outcome_fee(out_cost, &fees);
            let total_cost = pm_cost + out_cost + pm_fee + out_fee;
            assert_eq!(confirmed.pm.cost, pm_cost);
            assert_eq!(confirmed.outcome.cost, out_cost);
            assert_eq!(confirmed.pm.avg_price, d(pm_px));
            assert_eq!(confirmed.outcome.avg_price, d(out_px));
            assert_eq!(confirmed.pm.fee, pm_fee);
            assert_eq!(confirmed.outcome.fee, out_fee);
            assert_eq!(confirmed.net_shares, d("50"));
            assert_eq!(confirmed.total_cost, total_cost);
            assert_eq!(confirmed.profit, d("50") - total_cost);
            assert_eq!(confirmed.roi, confirmed.profit / total_cost);
            // APR 由展示成本的精确比值独立投影，不复用已经舍入的 ROI。
            assert_eq!(
                confirmed.apr,
                exact::project(
                    &(exact::rational(confirmed.profit) / exact::rational(total_cost)
                        * exact::rational(d("365"))
                        / exact::rational(d("30"))),
                    false
                )
                .unwrap()
            );
            for (actual, before) in [
                (&confirmed.pm, &original.pm),
                (&confirmed.outcome, &original.outcome),
            ] {
                assert_eq!(actual.platform, before.platform);
                assert_eq!(actual.token_id, before.token_id);
                assert_eq!(actual.label, before.label);
                assert_eq!(actual.shares, before.shares);
                assert_eq!(actual.cap_price, before.cap_price);
            }
            assert_eq!(original.pm.cost, d("19.75"));
        }
    }

    #[test]
    fn confirm_plan_checks_original_cap_against_current_tick_without_realigning() {
        let now = Instant::now();
        let mut books = BookStore::default();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let mut first = plan_with(&books, now, &limits("1", "100"));
        first.pm.cap_price = d("0.451");
        let mut pm = books.get(POLYMARKET, "pm-yes").unwrap().clone();
        let out = books.get(OUTCOME, "#10").unwrap();
        pm.tick_size = Some(d("0.001"));
        let confirmed = confirm_plan(
            &sample_topic(),
            &first,
            &pm,
            out,
            &fees_zero(),
            &limits("1", "100"),
        )
        .unwrap();
        assert_eq!(confirmed.pm.cap_price, d("0.451"));
        pm.tick_size = Some(d("0.01"));
        assert_eq!(
            confirm_plan_reason(
                &sample_topic(),
                &first,
                &pm,
                out,
                &fees_zero(),
                &limits("1", "100")
            ),
            "pm_unfillable"
        );
        for tick in [None, Some(Decimal::ZERO), Some(d("-0.01")), Some(d("1.01"))] {
            pm.tick_size = tick;
            assert!(confirm_plan(
                &sample_topic(),
                &first,
                &pm,
                out,
                &fees_zero(),
                &limits("1", "100")
            )
            .is_none());
        }
        pm.tick_size = Some(d("0.01"));
        for cap in [d("0"), d("0.001"), d("0.995"), d("1")] {
            first.pm.cap_price = cap;
            assert!(confirm_plan(
                &sample_topic(),
                &first,
                &pm,
                out,
                &fees_zero(),
                &limits("1", "100")
            )
            .is_none());
        }
    }

    #[test]
    fn confirm_plan_preserves_financial_and_identity_rejections() {
        let now = Instant::now();
        let mut books = BookStore::default();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let first = plan_with(&books, now, &limits("1", "100"));
        let pm = books.get(POLYMARKET, "pm-yes").unwrap();
        let out = books.get(OUTCOME, "#10").unwrap();
        for (bounds, expected) in [
            (limits("1", "39.99"), "cost_limit"),
            (limits("10.01", "100"), "unprofitable"),
            (
                ArbLimits {
                    min_apr: d("0.26"),
                    days: 365,
                    ..limits("1", "100")
                },
                "unprofitable",
            ),
        ] {
            assert_eq!(
                confirm_plan_reason(&sample_topic(), &first, pm, out, &fees_zero(), &bounds),
                expected
            );
        }
        let mut wrong = first.clone();
        wrong.pm.token_id = "other".into();
        assert_eq!(
            confirm_plan_reason(
                &sample_topic(),
                &wrong,
                pm,
                out,
                &fees_zero(),
                &limits("1", "100")
            ),
            "token_mismatch"
        );
        wrong = first.clone();
        wrong.pm.label = "unknown".into();
        assert_eq!(
            confirm_plan_reason(
                &sample_topic(),
                &wrong,
                pm,
                out,
                &fees_zero(),
                &limits("1", "100")
            ),
            "missing_book"
        );
        let mut thin = out.clone();
        thin.asks = levels(&[("0.40", "49")]);
        assert_eq!(
            confirm_plan_reason(
                &sample_topic(),
                &first,
                pm,
                &thin,
                &fees_zero(),
                &limits("1", "100")
            ),
            "out_unfillable"
        );
        thin.asks = levels(&[("0.41", "50")]);
        assert_eq!(
            confirm_plan_reason(
                &sample_topic(),
                &first,
                pm,
                &thin,
                &fees_zero(),
                &limits("1", "100")
            ),
            "out_unfillable"
        );
        let mut cheap = pm.clone();
        cheap.asks = levels(&[("0.01", "50")]);
        assert_eq!(
            confirm_plan_reason(
                &sample_topic(),
                &first,
                &cheap,
                out,
                &fees_zero(),
                &limits("1", "100")
            ),
            "venue_min"
        );
    }

    #[test]
    fn confirm_plan_rejects_when_http_worse_than_cap() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let first = plan_with(&books, now, &limits("3", "100"));
        let mut http = BookStore::default();
        snapshot(&mut http, POLYMARKET, "pm-yes", vec![("0.42", "80")], now);
        snapshot(&mut http, OUTCOME, "#10", vec![("0.42", "80")], now);
        assert!(confirm_plan(
            &sample_topic(),
            &first,
            http.get(POLYMARKET, "pm-yes").unwrap(),
            http.get(OUTCOME, "#10").unwrap(),
            &fees_zero(),
            &limits("3", "100"),
        )
        .is_none());
        assert_eq!(
            confirm_plan_reason(
                &sample_topic(),
                &first,
                http.get(POLYMARKET, "pm-yes").unwrap(),
                http.get(OUTCOME, "#10").unwrap(),
                &fees_zero(),
                &limits("3", "100"),
            ),
            "pm_unfillable"
        );
    }

    #[test]
    fn confirm_plan_rejects_when_http_depth_thin() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let first = plan_with(&books, now, &limits("3", "100"));
        let mut http = BookStore::default();
        snapshot(&mut http, POLYMARKET, "pm-yes", vec![("0.40", "10")], now);
        snapshot(&mut http, OUTCOME, "#10", vec![("0.40", "10")], now);
        assert_eq!(
            confirm_plan_reason(
                &sample_topic(),
                &first,
                http.get(POLYMARKET, "pm-yes").unwrap(),
                http.get(OUTCOME, "#10").unwrap(),
                &fees_zero(),
                &limits("3", "100"),
            ),
            "pm_unfillable"
        );
    }

    #[test]
    fn first_usable_ask_skips_zero_and_picks_cheapest() {
        let asks = vec![
            Level {
                price: d("0.90"),
                size: d("10"),
            },
            Level {
                price: d("0.40"),
                size: d("0"),
            },
            Level {
                price: d("0.44"),
                size: d("8"),
            },
        ];
        assert_eq!(first_usable_ask(&asks, false), Some((d("0.44"), d("8"))));
        assert_eq!(
            first_usable_ask(
                &[Level {
                    price: d("0.40"),
                    size: d("0.9"),
                }],
                true
            ),
            None
        );
        assert_eq!(
            first_usable_ask(
                &[Level {
                    price: d("0.40"),
                    size: d("1.9"),
                }],
                true
            ),
            Some((d("0.40"), d("1")))
        );
    }

    #[test]
    fn confirm_plan_reads_cheapest_ask_when_unsorted() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let first = plan_with(&books, now, &limits("3", "100"));
        let pm = OrderBook {
            platform: POLYMARKET.to_string(),
            token_id: "pm-yes".into(),
            bids: vec![],
            asks: vec![
                Level {
                    price: d("0.90"),
                    size: d("50"),
                },
                Level {
                    price: d("0.40"),
                    size: d("50"),
                },
            ],
            exchange_ts_ms: 1,
            received_at: now,
            stale: false,
            tick_size: Some(d("0.01")),
        };
        let out = OrderBook {
            platform: OUTCOME.to_string(),
            token_id: "#10".into(),
            bids: vec![],
            asks: vec![Level {
                price: d("0.40"),
                size: d("50"),
            }],
            exchange_ts_ms: 1,
            received_at: now,
            stale: false,
            tick_size: None,
        };
        let confirmed = confirm_plan(
            &sample_topic(),
            &first,
            &pm,
            &out,
            &fees_zero(),
            &limits("3", "100"),
        )
        .expect("cheap ask still fillable");
        assert_eq!(confirmed.pm.shares, first.pm.shares);
        assert_eq!(confirmed.pm.cap_price, first.pm.cap_price);
        assert_eq!(confirmed.pm.avg_price, first.pm.avg_price);
    }

    #[test]
    fn diagnose_books_reports_venue_min_when_http_size_thin() {
        let now = Instant::now();
        let mut http = BookStore::default();
        snapshot(&mut http, POLYMARKET, "pm-yes", vec![("0.40", "2")], now);
        snapshot(&mut http, OUTCOME, "#10", vec![("0.40", "2")], now);
        let sample = diagnose_books(
            &sample_topic(),
            http.get(POLYMARKET, "pm-yes").unwrap(),
            http.get(OUTCOME, "#10").unwrap(),
            &fees_zero(),
            &limits("0.1", "100"),
            "yes",
            "no",
        );
        assert_eq!(sample.reason, "venue_min");
        assert_eq!(sample.pm_ask, Some(d("0.40")));
        assert_eq!(sample.pm_sz, Some(d("2")));
    }
}
