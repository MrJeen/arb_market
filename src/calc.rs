use crate::book::{Level, OrderBook};
use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::{TokenRef, Topic};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;

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
}

#[derive(Clone, Default)]
struct Acc {
    pm_shares: Decimal,
    out_shares: Decimal,
    pm_cost: Decimal,
    out_cost: Decimal,
    pm_cap: Decimal,
    out_cap: Decimal,
}

impl Acc {
    fn plus(&self, net: Decimal, pm: &PmQuote, out_px: Decimal) -> Self {
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
/// 通过后原样返回 plan，不重算数量和限价。
pub fn confirm_plan(
    topic: &Topic,
    plan: &ArbPlan,
    pm_book: &OrderBook,
    out_book: &OrderBook,
    fees: &FeeContext,
    limits: &ArbLimits,
) -> Option<ArbPlan> {
    validate_plan_on_books(topic, plan, pm_book, out_book, fees, limits)
        .ok()
        .map(|_| plan.clone())
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
) -> Result<(), &'static str> {
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
    let pm_cost = take_asks_cost(&pm_book.asks, plan.pm.shares, plan.pm.cap_price, false)
        .ok_or("pm_unfillable")?;
    let out_cost = take_asks_cost(
        &out_book.asks,
        plan.outcome.shares,
        plan.outcome.cap_price,
        true,
    )
    .ok_or("out_unfillable")?;
    let acc = Acc {
        pm_shares: plan.pm.shares,
        out_shares: plan.outcome.shares,
        pm_cost,
        out_cost,
        pm_cap: plan.pm.cap_price,
        out_cap: plan.outcome.cap_price,
    };
    if !acc.passes_mins() {
        return Err("venue_min");
    }
    let metrics = acc.metrics(fees, limits);
    if metrics.total_cost > limits.cost_limit {
        return Err("cost_limit");
    }
    if metrics.profit < limits.min_profit || metrics.apr < limits.min_apr {
        return Err("unprofitable");
    }
    Ok(())
}

fn take_asks_cost(
    asks: &[Level],
    shares: Decimal,
    cap: Decimal,
    floor_out: bool,
) -> Option<Decimal> {
    if shares <= Decimal::ZERO {
        return None;
    }
    let mut asks = asks.to_vec();
    asks.sort_by(|a, b| a.price.cmp(&b.price));
    let mut remain = shares;
    let mut cost = Decimal::ZERO;
    while remain > Decimal::ZERO {
        drop_unusable(&mut asks, floor_out);
        let level = asks.first()?;
        if level.price > cap {
            return None;
        }
        let available = if floor_out {
            floor_shares(level.size)
        } else {
            level.size
        };
        let take = remain.min(available);
        cost += level.price * take;
        remain -= take;
        consume_qty(&mut asks, take, floor_out);
    }
    Some(cost)
}

/// 一个物理终点档的整数报价：首股可跨档，后续整股都在 cap 价成交。
struct PmQuote {
    max_net: Decimal,
    first_cost: Decimal,
    cap: Decimal,
}

impl PmQuote {
    fn cost(&self, net: Decimal) -> Decimal {
        debug_assert!(net >= Decimal::ONE && net <= self.max_net);
        self.first_cost + (net - Decimal::ONE) * self.cap
    }

    fn average_price(&self, net: Decimal) -> Decimal {
        self.cost(net) / net
    }
}

struct PmCursor {
    asks: Vec<Level>,
    index: usize,
}

impl PmCursor {
    fn new(asks: &[Level]) -> Self {
        let mut asks = asks.to_vec();
        asks.sort_by(|a, b| a.price.cmp(&b.price));
        Self { asks, index: 0 }
    }

    fn current(&mut self) -> Option<&mut Level> {
        while let Some(level) = self.asks.get(self.index) {
            if level.price > Decimal::ZERO && level.size > Decimal::ZERO {
                break;
            }
            self.index += 1;
        }
        self.asks.get_mut(self.index)
    }

    // 游标先取报价；调用方只有完整消费该报价才继续，否则必须结束搜索。
    fn next_quote(&mut self, max_net: Decimal) -> Option<PmQuote> {
        debug_assert!(max_net >= Decimal::ONE && max_net == floor_shares(max_net));
        let level = self.current()?;
        let whole = floor_shares(level.size).min(max_net);
        if whole >= Decimal::ONE {
            level.size -= whole;
            return Some(PmQuote {
                max_net: whole,
                first_cost: level.price,
                cap: level.price,
            });
        }

        // 小数尾量按物理档推进，凑满一股才报价，不受候选循环次数限制。
        let mut remain = Decimal::ONE;
        let mut cost = Decimal::ZERO;
        let mut cap = Decimal::ZERO;
        while remain > Decimal::ZERO {
            let level = self.current()?;
            let take = level.size.min(remain);
            cost += level.price * take;
            cap = cap.max(level.price);
            level.size -= take;
            remain -= take;
        }
        // 桥接股不是独立候选区间：连同终点档余下整股一起评估，避免刚凑够5股就早返。
        let level = &mut self.asks[self.index];
        let extra = floor_shares(level.size).min(max_net - Decimal::ONE);
        level.size -= extra;
        Some(PmQuote {
            max_net: Decimal::ONE + extra,
            first_cost: cost,
            cap,
        })
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
    let mut pm = PmCursor::new(pm_asks);
    let mut out_asks = out_asks.to_vec();
    out_asks.sort_by(|a, b| a.price.cmp(&b.price));
    let mut acc = Acc::default();
    let mut remain = limits.cost_limit;

    loop {
        drop_unusable(&mut out_asks, true);
        let Some(out) = out_asks.first() else {
            break;
        };
        let out_px = out.price;
        let Some(quote) = pm.next_quote(floor_shares(out.size)) else {
            break;
        };
        let max_net = quote.max_net;
        let unit_cost = all_in_unit_cost(quote.average_price(max_net), out_px, fees);
        if unit_cost >= Decimal::ONE || unit_cost <= Decimal::ZERO {
            break;
        }

        let (take, ended) = if quote.first_cost != quote.cap {
            // 跨档首股有折价，缩量后均价会变；只在本报价内按真实成本找预算边界。
            let take = quote_net_to_budget(&acc, &quote, out_px, fees, limits);
            (take, take < max_net)
        } else {
            clip_to_remain(max_net, unit_cost, remain)
        };
        // 门槛只判断能否做；当前物理终点区间能过线就买满预算或深度。
        if let Some(net) = fill_to_budget(&acc, remain, take, &quote, out_px, fees, limits) {
            return acc
                .plus(net, &quote, out_px)
                .to_plan(pm_token, out_token, fees, limits, pm_tick);
        }

        if take <= Decimal::ZERO {
            break;
        }
        acc = acc.plus(take, &quote, out_px);
        remain = (limits.cost_limit - acc.metrics(fees, limits).total_cost).max(Decimal::ZERO);
        if acc.metrics(fees, limits).total_cost > limits.cost_limit {
            break;
        }
        if acc.passes_all(fees, limits) {
            return acc.to_plan(pm_token, out_token, fees, limits, pm_tick);
        }
        if ended {
            break;
        }
        consume_qty(&mut out_asks, take, true);
    }
    None
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

fn consume_qty(asks: &mut Vec<Level>, mut qty: Decimal, floor_out: bool) {
    while qty > Decimal::ZERO && !asks.is_empty() {
        let available = if floor_out {
            floor_shares(asks[0].size)
        } else {
            asks[0].size
        };
        if available <= Decimal::ZERO {
            asks.remove(0);
            continue;
        }
        let take = qty.min(available);
        asks[0].size -= take;
        qty -= take;
        let leftover = if floor_out {
            floor_shares(asks[0].size)
        } else {
            asks[0].size
        };
        if leftover <= Decimal::ZERO {
            asks.remove(0);
        }
    }
}

fn all_in_unit_cost(pm_px: Decimal, out_px: Decimal, fees: &FeeContext) -> Decimal {
    pm_px
        + out_px
        + estimate_polymarket_fee(Decimal::ONE, pm_px, fees)
        + estimate_outcome_fee(out_px, fees)
}

fn clip_to_remain(max_net: Decimal, unit_cost: Decimal, remain: Decimal) -> (Decimal, bool) {
    let max_cost = unit_cost * max_net;
    if max_cost > remain {
        let clipped = floor_shares(remain / unit_cost);
        (clipped, true)
    } else {
        (max_net, false)
    }
}

/// 仅桥接报价需要重算预算边界。合法预测价格及 [0,1] 费率下，新增成本严格递增：
/// PM 的边际费用 = rate * (p * (1-p) + (p-avg)^2)，Outcome 费用也非负。
/// Decimal 最多96位整数，整数二分不超过96次；不逐股扫描大档。
fn quote_net_to_budget(
    acc: &Acc,
    quote: &PmQuote,
    out_px: Decimal,
    fees: &FeeContext,
    limits: &ArbLimits,
) -> Decimal {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&fees.polymarket_fee_rate)
        || !(Decimal::ZERO..=Decimal::ONE).contains(&fees.outcome_taker_rate)
        || quote.cap > Decimal::ONE
        || out_px > Decimal::ONE
    {
        // 不扩大费率配置校验范围；无法证明单调的桥接报价保守拒绝。
        return Decimal::ZERO;
    }
    let mut low = Decimal::ZERO;
    let mut high = quote.max_net;
    while low < high {
        let mid = low + ((high - low) / Decimal::from(2)).ceil();
        if acc
            .plus(mid, quote, out_px)
            .metrics(fees, limits)
            .total_cost
            <= limits.cost_limit
        {
            low = mid;
        } else {
            high = mid - Decimal::ONE;
        }
    }
    low
}

fn fill_to_budget(
    acc: &Acc,
    remain: Decimal,
    mut net: Decimal,
    quote: &PmQuote,
    out_px: Decimal,
    fees: &FeeContext,
    limits: &ArbLimits,
) -> Option<Decimal> {
    if remain <= Decimal::ZERO {
        return None;
    }
    let max_net = quote.max_net;
    for _ in 0..8 {
        if net <= Decimal::ZERO {
            return None;
        }
        let trial = acc.plus(net, quote, out_px);
        if trial.passes_all(fees, limits) {
            return Some(net);
        }
        let metrics = trial.metrics(fees, limits);
        // 利润/APR/场地下限在更小仓位上只会更差，只有超预算才缩仓。
        if metrics.total_cost <= limits.cost_limit {
            return None;
        }
        let acc_cost = acc.metrics(fees, limits).total_cost;
        let candidate_cost = metrics.total_cost - acc_cost;
        if candidate_cost <= Decimal::ZERO {
            return None;
        }
        net = floor_shares((net * remain / candidate_cost).min(max_net));
    }
    None
}

impl Acc {
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

    fn to_plan(
        &self,
        pm_token: &TokenRef,
        out_token: &TokenRef,
        fees: &FeeContext,
        limits: &ArbLimits,
        pm_tick: Decimal,
    ) -> Option<ArbPlan> {
        if !self.passes_all(fees, limits) {
            return None;
        }
        let m = self.metrics(fees, limits);
        Some(ArbPlan {
            pm: LegPlan {
                platform: POLYMARKET.to_string(),
                token_id: pm_token.token_id.clone(),
                label: pm_token.label.clone(),
                shares: self.pm_shares,
                avg_price: m.pm_avg,
                cap_price: align_polymarket_price(self.pm_cap, pm_tick),
                cost: self.pm_cost,
                fee: m.pm_fee,
            },
            outcome: LegPlan {
                platform: OUTCOME.to_string(),
                token_id: out_token.token_id.clone(),
                label: out_token.label.clone(),
                shares: floor_shares(self.out_shares),
                avg_price: m.out_avg,
                cap_price: align_outcome_price(self.out_cap),
                cost: self.out_cost,
                fee: m.out_fee,
            },
            net_shares: m.net,
            total_cost: m.total_cost,
            profit: m.profit,
            roi: m.roi,
            apr: m.apr,
        })
    }
}

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
                Some(cur) => {
                    plan.roi > cur.roi || (plan.roi == cur.roi && plan.profit > cur.profit)
                }
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
    topic: &Topic,
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
            sample.unit_cost = Some(all_in_unit_cost(pm_px, out_px, fees));
        }
    }
    if plan_arbitrage(topic, pm_book, out_book, pm_token, out_token, fees, limits).is_some() {
        sample.reason = "ok";
        return sample;
    }
    if pm_book.tick_size.is_none() {
        sample.reason = "no_tick";
        return sample;
    }
    let Some(unit) = sample.unit_cost else {
        sample.reason = "empty_ask";
        return sample;
    };
    if unit >= Decimal::ONE || unit <= Decimal::ZERO {
        sample.reason = "unit_cost_ge_1";
        return sample;
    }
    let pm_px = sample.pm_ask.unwrap_or_default();
    let out_px = sample.out_ask.unwrap_or_default();
    let max_net = floor_shares(
        sample
            .pm_sz
            .unwrap_or_default()
            .min(sample.out_sz.unwrap_or_default()),
    );
    let need = min_shares_for_venue(pm_px, out_px);
    if need * unit > limits.cost_limit {
        sample.reason = "cost_limit";
        return sample;
    }
    if max_net < need {
        sample.reason = "venue_min";
        return sample;
    }
    sample.reason = "unprofitable";
    sample
}

fn min_shares_for_venue(pm_px: Decimal, out_px: Decimal) -> Decimal {
    let mut need = Decimal::from(5);
    if pm_px > Decimal::ZERO {
        need = need.max((Decimal::ONE / pm_px).ceil());
    }
    if out_px > Decimal::ZERO {
        need = need.max((Decimal::ONE / out_px).ceil());
    }
    need
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

    fn d(s: &str) -> Decimal {
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

    fn sample_topic() -> Topic {
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

    fn fees_zero() -> FeeContext {
        FeeContext {
            polymarket_fee_rate: Decimal::ZERO,
            outcome_taker_rate: Decimal::ZERO,
        }
    }

    fn limits(min_profit: &str, cost_limit: &str) -> ArbLimits {
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

    fn levels(rows: &[(&str, &str)]) -> Vec<Level> {
        rows.iter()
            .map(|(price, size)| Level {
                price: d(price),
                size: d(size),
            })
            .collect()
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
        let quote = PmQuote {
            max_net: d("100"),
            first_cost: d("0.35"),
            cap: d("0.40"),
        };
        for (pm_rate, out_rate) in [("-0.01", "0"), ("1.01", "0"), ("0", "-0.01"), ("0", "1.01")] {
            let fees = FeeContext {
                polymarket_fee_rate: d(pm_rate),
                outcome_taker_rate: d(out_rate),
            };
            assert_eq!(
                quote_net_to_budget(
                    &Acc::default(),
                    &quote,
                    d("0.40"),
                    &fees,
                    &limits("0", "100")
                ),
                Decimal::ZERO
            );
        }
    }

    #[test]
    fn fractional_quotes_scale_with_levels_not_share_count() {
        let mut cursor = PmCursor::new(&levels(&[("0.30", "0.5"), ("0.40", "1000000000000")]));
        let quote = cursor.next_quote(d("2000000000000")).unwrap();
        assert_eq!(quote.max_net, d("1000000000000"));
        assert_eq!(quote.cost(d("5")), d("1.95"));
        assert_eq!(quote.cap, d("0.40"));
        assert!(cursor.next_quote(d("2000000000000")).is_none());
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
        assert_eq!(confirmed.pm.avg_price, first.pm.avg_price);
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
