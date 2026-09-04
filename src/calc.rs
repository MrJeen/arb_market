use crate::book::{Level, OrderBook};
use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::{TokenRef, Topic};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct FeeContext {
    /// Polymarket `feeSchedule.rate` (0.07 crypto, not 700 bps).
    pub polymarket_fee_rate: Decimal,
    pub outcome_taker_rate: Decimal,
    pub extra_cost_multiplier: Decimal,
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
    fn plus(&self, net: Decimal, pm_px: Decimal, out_px: Decimal) -> Self {
        Self {
            pm_shares: self.pm_shares + net,
            out_shares: self.out_shares + net,
            pm_cost: self.pm_cost + pm_px * net,
            out_cost: self.out_cost + out_px * net,
            pm_cap: self.pm_cap.max(pm_px),
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

pub fn min_trade_cost(platform: &str) -> Decimal {
    if platform == POLYMARKET {
        Decimal::ONE
    } else {
        Decimal::from(10)
    }
}

pub fn min_trade_amount(platform: &str) -> Decimal {
    if platform == POLYMARKET {
        Decimal::from(5)
    } else {
        Decimal::ONE
    }
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

fn search_pair(
    pm_token: &TokenRef,
    out_token: &TokenRef,
    pm_asks: &[Level],
    out_asks: &[Level],
    fees: &FeeContext,
    limits: &ArbLimits,
    pm_tick: Decimal,
) -> Option<ArbPlan> {
    let mut pm_asks = pm_asks.to_vec();
    let mut out_asks = out_asks.to_vec();
    let mut acc = Acc::default();
    let mut remain = limits.cost_limit;

    for _ in 0..64 {
        drop_unusable(&mut pm_asks, false);
        drop_unusable(&mut out_asks, true);
        let Some((pm_px, pm_sz, out_px, out_sz)) = peek_first(&pm_asks, &out_asks) else {
            break;
        };
        let max_net = floor_shares(pm_sz.min(out_sz));
        if max_net <= Decimal::ZERO {
            break;
        }
        let unit_cost = all_in_unit_cost(pm_px, out_px, fees);
        if unit_cost >= Decimal::ONE || unit_cost <= Decimal::ZERO {
            break;
        }

        let unclipped_cost = unit_cost * max_net;
        // 第一档规模超过预算 3 倍时按剩余额度买满，避免最小 shares 二分把仓位截得过小。
        if unclipped_cost > limits.cost_limit * Decimal::from(3) {
            if let Some(net) = fill_to_budget(&acc, remain, unit_cost, pm_px, out_px, max_net, fees, limits)
            {
                return acc.plus(net, pm_px, out_px).to_plan(pm_token, out_token, fees, limits, pm_tick);
            }
        }

        if let Some(net) =
            find_min_passing(&acc, max_net, pm_px, out_px, fees, limits)
        {
            let trial = acc.plus(net, pm_px, out_px);
            if trial.passes_all(fees, limits) {
                return trial.to_plan(pm_token, out_token, fees, limits, pm_tick);
            }
        }

        let (take, ended) = clip_to_remain(max_net, unit_cost, remain);
        if take <= Decimal::ZERO {
            break;
        }
        acc = acc.plus(take, pm_px, out_px);
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
        consume_qty(&mut pm_asks, take, false);
        consume_qty(&mut out_asks, take, true);
    }
    None
}

fn peek_first(pm_asks: &[Level], out_asks: &[Level]) -> Option<(Decimal, Decimal, Decimal, Decimal)> {
    let pm = pm_asks.first()?;
    let out = out_asks.first()?;
    let out_sz = floor_shares(out.size);
    if pm.size <= Decimal::ZERO || out_sz <= Decimal::ZERO {
        return None;
    }
    Some((pm.price, pm.size, out.price, out_sz))
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

fn fill_to_budget(
    acc: &Acc,
    remain: Decimal,
    unit_cost: Decimal,
    pm_px: Decimal,
    out_px: Decimal,
    max_net: Decimal,
    fees: &FeeContext,
    limits: &ArbLimits,
) -> Option<Decimal> {
    if remain <= Decimal::ZERO || unit_cost <= Decimal::ZERO {
        return None;
    }
    let mut net = floor_shares((remain / unit_cost).min(max_net));
    for _ in 0..8 {
        if net <= Decimal::ZERO {
            return None;
        }
        let trial = acc.plus(net, pm_px, out_px);
        if trial.passes_all(fees, limits) {
            return Some(net);
        }
        let metrics = trial.metrics(fees, limits);
        if metrics.total_cost <= Decimal::ZERO {
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

fn find_min_passing(
    acc: &Acc,
    max_net: Decimal,
    pm_px: Decimal,
    out_px: Decimal,
    fees: &FeeContext,
    limits: &ArbLimits,
) -> Option<Decimal> {
    let max_trial = acc.plus(max_net, pm_px, out_px);
    if !max_trial.passes_profit_and_mins(fees, limits) {
        return None;
    }
    let mut low = Decimal::ONE;
    let mut high = max_net;
    let mut best = None;
    for _ in 0..32 {
        if low > high {
            break;
        }
        let mid = floor_shares((low + high) / Decimal::from(2)).max(Decimal::ONE);
        if mid > high {
            break;
        }
        let trial = acc.plus(mid, pm_px, out_px);
        if trial.passes_profit_and_mins(fees, limits) {
            best = Some(mid);
            high = mid - Decimal::ONE;
        } else {
            low = mid + Decimal::ONE;
        }
    }
    best.filter(|net| acc.plus(*net, pm_px, out_px).passes_all(fees, limits))
        .or_else(|| {
            let trial = acc.plus(max_net, pm_px, out_px);
            trial.passes_all(fees, limits).then_some(max_net)
        })
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
        self.pm_shares >= min_trade_amount(POLYMARKET)
            && self.pm_cost >= min_trade_cost(POLYMARKET)
            && self.out_shares >= min_trade_amount(OUTCOME)
            && self.out_cap * self.out_shares >= min_trade_cost(OUTCOME)
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
        if let Some(plan) = plan_arbitrage(topic, pm_book, out_book, pm_token, out_token, fees, limits)
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

pub fn estimate_polymarket_fee(shares: Decimal, price: Decimal, fees: &FeeContext) -> Decimal {
    if fees.polymarket_fee_rate.is_zero() {
        return Decimal::ZERO;
    }
    let one_minus = Decimal::ONE - price;
    // Official: fee = C × feeRate × p × (1 - p)
    shares * fees.polymarket_fee_rate * price * one_minus * fees.extra_cost_multiplier
}

pub fn estimate_outcome_fee(notional: Decimal, fees: &FeeContext) -> Decimal {
    notional * fees.outcome_taker_rate * fees.extra_cost_multiplier
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
            extra_cost_multiplier: d("1.3"),
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
        best_plan(&sample_topic(), books, &fees_zero(), limits, now, std::time::Duration::from_secs(5))
            .expect("plan")
    }

    #[test]
    fn takes_min_shares_when_first_level_is_shallow() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "50")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "50")], now);
        let plan = plan_with(&books, now, &limits("3", "100"));
        // Outcome $10 / 0.40 = 25 shares；利润 25 * 0.2 = 5，刚好过门槛而非吃满 50。
        assert_eq!(plan.net_shares, d("25"));
        assert_eq!(plan.profit, d("5"));
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
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(
            &mut books,
            POLYMARKET,
            "pm-yes",
            vec![("0.45", "10"), ("0.45", "30")],
            now,
        );
        snapshot(
            &mut books,
            OUTCOME,
            "#10",
            vec![("0.45", "10"), ("0.45", "30")],
            now,
        );
        let plan = plan_with(&books, now, &limits("3", "100"));
        // 单位利润 0.10；min_profit=3 需要 30 shares。第一档 10 不够，吃掉后再从第二档补 20。
        assert_eq!(plan.net_shares, d("30"));
        assert_eq!(plan.profit, d("3"));
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
        // yes+no 单位成本 0.80 ROI=0.25；no+yes 单位成本 0.50 ROI=1.00。
        assert_eq!(plan.pm.label, "no");
        assert_eq!(plan.outcome.label, "yes");
        assert_eq!(plan.net_shares, d("34"));
    }

    #[test]
    fn rejects_outcome_below_min_notional() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "20")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "20")], now);
        let plan = best_plan(
            &sample_topic(),
            &books,
            &fees_zero(),
            &limits("0.1", "100"),
            now,
            std::time::Duration::from_secs(5),
        );
        // 20 * 0.40 = 8 < $10，整档吃完仍不够最小名义。
        assert!(plan.is_none());
    }

    #[test]
    fn floors_fractional_outcome_size() {
        let mut books = BookStore::default();
        let now = Instant::now();
        snapshot(&mut books, POLYMARKET, "pm-yes", vec![("0.40", "40.9")], now);
        snapshot(&mut books, OUTCOME, "#10", vec![("0.40", "40.9")], now);
        let plan = plan_with(&books, now, &limits("3", "100"));
        assert_eq!(plan.outcome.shares, floor_shares(plan.outcome.shares));
        assert_eq!(plan.net_shares, d("25"));
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
            extra_cost_multiplier: Decimal::ONE,
        };
        // Official crypto table: 100 shares @ $0.50 → $1.75
        assert_eq!(estimate_polymarket_fee(d("100"), d("0.50"), &fees), d("1.75"));
        let conservative = FeeContext {
            extra_cost_multiplier: d("1.3"),
            ..fees.clone()
        };
        assert_eq!(
            estimate_polymarket_fee(d("100"), d("0.50"), &conservative),
            d("2.275")
        );
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
        assert_eq!(align_polymarket_sell_price(d("0.456"), d("0.01")), d("0.45"));
    }
}
