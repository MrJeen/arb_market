//! 新套利的私有精确口径；共享成交/hedge Decimal helper 不受影响。
use super::{align_outcome_price, ArbLimits, ArbPlan, FeeContext, LegPlan};
use crate::book::Level;
use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::TokenRef;
use num_bigint::BigInt;
use num_rational::BigRational;
use num_traits::{One, Signed, ToPrimitive, Zero};
use rust_decimal::Decimal;
use std::cmp::Ordering;

type Q = BigRational;

pub(super) fn rational(value: Decimal) -> Q {
    Q::new(
        BigInt::from(value.mantissa()),
        BigInt::from(10u8).pow(value.scale()),
    )
}

fn integer(value: &BigInt) -> Q {
    Q::from_integer(value.clone())
}

fn at_scale(value: &Q, scale: u32, upward: bool) -> Option<Decimal> {
    let scaled = value * integer(&BigInt::from(10u8).pow(scale));
    let mut n = scaled.numer() / scaled.denom();
    let rem = scaled.numer() % scaled.denom();
    if upward {
        if rem.is_positive() {
            n += 1;
        }
    } else {
        let twice = rem.abs() * 2;
        if twice > *scaled.denom()
            || (twice == *scaled.denom() && (&n % BigInt::from(2u8)) != BigInt::zero())
        {
            n += if rem.is_negative() { -1 } else { 1 };
        }
    }
    let n = n.to_i128()?;
    if n.unsigned_abs() > Decimal::MAX.mantissa() as u128 {
        return None;
    }
    Some(Decimal::from_i128_with_scale(n, scale))
}

pub(super) fn project(value: &Q, upward: bool) -> Option<Decimal> {
    // 即使 nearest-even 可向内舍入，也不能把超出 Decimal 值域的值伪装为可表示。
    if value.abs() > rational(Decimal::MAX) {
        return None;
    }
    (0..=28)
        .rev()
        .find_map(|scale| at_scale(value, scale, upward))
}

pub(super) fn project_down(value: &Q) -> Option<Decimal> {
    project(&(-value), true).map(|value| -value)
}

pub(super) fn unit_display(pm: Decimal, out: Decimal, fees: &FeeContext) -> Option<Decimal> {
    let p = rational(pm);
    let o = rational(out);
    project(
        &(&p + &o
            + rational(fees.polymarket_fee_rate) * &p * (Q::one() - &p)
            + rational(fees.outcome_builder_rate) * o),
        false,
    )
}

pub(super) fn valid_parameters(fees: &FeeContext, limits: &ArbLimits) -> bool {
    limits.cost_limit > Decimal::ZERO
        && (Decimal::ZERO..=Decimal::ONE).contains(&fees.polymarket_fee_rate)
        && (Decimal::ZERO..Decimal::ONE).contains(&fees.outcome_taker_rate)
        && (Decimal::ZERO..=Decimal::ONE).contains(&fees.outcome_builder_rate)
}

struct Rules {
    pm_rate: Q,
    out_rate: Q,
    builder_rate: Q,
    budget: Q,
    profit: Q,
    apr_days: Q,
}

impl Rules {
    fn new(fees: &FeeContext, limits: &ArbLimits) -> Result<Self, &'static str> {
        if !valid_parameters(fees, limits) {
            return Err("invalid_parameters");
        }
        Ok(Self {
            pm_rate: rational(fees.polymarket_fee_rate),
            out_rate: rational(fees.outcome_taker_rate),
            builder_rate: rational(fees.outcome_builder_rate),
            budget: rational(limits.cost_limit),
            profit: rational(limits.min_profit),
            apr_days: rational(limits.min_apr) * Q::from_integer(limits.days.max(1).into()),
        })
    }
}

#[derive(Clone)]
struct ExactLevel {
    price: Decimal,
    remaining: Q,
}

fn levels(asks: &[Level], floor_out: bool) -> Result<Vec<ExactLevel>, &'static str> {
    let mut result = Vec::with_capacity(asks.len());
    for level in asks {
        // 保留旧盘口零/负数量及零价占位过滤；正深度的非法预测价格不进入求解器。
        if level.size <= Decimal::ZERO || level.price.is_zero() {
            continue;
        }
        if level.price < Decimal::ZERO || level.price > Decimal::ONE {
            return Err("invalid_parameters");
        }
        let remaining = if floor_out {
            rational(level.size.trunc())
        } else {
            rational(level.size)
        };
        if remaining.is_positive() {
            result.push(ExactLevel {
                price: level.price,
                remaining,
            });
        }
    }
    result.sort_by_key(|level| level.price);
    Ok(result)
}

struct PmQuote {
    max_net: BigInt,
    first_cost: Q,
    cap: Decimal,
}

impl PmQuote {
    fn cost(&self, net: &BigInt) -> Q {
        &self.first_cost + integer(&(net - 1)) * rational(self.cap)
    }
}

struct PmCursor {
    asks: Vec<ExactLevel>,
    index: usize,
}

impl PmCursor {
    fn new(asks: &[Level]) -> Result<Self, &'static str> {
        Ok(Self {
            asks: levels(asks, false)?,
            index: 0,
        })
    }

    fn current(&mut self) -> Option<&mut ExactLevel> {
        while self
            .asks
            .get(self.index)
            .is_some_and(|l| l.remaining.is_zero())
        {
            self.index += 1;
        }
        self.asks.get_mut(self.index)
    }

    // 预消费报价：只有完整消费才可继续；否则搜索必须停止。
    fn next_quote(&mut self, max_net: &BigInt) -> Option<PmQuote> {
        let level = self.current()?;
        let whole = level.remaining.to_integer().min(max_net.clone());
        if whole.is_positive() {
            level.remaining -= integer(&whole);
            return Some(PmQuote {
                max_net: whole,
                first_cost: rational(level.price),
                cap: level.price,
            });
        }
        let mut remain = Q::one();
        let mut cost = Q::zero();
        let mut cap = Decimal::ZERO;
        while remain.is_positive() {
            let level = self.current()?;
            let take = level.remaining.clone().min(remain.clone());
            cost += rational(level.price) * &take;
            cap = level.price;
            level.remaining -= &take;
            remain -= take;
        }
        let level = &mut self.asks[self.index];
        let extra = level.remaining.to_integer().min(max_net - 1);
        level.remaining -= integer(&extra);
        Some(PmQuote {
            max_net: extra + 1,
            first_cost: cost,
            cap,
        })
    }
}

#[derive(Clone, Default)]
struct Acc {
    shares: BigInt,
    pm_cost: Q,
    out_cost: Q,
    pm_cap: Decimal,
    out_cap: Decimal,
}

impl Acc {
    fn plus(&self, n: &BigInt, pm: &PmQuote, out: Decimal) -> Self {
        Self {
            shares: &self.shares + n,
            pm_cost: &self.pm_cost + pm.cost(n),
            out_cost: &self.out_cost + rational(out) * integer(n),
            pm_cap: self.pm_cap.max(pm.cap),
            out_cap: self.out_cap.max(out),
        }
    }

    fn passes_mins(&self) -> bool {
        self.shares >= BigInt::from(5u8)
            && self.pm_cost >= Q::one()
            && integer(&self.shares) * rational(self.out_cap) >= Q::one()
    }

    fn metrics(&self, rules: &Rules) -> Values {
        #[cfg(test)]
        EVALUATIONS.with(|n| n.set(n.get() + 1));
        let s = integer(&self.shares);
        let pm_fee = &rules.pm_rate * (&self.pm_cost - &self.pm_cost * &self.pm_cost / &s);
        let out_fee = &rules.builder_rate * &self.out_cost;
        let total = &self.pm_cost + &self.out_cost + &pm_fee + &out_fee;
        let settlement_reserve = &s * &rules.out_rate;
        let expected_revenue = &s - &settlement_reserve;
        let profit = &expected_revenue - &total;
        let (worst_total, worst_profit) =
            worst_case_metrics(&self.shares, self.pm_cap, self.out_cap, rules);
        Values {
            s,
            pm_cost: self.pm_cost.clone(),
            out_cost: self.out_cost.clone(),
            pm_fee,
            out_fee,
            settlement_reserve,
            expected_revenue,
            total,
            profit,
            worst_total,
            worst_profit,
        }
    }
}

fn worst_case_metrics(shares: &BigInt, pm_cap: Decimal, out_cap: Decimal, rules: &Rules) -> (Q, Q) {
    let s = integer(shares);
    if s.is_zero() {
        return (Q::zero(), Q::zero());
    }
    let p_pm = rational(pm_cap);
    let p_out = rational(out_cap);
    let worst_pm_cost = &s * &p_pm;
    let worst_pm_fee = &rules.pm_rate * (&worst_pm_cost - &worst_pm_cost * &p_pm);
    let worst_out_cost = &s * &p_out;
    let worst_out_fee = &rules.builder_rate * &worst_out_cost;
    let worst_total = worst_pm_cost + worst_pm_fee + worst_out_cost + worst_out_fee;
    let expected_revenue = &s * (Q::one() - &rules.out_rate);
    let worst_profit = &expected_revenue - &worst_total;
    (worst_total, worst_profit)
}

#[derive(Debug, Clone)]
struct Values {
    s: Q,
    pm_cost: Q,
    out_cost: Q,
    pm_fee: Q,
    out_fee: Q,
    settlement_reserve: Q,
    expected_revenue: Q,
    total: Q,
    profit: Q,
    worst_total: Q,
    worst_profit: Q,
}

impl Values {
    fn profit_passes(&self, rules: &Rules) -> bool {
        self.profit >= rules.profit && self.worst_profit >= Q::zero()
    }
    fn apr_passes(&self, rules: &Rules) -> bool {
        &self.profit * Q::from_integer(365.into()) >= &rules.apr_days * &self.total
    }
    fn in_budget(&self, rules: &Rules) -> bool {
        self.total <= rules.budget
    }
    fn reason(&self, rules: &Rules) -> Result<(), &'static str> {
        if self.total > rules.budget {
            return Err("cost_limit");
        }
        if !self.profit_passes(rules) || !self.apr_passes(rules) {
            return Err("unprofitable");
        }
        Ok(())
    }
}

// 已知 predicate(low) 为真，找单调通过前缀最后一项；仅用于单项凸收益的左支或预算。
fn last_true(
    mut low: BigInt,
    mut high: BigInt,
    mut predicate: impl FnMut(&BigInt) -> bool,
) -> BigInt {
    while low < high {
        let mid = &low + (&high - &low + 1) / 2;
        if predicate(&mid) {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    low
}

fn interval(
    acc: &Acc,
    quote: &PmQuote,
    out: Decimal,
    rules: &Rules,
) -> (Option<BigInt>, bool, &'static str) {
    let full = acc.plus(&quote.max_net, quote, out);
    let full_in_budget = full.metrics(rules).in_budget(rules);
    let mut upper = if full_in_budget {
        quote.max_net.clone()
    } else {
        last_true(BigInt::zero(), quote.max_net.clone(), |n| {
            acc.plus(n, quote, out).metrics(rules).in_budget(rules)
        })
    };
    if upper.is_zero() {
        return (None, false, "cost_limit");
    }
    if !acc.plus(&upper, quote, out).passes_mins() {
        return (
            None,
            full_in_budget,
            if full.passes_mins() {
                "cost_limit"
            } else {
                "venue_min"
            },
        );
    }
    let mut lower = BigInt::one();
    let mut hi = upper.clone();
    while lower < hi {
        let mid = &lower + (&hi - &lower) / 2;
        if acc.plus(&mid, quote, out).passes_mins() {
            hi = mid;
        } else {
            lower = mid + 1;
        }
    }
    // builder 买费只增加线性成本，结算准备只减少线性收入：
    // C(S)=A*S+K-pm_rate*B²/S 仍递增凹；profit 及非自动通过的 APR 门槛仍凸。
    // U 失败且 L 通过时只二分该单项的通过前缀；收缩后从头复验另一门槛。
    // 已收缩的项在 [L,U] 全通过，故至多两次实际收缩，无逐股扫描。
    loop {
        let u = acc.plus(&upper, quote, out).metrics(rules);
        let profit_failed = !u.profit_passes(rules);
        if !profit_failed && u.apr_passes(rules) {
            return (Some(upper), full_in_budget, "ok");
        }
        let passes = |n: &BigInt| {
            let v = acc.plus(n, quote, out).metrics(rules);
            if profit_failed {
                v.profit_passes(rules)
            } else {
                v.apr_passes(rules)
            }
        };
        if !passes(&lower) {
            return (None, full_in_budget, "unprofitable");
        }
        #[cfg(test)]
        SHRINKS.with(|n| n.set(n.get() + 1));
        upper = last_true(lower.clone(), upper, passes);
    }
}

pub(super) fn search(
    pm_token: &TokenRef,
    out_token: &TokenRef,
    pm_asks: &[Level],
    out_asks: &[Level],
    fees: &FeeContext,
    limits: &ArbLimits,
    pm_tick: Decimal,
) -> Result<ArbPlan, &'static str> {
    let rules = Rules::new(fees, limits)?;
    if pm_tick <= Decimal::ZERO || pm_tick >= Decimal::ONE {
        return Err("no_tick");
    }
    let mut pm = PmCursor::new(pm_asks)?;
    let mut out = levels(out_asks, true)?;
    let mut oi = 0;
    let mut acc = Acc::default();
    let mut reason = "empty_ask";
    while let Some(level) = out.get_mut(oi) {
        if level.remaining.is_zero() {
            oi += 1;
            continue;
        }
        let Some(quote) = pm.next_quote(&level.remaining.to_integer()) else {
            break;
        };
        let (chosen, full_in_budget, miss) = interval(&acc, &quote, level.price, &rules);
        reason = miss;
        if let Some(n) = chosen {
            let final_acc = acc.plus(&n, &quote, level.price);
            // 保持原向上 tick 对齐/区间夹取语义，但不在 Decimal 除法中溢出。
            let tick = rational(pm_tick);
            let units = rational(final_acc.pm_cap) / &tick;
            let rounded_units = units.to_integer()
                + if units.is_integer() {
                    BigInt::zero()
                } else {
                    BigInt::one()
                };
            let cap = (integer(&rounded_units) * &tick)
                .max(tick.clone())
                .min((Q::one() - &tick).max(tick));
            let pm_cap = project(&cap, false).ok_or("unrepresentable")?;
            let out_cap = align_outcome_price(final_acc.out_cap);
            let (aligned_worst_total, aligned_worst_profit) =
                worst_case_metrics(&n, pm_cap, out_cap, &rules);
            let _ = aligned_worst_total;
            if aligned_worst_profit < Q::zero() {
                reason = "unprofitable";
                break;
            }
            return make_plan(
                &final_acc, &rules, limits, pm_token, out_token, pm_cap, out_cap,
            )
            .ok_or("unrepresentable");
        }
        if !full_in_budget {
            break;
        }
        acc = acc.plus(&quote.max_net, &quote, level.price);
        level.remaining -= integer(&quote.max_net);
    }
    // 仅在精确搜索确已拒绝后细分首档无正收益原因，不用展示值改变决策。
    if reason == "unprofitable" {
        if let (Some(p), Some(o)) = (
            levels(pm_asks, false)?.first(),
            levels(out_asks, true)?.first(),
        ) {
            let p = rational(p.price);
            let o = rational(o.price);
            let unit = &p + &o + &rules.pm_rate * &p * (Q::one() - &p) + &rules.builder_rate * o;
            if unit >= Q::one() - &rules.out_rate {
                reason = "unit_cost_ge_revenue";
            }
        }
    }
    Err(reason)
}

pub(super) fn take_asks_cost(
    asks: &[Level],
    shares: Decimal,
    cap: Decimal,
    floor_out: bool,
) -> Option<Q> {
    if shares <= Decimal::ZERO || shares.fract() != Decimal::ZERO {
        return None;
    }
    let mut remain = rational(shares);
    let mut cost = Q::zero();
    for level in levels(asks, floor_out).ok()? {
        if level.price > cap {
            break;
        }
        let take = remain.clone().min(level.remaining);
        cost += rational(level.price) * &take;
        remain -= take;
        if remain.is_zero() {
            return Some(cost);
        }
    }
    None
}

#[derive(Debug, Clone, PartialEq)]
struct Binding {
    platform: String,
    token: String,
    label: String,
    shares: Decimal,
    cap: Decimal,
}

impl Binding {
    fn matches(&self, leg: &LegPlan) -> bool {
        self.platform == leg.platform
            && self.token == leg.token_id
            && self.label == leg.label
            && self.shares == leg.shares
            && self.cap == leg.cap_price
    }

    fn new(leg: &LegPlan) -> Self {
        Self {
            platform: leg.platform.clone(),
            token: leg.token_id.clone(),
            label: leg.label.clone(),
            shares: leg.shares,
            cap: leg.cap_price,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ExactMetrics {
    values: Values,
    pm: Binding,
    out: Binding,
    out_builder_rate: Q,
}

impl ExactMetrics {
    fn bound(&self, plan: &ArbPlan) -> bool {
        self.pm.matches(&plan.pm)
            && self.out.matches(&plan.outcome)
            && rational(plan.net_shares) == self.values.s
    }
    pub(super) fn balance_sufficient(&self, plan: &ArbPlan, balance: Decimal, pm: bool) -> bool {
        self.bound(plan) && rational(balance) >= self.required_value(pm)
    }
    fn required_value(&self, pm: bool) -> Q {
        if pm {
            &self.values.pm_cost + &self.values.pm_fee
        } else {
            &self.values.s * rational(self.out.cap) * (Q::one() + &self.out_builder_rate)
        }
    }
    pub(super) fn required(&self, plan: &ArbPlan, pm: bool) -> Option<Decimal> {
        self.bound(plan)
            .then(|| project(&self.required_value(pm), true))
            .flatten()
    }
    pub(super) fn compare(&self, other: &Self) -> Ordering {
        (&self.values.profit * &other.values.total)
            .cmp(&(&other.values.profit * &self.values.total))
            .then_with(|| self.values.profit.cmp(&other.values.profit))
    }
}

struct DisplayValues {
    pm_cost: Decimal,
    out_cost: Decimal,
    pm_fee: Decimal,
    out_fee: Decimal,
    settlement_reserve: Decimal,
    expected_revenue: Decimal,
    total: Decimal,
    profit: Decimal,
    worst_profit: Decimal,
    worst_cost: Decimal,
    roi: Decimal,
    apr: Decimal,
}

fn display(v: &Values, days: i64) -> Option<DisplayValues> {
    project(&v.s, false)?;
    for scale in (0..=28).rev() {
        let attempt = || {
            let pm_cost = at_scale(&v.pm_cost, scale, true)?;
            let out_cost = at_scale(&v.out_cost, scale, true)?;
            let pm_fee = at_scale(&v.pm_fee, scale, true)?;
            let out_fee = at_scale(&v.out_fee, scale, true)?;
            // 直接检查共同 scale 下的整数和，防止 Decimal checked_add 静默降精度。
            let total_n = BigInt::from(pm_cost.mantissa())
                + BigInt::from(out_cost.mantissa())
                + BigInt::from(pm_fee.mantissa())
                + BigInt::from(out_fee.mantissa());
            let unit = integer(&BigInt::from(10u8).pow(scale));
            let total_q = integer(&total_n) / &unit;
            let total = at_scale(&total_q, scale, false)?;
            let settlement_reserve = at_scale(&v.settlement_reserve, scale, true)?;
            // 同一 scale 上费用向上投影，收入/利润向下；保持收入-现金=利润。
            let revenue_q =
                &v.expected_revenue - (rational(settlement_reserve) - &v.settlement_reserve);
            let expected_revenue = at_scale(&revenue_q, scale, false)?;
            let profit_q = &revenue_q - &total_q;
            let profit = at_scale(&profit_q, scale, false)?;
            let worst_profit = at_scale(&v.worst_profit, scale, false)?;
            let worst_cost = at_scale(&v.worst_total, scale, true)?;
            if !total_q.is_positive() {
                return None;
            }
            let roi_q = profit_q / total_q;
            let roi = project_down(&roi_q)?;
            let apr = project_down(
                &(roi_q * Q::from_integer(365.into()) / Q::from_integer(days.max(1).into())),
            )?;
            Some(DisplayValues {
                pm_cost,
                out_cost,
                pm_fee,
                out_fee,
                settlement_reserve,
                expected_revenue,
                total,
                profit,
                worst_profit,
                worst_cost,
                roi,
                apr,
            })
        };
        if let Some(result) = attempt() {
            return Some(result);
        }
    }
    None
}

fn make_plan(
    acc: &Acc,
    rules: &Rules,
    limits: &ArbLimits,
    pm_token: &TokenRef,
    out_token: &TokenRef,
    pm_cap: Decimal,
    out_cap: Decimal,
) -> Option<ArbPlan> {
    let mut aligned_acc = acc.clone();
    aligned_acc.pm_cap = pm_cap;
    aligned_acc.out_cap = out_cap;
    let values = aligned_acc.metrics(rules);
    if values.worst_profit < Q::zero() {
        return None;
    }
    let shares = at_scale(&values.s, 0, false)?;
    let d = display(&values, limits.days)?;
    project(&(&values.pm_cost + &values.pm_fee), true)?;
    project(
        &(&values.s * rational(out_cap) * (Q::one() + &rules.builder_rate)),
        true,
    )?;
    let pm = LegPlan {
        platform: POLYMARKET.into(),
        token_id: pm_token.token_id.clone(),
        label: pm_token.label.clone(),
        shares,
        avg_price: project(&(&values.pm_cost / &values.s), false)?,
        cap_price: pm_cap,
        cost: d.pm_cost,
        fee: d.pm_fee,
    };
    let outcome = LegPlan {
        platform: OUTCOME.into(),
        token_id: out_token.token_id.clone(),
        label: out_token.label.clone(),
        shares,
        avg_price: project(&(&values.out_cost / &values.s), false)?,
        cap_price: out_cap,
        cost: d.out_cost,
        fee: d.out_fee,
    };
    let exact = ExactMetrics {
        values,
        pm: Binding::new(&pm),
        out: Binding::new(&outcome),
        out_builder_rate: rules.builder_rate.clone(),
    };
    Some(ArbPlan {
        pm,
        outcome,
        net_shares: shares,
        expected_revenue: d.expected_revenue,
        settlement_reserve: d.settlement_reserve,
        total_cost: d.total,
        profit: d.profit,
        worst_profit: d.worst_profit,
        worst_cost: d.worst_cost,
        roi: d.roi,
        apr: d.apr,
        exact,
    })
}

pub(super) fn confirm(
    plan: &ArbPlan,
    pm_asks: &[Level],
    out_asks: &[Level],
    pm_token: &TokenRef,
    out_token: &TokenRef,
    fees: &FeeContext,
    limits: &ArbLimits,
) -> Result<ArbPlan, &'static str> {
    let rules = Rules::new(fees, limits)?;
    if plan.pm.platform != POLYMARKET || plan.outcome.platform != OUTCOME {
        return Err("token_mismatch");
    }
    if plan.pm.shares != plan.outcome.shares
        || plan.pm.shares <= Decimal::ZERO
        || plan.pm.shares.fract() != Decimal::ZERO
        || plan.net_shares != plan.pm.shares
    {
        return Err("venue_min");
    }
    let acc = Acc {
        shares: rational(plan.pm.shares).to_integer(),
        pm_cost: take_asks_cost(pm_asks, plan.pm.shares, plan.pm.cap_price, false)
            .ok_or("pm_unfillable")?,
        out_cost: take_asks_cost(out_asks, plan.outcome.shares, plan.outcome.cap_price, true)
            .ok_or("out_unfillable")?,
        pm_cap: plan.pm.cap_price,
        out_cap: plan.outcome.cap_price,
    };
    if !acc.passes_mins() {
        return Err("venue_min");
    }
    acc.metrics(&rules).reason(&rules)?;
    make_plan(
        &acc,
        &rules,
        limits,
        pm_token,
        out_token,
        plan.pm.cap_price,
        plan.outcome.cap_price,
    )
    .ok_or("unrepresentable")
}

#[cfg(test)]
thread_local! {
    static EVALUATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static SHRINKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
#[path = "../../tests/unit/calc/exact.rs"]
mod tests;
