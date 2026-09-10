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
        }
    }
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
}

impl Values {
    fn profit_passes(&self, rules: &Rules) -> bool {
        self.profit >= rules.profit
    }
    fn apr_passes(&self, rules: &Rules) -> bool {
        &self.profit * Q::from_integer(365.into()) >= &rules.apr_days * &self.total
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
    let full_in_budget = full.metrics(rules).total <= rules.budget;
    let mut upper = if full_in_budget {
        quote.max_net.clone()
    } else {
        last_true(BigInt::zero(), quote.max_net.clone(), |n| {
            acc.plus(n, quote, out).metrics(rules).total <= rules.budget
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
    let values = acc.metrics(rules);
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
mod tests {
    use super::super::tests::{d, fees_zero, levels as book_levels, limits, sample_topic};
    use super::*;

    fn run(pm: &[Level], out: &[Level], fees: &FeeContext, bounds: &ArbLimits) -> Option<ArbPlan> {
        let topic = sample_topic();
        search(
            &topic.tokens[0],
            &topic.tokens[2],
            pm,
            out,
            fees,
            bounds,
            d("0.01"),
        )
        .ok()
    }

    // 独立 oracle：逐股从原 Decimal 输入逐档吃单；不调用生产 cursor/acc/费用/区间 helper。
    // 连续整数成交的物理终点 pair 相同即同一区间，首个有解区间内取最大整数。
    fn oracle(pm: &[Level], out: &[Level], fees: &FeeContext, bounds: &ArbLimits) -> Option<usize> {
        fn consume(rows: &mut [(Q, Q)], i: &mut usize, n: Q) -> Option<(Q, usize)> {
            let mut remain = n;
            let mut cost = Q::zero();
            loop {
                let (price, size) = rows.get_mut(*i)?;
                if size.is_zero() {
                    *i += 1;
                    continue;
                }
                let take = size.clone().min(remain.clone());
                cost += &*price * &take;
                *size -= &take;
                remain -= take;
                if remain.is_zero() {
                    return Some((cost, *i));
                }
            }
        }
        let convert = |asks: &[Level], out: bool| {
            let mut rows: Vec<_> = asks
                .iter()
                .filter(|l| l.price > Decimal::ZERO && l.size > Decimal::ZERO)
                .map(|l| {
                    (
                        rational(l.price),
                        rational(if out { l.size.trunc() } else { l.size }),
                    )
                })
                .collect();
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            rows
        };
        let mut p = convert(pm, false);
        let mut o = convert(out, true);
        let (mut pi, mut oi) = (0, 0);
        let (mut pc, mut oc) = (Q::zero(), Q::zero());
        let mut endpoint = None;
        let mut best = None;
        for n in 1..=200 {
            let Some((padd, pe)) = consume(&mut p, &mut pi, Q::one()) else {
                break;
            };
            let Some((oadd, oe)) = consume(&mut o, &mut oi, Q::one()) else {
                break;
            };
            if endpoint != Some((pe, oe)) && best.is_some() {
                return best;
            }
            endpoint = Some((pe, oe));
            pc += padd;
            oc += oadd;
            let s = Q::from_integer(n.into());
            let avg = &pc / &s;
            let fee = &s * rational(fees.polymarket_fee_rate) * &avg * (Q::one() - avg);
            let c = &pc + &oc + fee + &oc * rational(fees.outcome_builder_rate);
            let profit = &s * (Q::one() - rational(fees.outcome_taker_rate)) - &c;
            if n >= 5
                && pc >= Q::one()
                && &s * &o[oe].0 >= Q::one()
                && c <= rational(bounds.cost_limit)
                && profit >= rational(bounds.min_profit)
                && &profit * Q::from_integer(365.into())
                    >= rational(bounds.min_apr) * Q::from_integer(bounds.days.max(1).into()) * c
            {
                best = Some(n);
            }
        }
        best
    }

    #[test]
    fn independent_oracle_fixed_seed_physical_intervals() {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x39391428);
        for case in 0..600 {
            let mut pm = Vec::new();
            let mut out = Vec::new();
            for i in 0..rng.gen_range(2..=5) {
                pm.push(Level {
                    price: Decimal::new(20 + i * 10, 2) + Decimal::new(rng.gen_range(0..=9), 28),
                    size: Decimal::new(rng.gen_range(1..=80), 1)
                        + Decimal::new(rng.gen_range(0..=9), 28),
                });
                out.push(Level {
                    price: Decimal::new(25 + i * 8, 2) + Decimal::new(rng.gen_range(0..=9), 28),
                    size: Decimal::new(rng.gen_range(10..=100), 1)
                        + Decimal::new(rng.gen_range(0..=9), 28),
                });
            }
            let fees = FeeContext {
                polymarket_fee_rate: Decimal::new(rng.gen_range(0..=10), 2)
                    + Decimal::new(rng.gen_range(0..=9), 28),
                outcome_taker_rate: Decimal::new(rng.gen_range(0..=100), 5),
                outcome_builder_rate: Decimal::new(rng.gen_range(0..=100), 5),
            };
            let mut bounds = limits("0", "100");
            bounds.cost_limit = Decimal::new(rng.gen_range(10..=300), 1);
            bounds.min_profit = Decimal::new(rng.gen_range(-20..=50), 1);
            bounds.min_apr = [d("-400"), d("-1"), d("0"), d("0.1"), d("1")][rng.gen_range(0..5)];
            bounds.days = 365;
            let expected = oracle(&pm, &out, &fees, &bounds);
            let actual = run(&pm, &out, &fees, &bounds).map(|p| p.net_shares.to_usize().unwrap());
            assert_eq!(
                actual, expected,
                "case={case} pm={pm:?} out={out:?} fees={fees:?} bounds={bounds:?}"
            );
        }
    }

    #[test]
    fn settlement_reserve_is_not_cash_and_confirm_refreshes_both_rates() {
        let pm = book_levels(&[("0.4", "30")]);
        let out = book_levels(&[("0.4", "30")]);
        let fees = FeeContext {
            outcome_taker_rate: d("0.001344"),
            ..fees_zero()
        };
        let bounds = limits("5.95968", "24");
        let plan = run(&pm, &out, &fees, &bounds).unwrap();
        assert_eq!(plan.net_shares, d("30"));
        assert_eq!(plan.settlement_reserve, d("0.04032"));
        assert_eq!(plan.expected_revenue, d("29.95968"));
        assert_eq!(plan.outcome.fee, Decimal::ZERO);
        assert_eq!(plan.total_cost, d("24"));
        assert_eq!(plan.profit, d("5.95968"));
        assert_eq!(plan.outcome_required(), Some(d("12")));
        assert!(plan.outcome_balance_sufficient(d("12")));
        let mut strict = bounds.clone();
        strict.min_profit += Decimal::new(1, 28);
        assert!(run(&pm, &out, &fees, &strict).is_none());
        strict = bounds.clone();
        strict.min_apr = d("0.24832");
        strict.days = 365;
        assert!(run(&pm, &out, &fees, &strict).is_some());
        strict.min_apr += Decimal::new(1, 28);
        assert!(run(&pm, &out, &fees, &strict).is_none());

        let topic = sample_topic();
        let mut capped = plan.clone();
        capped.outcome.cap_price = d("0.9");
        let changed = FeeContext {
            outcome_taker_rate: d("0.002"),
            outcome_builder_rate: d("0.0003"),
            ..fees_zero()
        };
        assert!(confirm(
            &capped,
            &pm,
            &out,
            &topic.tokens[0],
            &topic.tokens[2],
            &changed,
            &bounds
        )
        .is_err());
        let refreshed = confirm(
            &capped,
            &pm,
            &out,
            &topic.tokens[0],
            &topic.tokens[2],
            &changed,
            &limits("0", "24.0036"),
        )
        .unwrap();
        assert_eq!(refreshed.net_shares, d("30"));
        assert_eq!(refreshed.outcome.fee, d("0.0036"));
        assert_eq!(refreshed.settlement_reserve, d("0.06"));
        assert_eq!(refreshed.expected_revenue, d("29.94"));
        assert_eq!(refreshed.total_cost, d("24.0036"));
        assert_eq!(
            refreshed.expected_revenue - refreshed.total_cost,
            refreshed.profit
        );
        assert_eq!(refreshed.outcome_required(), Some(d("27.0081")));
        assert!(!refreshed.outcome_balance_sufficient(d("27.0036")));
        assert!(!refreshed.outcome_balance_sufficient(d("27.00809999")));
        assert!(refreshed.outcome_balance_sufficient(d("27.0081")));
    }

    #[test]
    fn reserve_and_profit_projection_are_conservative_below_decimal_precision() {
        let pm = book_levels(&[("0.3333333333333333333333333333", "30")]);
        let out = book_levels(&[("0.4", "30")]);
        let fees = FeeContext {
            polymarket_fee_rate: d("0.07"),
            outcome_taker_rate: d("0.0013440000000000000000000001"),
            outcome_builder_rate: d("0.0003"),
        };
        let plan = run(&pm, &out, &fees, &limits("0", "100")).unwrap();
        let v = &plan.exact.values;
        assert!(rational(plan.settlement_reserve) >= v.settlement_reserve);
        assert!(rational(plan.expected_revenue) <= v.expected_revenue);
        assert!(rational(plan.total_cost) >= v.total);
        assert!(rational(plan.profit) <= v.profit);
        assert!(rational(plan.roi) <= &v.profit / &v.total);
        assert_eq!(plan.expected_revenue - plan.total_cost, plan.profit);
        assert_eq!(
            plan.net_shares - plan.settlement_reserve,
            plan.expected_revenue
        );
    }

    #[test]
    fn original_fee_regression_fourteen_and_exact_legacy_counterexample() {
        let pm = book_levels(&[("0.30", "4.5"), ("0.61", "100")]);
        let out = book_levels(&[("0.40", "200")]);
        let fees = FeeContext {
            polymarket_fee_rate: d("0.07"),
            outcome_taker_rate: Decimal::ZERO,
            outcome_builder_rate: Decimal::ZERO,
        };
        assert_eq!(
            run(&pm, &out, &fees, &limits("1", "100"))
                .unwrap()
                .net_shares,
            d("14")
        );
        let bounds = ArbLimits {
            days: 365,
            ..limits("0.000000000000000000000000013", "100")
        };
        let fees = FeeContext {
            polymarket_fee_rate: Decimal::ONE,
            outcome_taker_rate: Decimal::ZERO,
            outcome_builder_rate: Decimal::ZERO,
        };
        let rules = Rules::new(&fees, &bounds).unwrap();
        let quote = PmQuote {
            max_net: 40.into(),
            first_cost: rational(d("0.39999999999999999999999999")),
            cap: d("0.4"),
        };
        for n in [5, 39, 40] {
            let trial = Acc::default().plus(&n.into(), &quote, d("0.36"));
            assert!(trial.passes_mins());
            assert!(
                !trial.metrics(&rules).profit_passes(&rules),
                "exact {n} must reject legacy false positive"
            );
        }
    }

    #[test]
    fn exact_equality_neighbors_profit_budget_apr_and_negative_apr() {
        let pm = book_levels(&[("0.4", "10")]);
        let out = book_levels(&[("0.4", "10")]);
        let mut b = ArbLimits {
            days: 365,
            min_apr: d("0.25"),
            ..limits("2", "8")
        };
        assert_eq!(
            run(&pm, &out, &fees_zero(), &b).unwrap().net_shares,
            d("10")
        );
        b.cost_limit = d("7.999999999999999999999999999");
        assert!(run(&pm, &out, &fees_zero(), &b).is_none());
        b.cost_limit = d("8");
        b.min_profit = d("2.0000000000000000000000000001");
        assert!(run(&pm, &out, &fees_zero(), &b).is_none());
        b.min_profit = d("2");
        b.min_apr = d("0.2500000000000000000000000001");
        assert!(run(&pm, &out, &fees_zero(), &b).is_none());
        for apr in ["-1", "-1.0000000000000000000000000001", "-400"] {
            b.min_apr = d(apr);
            assert!(run(&pm, &out, &fees_zero(), &b).is_some());
        }
    }

    #[test]
    fn projection_ties_carry_scale_max_and_required_ceiling() {
        for (input, expected) in [
            ("1.25", "1.2"),
            ("1.35", "1.4"),
            ("-1.25", "-1.2"),
            ("-1.35", "-1.4"),
            ("9.95", "10"),
        ] {
            assert_eq!(at_scale(&rational(d(input)), 1, false), Some(d(expected)));
        }
        let max = rational(Decimal::MAX);
        assert_eq!(project(&max, false), Some(Decimal::MAX));
        assert_eq!(project(&(&max + Q::new(1.into(), 10.into())), false), None);
        assert_eq!(project(&(-&max - Q::one()), false), None);
        let third = Q::new(1.into(), 3.into());
        let ceiling = project(&third, true).unwrap();
        assert!(rational(ceiling) >= third);
        assert!(rational(ceiling - Decimal::new(1, ceiling.scale())) < third);
        let near = &max - Q::new(1.into(), 10.into());
        assert_eq!(project(&near, true), Some(Decimal::MAX));
        let v = Values {
            s: max.clone(),
            settlement_reserve: Q::zero(),
            expected_revenue: max.clone(),
            pm_cost: &max / Q::from_integer(2.into()),
            out_cost: Q::zero(),
            pm_fee: Q::zero(),
            out_fee: Q::zero(),
            total: &max / Q::from_integer(2.into()),
            profit: &max / Q::from_integer(2.into()),
        };
        let shown = display(&v, 365).unwrap();
        assert_eq!(shown.pm_cost.scale(), 0);
        assert_eq!(
            shown.total,
            shown.pm_cost + shown.pm_fee + shown.out_cost + shown.out_fee
        );
        assert_eq!(shown.profit, Decimal::MAX - shown.total);
        assert_eq!(at_scale(&max, 1, false), None);
    }

    #[test]
    fn fine_balance_binding_and_http_recompute() {
        let pm = book_levels(&[("0.3333333333333333333333333333", "10")]);
        let out = book_levels(&[("0.4", "10")]);
        let fees = FeeContext {
            polymarket_fee_rate: d("0.07"),
            outcome_taker_rate: d("0.00035"),
            outcome_builder_rate: Decimal::ZERO,
        };
        let b = limits("0", "100");
        let plan = run(&pm, &out, &fees, &b).unwrap();
        let exact_need = plan.exact.required_value(true);
        let shown_need = plan.pm.cost + plan.pm.fee;
        let need = plan.pm_required().unwrap();
        assert!(rational(need) >= exact_need);
        assert!(plan.pm_balance_sufficient(need));
        assert!(!plan.pm_balance_sufficient(need - Decimal::new(1, need.scale())));
        assert_eq!(
            plan.pm_balance_sufficient(shown_need),
            rational(shown_need) >= exact_need
        );
        let topic = sample_topic();
        let refreshed = confirm(
            &plan,
            &pm,
            &out,
            &topic.tokens[0],
            &topic.tokens[2],
            &fees,
            &b,
        )
        .unwrap();
        assert_eq!(refreshed.exact.values.pm_cost, plan.exact.values.pm_cost);
        assert_eq!(refreshed.pm_required(), plan.pm_required());
        for edit in 0..11 {
            let mut bad = plan.clone();
            match edit {
                0 => bad.pm.token_id.push('x'),
                1 => bad.outcome.token_id.push('x'),
                2 => bad.pm.platform.push('x'),
                3 => bad.outcome.platform.push('x'),
                4 => bad.pm.label.push('x'),
                5 => bad.outcome.label.push('x'),
                6 => bad.pm.shares += Decimal::ONE,
                7 => bad.outcome.shares += Decimal::ONE,
                8 => bad.pm.cap_price += d("0.01"),
                9 => bad.outcome.cap_price += d("0.01"),
                _ => bad.net_shares += Decimal::ONE,
            }
            assert!(!bad.pm_balance_sufficient(Decimal::MAX));
            assert!(!bad.outcome_balance_sufficient(Decimal::MAX));
            assert!(bad.pm_required().is_none());
            assert!(bad.outcome_required().is_none());
        }
        // 展示字段不属于决策绑定：改展示不能放大或削弱精确余额要求。
        let mut display_only = plan.clone();
        display_only.pm.cost = Decimal::ZERO;
        display_only.pm.fee = Decimal::ZERO;
        assert_eq!(display_only.pm_required(), plan.pm_required());
    }

    #[test]
    fn actual_subdecimal_books_rank_and_http_profit_and_balance() {
        use super::super::{best_plan, confirm_plan, plan_arbitrage};
        use crate::book::BookStore;
        let topic = sample_topic();
        let now = std::time::Instant::now();
        let mut books = BookStore::default();
        for (id, price) in [
            ("pm-yes", "0.3999999999999999999999999999"),
            ("pm-no", "0.3999999999999999999999999998"),
        ] {
            books.replace_snapshot(
                POLYMARKET,
                id,
                vec![],
                book_levels(&[(price, "0.0000000000000000000000000001"), ("0.4", "10")]),
                1,
                now,
            );
            books.set_tick_size(POLYMARKET, id, d("0.01"));
        }
        for id in ["#10", "#11"] {
            books.replace_snapshot(OUTCOME, id, vec![], book_levels(&[("0.4", "10")]), 1, now);
        }
        let b = limits("2", "8");
        let p1 = plan_arbitrage(
            &topic,
            books.get(POLYMARKET, "pm-yes").unwrap(),
            books.get(OUTCOME, "#10").unwrap(),
            &topic.tokens[0],
            &topic.tokens[2],
            &fees_zero(),
            &b,
        )
        .unwrap();
        let p2 = plan_arbitrage(
            &topic,
            books.get(POLYMARKET, "pm-no").unwrap(),
            books.get(OUTCOME, "#11").unwrap(),
            &topic.tokens[1],
            &topic.tokens[3],
            &fees_zero(),
            &b,
        )
        .unwrap();
        assert_eq!(p1.roi, p2.roi);
        assert_eq!(p1.profit, p2.profit);
        assert!(p2.exact.compare(&p1.exact).is_gt());
        assert_eq!(
            best_plan(&topic, &books, &fees_zero(), &b)
                .unwrap()
                .pm
                .token_id,
            "pm-no"
        );
        // 最后一档只高 1e-28，乘不足一股后的 HTTP 成本差仅 1e-56。
        let mut http = books.get(POLYMARKET, "pm-yes").unwrap().clone();
        http.asks = book_levels(&[
            ("0.4", "9.999999999999999999999999999"),
            ("0.4000000000000000000000000001", "1"),
        ]);
        let mut request = p1.clone();
        request.pm.cap_price = d("0.41");
        assert!(!request.pm_balance_sufficient(Decimal::MAX));
        let out = books.get(OUTCOME, "#10").unwrap();
        assert!(
            confirm_plan(&topic, &request, &http, out, &fees_zero(), &b).is_none(),
            "精确收益低于2，即使展示利润等于2也拒绝"
        );
        let refreshed = confirm_plan(
            &topic,
            &request,
            &http,
            out,
            &fees_zero(),
            &limits("0", "100"),
        )
        .unwrap();
        assert!(refreshed.pm.cost > d("4"));
        assert!(refreshed.profit < d("2"));
        assert!(
            !refreshed.pm_balance_sufficient(d("4")),
            "展示需求4不能放行精确大于4的成本"
        );
        assert!(refreshed.pm_required().unwrap() > d("4"));
        assert!(refreshed.pm_balance_sufficient(refreshed.pm_required().unwrap()));
    }

    #[test]
    fn precise_roi_sort_ignores_identical_display() {
        let out = book_levels(&[("0.4", "10")]);
        let p1 = run(
            &book_levels(&[("0.3333333333333333333333333333", "10")]),
            &out,
            &fees_zero(),
            &limits("0", "100"),
        )
        .unwrap();
        let mut p2 = p1.clone();
        // 子 Decimal 位的精确成本差模拟合法逐档小数乘积，排序不得读取展示 ROI。
        p2.exact.values.total += Q::new(1.into(), BigInt::from(10u8).pow(50));
        p2.exact.values.profit = &p2.exact.values.expected_revenue - &p2.exact.values.total;
        assert_eq!(p1.roi, p2.roi);
        assert!(p1.exact.compare(&p2.exact).is_gt());
        let mut equal_roi = p1.exact.clone();
        equal_roi.values.total *= Q::from_integer(2.into());
        equal_roi.values.profit *= Q::from_integer(2.into());
        assert!(equal_roi.compare(&p1.exact).is_gt());
    }

    #[test]
    fn convex_endpoints_and_independent_interval_enumeration() {
        let quote = PmQuote {
            max_net: 100.into(),
            first_cost: rational(d("0.3")),
            cap: d("0.4"),
        };
        let fees = FeeContext {
            polymarket_fee_rate: Decimal::ONE,
            outcome_taker_rate: Decimal::ZERO,
            outcome_builder_rate: Decimal::ZERO,
        };
        let bounds = limits("0.1223", "200");
        // 非零 builder 和结算准备仍可同时出现通过/失败/通过的凸收益两支。
        let nonzero = Rules::new(
            &FeeContext {
                outcome_taker_rate: d("0.000001"),
                outcome_builder_rate: d("0.00001"),
                ..fees.clone()
            },
            &bounds,
        )
        .unwrap();
        for (s, passes) in [(5, true), (10, false), (100, true)] {
            assert_eq!(
                Acc::default()
                    .plus(&s.into(), &quote, d("0.3599"))
                    .metrics(&nonzero)
                    .profit_passes(&nonzero),
                passes
            );
        }
        let values: Vec<_> = (5..=100)
            .map(|s| {
                Acc::default()
                    .plus(&s.into(), &quote, d("0.3599"))
                    .metrics(&nonzero)
            })
            .collect();
        for triple in values.windows(3) {
            assert!(triple[0].total < triple[1].total);
            assert!(triple[1].total < triple[2].total);
            assert!(
                &triple[1].profit * Q::from_integer(2.into())
                    <= &triple[0].profit + &triple[2].profit
            );
        }
        assert_eq!(
            interval(&Acc::default(), &quote, d("0.3599"), &nonzero).0,
            Some(100.into())
        );
        let rules = Rules::new(&fees, &bounds).unwrap();
        let acc = Acc::default();
        for (s, passes) in [(5, true), (10, false), (100, true)] {
            assert_eq!(
                acc.plus(&s.into(), &quote, d("0.3599"))
                    .metrics(&rules)
                    .profit_passes(&rules),
                passes
            );
        }
        assert_eq!(
            interval(&acc, &quote, d("0.3599"), &rules).0,
            Some(100.into())
        );
        // 改预算把 U 放在凸函数谷底，必须缩到左侧通过前缀，而不是二分 passes_all。
        let mut prefix = bounds.clone();
        prefix.cost_limit = d("9.878");
        let prefix_rules = Rules::new(&fees, &prefix).unwrap();
        assert_eq!(
            interval(&acc, &quote, d("0.3599"), &prefix_rules).0,
            Some(5.into())
        );
        let both = ArbLimits {
            min_apr: d("0.0124"),
            days: 365,
            ..bounds.clone()
        };
        SHRINKS.with(|n| n.set(0));
        assert_eq!(
            interval(
                &acc,
                &quote,
                d("0.3599"),
                &Rules::new(&fees, &both).unwrap()
            )
            .0,
            Some(5.into())
        );
        assert_eq!(
            SHRINKS.with(|n| n.get()),
            2,
            "APR 收缩进入利润谷底后必须重新检查利润"
        );
        // 固定原始精确档成本独立逐整数计算，同时覆盖 APR 自动通过域及极小收益。
        for pm_rate in ["0", "0.07", "1"] {
            for out_px in ["0.3599", "0.36", "0.4", "0.6000000000000000000000000001"] {
                for profit in ["-1", "0", "0.0000000000000000000000000001", "0.1223", "1"] {
                    for apr in ["-400", "-1", "-0.1", "0", "0.001", "0.02", "1"] {
                        let fees = FeeContext {
                            polymarket_fee_rate: d(pm_rate),
                            outcome_taker_rate: d("0.001344"),
                            outcome_builder_rate: d("0.0003"),
                        };
                        let b = ArbLimits {
                            min_apr: d(apr),
                            days: 365,
                            ..limits(profit, "50")
                        };
                        let r = Rules::new(&fees, &b).unwrap();
                        let expected = (1..=100).rev().find(|n| {
                            let s = Q::from_integer((*n).into());
                            let pc = rational(d("0.3")) + (&s - Q::one()) * rational(d("0.4"));
                            let oc = &s * rational(d(out_px));
                            let avg = &pc / &s;
                            let c = &pc
                                + &oc
                                + &s * rational(d(pm_rate)) * &avg * (Q::one() - avg)
                                + oc * rational(d("0.0003"));
                            let gain = &s * (Q::one() - rational(d("0.001344"))) - &c;
                            *n >= 5
                                && pc >= Q::one()
                                && &s * rational(d(out_px)) >= Q::one()
                                && c <= rational(b.cost_limit)
                                && gain >= rational(b.min_profit)
                                && &gain * Q::from_integer(365.into())
                                    >= rational(b.min_apr) * Q::from_integer(365.into()) * c
                        });
                        assert_eq!(
                            interval(&acc, &quote, d(out_px), &r).0,
                            expected.map(BigInt::from),
                            "rate={pm_rate} out={out_px} profit={profit} apr={apr}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn bridge_discount_survives_next_ordinary_quote() {
        let pm = book_levels(&[("0.2", "0.5"), ("0.3", "3.5"), ("0.65", "50")]);
        let out = book_levels(&[("0.4", "100")]);
        let b = limits("0.5", "100");
        let expected = oracle(&pm, &out, &fees_zero(), &b).unwrap();
        assert_eq!(
            run(&pm, &out, &fees_zero(), &b)
                .unwrap()
                .net_shares
                .to_usize(),
            Some(expected)
        );
        assert!(expected > 5);
    }

    #[test]
    fn unsupported_parameters_and_unrepresentable_plans_reject_without_panic() {
        let pm = book_levels(&[("0.4", "10")]);
        let out = book_levels(&[("0.4", "10")]);
        for budget in ["0", "-1"] {
            assert!(run(&pm, &out, &fees_zero(), &limits("0", budget)).is_none());
        }
        for price in ["-0.1", "1.1"] {
            assert!(run(
                &book_levels(&[(price, "10")]),
                &out,
                &fees_zero(),
                &limits("0", "100")
            )
            .is_none());
        }
        let s = rational(Decimal::MAX) + Q::one();
        let v = Values {
            s: s.clone(),
            settlement_reserve: Q::zero(),
            expected_revenue: s.clone(),
            pm_cost: &s / Q::from_integer(2.into()),
            out_cost: Q::zero(),
            pm_fee: Q::zero(),
            out_fee: Q::zero(),
            total: &s / Q::from_integer(2.into()),
            profit: &s / Q::from_integer(2.into()),
        };
        assert!(display(&v, 365).is_none());
        let rules = Rules::new(&fees_zero(), &limits("0", "100")).unwrap();
        let acc = Acc {
            shares: s.to_integer(),
            pm_cost: Q::one(),
            out_cost: Q::one(),
            pm_cap: d("0.4"),
            out_cap: d("0.4"),
        };
        let topic = sample_topic();
        assert!(make_plan(
            &acc,
            &rules,
            &limits("0", "100"),
            &topic.tokens[0],
            &topic.tokens[2],
            d("0.4"),
            d("0.4")
        )
        .is_none());
    }

    fn performance_cases() -> Vec<(&'static str, Vec<Level>, Vec<Level>, ArbLimits)> {
        vec![
            (
                "ordinary",
                book_levels(&[("0.4", "1000")]),
                book_levels(&[("0.4", "1000")]),
                limits("0", "500"),
            ),
            (
                "bridge_1e12",
                book_levels(&[("0.3", "0.5"), ("0.4", "1000000000000")]),
                book_levels(&[("0.4", "1000000000000")]),
                limits("0", "500000000000"),
            ),
            (
                "tiny_1000",
                (0..1000)
                    .map(|i| Level {
                        price: d("0.2") + Decimal::new(i, 4),
                        size: d("0.01"),
                    })
                    .collect(),
                book_levels(&[("0.4", "10")]),
                limits("3", "100"),
            ),
        ]
    }

    #[test]
    fn evaluation_counts_are_bounded_for_each_shape() {
        for (name, pm, out, b) in performance_cases() {
            EVALUATIONS.with(|n| n.set(0));
            assert!(run(&pm, &out, &fees_zero(), &b).is_some());
            let count = EVALUATIONS.with(|n| n.get());
            assert!(count < 220, "{name}: {count}");
        }
    }

    #[test]
    #[ignore = "manual CPU benchmark; no database, network or trade"]
    fn manual_exact_search_performance() {
        for (name, pm, out, b) in performance_cases() {
            let samples = 100;
            EVALUATIONS.with(|n| n.set(0));
            let begin = std::time::Instant::now();
            for _ in 0..samples {
                assert!(std::hint::black_box(run(&pm, &out, &fees_zero(), &b)).is_some());
            }
            let elapsed = begin.elapsed();
            let evaluations = EVALUATIONS.with(|n| n.get());
            println!("profile={} shape={name} samples={samples} evaluations={evaluations} evaluations_per_sample={} elapsed_us={} mean_us={}",
                if cfg!(debug_assertions) { "debug" } else { "release" }, evaluations / samples, elapsed.as_micros(), elapsed.as_micros() / samples as u128);
        }
    }
}
