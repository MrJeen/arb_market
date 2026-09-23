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
        let worst_pm_c = &s * &p[pe].0;
        let worst_pm_fee =
            rational(fees.polymarket_fee_rate) * (&worst_pm_c - &worst_pm_c * &p[pe].0);
        let worst_out_c = &s * &o[oe].0;
        let worst_out_fee = rational(fees.outcome_builder_rate) * &worst_out_c;
        let worst_cost = worst_pm_c + worst_pm_fee + worst_out_c + worst_out_fee;
        let worst_gain = &s * (Q::one() - rational(fees.outcome_taker_rate)) - worst_cost;
        if n >= 5
            && pc >= Q::one()
            && &s * &o[oe].0 >= Q::one()
            && c <= rational(bounds.cost_limit)
            && profit >= rational(bounds.min_profit)
            && worst_gain >= Q::zero()
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
    let mut bad_cap = plan.clone();
    bad_cap.outcome.cap_price = d("0.9");
    let changed = FeeContext {
        outcome_taker_rate: d("0.002"),
        outcome_builder_rate: d("0.0003"),
        ..fees_zero()
    };
    assert_eq!(
        confirm(
            &bad_cap,
            &pm,
            &out,
            &topic.tokens[0],
            &topic.tokens[2],
            &changed,
            &limits("0", "24.0036"),
        )
        .unwrap_err(),
        "unprofitable"
    );
    let mut capped = plan.clone();
    capped.outcome.cap_price = d("0.55");
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
    assert_eq!(refreshed.outcome_required(), Some(d("16.50495")));
    assert!(!refreshed.outcome_balance_sufficient(d("16.5036")));
    assert!(!refreshed.outcome_balance_sufficient(d("16.50494999")));
    assert!(refreshed.outcome_balance_sufficient(d("16.50495")));
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
    let bad_pm = book_levels(&[("0.30", "4.5"), ("0.61", "100")]);
    let out = book_levels(&[("0.40", "200")]);
    let fees = FeeContext {
        polymarket_fee_rate: d("0.07"),
        outcome_taker_rate: Decimal::ZERO,
        outcome_builder_rate: Decimal::ZERO,
    };
    assert!(
        run(&bad_pm, &out, &fees, &limits("1", "100")).is_none(),
        "0.61 + 0.40 = 1.01 破坏最坏情况，必须拒绝"
    );
    let pm = book_levels(&[("0.30", "4.5"), ("0.55", "100")]);
    assert!(run(&pm, &out, &fees, &limits("1", "100")).is_some());
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
        worst_total: &max / Q::from_integer(2.into()),
        worst_profit: &max / Q::from_integer(2.into()),
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
            &triple[1].profit * Q::from_integer(2.into()) <= &triple[0].profit + &triple[2].profit
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
                        let worst_c = &s * rational(d("0.4"))
                            + &s * rational(d(pm_rate))
                                * rational(d("0.4"))
                                * (Q::one() - rational(d("0.4")))
                            + &s * rational(d(out_px)) * (Q::one() + rational(d("0.0003")));
                        let worst_gain = &s * (Q::one() - rational(d("0.001344"))) - worst_c;
                        *n >= 5
                            && pc >= Q::one()
                            && &s * rational(d(out_px)) >= Q::one()
                            && c <= rational(b.cost_limit)
                            && gain >= rational(b.min_profit)
                            && worst_gain >= Q::zero()
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
    let invalid_pm = book_levels(&[("0.2", "0.5"), ("0.3", "3.5"), ("0.65", "50")]);
    let out = book_levels(&[("0.4", "100")]);
    let b = limits("0.5", "100");
    assert!(run(&invalid_pm, &out, &fees_zero(), &b).is_none());

    let pm = book_levels(&[("0.2", "0.5"), ("0.3", "3.5"), ("0.55", "50")]);
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
        worst_total: &s / Q::from_integer(2.into()),
        worst_profit: &s / Q::from_integer(2.into()),
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
