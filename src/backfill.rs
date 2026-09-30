//! 把已成交的互补持仓配成套利订单。不访问数据库，也不下单。
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::str::FromStr;

pub const MATCH_WINDOW_SECS: i64 = 300;
pub const BACKFILL_TAG: &str = "epl-1473-1477";

fn tolerance() -> Decimal {
    Decimal::from_str("1.5").expect("1.5")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct TradeFillInput {
    pub ts: i64,
    pub account: String,
    pub token_id: String,
    pub side: TradeSide,
    pub shares: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
    pub source_id: String,
}

#[derive(Debug, Clone)]
pub struct ComplementaryPair {
    pub unified_index: i32,
    pub market_title: String,
    pub pm_token: String,
    pub pm_label: String,
    pub out_token: String,
    pub out_label: String,
    pub condition_id: String,
    pub option_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedOrder {
    pub unified_index: i32,
    pub market_title: String,
    pub pm_wallet: String,
    pub pm_token: String,
    pub pm_label: String,
    pub out_token: String,
    pub out_label: String,
    pub condition_id: String,
    pub option_id: String,
    pub pm_shares: Decimal,
    pub pm_price: Decimal,
    pub pm_fee: Decimal,
    pub out_shares: Decimal,
    pub out_price: Decimal,
    pub out_fee: Decimal,
    pub submitted_at: i64,
    pub pm_sources: Vec<String>,
    pub out_sources: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IgnoredPosition {
    pub wallet: String,
    pub token_id: String,
    pub shares: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillPlan {
    pub orders: Vec<PlannedOrder>,
    pub ignored_pm: Vec<IgnoredPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchError {
    Oversell {
        account: String,
        token_id: String,
        excess: Decimal,
    },
    OutcomeUnmatched {
        token_id: String,
        shares: Decimal,
    },
}

impl std::fmt::Display for MatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Oversell {
                account,
                token_id,
                excess,
            } => write!(f, "sell exceeds buys for {account} {token_id} by {excess}"),
            Self::OutcomeUnmatched { token_id, shares } => {
                write!(f, "outcome {token_id} left unmatched shares {shares}")
            }
        }
    }
}

#[derive(Clone)]
struct Lot {
    ts: i64,
    shares: Decimal,
    price: Decimal,
    fee: Decimal,
    source_id: String,
    wallet: String,
}

struct Consumed {
    ts: i64,
    shares: Decimal,
    price: Decimal,
    fee: Decimal,
    source_id: String,
}

pub fn match_backfill(
    pairs: &[ComplementaryPair],
    pm: &[TradeFillInput],
    outcome: &[TradeFillInput],
) -> Result<BackfillPlan, MatchError> {
    let mut orders = Vec::new();
    let mut ignored = Vec::new();
    for pair in pairs {
        let pm_lots = open_lots(pm.iter().filter(|fill| fill.token_id == pair.pm_token))?;
        let out_lots = open_lots(
            outcome
                .iter()
                .filter(|fill| fill.token_id == pair.out_token),
        )?;
        let (pair_orders, pair_ignored) = match_pair(pair, pm_lots, out_lots)?;
        orders.extend(pair_orders);
        ignored.extend(pair_ignored);
    }
    orders.sort_by(|a, b| {
        (
            a.unified_index,
            a.pm_wallet.as_str(),
            a.pm_token.as_str(),
            a.submitted_at,
        )
            .cmp(&(
                b.unified_index,
                b.pm_wallet.as_str(),
                b.pm_token.as_str(),
                b.submitted_at,
            ))
    });
    ignored.sort_by(|a, b| {
        (a.token_id.as_str(), a.wallet.as_str()).cmp(&(b.token_id.as_str(), b.wallet.as_str()))
    });
    Ok(BackfillPlan {
        orders,
        ignored_pm: ignored,
    })
}

fn open_lots<'a>(fills: impl Iterator<Item = &'a TradeFillInput>) -> Result<Vec<Lot>, MatchError> {
    let mut grouped: BTreeMap<(String, String), Vec<&TradeFillInput>> = BTreeMap::new();
    for fill in fills {
        grouped
            .entry((fill.account.clone(), fill.token_id.clone()))
            .or_default()
            .push(fill);
    }
    let mut lots = Vec::new();
    for ((account, token_id), mut fills) in grouped {
        fills.sort_by(|a, b| (a.ts, a.source_id.as_str()).cmp(&(b.ts, b.source_id.as_str())));
        let mut buys: Vec<Lot> = Vec::new();
        for fill in fills {
            match fill.side {
                TradeSide::Buy => buys.push(Lot {
                    ts: fill.ts,
                    shares: fill.shares,
                    price: fill.price,
                    fee: fill.fee,
                    source_id: fill.source_id.clone(),
                    wallet: fill.account.clone(),
                }),
                TradeSide::Sell => {
                    let mut left = fill.shares;
                    let mut index = 0;
                    while left > Decimal::ZERO && index < buys.len() {
                        let take = left.min(buys[index].shares);
                        take_from(&mut buys[index], take);
                        left -= take;
                        if buys[index].shares <= Decimal::ZERO {
                            buys.remove(index);
                        } else {
                            index += 1;
                        }
                    }
                    if left > Decimal::ZERO {
                        return Err(MatchError::Oversell {
                            account,
                            token_id,
                            excess: left,
                        });
                    }
                }
            }
        }
        lots.extend(buys.into_iter().filter(|lot| lot.shares > Decimal::ZERO));
    }
    lots.sort_by(|a, b| {
        (a.ts, a.wallet.as_str(), a.source_id.as_str()).cmp(&(
            b.ts,
            b.wallet.as_str(),
            b.source_id.as_str(),
        ))
    });
    Ok(lots)
}

fn match_pair(
    pair: &ComplementaryPair,
    mut pm: Vec<Lot>,
    mut outcome: Vec<Lot>,
) -> Result<(Vec<PlannedOrder>, Vec<IgnoredPosition>), MatchError> {
    let tol = tolerance();
    let mut orders = Vec::new();
    // 先按时间就近把单笔 PM 剩余买单和 5 分钟内的 Outcome 买单配上。
    // Outcome 份数可以汇总，最后一笔允许部分占用。
    loop {
        let mut best: Option<(i64, usize)> = None;
        for (index, lot) in pm.iter().enumerate() {
            if lot.shares <= Decimal::ZERO {
                continue;
            }
            let idxs = in_window(
                &outcome,
                lot.ts - MATCH_WINDOW_SECS,
                lot.ts + MATCH_WINDOW_SECS,
            );
            let available = sum_shares(&outcome, &idxs);
            if take_plan(lot.shares, available, tol).is_none() {
                continue;
            }
            let Some(distance) = closest_distance(&outcome, lot.ts, &idxs) else {
                continue;
            };
            let closer = best.is_none_or(|(prev_distance, prev)| {
                let prev_lot = &pm[prev];
                (distance, lot.wallet.as_str(), lot.source_id.as_str(), index)
                    < (
                        prev_distance,
                        prev_lot.wallet.as_str(),
                        prev_lot.source_id.as_str(),
                        prev,
                    )
            });
            if closer {
                best = Some((distance, index));
            }
        }
        let Some((_, index)) = best else {
            break;
        };
        let lot_shares = pm[index].shares;
        let ts = pm[index].ts;
        let wallet = pm[index].wallet.clone();
        let idxs = in_window(&outcome, ts - MATCH_WINDOW_SECS, ts + MATCH_WINDOW_SECS);
        let (_, out_used) =
            take_plan(lot_shares, sum_shares(&outcome, &idxs), tol).expect("window was matchable");
        let out_parts = consume(&mut outcome, &idxs, out_used, None);
        let pm_parts = consume_one(&mut pm[index]);
        orders.push(order_from(pair, &wallet, pm_parts, out_parts));
    }

    // 5 分钟窗口没收口的，按同一钱包汇总，再用剩余 Outcome 按时间就近分配。
    let mut wallets: Vec<String> = pm
        .iter()
        .filter(|lot| lot.shares > Decimal::ZERO)
        .map(|lot| lot.wallet.clone())
        .collect();
    wallets.sort();
    wallets.dedup();
    wallets.sort_by_key(|wallet| {
        pm.iter()
            .filter(|lot| lot.wallet == *wallet && lot.shares > Decimal::ZERO)
            .map(|lot| lot.ts)
            .min()
            .unwrap_or(i64::MAX)
    });
    let mut ignored = Vec::new();
    for wallet in wallets {
        let pm_sum: Decimal = pm
            .iter()
            .filter(|lot| lot.wallet == wallet && lot.shares > Decimal::ZERO)
            .map(|lot| lot.shares)
            .sum();
        let anchor = pm
            .iter()
            .filter(|lot| lot.wallet == wallet && lot.shares > Decimal::ZERO)
            .map(|lot| lot.ts)
            .min()
            .unwrap_or(0);
        let available: Decimal = outcome
            .iter()
            .filter(|lot| lot.shares > Decimal::ZERO)
            .map(|lot| lot.shares)
            .sum();
        if let Some((pm_used, out_used)) = take_plan(pm_sum, available, tol) {
            let out_idxs: Vec<usize> = (0..outcome.len()).collect();
            let out_parts = consume(&mut outcome, &out_idxs, out_used, Some(anchor));
            let pm_parts = consume_wallet(&mut pm, &wallet, pm_used, anchor);
            orders.push(order_from(pair, &wallet, pm_parts, out_parts));
        } else if available > Decimal::ZERO {
            let out_idxs: Vec<usize> = (0..outcome.len()).collect();
            let out_parts = consume(&mut outcome, &out_idxs, available, Some(anchor));
            let pm_parts = consume_wallet(&mut pm, &wallet, available, anchor);
            let leftover: Decimal = pm
                .iter()
                .filter(|lot| lot.wallet == wallet)
                .map(|lot| lot.shares)
                .sum();
            if leftover > Decimal::ZERO {
                ignored.push(IgnoredPosition {
                    wallet: wallet.clone(),
                    token_id: pair.pm_token.clone(),
                    shares: leftover,
                });
                for lot in pm.iter_mut().filter(|lot| lot.wallet == wallet) {
                    lot.shares = Decimal::ZERO;
                    lot.fee = Decimal::ZERO;
                }
            }
            if !pm_parts.is_empty() {
                orders.push(order_from(pair, &wallet, pm_parts, out_parts));
            }
        } else if pm_sum > Decimal::ZERO {
            ignored.push(IgnoredPosition {
                wallet: wallet.clone(),
                token_id: pair.pm_token.clone(),
                shares: pm_sum,
            });
            for lot in pm.iter_mut().filter(|lot| lot.wallet == wallet) {
                lot.shares = Decimal::ZERO;
                lot.fee = Decimal::ZERO;
            }
        }
    }
    let outcome_left: Decimal = outcome
        .iter()
        .map(|lot| lot.shares.max(Decimal::ZERO))
        .sum();
    if outcome_left > tol {
        return Err(MatchError::OutcomeUnmatched {
            token_id: pair.out_token.clone(),
            shares: outcome_left,
        });
    }
    Ok((orders, ignored))
}

/// `Some((pm_used, out_used))` 表示两边差额不超过容忍度，或 Outcome 更多时只取与 PM 相同的份数。
fn take_plan(pm_sum: Decimal, available: Decimal, tol: Decimal) -> Option<(Decimal, Decimal)> {
    if pm_sum <= Decimal::ZERO || available <= Decimal::ZERO {
        return None;
    }
    if (pm_sum - available).abs() <= tol {
        return Some((pm_sum, available));
    }
    if available > pm_sum {
        return Some((pm_sum, pm_sum));
    }
    None
}

fn in_window(lots: &[Lot], start: i64, end: i64) -> Vec<usize> {
    lots.iter()
        .enumerate()
        .filter(|(_, lot)| lot.shares > Decimal::ZERO && lot.ts >= start && lot.ts <= end)
        .map(|(index, _)| index)
        .collect()
}

fn sum_shares(lots: &[Lot], idxs: &[usize]) -> Decimal {
    idxs.iter().map(|index| lots[*index].shares).sum()
}

fn closest_distance(lots: &[Lot], ts: i64, idxs: &[usize]) -> Option<i64> {
    idxs.iter().map(|index| (lots[*index].ts - ts).abs()).min()
}

fn consume(lots: &mut [Lot], idxs: &[usize], need: Decimal, anchor: Option<i64>) -> Vec<Consumed> {
    let mut order: Vec<usize> = idxs
        .iter()
        .copied()
        .filter(|index| lots[*index].shares > Decimal::ZERO)
        .collect();
    order.sort_by(|a, b| match anchor {
        Some(anchor) => {
            let left = (lots[*a].ts - anchor).abs();
            let right = (lots[*b].ts - anchor).abs();
            (left, lots[*a].ts, lots[*a].source_id.as_str()).cmp(&(
                right,
                lots[*b].ts,
                lots[*b].source_id.as_str(),
            ))
        }
        None => (lots[*a].ts, lots[*a].source_id.as_str())
            .cmp(&(lots[*b].ts, lots[*b].source_id.as_str())),
    });
    let mut left = need;
    let mut taken = Vec::new();
    for index in order {
        if left <= Decimal::ZERO {
            break;
        }
        let take = left.min(lots[index].shares);
        if take <= Decimal::ZERO {
            continue;
        }
        taken.push(take_from(&mut lots[index], take));
        left -= take;
    }
    taken
}

fn consume_one(lot: &mut Lot) -> Vec<Consumed> {
    let shares = lot.shares;
    vec![take_from(lot, shares)]
}

fn consume_wallet(lots: &mut [Lot], wallet: &str, need: Decimal, anchor: i64) -> Vec<Consumed> {
    let mut idxs: Vec<usize> = lots
        .iter()
        .enumerate()
        .filter(|(_, lot)| lot.wallet == wallet && lot.shares > Decimal::ZERO)
        .map(|(index, _)| index)
        .collect();
    idxs.sort_by(|a, b| {
        let da = (lots[*a].ts - anchor).abs();
        let db = (lots[*b].ts - anchor).abs();
        (da, lots[*a].ts, lots[*a].source_id.as_str()).cmp(&(
            db,
            lots[*b].ts,
            lots[*b].source_id.as_str(),
        ))
    });
    let mut left = need;
    let mut taken = Vec::new();
    for index in idxs {
        if left <= Decimal::ZERO {
            break;
        }
        let take = left.min(lots[index].shares);
        if take <= Decimal::ZERO {
            continue;
        }
        taken.push(take_from(&mut lots[index], take));
        left -= take;
    }
    taken
}

fn take_from(lot: &mut Lot, take: Decimal) -> Consumed {
    let fee = if take >= lot.shares {
        let fee = lot.fee.max(Decimal::ZERO);
        lot.shares = Decimal::ZERO;
        lot.fee = Decimal::ZERO;
        fee
    } else {
        let mut fee = (lot.fee * take / lot.shares).round_dp(8);
        if fee > lot.fee {
            fee = lot.fee;
        }
        lot.fee -= fee;
        lot.shares -= take;
        fee
    };
    Consumed {
        ts: lot.ts,
        shares: take,
        price: lot.price,
        fee,
        source_id: lot.source_id.clone(),
    }
}

fn order_from(
    pair: &ComplementaryPair,
    wallet: &str,
    pm: Vec<Consumed>,
    outcome: Vec<Consumed>,
) -> PlannedOrder {
    let (pm_shares, pm_price, pm_fee, pm_sources, pm_ts) = summarize(&pm);
    let (out_shares, out_price, out_fee, out_sources, out_ts) = summarize(&outcome);
    PlannedOrder {
        unified_index: pair.unified_index,
        market_title: pair.market_title.clone(),
        pm_wallet: wallet.to_string(),
        pm_token: pair.pm_token.clone(),
        pm_label: pair.pm_label.clone(),
        out_token: pair.out_token.clone(),
        out_label: pair.out_label.clone(),
        condition_id: pair.condition_id.clone(),
        option_id: pair.option_id.clone(),
        pm_shares,
        pm_price,
        pm_fee,
        out_shares,
        out_price,
        out_fee,
        submitted_at: pm_ts.max(out_ts),
        pm_sources,
        out_sources,
    }
}

fn summarize(parts: &[Consumed]) -> (Decimal, Decimal, Decimal, Vec<String>, i64) {
    let shares: Decimal = parts.iter().map(|part| part.shares).sum();
    let notional: Decimal = parts.iter().map(|part| part.price * part.shares).sum();
    let price = if shares > Decimal::ZERO {
        (notional / shares).round_dp(8)
    } else {
        Decimal::ZERO
    };
    let fee: Decimal = parts.iter().map(|part| part.fee).sum();
    let mut sources: Vec<String> = parts.iter().map(|part| part.source_id.clone()).collect();
    sources.sort();
    sources.dedup();
    let ts = parts.iter().map(|part| part.ts).max().unwrap_or(0);
    (shares.round_dp(8), price, fee.round_dp(8), sources, ts)
}

#[cfg(test)]
#[path = "../tests/unit/backfill.rs"]
mod tests;
