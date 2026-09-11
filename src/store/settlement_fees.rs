//! Outcome 结算费用独立账本。锁序：全局写锁 -> 父单 id 升序 -> 腿。
use super::*;
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use std::collections::{BTreeMap, HashMap};

const WRITE_LOCK: i64 = 0x6d61726b_66656501;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupKey {
    pub network: String,
    pub wallet: String,
    pub token: String,
}
impl GroupKey {
    fn validate(&self) -> Result<()> {
        if self.network.is_empty()
            || self.wallet.is_empty()
            || self.wallet != self.wallet.to_ascii_lowercase()
            || self.token.is_empty()
        {
            return Err(Error::msg(
                "invalid settlement fee group key (wallet must be lowercase)",
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct SnapshotFill {
    pub trade_id: String,
    pub third_order_id: String,
    pub shares: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
    pub raw: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotLeg {
    pub leg: LegRow,
    pub actual_shares: Option<Decimal>,
    pub actual_price: Option<Decimal>,
    pub actual_fee: Option<Decimal>,
    pub fills: Vec<SnapshotFill>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSnapshot {
    pub key: GroupKey,
    pub legs: Vec<SnapshotLeg>,
    pub allocations: Vec<Allocation>,
    pub sealed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, sqlx::FromRow)]
pub struct FeeEvent {
    pub tid: String,
    pub quantity: Decimal,
    pub payout: Decimal,
    pub fee: Decimal,
    pub fee_token: String,
    pub evidence: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Allocation {
    pub order_id: i64,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub payout: Decimal,
    pub status: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct HistoricalSettlementSnapshot {
    pub order_id: i64,
    pub actual_cost: Decimal,
    pub actual_rev: Decimal,
    pub actual_profit: Decimal,
    pub settled_at: chrono::DateTime<chrono::Utc>,
}

pub(super) async fn lock_writes(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(WRITE_LOCK)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
pub(super) async fn reject_sealed_legs(
    tx: &mut Transaction<'_, Postgres>,
    legs: &[NewLeg<'_>],
) -> Result<()> {
    for leg in legs.iter().filter(|l| l.platform == OUTCOME) {
        let wallet = leg
            .wallet
            .ok_or_else(|| Error::msg("Outcome leg missing wallet"))?;
        let sealed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM outcome_settlement_fee_groups WHERE wallet=lower($1) AND token=$2 AND sealed)")
            .bind(wallet).bind(leg.token_id).fetch_one(&mut **tx).await?;
        if sealed {
            return Err(Error::msg("Outcome settlement fee group already sealed"));
        }
    }
    Ok(())
}
pub(super) async fn reject_sealed_leg(
    tx: &mut Transaction<'_, Postgres>,
    leg_id: i64,
) -> Result<()> {
    let sealed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM legs l JOIN outcome_settlement_fee_groups g ON g.wallet=lower(l.wallet_address) AND g.token=l.token_id WHERE l.id=$1 AND l.platform=$2 AND g.sealed)")
        .bind(leg_id).bind(OUTCOME).fetch_one(&mut **tx).await?;
    if sealed {
        return Err(Error::msg("cannot reconcile sealed settlement fee group"));
    }
    Ok(())
}
async fn ensure_group(tx: &mut Transaction<'_, Postgres>, key: &GroupKey) -> Result<i64> {
    key.validate()?;
    // 旧 legs 没有 network 列；同一钱包/token 不允许映射到两个网络重复收费。
    let conflict:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM outcome_settlement_fee_groups WHERE wallet=$1 AND token=$2 AND network<>$3)")
        .bind(&key.wallet).bind(&key.token).bind(&key.network).fetch_one(&mut **tx).await?;
    if conflict {
        return Err(Error::msg(
            "wallet/token already assigned to another settlement network",
        ));
    }
    Ok(sqlx::query_scalar("INSERT INTO outcome_settlement_fee_groups(network,wallet,token) VALUES($1,$2,$3) ON CONFLICT(network,wallet,token) DO UPDATE SET network=EXCLUDED.network RETURNING id")
       .bind(&key.network).bind(&key.wallet).bind(&key.token).fetch_one(&mut **tx).await?)
}
async fn snapshot(
    tx: &mut Transaction<'_, Postgres>,
    key: &GroupKey,
    tables: bool,
) -> Result<GroupSnapshot> {
    key.validate()?;
    let legs: Vec<LegRow> = sqlx::query_as("SELECT * FROM legs WHERE platform=$1 AND lower(wallet_address)=$2 AND token_id=$3 ORDER BY order_id,id")
        .bind(OUTCOME).bind(&key.wallet).bind(&key.token).fetch_all(&mut **tx).await?;
    let mut result = GroupSnapshot {
        key: key.clone(),
        legs: vec![],
        allocations: vec![],
        sealed: false,
    };
    for leg in legs {
        let (actual_shares, actual_price, actual_fee): (
            Option<Decimal>,
            Option<Decimal>,
            Option<Decimal>,
        ) = sqlx::query_as("SELECT actual_shares,actual_price,actual_fee FROM legs WHERE id=$1")
            .bind(leg.id)
            .fetch_one(&mut **tx)
            .await?;
        let fills = sqlx::query_as("SELECT trade_id,third_order_id,shares,price,fee,raw FROM fills WHERE leg_id=$1 ORDER BY trade_id,third_order_id")
            .bind(leg.id).fetch_all(&mut **tx).await?;
        result.legs.push(SnapshotLeg {
            leg,
            actual_shares,
            actual_price,
            actual_fee,
            fills,
        });
    }
    if tables {
        result.sealed = sqlx::query_scalar("SELECT sealed FROM outcome_settlement_fee_groups WHERE network=$1 AND wallet=$2 AND token=$3")
            .bind(&key.network).bind(&key.wallet).bind(&key.token).fetch_optional(&mut **tx).await?.unwrap_or(false);
        result.allocations = sqlx::query_as("SELECT a.order_id,a.quantity,a.fee,g.payout,a.status FROM outcome_settlement_fee_allocations a JOIN outcome_settlement_fee_groups g ON g.id=a.group_id WHERE g.network=$1 AND g.wallet=$2 AND g.token=$3 ORDER BY a.order_id")
            .bind(&key.network).bind(&key.wallet).bind(&key.token).fetch_all(&mut **tx).await?;
    }
    Ok(result)
}
fn quantities(s: &GroupSnapshot) -> Result<BTreeMap<i64, Decimal>> {
    let mut result = BTreeMap::new();
    for row in &s.legs {
        if !matches!(
            row.leg.status.as_str(),
            "matched" | "completed" | "failed" | "cancelled"
        ) {
            return Err(Error::msg("unresolved Outcome leg in settlement group"));
        }
        let qty = match row.actual_shares {
            Some(q) if q >= Decimal::ZERO => q,

            _ => return Err(Error::msg("missing or negative actual Outcome shares")),
        };
        let filled = row.fills.iter().try_fold(Decimal::ZERO, |sum, f| {
            if f.trade_id.is_empty()
                || f.third_order_id.is_empty()
                || f.shares <= Decimal::ZERO
                || f.price < Decimal::ZERO
            {
                return Err(Error::msg("invalid durable trade identity/quantity/price"));
            }
            sum.checked_add(f.shares)
                .ok_or_else(|| Error::msg("fill quantity overflow"))
        })?;
        if filled != qty {
            return Err(Error::msg("durable fills do not match leg actual shares"));
        }
        let delta = match row.leg.side.to_ascii_uppercase().as_str() {
            "BUY" => qty,
            "SELL" => -qty,
            _ => return Err(Error::msg("unknown Outcome side")),
        };
        let entry = result.entry(row.leg.order_id).or_insert(Decimal::ZERO);
        *entry = entry
            .checked_add(delta)
            .ok_or_else(|| Error::msg("quantity overflow"))?;
    }
    if result.values().any(|q| *q < Decimal::ZERO) {
        return Err(Error::msg("negative wallet/token order position"));
    }
    Ok(result)
}
async fn lock_parents_and_check(tx: &mut Transaction<'_, Postgres>, key: &GroupKey) -> Result<()> {
    let rows: Vec<(i64, bool)> = sqlx::query_as("SELECT o.id,(o.lifecycle_action IS NOT NULL OR o.lifecycle_claim_id IS NOT NULL OR o.lifecycle_claimed_at IS NOT NULL OR EXISTS(SELECT 1 FROM legs x WHERE x.order_id=o.id AND x.status NOT IN ('matched','completed','failed','cancelled'))) FROM arb_orders o WHERE EXISTS(SELECT 1 FROM legs l WHERE l.order_id=o.id AND l.platform=$1 AND lower(l.wallet_address)=$2 AND l.token_id=$3) ORDER BY o.id FOR UPDATE OF o")
        .bind(OUTCOME).bind(&key.wallet).bind(&key.token).fetch_all(&mut **tx).await?;
    if rows.iter().any(|r| r.1) {
        return Err(Error::msg(
            "settlement group has unresolved legs or lifecycle claims",
        ));
    }
    Ok(())
}
// 用整数最小费用单位整除，避免 Decimal 中间乘除隐式降低精度。
fn exact_prorata(fee: Decimal, qty: Decimal, total: Decimal, scale: u32) -> Result<Decimal> {
    let shares_scale = qty.scale().max(total.scale());
    let q = qty
        .mantissa()
        .checked_mul(10_i128.pow(shares_scale - qty.scale()))
        .ok_or_else(|| Error::msg("allocation quantity overflow"))?;
    let t = total
        .mantissa()
        .checked_mul(10_i128.pow(shares_scale - total.scale()))
        .ok_or_else(|| Error::msg("allocation quantity overflow"))?;
    let units = fee
        .mantissa()
        .checked_mul(q)
        .ok_or_else(|| Error::msg("allocation exact arithmetic overflow"))?
        / t;
    Decimal::try_from_i128_with_scale(units, scale).map_err(|e| Error::msg(e.to_string()))
}
pub fn preview_allocations(
    s: &GroupSnapshot,
    payout: Decimal,
    events: &[FeeEvent],
) -> Result<Vec<Allocation>> {
    let quantities = quantities(s)?;
    let total = quantities
        .values()
        .try_fold(Decimal::ZERO, |a, b| a.checked_add(*b))
        .ok_or_else(|| Error::msg("quantity overflow"))?;
    if payout < Decimal::ZERO || payout > Decimal::ONE {
        return Err(Error::msg("invalid payout"));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut event_qty = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    let mut fee_token = None;
    for e in events {
        if e.tid.is_empty()
            || !seen.insert(&e.tid)
            || e.quantity <= Decimal::ZERO
            || e.payout != payout
            || e.fee < Decimal::ZERO
            || e.fee_token != "USDC"
        {
            return Err(Error::msg(
                "invalid or duplicate fee event / payout mismatch",
            ));
        }
        if fee_token.is_some_and(|t| t != &e.fee_token) {
            return Err(Error::msg("mixed settlement fee currencies"));
        }
        fee_token = Some(&e.fee_token);
        event_qty = event_qty
            .checked_add(e.quantity)
            .ok_or_else(|| Error::msg("quantity overflow"))?;
        fee = fee
            .checked_add(e.fee)
            .ok_or_else(|| Error::msg("fee overflow"))?;
    }
    if event_qty != total || (total > Decimal::ZERO && events.is_empty()) {
        return Err(Error::msg(
            "settlement quantity does not exactly match local wallet/token shares",
        ));
    }
    // 以输入费用精度分配整数最小单位；余数全部归最后一个正仓父单（id 固定）。
    let scale = fee.scale();
    let last = quantities
        .iter()
        .filter(|(_, q)| **q > Decimal::ZERO)
        .map(|(id, _)| *id)
        .next_back();
    let mut remaining = fee;
    let mut result = vec![];
    for (id, qty) in quantities {
        let share = if qty.is_zero() {
            Decimal::ZERO
        } else if Some(id) == last {
            remaining
        } else {
            exact_prorata(fee, qty, total, scale)?
        };
        remaining = remaining
            .checked_sub(share)
            .ok_or_else(|| Error::msg("allocation overflow"))?;
        result.push(Allocation {
            order_id: id,
            quantity: qty,
            fee: share,
            payout,
            status: if qty.is_zero() {
                "not_applicable"
            } else {
                "verified"
            }
            .into(),
        });
    }
    if !remaining.is_zero() {
        return Err(Error::msg("allocation conservation failed"));
    }
    Ok(result)
}
impl Store {
    pub async fn settlement_collection_keys(
        &self,
        order_id: i64,
        network: &str,
    ) -> Result<Vec<GroupKey>> {
        let rows: Vec<(Option<String>, String)> = sqlx::query_as(
            "SELECT DISTINCT wallet_address,token_id FROM legs WHERE order_id=$1 AND platform=$2",
        )
        .bind(order_id)
        .bind(OUTCOME)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(wallet, token)| {
                let key = GroupKey {
                    network: network.into(),
                    wallet: wallet
                        .ok_or_else(|| Error::msg("missing Outcome wallet"))?
                        .to_ascii_lowercase(),
                    token,
                };
                key.validate()?;
                Ok(key)
            })
            .collect()
    }

    pub async fn settlement_fee_keys_for_order(
        &self,
        order_id: i64,
        network: &str,
    ) -> Result<Vec<GroupKey>> {
        let rows: Vec<(Option<String>,String,String,String,Option<Decimal>,bool)> = sqlx::query_as("SELECT wallet_address,token_id,side,status,actual_shares,EXISTS(SELECT 1 FROM fills f WHERE f.leg_id=l.id) FROM legs l WHERE order_id=$1 AND platform=$2 ORDER BY id")
            .bind(order_id).bind(OUTCOME).fetch_all(&self.pool).await?;
        let mut quantities = BTreeMap::new();
        for (wallet, token, side, status, qty, _has_fills) in rows {
            if !matches!(
                status.as_str(),
                "matched" | "completed" | "failed" | "cancelled"
            ) {
                return Err(Error::msg("unresolved Outcome leg"));
            }
            let qty = match qty {
                Some(q) if q >= Decimal::ZERO => q,
                _ => return Err(Error::msg("unknown Outcome quantity")),
            };
            if qty.is_zero() {
                continue;
            }
            let wallet = wallet
                .filter(|w| !w.is_empty())
                .ok_or_else(|| Error::msg("missing Outcome wallet"))?
                .to_ascii_lowercase();
            let q = quantities.entry((wallet, token)).or_insert(Decimal::ZERO);
            *q += match side.to_ascii_uppercase().as_str() {
                "BUY" => qty,
                "SELL" => -qty,
                _ => return Err(Error::msg("unknown side")),
            };
        }
        quantities
            .into_iter()
            .filter(|(_, q)| !q.is_zero())
            .map(|((wallet, token), q)| {
                if q < Decimal::ZERO {
                    return Err(Error::msg("negative wallet/token position"));
                }
                let key = GroupKey {
                    network: network.into(),
                    wallet,
                    token,
                };
                key.validate()?;
                Ok(key)
            })
            .collect()
    }
    pub async fn settlement_fee_group_snapshot(&self, key: &GroupKey) -> Result<GroupSnapshot> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        let tables: bool =
            sqlx::query_scalar("SELECT to_regclass('outcome_settlement_fee_groups') IS NOT NULL")
                .fetch_one(&mut *tx)
                .await?;
        let s = snapshot(&mut tx, key, tables).await?;
        tx.commit().await?;
        Ok(s)
    }
    pub async fn prepare_settlement_fee_group(&self, key: &GroupKey) -> Result<GroupSnapshot> {
        let mut tx = self.pool.begin().await?;
        lock_writes(&mut tx).await?;
        ensure_group(&mut tx, key).await?;
        lock_parents_and_check(&mut tx, key).await?;
        let s = snapshot(&mut tx, key, true).await?;
        quantities(&s)?;
        tx.commit().await?;
        Ok(s)
    }
    pub async fn commit_settlement_fee_group(
        &self,
        expected: &GroupSnapshot,
        payout: Decimal,
        events: &[FeeEvent],
        evidence: &Value,
    ) -> Result<Vec<Allocation>> {
        let mut tx = self.pool.begin().await?;
        lock_writes(&mut tx).await?;
        let id = ensure_group(&mut tx, &expected.key).await?;
        lock_parents_and_check(&mut tx, &expected.key).await?;
        let current = snapshot(&mut tx, &expected.key, true).await?;
        if serde_json::to_value(&current.legs)? != serde_json::to_value(&expected.legs)? {
            return Err(Error::msg(
                "settlement group snapshot changed; rescan required",
            ));
        }
        if current.sealed {
            if current.allocations.iter().any(|a| a.payout != payout) {
                return Err(Error::msg("sealed group payout mismatch"));
            }
            let stored: Vec<FeeEvent> = sqlx::query_as("SELECT tid,quantity,payout,fee,fee_token,evidence FROM outcome_settlement_fee_events WHERE group_id=$1 ORDER BY tid")
                .bind(id).fetch_all(&mut *tx).await?;
            let mut supplied = events.to_vec();
            supplied.sort_by(|a, b| a.tid.cmp(&b.tid));
            if stored != supplied {
                return Err(Error::msg("sealed settlement event conflict"));
            }
            tx.commit().await?;
            return Ok(current.allocations);
        }
        let rows = preview_allocations(&current, payout, events)?;
        for e in events {
            sqlx::query("INSERT INTO outcome_settlement_fee_events(group_id,network,wallet,tid,quantity,payout,fee,fee_token,evidence) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
                .bind(id).bind(&expected.key.network).bind(&expected.key.wallet).bind(&e.tid).bind(e.quantity).bind(e.payout).bind(e.fee).bind(&e.fee_token).bind(&e.evidence).execute(&mut *tx).await?;
        }
        for a in &rows {
            sqlx::query("INSERT INTO outcome_settlement_fee_allocations(group_id,order_id,quantity,fee,status) VALUES($1,$2,$3,$4,$5)")
                .bind(id).bind(a.order_id).bind(a.quantity).bind(a.fee).bind(&a.status).execute(&mut *tx).await?;
        }
        let qty: Decimal = rows.iter().map(|r| r.quantity).sum();
        let fee: Decimal = rows.iter().map(|r| r.fee).sum();
        sqlx::query("UPDATE outcome_settlement_fee_groups SET sealed=true,snapshot=$2,evidence=$3,payout=$4,total_quantity=$5,total_fee=$6,updated_at=NOW() WHERE id=$1")
            .bind(id).bind(serde_json::to_value(&current)?).bind(evidence).bind(payout).bind(qty).bind(fee).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(rows)
    }
    pub async fn save_settlement_fee_progress(
        &self,
        key: &GroupKey,
        expected: Option<&Value>,
        progress: &Value,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_writes(&mut tx).await?;
        let id = ensure_group(&mut tx, key).await?;
        let changed = sqlx::query("UPDATE outcome_settlement_fee_groups SET scan_progress=$2,last_error=NULL,updated_at=NOW() WHERE id=$1 AND NOT sealed AND scan_progress IS NOT DISTINCT FROM $3").bind(id).bind(progress).bind(expected).execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            return Err(Error::msg(
                "settlement progress CAS conflict; discard stale page",
            ));
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn settlement_fee_blocked(
        &self,
        key: &GroupKey,
        expected: Option<&Value>,
        reason: &str,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_writes(&mut tx).await?;
        let id = ensure_group(&mut tx, key).await?;
        let previous: Option<String> =
            sqlx::query_scalar("SELECT last_error FROM outcome_settlement_fee_groups WHERE id=$1")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        let changed = sqlx::query("UPDATE outcome_settlement_fee_groups SET last_error=$2,updated_at=NOW() WHERE id=$1 AND NOT sealed AND scan_progress IS NOT DISTINCT FROM $3")
            .bind(id).bind(reason).bind(expected).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        if changed == 1 && previous.as_deref() != Some(reason) {
            tracing::warn!(service="outcome", token_id=%key.token, group_id=id, reason, "settlement verification blocked");
        }
        Ok(())
    }

    pub async fn settlement_fee_progress(&self, key: &GroupKey) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar::<_,Option<Value>>("SELECT scan_progress FROM outcome_settlement_fee_groups WHERE network=$1 AND wallet=$2 AND token=$3")
            .bind(&key.network).bind(&key.wallet).bind(&key.token).fetch_optional(&self.pool).await?.flatten())
    }
    pub async fn historical_settlement_snapshot(
        &self,
        order_id: i64,
    ) -> Result<Option<HistoricalSettlementSnapshot>> {
        Ok(sqlx::query_as("SELECT id AS order_id,actual_cost,actual_rev,actual_profit,settled_at FROM arb_orders WHERE id=$1 AND settled_at IS NOT NULL AND position_status='settled' AND COALESCE(settlement_result->>'settlement_fee_status','unknown')='unknown' AND COALESCE(actuals_projection->>'status','unknown') NOT IN ('estimated','final') AND COALESCE(settlement_result->>'profit_basis','gross_payout_less_trade_costs') IN ('gross','gross_payout_less_trade_costs')").bind(order_id).fetch_optional(&self.pool).await?)
    }
    pub async fn apply_historical_settlement_fees(
        &self,
        expected: &HistoricalSettlementSnapshot,
        payouts: &HashMap<(String, String), Decimal>,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        lock_writes(&mut tx).await?;
        let exists:Option<i64>=sqlx::query_scalar("SELECT id FROM arb_orders WHERE id=$1 AND actual_cost=$2 AND actual_rev=$3 AND actual_profit=$4 AND settled_at=$5 AND position_status='settled' AND COALESCE(settlement_result->>'settlement_fee_status','unknown')='unknown' AND COALESCE(actuals_projection->>'status','unknown') NOT IN ('estimated','final') AND COALESCE(settlement_result->>'profit_basis','gross_payout_less_trade_costs') IN ('gross','gross_payout_less_trade_costs') AND lifecycle_action IS NULL AND lifecycle_claim_id IS NULL AND lifecycle_claimed_at IS NULL FOR UPDATE")
            .bind(expected.order_id).bind(expected.actual_cost).bind(expected.actual_rev).bind(expected.actual_profit).bind(expected.settled_at).fetch_optional(&mut *tx).await?;
        if exists.is_none() {
            return Ok(false);
        }
        let fee = consume_allocations(&mut tx, expected.order_id, payouts).await?;
        let rev = expected
            .actual_rev
            .checked_sub(fee)
            .ok_or_else(|| Error::msg("rev overflow"))?;
        let profit = expected
            .actual_profit
            .checked_sub(fee)
            .ok_or_else(|| Error::msg("profit overflow"))?;
        let has_position: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM legs WHERE order_id=$1 AND platform=$2 GROUP BY lower(wallet_address),token_id HAVING SUM(CASE WHEN upper(side)='SELL' THEN -actual_shares ELSE actual_shares END)>0)")
            .bind(expected.order_id).bind(OUTCOME).fetch_one(&mut *tx).await?;
        let status = if has_position {
            "verified"
        } else {
            "not_applicable"
        };
        let evidence = json!({"settlement_fee_status":status,"profit_basis":"net_payout_less_trade_costs","verified_at":chrono::Utc::now(),"outcome_settlement_fee":{"status":status,"fee":fee,"accounting_basis":"net","gross_rev":expected.actual_rev,"net_rev":rev}});
        sqlx::query(
            "UPDATE arb_orders SET actual_rev=$2,actual_profit=$3,settlement_result=COALESCE(settlement_result,'{}'::jsonb)||$4::jsonb,updated_at=NOW() WHERE id=$1",
        )
        .bind(expected.order_id)
        .bind(rev)
        .bind(profit)
        .bind(evidence)
        .execute(&mut *tx)
        .await?;
        mark_applied(&mut tx,expected.order_id,&json!({"mode":"historical_cas","before":expected,"after":{"actual_cost":expected.actual_cost,"actual_rev":rev,"actual_profit":profit,"settled_at":expected.settled_at}})).await?;
        tx.commit().await?;
        Ok(true)
    }
}
pub(super) async fn consume_allocations(
    tx: &mut Transaction<'_, Postgres>,
    order_id: i64,
    payouts: &HashMap<(String, String), Decimal>,
) -> Result<Decimal> {
    let open:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM legs WHERE order_id=$1 AND status NOT IN ('matched','completed','failed','cancelled'))").bind(order_id).fetch_one(&mut **tx).await?;
    if open {
        return Err(Error::msg("unresolved legs prevent settlement"));
    }
    let rows:Vec<(Option<String>,String,String,Option<Decimal>,String,bool)>=sqlx::query_as("SELECT wallet_address,token_id,side,actual_shares,status,EXISTS(SELECT 1 FROM fills f WHERE f.leg_id=l.id) FROM legs l WHERE order_id=$1 AND platform=$2")
        .bind(order_id).bind(OUTCOME).fetch_all(&mut **tx).await?;
    let mut positions = BTreeMap::new();
    for (wallet, token, side, qty, _status, _has_fills) in rows {
        let qty = match qty {
            Some(q) if q >= Decimal::ZERO => q,
            _ => return Err(Error::msg("unknown Outcome quantity")),
        };
        if qty.is_zero() {
            continue;
        }
        let wallet = wallet
            .filter(|w| !w.is_empty())
            .ok_or_else(|| Error::msg("missing Outcome wallet"))?
            .to_ascii_lowercase();
        let q = positions.entry((wallet, token)).or_insert(Decimal::ZERO);
        *q += match side.to_ascii_uppercase().as_str() {
            "BUY" => qty,
            "SELL" => -qty,
            _ => return Err(Error::msg("unknown side")),
        };
    }
    let mut fee = Decimal::ZERO;
    for ((wallet, token), qty) in positions {
        if qty < Decimal::ZERO {
            return Err(Error::msg("negative wallet/token position"));
        }
        if qty.is_zero() {
            continue;
        }
        let rows:Vec<(Decimal,Decimal,Decimal,String,bool)>=sqlx::query_as("SELECT a.quantity,a.fee,g.payout,a.status,a.applied_at IS NOT NULL FROM outcome_settlement_fee_allocations a JOIN outcome_settlement_fee_groups g ON g.id=a.group_id WHERE a.order_id=$1 AND g.wallet=$2 AND g.token=$3 AND g.sealed")
            .bind(order_id).bind(wallet).bind(&token).fetch_all(&mut **tx).await?;
        if rows.len() != 1 {
            return Err(Error::msg(
                "missing or ambiguous verified settlement fee allocation",
            ));
        }
        let (stored_qty, allocated, payout, status, applied) = &rows[0];
        if *stored_qty != qty
            || status != "verified"
            || payouts.get(&(OUTCOME.into(), token)) != Some(payout)
        {
            return Err(Error::msg("settlement allocation quantity/payout mismatch"));
        }
        if !applied {
            fee = fee
                .checked_add(*allocated)
                .ok_or_else(|| Error::msg("fee overflow"))?;
        }
    }
    Ok(fee)
}
pub(super) async fn mark_applied(
    tx: &mut Transaction<'_, Postgres>,
    order_id: i64,
    audit: &Value,
) -> Result<()> {
    sqlx::query("UPDATE outcome_settlement_fee_allocations SET applied_at=NOW(),application_audit=$2 WHERE order_id=$1 AND applied_at IS NULL")
        .bind(order_id).bind(audit).execute(&mut **tx).await?;
    Ok(())
}
