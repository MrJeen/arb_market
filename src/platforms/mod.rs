pub mod outcome;
pub mod polymarket;

use crate::error::Result;
use rust_decimal::Decimal;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct PreparedOrder {
    pub order_hash: String,
    pub envelope: serde_json::Value,
    pub payload: serde_json::Value,
    pub funder: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MarketOrderRequest {
    pub token_id: String,
    pub shares: Decimal,
    pub cap_price: Decimal,
    pub side: OrderSide,
    pub neg_risk: Option<bool>,
    pub tick_size: Option<Decimal>,
    pub asset_id: Option<u64>,
    pub funder_address: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone)]
pub enum SubmitResult {
    Ack {
        order_id: String,
        order_hash: String,
        envelope: Value,
        making: Option<Decimal>,
        taking: Option<Decimal>,
        avg_px: Option<Decimal>,
    },
    NoMatch {
        order_hash: String,
        envelope: Value,
        message: String,
    },
    Unknown {
        order_id: Option<String>,
        order_hash: String,
        envelope: Value,
        message: String,
    },
}

#[derive(Debug, Clone)]
pub struct TradeFill {
    pub trade_id: String,
    pub order_id: Option<String>,
    pub order_ids: Vec<String>,
    pub coin: Option<String>,
    pub shares: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
    pub fee_rate_bps: Option<Decimal>,
    pub raw: Value,
}

impl TradeFill {
    pub fn matches(&self, order_id: Option<&str>, client_id: Option<&str>) -> bool {
        let ids = self.match_ids();
        if let Some(oid) = order_id {
            if ids.iter().any(|id| *id == oid) {
                return true;
            }
        }
        if let Some(cid) = client_id {
            if ids.iter().any(|id| *id == cid) {
                return true;
            }
        }
        false
    }

    fn match_ids(&self) -> Vec<&str> {
        let mut ids: Vec<&str> = self.order_ids.iter().map(String::as_str).collect();
        if let Some(id) = self.order_id.as_deref() {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        ids
    }
}

/// Polymarket FAK: BUY 股数 = taking，SELL 股数 = making。
pub fn pm_fak_fill(
    side: OrderSide,
    making: Option<Decimal>,
    taking: Option<Decimal>,
) -> Option<(Decimal, Decimal)> {
    let making = making.filter(|v| *v > Decimal::ZERO)?;
    let taking = taking.filter(|v| *v > Decimal::ZERO)?;
    match side {
        OrderSide::Buy => Some((taking, making / taking)),
        OrderSide::Sell => Some((making, taking / making)),
    }
}

/// Outcome IOC: `filled/totalSz` 为股数。
pub fn ioc_fill(shares: Option<Decimal>, price: Option<Decimal>) -> Option<(Decimal, Decimal)> {
    let shares = shares.filter(|v| *v > Decimal::ZERO)?;
    let price = price.filter(|v| *v > Decimal::ZERO)?;
    Some((shares, price))
}

#[derive(Debug, Clone)]
pub struct OrderPoll {
    pub found: bool,
    pub status: String,
    pub order_id: Option<String>,
    pub shares: Option<Decimal>,
    pub price: Option<Decimal>,
    pub fee: Option<Decimal>,
    pub raw: Value,
}

pub fn json_id(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

pub fn parse_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(s) if !s.is_empty() => s.parse().ok(),
        Value::Number(n) => n.to_string().parse().ok(),
        _ => None,
    }
}

pub fn require_positive(shares: Decimal, price: Decimal) -> Result<()> {
    if shares <= Decimal::ZERO || price <= Decimal::ZERO {
        return Err(crate::error::Error::msg(
            "shares and price must be positive",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn pm_fak_fill_buy_uses_taking_as_shares() {
        let (shares, price) = pm_fak_fill(OrderSide::Buy, Some(d("1.2")), Some(d("3"))).unwrap();
        assert_eq!(shares.to_string(), "3");
        assert_eq!(price.to_string(), "0.4");
    }

    #[test]
    fn pm_fak_fill_sell_uses_making_as_shares() {
        let (shares, price) = pm_fak_fill(OrderSide::Sell, Some(d("3")), Some(d("1.2"))).unwrap();
        assert_eq!(shares.to_string(), "3");
        assert_eq!(price.to_string(), "0.4");
        assert!(pm_fak_fill(OrderSide::Sell, Some(d("3")), None).is_none());
    }

    #[test]
    fn ioc_fill_requires_positive_shares_and_price() {
        assert_eq!(
            ioc_fill(Some(d("5")), Some(d("0.4")))
                .unwrap()
                .0
                .to_string(),
            "5"
        );
        assert!(ioc_fill(Some(d("0")), Some(d("0.4"))).is_none());
        assert!(ioc_fill(Some(d("5")), None).is_none());
    }
}
