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
    pub shares: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
    pub fee_rate_bps: Option<Decimal>,
    pub raw: Value,
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
