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

#[test]
fn only_definitive_http_client_errors_prove_rejection() {
    for status in [400, 401, 403, 404, 409, 422] {
        assert!(http_status_proves_reject(status), "{status}");
    }
    for status in [408, 425, 429, 500, 502, 503, 504] {
        assert!(!http_status_proves_reject(status), "{status}");
    }
}

#[test]
fn submit_http_error_response_keeps_status_and_json_body() {
    let stored = SubmissionResponse::http(400, Ok(r#"{"error":"Invalid order payload"}"#.into()));
    assert_eq!(stored["http_status"], 400);
    assert_eq!(stored["body"]["error"], "Invalid order payload");
}
