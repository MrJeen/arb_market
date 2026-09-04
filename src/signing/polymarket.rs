use super::{
    domain_separator, domain_separator_no_contract, encode_address, encode_bytes32, encode_u256,
    encode_u8, keccak, parse_address, typed_data_digest, CLOB_AUTH_MESSAGE, CLOB_AUTH_TYPE,
    NEG_RISK_EXCHANGE, ORDER_TYPE, POLYMARKET_CHAIN_ID, STANDARD_EXCHANGE,
};
use alloy_primitives::{Address, B256, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use anyhow::Context;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct SignedOrder {
    pub builder: B256,
    pub expiration: u64,
    pub maker: Address,
    pub maker_amount: u128,
    pub metadata: B256,
    pub order_type: String,
    pub salt: u64,
    pub side: String,
    pub signature: String,
    pub signature_type: u8,
    pub signer: Address,
    pub taker_amount: u128,
    pub timestamp: u64,
    pub token_id: String,
    pub post_only: bool,
}

pub fn order_digest(order: &SignedOrder, neg_risk: bool) -> anyhow::Result<B256> {
    let exchange = if neg_risk {
        NEG_RISK_EXCHANGE
    } else {
        STANDARD_EXCHANGE
    };
    let verifying = parse_address(exchange)?;
    let domain = domain_separator(
        "Polymarket CTF Exchange",
        "2",
        POLYMARKET_CHAIN_ID,
        verifying,
    );
    Ok(typed_data_digest(domain, order_struct_hash(order)?))
}

pub fn order_hash_hex(order: &SignedOrder, neg_risk: bool) -> anyhow::Result<String> {
    Ok(format!("{:#x}", order_digest(order, neg_risk)?))
}

pub fn order_struct_hash(order: &SignedOrder) -> anyhow::Result<B256> {
    let side = if order.side.eq_ignore_ascii_case("BUY") {
        0u8
    } else {
        1u8
    };
    let token: U256 = order.token_id.parse().context("token_id")?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(keccak(ORDER_TYPE.as_bytes()).as_slice());
    encoded.extend_from_slice(&encode_u256(U256::from(order.salt)));
    encoded.extend_from_slice(&encode_address(order.maker));
    encoded.extend_from_slice(&encode_address(order.signer));
    encoded.extend_from_slice(&encode_u256(token));
    encoded.extend_from_slice(&encode_u256(U256::from(order.maker_amount)));
    encoded.extend_from_slice(&encode_u256(U256::from(order.taker_amount)));
    encoded.extend_from_slice(&encode_u8(side));
    encoded.extend_from_slice(&encode_u8(order.signature_type));
    encoded.extend_from_slice(&encode_u256(U256::from(order.timestamp)));
    encoded.extend_from_slice(&encode_bytes32(order.metadata));
    encoded.extend_from_slice(&encode_bytes32(order.builder));
    Ok(keccak(&encoded))
}

pub fn sign_order(
    signer: &PrivateKeySigner,
    mut order: SignedOrder,
    neg_risk: bool,
) -> anyhow::Result<SignedOrder> {
    let digest = order_digest(&order, neg_risk)?;
    let sig = signer.sign_hash_sync(&digest)?;
    let mut signature = format!("0x{}", hex::encode(sig.as_bytes()));
    if order.signature_type == 3 {
        signature.push_str(&poly1271_suffix(&order, neg_risk)?);
    }
    order.signature = signature;
    Ok(order)
}

fn poly1271_suffix(order: &SignedOrder, neg_risk: bool) -> anyhow::Result<String> {
    let exchange = parse_address(if neg_risk {
        NEG_RISK_EXCHANGE
    } else {
        STANDARD_EXCHANGE
    })?;
    let domain_sep = domain_separator(
        "Polymarket CTF Exchange",
        "2",
        POLYMARKET_CHAIN_ID,
        exchange,
    );
    let contents = order_struct_hash(order)?;
    let mut suffix = String::new();
    suffix.push_str(&hex::encode(domain_sep.as_slice()));
    suffix.push_str(&hex::encode(contents.as_slice()));
    suffix.push_str(&hex::encode(ORDER_TYPE.as_bytes()));
    suffix.push_str(&format!("{:04x}", ORDER_TYPE.len()));
    Ok(suffix)
}

pub fn sign_clob_auth(
    signer: &PrivateKeySigner,
    timestamp: u64,
    nonce: u64,
) -> anyhow::Result<String> {
    let domain = domain_separator_no_contract("ClobAuthDomain", "1", POLYMARKET_CHAIN_ID);
    let mut encoded = Vec::new();
    encoded.extend_from_slice(keccak(CLOB_AUTH_TYPE.as_bytes()).as_slice());
    encoded.extend_from_slice(&encode_address(signer.address()));
    encoded.extend_from_slice(keccak(timestamp.to_string().as_bytes()).as_slice());
    encoded.extend_from_slice(&encode_u256(U256::from(nonce)));
    encoded.extend_from_slice(keccak(CLOB_AUTH_MESSAGE.as_bytes()).as_slice());
    let digest = typed_data_digest(domain, keccak(&encoded));
    let sig = signer.sign_hash_sync(&digest)?;
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

pub fn l2_hmac_signature(
    secret_b64: &str,
    timestamp: u64,
    method: &str,
    path: &str,
    body: &[u8],
) -> anyhow::Result<String> {
    use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
    use base64::Engine;
    let padded = format!(
        "{}{}",
        secret_b64,
        "=".repeat((4 - secret_b64.len() % 4) % 4)
    );
    let key = URL_SAFE
        .decode(&padded)
        .or_else(|_| URL_SAFE_NO_PAD.decode(secret_b64))
        .or_else(|_| STANDARD.decode(&padded))
        .context("api secret")?;
    let mut mac = HmacSha256::new_from_slice(&key).context("hmac key")?;
    mac.update(timestamp.to_string().as_bytes());
    mac.update(method.to_ascii_uppercase().as_bytes());
    mac.update(path.as_bytes());
    mac.update(body);
    Ok(URL_SAFE.encode(mac.finalize().into_bytes()))
}

pub fn clob_auth_headers(
    signer: &PrivateKeySigner,
    timestamp: u64,
    nonce: u64,
) -> anyhow::Result<Vec<(String, String)>> {
    let signature = sign_clob_auth(signer, timestamp, nonce)?;
    Ok(vec![
        ("POLY_ADDRESS".into(), format!("{:#x}", signer.address())),
        ("POLY_NONCE".into(), nonce.to_string()),
        ("POLY_SIGNATURE".into(), signature),
        ("POLY_TIMESTAMP".into(), timestamp.to_string()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_hash_is_deterministic() {
        let order = SignedOrder {
            builder: B256::ZERO,
            expiration: 0,
            maker: Address::ZERO,
            maker_amount: 1_000_000,
            metadata: B256::ZERO,
            order_type: "FAK".into(),
            salt: 1,
            side: "BUY".into(),
            signature: "0x".into(),
            signature_type: 2,
            signer: Address::ZERO,
            taker_amount: 2_000_000,
            timestamp: 1,
            token_id: "1".into(),
            post_only: false,
        };
        let a = order_hash_hex(&order, true).unwrap();
        let b = order_hash_hex(&order, true).unwrap();
        let c = order_hash_hex(&order, false).unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a.starts_with("0x"));
        assert_eq!(a.len(), 66);
    }
}
