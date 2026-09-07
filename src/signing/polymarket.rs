use super::{
    domain_separator, domain_separator_no_contract, encode_address, encode_bytes32, encode_u256,
    encode_u8, keccak, parse_address, typed_data_digest, CLOB_AUTH_MESSAGE, CLOB_AUTH_TYPE,
    DEPOSIT_WALLET_NAME, DEPOSIT_WALLET_VERSION, NEG_RISK_EXCHANGE, ORDER_TYPE, POLYMARKET_CHAIN_ID,
    STANDARD_EXCHANGE, TYPED_DATA_SIGN_TYPE,
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
    if order.signature_type == 3 {
        let digest = typed_data_sign_digest(&order, neg_risk)?;
        let sig = signer.sign_hash_sync(&digest)?;
        let wrapped = wrap_deposit_wallet_signature(&order, neg_risk, &sig.as_bytes())?;
        order.signature = format!("0x{}", hex::encode(wrapped));
        return Ok(order);
    }
    let digest = order_digest(&order, neg_risk)?;
    let sig = signer.sign_hash_sync(&digest)?;
    order.signature = format!("0x{}", hex::encode(sig.as_bytes()));
    Ok(order)
}

fn exchange_domain(neg_risk: bool) -> anyhow::Result<B256> {
    let exchange = parse_address(if neg_risk {
        NEG_RISK_EXCHANGE
    } else {
        STANDARD_EXCHANGE
    })?;
    Ok(domain_separator(
        "Polymarket CTF Exchange",
        "2",
        POLYMARKET_CHAIN_ID,
        exchange,
    ))
}

fn typed_data_sign_typehash() -> B256 {
    let mut encoded = String::with_capacity(TYPED_DATA_SIGN_TYPE.len() + ORDER_TYPE.len());
    encoded.push_str(TYPED_DATA_SIGN_TYPE);
    encoded.push_str(ORDER_TYPE);
    keccak(encoded.as_bytes())
}

fn typed_data_sign_struct_hash(order: &SignedOrder) -> anyhow::Result<B256> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(typed_data_sign_typehash().as_slice());
    encoded.extend_from_slice(order_struct_hash(order)?.as_slice());
    encoded.extend_from_slice(keccak(DEPOSIT_WALLET_NAME.as_bytes()).as_slice());
    encoded.extend_from_slice(keccak(DEPOSIT_WALLET_VERSION.as_bytes()).as_slice());
    encoded.extend_from_slice(&encode_u256(U256::from(POLYMARKET_CHAIN_ID)));
    encoded.extend_from_slice(&encode_address(order.signer));
    encoded.extend_from_slice(&encode_bytes32(B256::ZERO));
    Ok(keccak(&encoded))
}

fn typed_data_sign_digest(order: &SignedOrder, neg_risk: bool) -> anyhow::Result<B256> {
    Ok(typed_data_digest(
        exchange_domain(neg_risk)?,
        typed_data_sign_struct_hash(order)?,
    ))
}

fn wrap_deposit_wallet_signature(
    order: &SignedOrder,
    neg_risk: bool,
    inner_sig: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(inner_sig.len() + 64 + ORDER_TYPE.len() + 2);
    out.extend_from_slice(inner_sig);
    out.extend_from_slice(exchange_domain(neg_risk)?.as_slice());
    out.extend_from_slice(order_struct_hash(order)?.as_slice());
    out.extend_from_slice(ORDER_TYPE.as_bytes());
    out.extend_from_slice(&(ORDER_TYPE.len() as u16).to_be_bytes());
    Ok(out)
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

    fn sample_order(signature_type: u8, maker: Address) -> SignedOrder {
        SignedOrder {
            builder: B256::ZERO,
            expiration: 0,
            maker,
            maker_amount: 1_320_000,
            metadata: B256::ZERO,
            order_type: "FAK".into(),
            salt: 1,
            side: "BUY".into(),
            signature: "0x".into(),
            signature_type,
            signer: maker,
            taker_amount: 30_000_000,
            timestamp: 1,
            token_id: "1".into(),
            post_only: false,
        }
    }

    fn test_signer() -> PrivateKeySigner {
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
            .parse()
            .unwrap()
    }

    #[test]
    fn type2_signature_is_plain_ecdsa() {
        let signer = test_signer();
        let signed = sign_order(&signer, sample_order(2, signer.address()), false).unwrap();
        assert_eq!(signed.signature.len(), 132);
    }

    #[test]
    fn type3_uses_sdk_erc7739_wrap() {
        let signer = test_signer();
        let funder: Address = "0x08a6702b91efea0141916e602e5d07e78db8f6fb"
            .parse()
            .unwrap();
        let order = sample_order(3, funder);
        assert_ne!(
            typed_data_sign_digest(&order, false).unwrap(),
            order_digest(&order, false).unwrap()
        );
        let signed = sign_order(&signer, order.clone(), false).unwrap();
        let raw = hex::decode(signed.signature.trim_start_matches("0x")).unwrap();
        let type_len = (ORDER_TYPE.len() as u16).to_be_bytes();
        assert_eq!(raw.len(), 65 + 32 + 32 + ORDER_TYPE.len() + 2);
        assert_eq!(&raw[129..129 + ORDER_TYPE.len()], ORDER_TYPE.as_bytes());
        assert_eq!(&raw[raw.len() - 2..], &type_len);
        assert_eq!(&raw[65..97], exchange_domain(false).unwrap().as_slice());
        assert_eq!(&raw[97..129], order_struct_hash(&order).unwrap().as_slice());
    }

    #[test]
    fn typed_data_sign_binds_order_signer() {
        let funder: Address = "0x08a6702b91efea0141916e602e5d07e78db8f6fb"
            .parse()
            .unwrap();
        let mut order = sample_order(3, funder);
        let with_maker = typed_data_sign_struct_hash(&order).unwrap();
        order.signer = Address::from([0x11; 20]);
        assert_ne!(typed_data_sign_struct_hash(&order).unwrap(), with_maker);
    }
}
