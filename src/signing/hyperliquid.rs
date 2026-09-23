use super::{domain_separator, encode_bytes32, keccak, typed_data_digest, AGENT_TYPE};
use alloy_primitives::{Address, B256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use anyhow::Context;
use serde_json::{json, Value};

pub fn encode_l1_action(action: &Value) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    encode_value(action, &mut buf)?;
    Ok(buf)
}

pub fn action_hash(
    action: &Value,
    vault_address: Option<Address>,
    nonce: u64,
    expires_after: Option<u64>,
) -> anyhow::Result<B256> {
    // 官方顺序：msgpack(action) || nonce_be64 || vault_flag || [expires]
    // https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/utils/signing.py
    let mut data = encode_l1_action(action)?;
    data.extend_from_slice(&nonce.to_be_bytes());
    if let Some(vault) = vault_address {
        data.push(0x01);
        data.extend_from_slice(vault.as_slice());
    } else {
        data.push(0x00);
    }
    if let Some(expires) = expires_after {
        data.push(0x00);
        data.extend_from_slice(&expires.to_be_bytes());
    }
    Ok(keccak(&data))
}

pub fn sign_l1_action(
    signer: &PrivateKeySigner,
    action: &Value,
    nonce: u64,
    is_mainnet: bool,
) -> anyhow::Result<(String, String, u8)> {
    let hash = action_hash(action, None, nonce, None)?;
    let source = if is_mainnet { "a" } else { "b" };
    let domain = domain_separator("Exchange", "1", 1337, Address::ZERO);
    let mut encoded = Vec::new();
    encoded.extend_from_slice(keccak(AGENT_TYPE.as_bytes()).as_slice());
    encoded.extend_from_slice(keccak(source.as_bytes()).as_slice());
    encoded.extend_from_slice(&encode_bytes32(hash));
    let digest = typed_data_digest(domain, keccak(&encoded));
    let sig = signer.sign_hash_sync(&digest)?;
    let bytes = sig.as_bytes();
    let r = format!("0x{}", hex::encode(&bytes[0..32]));
    let s = format!("0x{}", hex::encode(&bytes[32..64]));
    let v = bytes[64];
    Ok((r, s, v))
}

pub fn order_action(
    asset: u64,
    is_buy: bool,
    price: &str,
    size: &str,
    cloid: Option<&str>,
    builder: Option<(&str, u32)>,
) -> Value {
    let mut order = serde_json::Map::new();
    order.insert("a".into(), json!(asset));
    order.insert("b".into(), json!(is_buy));
    order.insert("p".into(), json!(price));
    order.insert("s".into(), json!(size));
    order.insert("r".into(), json!(false));
    order.insert("t".into(), json!({"limit": {"tif": "Ioc"}}));
    if let Some(c) = cloid {
        order.insert("c".into(), json!(c));
    }
    let mut action = serde_json::Map::new();
    action.insert("type".into(), json!("order"));
    action.insert("orders".into(), json!([Value::Object(order)]));
    action.insert("grouping".into(), json!("na"));
    if let Some((addr, fee)) = builder {
        action.insert("builder".into(), json!({"b": addr, "f": fee}));
    }
    Value::Object(action)
}

fn encode_value(value: &Value, buf: &mut Vec<u8>) -> anyhow::Result<()> {
    match value {
        Value::Null => rmp::encode::write_nil(buf).context("msgpack nil")?,
        Value::Bool(v) => rmp::encode::write_bool(buf, *v).context("msgpack bool")?,
        Value::Number(n) => {
            // 与 Python msgpack.packb 一致：非负整数走无符号最短编码。
            if let Some(i) = n.as_i64() {
                if i < 0 {
                    rmp::encode::write_sint(buf, i).context("msgpack i64")?;
                } else {
                    rmp::encode::write_uint(buf, i as u64).context("msgpack u64")?;
                }
            } else if let Some(u) = n.as_u64() {
                rmp::encode::write_uint(buf, u).context("msgpack u64")?;
            } else {
                anyhow::bail!("unsupported msgpack float");
            }
        }
        Value::String(s) => rmp::encode::write_str(buf, s).context("msgpack str")?,
        Value::Array(items) => {
            rmp::encode::write_array_len(buf, items.len() as u32).context("msgpack array")?;
            for item in items {
                encode_value(item, buf)?;
            }
        }
        Value::Object(map) => {
            rmp::encode::write_map_len(buf, map.len() as u32).context("msgpack map")?;
            for (k, v) in map {
                rmp::encode::write_str(buf, k).context("msgpack key")?;
                encode_value(v, buf)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/signing/hyperliquid.rs"]
mod tests;
