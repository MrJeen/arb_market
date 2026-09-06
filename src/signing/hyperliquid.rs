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
mod tests {
    use super::*;
    use alloy_signer_local::PrivateKeySigner;

    #[test]
    fn action_hash_is_deterministic() {
        let action = order_action(100_005_160, true, "0.55", "10", Some("0x1234"), None);
        let a = action_hash(&action, None, 1, None).unwrap();
        let b = action_hash(&action, None, 1, None).unwrap();
        assert_eq!(a, b);
        let c = action_hash(&action, None, 2, None).unwrap();
        assert_ne!(a, c);
    }

    #[test]
    fn signs_l1_action() {
        let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).unwrap();
        let action = order_action(100_005_160, true, "0.55", "10", None, None);
        let (r, s, v) = sign_l1_action(&signer, &action, 1, true).unwrap();
        assert!(r.starts_with("0x"));
        assert!(s.starts_with("0x"));
        assert!(v == 27 || v == 28);
    }

    #[test]
    fn official_phantom_agent_hash() {
        let action = json!({
            "type": "order",
            "orders": [{
                "a": 4,
                "b": true,
                "p": "1670.1",
                "s": "0.0147",
                "r": false,
                "t": {"limit": {"tif": "Ioc"}}
            }],
            "grouping": "na"
        });
        let hash = action_hash(&action, None, 1_677_777_606_040, None).unwrap();
        assert_eq!(
            format!("{hash:#x}"),
            "0x0fcbeda5ae3c4950a548021552a4fea2226858c4453571bf3f24ba017eac2908"
        );
    }

    fn test_wallet() -> PrivateKeySigner {
        let key = B256::from_slice(
            &hex::decode("0123456789012345678901234567890123456789012345678901234567890123")
                .unwrap(),
        );
        PrivateKeySigner::from_bytes(&key).unwrap()
    }

    fn assert_sig(got: (String, String, u8), want_r: &str, want_s: &str, want_v: u8) {
        let (r, s, v) = got;
        let norm = |x: &str| {
            let h = x.trim_start_matches("0x");
            format!("0x{h:0>64}")
        };
        assert_eq!(norm(&r), norm(want_r));
        assert_eq!(norm(&s), norm(want_s));
        assert_eq!(v, want_v);
    }

    #[test]
    fn official_l1_dummy_signature() {
        let action = json!({"type": "dummy", "num": 100_000_000_000u64});
        let (r, s, v) = sign_l1_action(&test_wallet(), &action, 0, true).unwrap();
        assert_sig(
            (r, s, v),
            "0x053749d5b30552aeb2fca34b530185976545bb22d0b3ce6f62e31be961a59298",
            "0x755c40ba9bf05223521753995abb2f73ab3229be8ec921f350cb447e384d8ed8",
            27,
        );
    }

    #[test]
    fn official_l1_order_signature() {
        let action = json!({
            "type": "order",
            "orders": [{
                "a": 1,
                "b": true,
                "p": "100",
                "s": "100",
                "r": false,
                "t": {"limit": {"tif": "Gtc"}}
            }],
            "grouping": "na"
        });
        let (r, s, v) = sign_l1_action(&test_wallet(), &action, 0, true).unwrap();
        assert_sig(
            (r, s, v),
            "0xd65369825a9df5d80099e513cce430311d7d26ddf477f5b3a33d2806b100d78e",
            "0x2b54116ff64054968aa237c20ca9ff68000f977c93289157748a3162b6ea940e",
            28,
        );
    }

    #[test]
    fn official_l1_order_with_cloid_signature() {
        let action = json!({
            "type": "order",
            "orders": [{
                "a": 1,
                "b": true,
                "p": "100",
                "s": "100",
                "r": false,
                "t": {"limit": {"tif": "Gtc"}},
                "c": "0x00000000000000000000000000000001"
            }],
            "grouping": "na"
        });
        let (r, s, v) = sign_l1_action(&test_wallet(), &action, 0, true).unwrap();
        assert_sig(
            (r, s, v),
            "0x041ae18e8239a56cacbc5dad94d45d0b747e5da11ad564077fcac71277a946e3",
            "0x3c61f667e747404fe7eea8f90ab0e76cc12ce60270438b2058324681a00116da",
            27,
        );
    }

    #[test]
    fn order_action_includes_outcome_builder() {
        let action = order_action(
            100_005_160,
            true,
            "0.55",
            "10",
            None,
            Some(("0xab5dbc057628bc18523c4cdfc0e1e2ebdbecb704", 0)),
        );
        assert_eq!(
            action["builder"]["b"],
            json!("0xab5dbc057628bc18523c4cdfc0e1e2ebdbecb704")
        );
        assert_eq!(action["builder"]["f"], json!(0));
        assert!(order_action(1, true, "0.5", "1", None, None)
            .get("builder")
            .is_none());
    }
}
