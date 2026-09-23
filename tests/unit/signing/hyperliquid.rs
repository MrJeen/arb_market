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
        &hex::decode("0123456789012345678901234567890123456789012345678901234567890123").unwrap(),
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
