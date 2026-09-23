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
