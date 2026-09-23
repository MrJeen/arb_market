use super::*;

#[test]
fn keccak_is_stable() {
    let a = keccak(b"abc");
    let b = keccak(b"abc");
    assert_eq!(a, b);
    assert_ne!(a, keccak(b"abd"));
}
