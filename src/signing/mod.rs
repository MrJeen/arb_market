use alloy_primitives::{keccak256, Address, B256, U256};

pub const POLYMARKET_CHAIN_ID: u64 = 137;
pub const STANDARD_EXCHANGE: &str = "0xE111180000d2663C0091e4f400237545B87B996B";
pub const NEG_RISK_EXCHANGE: &str = "0xe2222d279d744050d28e00520010520000310F59";
pub const ORDER_TYPE: &str = "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";
pub const DOMAIN_TYPE: &str = "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";
pub const AGENT_TYPE: &str = "Agent(string source,bytes32 connectionId)";
pub const CLOB_AUTH_TYPE: &str = "ClobAuth(address address,string timestamp,uint256 nonce,string message)";
pub const CLOB_AUTH_MESSAGE: &str = "This message attests that I control the given wallet";

pub mod hyperliquid;
pub mod polymarket;

pub fn keccak(data: &[u8]) -> B256 {
    keccak256(data)
}

pub fn encode_address(addr: Address) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[12..].copy_from_slice(addr.as_slice());
    out
}

pub fn encode_u256(value: U256) -> [u8; 32] {
    value.to_be_bytes()
}

pub fn encode_u8(value: u8) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[31] = value;
    out
}

pub fn encode_bytes32(value: B256) -> [u8; 32] {
    value.0
}

pub fn domain_separator(name: &str, version: &str, chain_id: u64, verifying: Address) -> B256 {
    let mut encoded = Vec::with_capacity(32 * 5);
    encoded.extend_from_slice(keccak(DOMAIN_TYPE.as_bytes()).as_slice());
    encoded.extend_from_slice(keccak(name.as_bytes()).as_slice());
    encoded.extend_from_slice(keccak(version.as_bytes()).as_slice());
    encoded.extend_from_slice(&encode_u256(U256::from(chain_id)));
    encoded.extend_from_slice(&encode_address(verifying));
    keccak(&encoded)
}

pub const DOMAIN_TYPE_NO_CONTRACT: &str =
    "EIP712Domain(string name,string version,uint256 chainId)";

pub fn domain_separator_no_contract(name: &str, version: &str, chain_id: u64) -> B256 {
    let mut encoded = Vec::with_capacity(32 * 4);
    encoded.extend_from_slice(keccak(DOMAIN_TYPE_NO_CONTRACT.as_bytes()).as_slice());
    encoded.extend_from_slice(keccak(name.as_bytes()).as_slice());
    encoded.extend_from_slice(keccak(version.as_bytes()).as_slice());
    encoded.extend_from_slice(&encode_u256(U256::from(chain_id)));
    keccak(&encoded)
}

pub fn typed_data_digest(domain_sep: B256, struct_hash: B256) -> B256 {
    let mut buf = Vec::with_capacity(66);
    buf.extend_from_slice(&[0x19, 0x01]);
    buf.extend_from_slice(domain_sep.as_slice());
    buf.extend_from_slice(struct_hash.as_slice());
    keccak(&buf)
}

pub fn parse_address(value: &str) -> anyhow::Result<Address> {
    value.parse().map_err(|e| anyhow::anyhow!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keccak_is_stable() {
        let a = keccak(b"abc");
        let b = keccak(b"abc");
        assert_eq!(a, b);
        assert_ne!(a, keccak(b"abd"));
    }
}
