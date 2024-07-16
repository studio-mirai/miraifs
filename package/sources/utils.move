module miraifs::utils {

    use sui::address::{from_bytes};
    use sui::hash::{blake2b256};

    public(package) fun calculate_hash(
        bytes: &vector<u8>,
    ): u256 {
        from_bytes(blake2b256(bytes)).to_u256()
    }
}