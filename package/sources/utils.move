module miraifs::utils {

    use sui::bcs::{to_bytes};
    use sui::hash::{blake2b256};

    // Create a unique identifier hash by combining chunk index and chunk bytes hash.
    public(package) fun calculate_chunk_identifier_hash(
        chunk_index: u16,
        chunk_hash: vector<u8>,
    ): vector<u8> {
        let mut chunk_index_bytes = to_bytes<u16>(&chunk_index);
        chunk_index_bytes.reverse();
        let mut v: vector<u8> = vector[];
        v.append(chunk_index_bytes);
        v.append(chunk_hash);
        calculate_hash(&v)
    }

    public(package) fun calculate_hash(
        bytes: &vector<u8>,
    ): vector<u8> {
        blake2b256(bytes)
    }

    public fun transfer_objs<T: key + store>(
        mut objs: vector<T>,
        recipient: address,
    ) {
        while (!objs.is_empty()) {
            transfer::public_transfer(objs.pop_back(), recipient);
        };
        objs.destroy_empty();
    }
}