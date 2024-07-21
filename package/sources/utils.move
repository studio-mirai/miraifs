module miraifs::utils {

    use std::string::{String};

    use sui::bcs::{to_bytes};
    use sui::hash::{blake2b256};
    use sui::hex;

    public struct ChunkIdentifierHashCalculatedEvent has copy, drop {
        input: vector<u8>,
        output: vector<u8>,
        index_bytes: vector<u8>,
        hash_bytes: vector<u8>,
    }

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

    public(package) fun bytes_to_hex_string(
        bytes: vector<u8>,
    ): String {
        hex::encode(bytes).to_string()
    }

    public(package) fun hex_string_to_bytes(
        s: String,
    ): vector<u8> {
        hex::decode(*s.bytes())
    }

    public fun transfer_objs<T: key + store>(
        mut objs: vector<T>,
        recipient: address,
    ) {
        while (!objs.is_empty()) {
            transfer::public_transfer(objs.remove(0), recipient);
        };
        objs.destroy_empty();
    }

    // #[test]
    // fun test_calculate_chunk_identifier_hash() {
    //     let hash = calculate_chunk_identifier_hash(
    //         0,
    //         52294933049660288214911065849129854450437051276229942185102121446562266219461
    //     );
    //     assert!(hash == 97406106090852497717012263813973313779842828453100017973126130187481921937763)
    // }
}