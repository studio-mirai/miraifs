// Copyright (c) Studio Mirai, Ltd.
// SPDX-License-Identifier: Apache-2.0

module miraifs::vec_test {

    use std::string::{Self, String};
    use sui::address::{Self};
    use sui::hash::{blake2b256};
    use sui::table_vec::{TableVec};
    use sui::vec_map::{VecMap};

    public struct Chunk has key, store {
        id: UID,
        data: vector<vector<u8>>,
        length: u64,
        hash: u256,
    }

    public struct ChunkStr has key, store {
        id: UID,
        data: vector<String>,
        length: u64,
        hash: u256,
    }

    public struct VerifyChunkCap {
        chunk_id: ID,
        verify: bool,
    }

    public struct RegisterChunkCap has key, store {
        id: UID,
        chunk_hash: u256,
        chunk_id: ID,
        file_id: ID,
    }

    public struct File has key, store {
        id: UID,
        chunks: TableVec<ID>
    }

    public fun new(
        verify: bool,
        hash: u256,
        ctx: &mut TxContext,
    ): (Chunk, VerifyChunkCap) {
        let chunk = Chunk {
            id: object::new(ctx),
            data: vector<vector<u8>>[],
            length: 0,
            hash: hash,
        };

        let cap = VerifyChunkCap {
            chunk_id: object::id(&chunk),
            verify: verify,
        };

        (chunk, cap)
    }

    public fun insert(
        chunk: &mut Chunk,
        data: vector<u8>,
    ) {
        assert!(data.length() <= 256, 1);
    
        chunk.data.push_back(data);
        chunk.length = chunk.length + data.length();
    }

    fun hash_to_u256(
        data: &vector<u8>,
    ): u256 {
        address::to_u256(address::from_bytes(blake2b256(data)))
    }

    public fun confirm(
        cap: VerifyChunkCap,
        chunk: &mut Chunk,
    ) {
        assert!(object::id(chunk) == cap.chunk_id, 1);

        if (cap.verify == true) {
            let mut data_concat = vector<u8>[];
            
            let mut i: u64 = 0;
            while (i < chunk.data.length()) {
                data_concat.append(*chunk.data.borrow(i));
                i = i + 1;
            };

            assert!(chunk.hash == hash_to_u256(&data_concat), 1)
        };

        let VerifyChunkCap { chunk_id: _, verify: _ } = cap;
    }
}