// Copyright (c) Studio Mirai, Ltd.
// SPDX-License-Identifier: Apache-2.0

module miraifs::file {

    use std::string::{String};

    use sui::clock::{Clock};
    use sui::dynamic_field::{Self as df};
    use sui::event;
    use sui::hash::{blake2b256};
    use sui::transfer::{Receiving};
    use sui::vec_map::{Self, VecMap};
    use sui::vec_set::{Self};

    use miraifs::chunk::{Self, RegisterChunkCap};

    const EHashMismatch: u64 = 1;
    const EFilePromiseMismatch: u64 = 2;
    const EChunksNotDeleted: u64 = 3;

    const MAX_CHUNK_SIZE_BYTES: u64 = 32768;

    public struct File has key, store {
        id: UID,
        chunks: VecMap<u256, Option<ID>>,
        created_at_ts: u64,
        hash: u256,
        mime_type: String,
        size: u64,
    }

    public struct FileCreatedEvent has copy, drop {
        file_id: ID,
    }

    public struct FileChunksAddedEvent has copy, drop {
        file_id: ID,
        chunk_count: u64,
    }
    
    public struct DeleteFilePromise {
        file_id: ID,
    }

    public fun add_chunks(
        file: &mut File,
        mut chunk_hashes: vector<u256>,
        ctx: &mut TxContext,
    ) {
        let mut i: u64 = file.chunks.size();
        
        while (!chunk_hashes.is_empty()) {
            let hash = chunk_hashes.remove(0);
            let create_chunk_cap = chunk::new_create_chunk_cap(
                object::id(file),
                hash,
                i,
                ctx,
            );
            file.chunks.insert(hash, option::none());
            transfer::public_transfer(create_chunk_cap, object::uid_to_address(&file.id));
            i = i + 1;
        };

        event::emit(
            FileChunksAddedEvent {
                file_id: object::id(file),
                chunk_count: file.chunks.size(),
            }
        );
    }

    public fun new(
        hash: u256,
        mime_type: String,
        clock: &Clock,
        ctx: &mut TxContext,
    ): File {
        let mut file = File {
            id: object::new(ctx),
            chunks: vec_map::empty(),
            created_at_ts: clock.timestamp_ms(),
            hash: hash,
            mime_type: mime_type,
            size: 0,
        };

        df::add(&mut file.id, b"create_chunk_cap_ids".to_string(), vec_set::empty<ID>());

        event::emit(
            FileCreatedEvent {
                file_id: object::id(&file),
            }
        );

        file
    }

    public fun delete(
        file: File,
        promise: DeleteFilePromise,
    ) {
        assert!(object::id(&file) == promise.file_id, EFilePromiseMismatch);
        assert!(file.chunks.is_empty(), EChunksNotDeleted);

        let File {
            id,
            chunks,
            created_at_ts: _,
            hash: _,
            mime_type: _,
            size: _,
        } = file;

        chunks.destroy_empty();
        id.delete();

        let DeleteFilePromise { file_id: _, } = promise;
    }

    public fun register_chunk(
        file: &mut File,
        cap_to_receive: Receiving<RegisterChunkCap>,
    ) {
        let cap = transfer::public_receive(&mut file.id, cap_to_receive);
        let chunk_hash = chunk::register_chunk_cap_chunk_hash(&cap);
        let chunk_id = chunk::register_chunk_cap_chunk_id(&cap);
        file.chunks.get_mut(&chunk_hash).fill(chunk_id);
        chunk::delete_register_chunk_cap(cap);
    }
}