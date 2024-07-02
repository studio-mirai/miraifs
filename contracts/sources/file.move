// Copyright (c) Studio Mirai, Ltd.
// SPDX-License-Identifier: Apache-2.0

module miraifs::file {

    use std::string::{String, utf8};

    use sui::clock::{Clock};
    use sui::dynamic_field::{Self as df};
    use sui::event;
    use sui::hash::{blake2b256};
    use sui::hex::{Self};
    use sui::table::{Self, Table};
    use sui::transfer::{Receiving};
    use sui::vec_map::{Self, VecMap};
    use sui::vec_set::{Self, VecSet};

    const EHashMismatch: u64 = 1;
    const EFilePromiseMismatch: u64 = 2;
    const EChunksNotDeleted: u64 = 3;

    public struct File has key, store {
        id: UID,
        chunks: VecMap<u256, Option<ID>>,
        mime_type: String,
    }

    public struct Chunk has key {
        id: UID,
        data: vector<vector<u8>>,
        hash: u256,
        index: u64,
        length: u64,
    }

    public struct CreateChunkCap has key, store {
        id: UID,
        file_id: ID,
        hash: u256,
        index: u64,
    }

    public struct RegisterChunkCap has key {
        id: UID,
        chunk_hash: u256,
        chunk_id: ID,
        file_id: ID,
        // ID of the CreateChunkCap that was used to create this RegisterChunkCap.
        created_with: ID,
    }

    public struct FileCreatedEvent has copy, drop {
        id: ID,
    }
    
    public struct CreateChunkCapCreatedEvent has copy, drop {
        id: ID,
        hash: u256,
        index: u64,
    }

    public struct ChunkCreatedEvent has copy, drop {
        id: ID,
        file_id: ID,
        hash: u256,
        index: u64,
    }

    public struct DeleteFilePromise {
        file_id: ID,
    }

    public fun create_file(
        mut chunk_hashes: vector<u256>,
        mime_type: String,
        ctx: &mut TxContext,
    ): File {
        let mut file = File {
            id: object::new(ctx),
            chunks: vec_map::empty(),
            mime_type: mime_type,
        };

        event::emit(
            FileCreatedEvent {
                id: object::id(&file),
            }
        );

        let mut create_chunk_cap_ids = vec_set::empty<ID>();

        let mut index: u64 = 0;
        while (!chunk_hashes.is_empty()) {
            // Remove the first hash from the list of expected chunk hashes.
            let chunk_hash = chunk_hashes.remove(0);
            // Insert the hash into the file's chunks as a key.
            file.chunks.insert(chunk_hash, option::none());
            // Create a cap to allow the creation of this file chunk.
            let create_chunk_cap = CreateChunkCap {
                id: object::new(ctx),
                file_id: object::id(&file),
                hash: chunk_hash,
                index: index,
            };
            // Emit CreateChunkCapCreated event.
            event::emit(
                CreateChunkCapCreatedEvent {
                    id: object::id(&create_chunk_cap),
                    hash: chunk_hash,
                    index: index,
                }
            );

            create_chunk_cap_ids.insert(object::id(&create_chunk_cap));
            transfer::transfer(create_chunk_cap, object::uid_to_address(&file.id));

            index = index + 1;
        };

        file
    }

    public fun delete_file(
        file: File,
        promise: DeleteFilePromise,
    ) {
        assert!(object::id(&file) == promise.file_id, EFilePromiseMismatch);
        assert!(file.chunks.is_empty(), EChunksNotDeleted);

        let File {
            id,
            chunks,
            mime_type: _,
        } = file;

        chunks.destroy_empty();
        id.delete();

        let DeleteFilePromise { file_id: _, } = promise;
    }
}