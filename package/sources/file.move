// Copyright (c) Studio Mirai, Ltd.
// SPDX-License-Identifier: Apache-2.0

module miraifs::file {

    use std::string::{String};

    use sui::bcs::{to_bytes};
    use sui::clock::{Clock};
    use sui::dynamic_field::{Self as df};
    use sui::event;
    use sui::hash::{blake2b256};
    use sui::hex;
    use sui::table::{Self, Table};
    use sui::transfer::{Receiving};
    use sui::vec_map::{Self, VecMap};
    use sui::vec_set::{Self};

    use miraifs::chunk::{Self, CreateChunkCap, RegisterChunkCap};
    use miraifs::utils::{calculate_chunk_identifier_hash, calculate_hash};

    const EHashMismatch: u64 = 1;
    const EFilePromiseMismatch: u64 = 2;
    const EChunksNotDeleted: u64 = 3;

    const MAX_CHUNK_SIZE_BYTES: u64 = 32768;

    public struct File has key, store {
        id: UID,
        chunks: VecMap<vector<u8>, Option<ID>>,
        created_at: u64,
        mime_type: String,
        size: u64,
    }

    public struct VerifyFileCap {
        file_id: ID,
        // Hash of all individual chunk identifier hashes combined into a single vector<u8>.
        verification_hash: vector<u8>,
    }

    public struct VerifyFileCapCreatedEvent has copy, drop {
        file_id: ID,
        chunk_count: u16,
    }

    public struct FileCreatedEvent has copy, drop {
        file_id: ID,
    }

    public struct FileVerifiedEvent has copy, drop {
        bytes: vector<u8>,
        hash: vector<u8>,
    }

    public struct FileChunkAddedEvent has copy, drop {
        file_id: ID,
        create_chunk_cap_id: ID,
    }
    
    public struct DeleteFilePromise {
        file_id: ID,
    }

    public fun add_chunk_hash(
        verify_file_cap: &VerifyFileCap,
        file: &mut File,
        hash: vector<u8>,
        ctx: &mut TxContext,
    ): CreateChunkCap {
        assert!(verify_file_cap.file_id == file.id());
        let index = (file.chunks.size() as u16);
        let chunk_identifier_hash = calculate_chunk_identifier_hash(index, hash);
        let create_chunk_cap = chunk::new_create_chunk_cap(
            object::id(file),
            chunk_identifier_hash,
            index,
            ctx,
        );
        file.chunks.insert(chunk_identifier_hash, option::none());
        event::emit(
            FileChunkAddedEvent {
                file_id: object::id(file),
                create_chunk_cap_id: create_chunk_cap.create_chunk_cap_id(),
            }
        );
        create_chunk_cap
    }

    public fun new(
        mime_type: String,
        verification_hash: vector<u8>,
        clock: &Clock,
        ctx: &mut TxContext,
    ): (File, VerifyFileCap) {
        let mut file = File {
            id: object::new(ctx),
            chunks: vec_map::empty(),
            created_at: clock.timestamp_ms(),
            mime_type: mime_type,
            size: 0,
        };

        df::add(&mut file.id, b"create_chunk_cap_ids".to_string(), vec_set::empty<ID>());

        let verify_file_cap = VerifyFileCap {
            file_id: object::id(&file),
            verification_hash: verification_hash,
        };

        event::emit(
            FileCreatedEvent {
                file_id: file.id(),
            }
        );

        (file, verify_file_cap)
    }

    public fun verify(
        cap: VerifyFileCap,
        file: &File,
    ) {
        assert!(cap.file_id == object::id(file), 1);

        let mut concat_chunk_hashes_bytes: vector<u8> = vector[];
        let mut i = 0;
        while (i < file.chunks.size()) {
            let (chunk_hash, _) = file.chunks.get_entry_by_idx(i);
            concat_chunk_hashes_bytes.append(*chunk_hash);
            i = i + 1;
        };

        let verification_hash = calculate_hash(&concat_chunk_hashes_bytes);
        assert!(verification_hash == cap.verification_hash);

        let VerifyFileCap {
            file_id: _,
            verification_hash: _,
        } = cap;
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
            created_at: _,
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

    fun id(
        file: &File,
    ): ID {
        object::id(file)
    }

    #[test]
    fun test_vector_equality() {
        let v1: vector<u8> = vector[37,88,16,238,173,243,169,238,19,107,217,168,75,117,36,247,43,123,8,46,226,144,63,119,132,138,104,19,31,252,184];
        let v2: vector<u8> = vector[37,88,16,238,173,243,169,238,19,107,217,168,75,117,36,247,43,123,8,46,226,144,63,119,132,138,104,19,31,252,184];
        assert!(v1 == v2, 1);
        let v3: vector<u8> = vector[37,88,16,238,173,243,169,238,19,107,217,168,75,117,36,247,43,123,8,46,226,144,63,119,132,138,104,19,31,252,184];
        let v4: vector<u8> = vector[184,252,31,19,104,138,132,119,63,144,226,46,8,123,43,247,36,117,75,168,217,107,19,238,169,243,173,238,16,88,37];
        assert!(v3 != v4, 1);
    }
}