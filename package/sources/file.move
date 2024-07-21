// Copyright (c) Studio Mirai, Ltd.
// SPDX-License-Identifier: Apache-2.0

module miraifs::file {

    use std::string::{String};

    use sui::clock::{Clock};
    use sui::dynamic_field::{Self as df};
    use sui::event;
    use sui::transfer::{Receiving};
    use sui::vec_map::{Self, VecMap};

    use miraifs::chunk::{Self, Chunk, CreateChunkCap};
    use miraifs::utils::{calculate_hash};

    const MAX_CHUNK_SIZE_BYTES: u32 = 128_000;

    const EChunksNotDeleted: u64 = 1;
    const EInvalidVerifyFileCapForFile: u64 = 2;
    const EMaxChunkSizeExceeded: u64 = 3;
    const EVerificationHashMismatch: u64 = 4;

    public struct File has key, store {
        id: UID,
        chunk_size: u32,
        chunks: VecMap<vector<u8>, Option<ID>>,
        created_at: u64,
        mime_type: String,
        size: u32,
    }

    public struct VerifyFileCap {
        file_id: ID,
        // Hash of all individual chunk identifier hashes combined into a single vector<u8>.
        verification_hash: vector<u8>,
    }

    public struct FileCreatedEvent has copy, drop {
        file_id: ID,
    }

    public struct FileChunkAddedEvent has copy, drop {
        file_id: ID,
        create_chunk_cap_id: ID,
    }

    public struct FileVerifiedEvent has copy, drop {
        input: vector<u8>,
        hash: vector<u8>,
    }

    public fun add_chunk_hash(
        verify_file_cap: &VerifyFileCap,
        file: &mut File,
        hash: vector<u8>,
        ctx: &mut TxContext,
    ): CreateChunkCap {
        assert!(verify_file_cap.file_id == object::id(file), EInvalidVerifyFileCapForFile);

        // let chunk_identifier_hash = calculate_chunk_identifier_hash(index, hash);
        let create_chunk_cap = chunk::new_create_chunk_cap(
            object::id(file),
            hash,
            (file.chunks.size() as u16),
            ctx,
        );

        let create_chunk_cap_ids: &mut vector<ID> = df::borrow_mut(&mut file.id, b"create_chunk_cap_ids");
        create_chunk_cap_ids.push_back(object::id(&create_chunk_cap));
        
        file.chunks.insert(
            hash,
            option::none(),
        );

        event::emit(
            FileChunkAddedEvent {
                file_id: object::id(file),
                create_chunk_cap_id: create_chunk_cap.create_chunk_cap_id(),
            }
        );

        create_chunk_cap
    }

    public fun new(
        chunk_size: u32,
        mime_type: String,
        verification_hash: vector<u8>,
        clock: &Clock,
        ctx: &mut TxContext,
    ): (File, VerifyFileCap) {
        assert!(chunk_size <= MAX_CHUNK_SIZE_BYTES, EMaxChunkSizeExceeded);

        let mut file = File {
            id: object::new(ctx),
            chunk_size: chunk_size,
            chunks: vec_map::empty(),
            created_at: clock.timestamp_ms(),
            mime_type: mime_type,
            size: 0,
        };

        df::add(&mut file.id, b"create_chunk_cap_ids", vector<ID>[]);

        let verify_file_cap = VerifyFileCap {
            file_id: object::id(&file),
            verification_hash: verification_hash,
        };

        event::emit(
            FileCreatedEvent {
                file_id: object::id(&file),
            }
        );

        (file, verify_file_cap)
    }

    public fun verify(
        cap: VerifyFileCap,
        file: &File,
    ) {
        assert!(cap.file_id == object::id(file), EInvalidVerifyFileCapForFile);

        let mut concat_chunk_hashes_bytes: vector<u8> = vector[];
        let mut i = 0;
        while (i < file.chunks.size()) {
            let (chunk_hash, _) = file.chunks.get_entry_by_idx(i);
            concat_chunk_hashes_bytes.append(*chunk_hash);
            i = i + 1;
        };

        event::emit(
            FileVerifiedEvent {
                input: concat_chunk_hashes_bytes,
                hash: calculate_hash(&concat_chunk_hashes_bytes),
            }
        );
        
        assert!(calculate_hash(&concat_chunk_hashes_bytes) == cap.verification_hash, EVerificationHashMismatch);

        let VerifyFileCap {
            file_id: _,
            verification_hash: _,
        } = cap;
    }

    public fun delete(
        file: File,
    ) {
        assert!(file.chunks.is_empty(), EChunksNotDeleted);

        let File {
            id,
            chunk_size: _,
            chunks,
            created_at: _,
            mime_type: _,
            size: _,
        } = file;

        chunks.destroy_empty();
        id.delete();
    }

    public fun receive_and_register_chunk(
        file: &mut File,
        chunk_to_receive: Receiving<Chunk>,
    ) {
        let chunk = chunk::receive(&mut file.id, chunk_to_receive);
        file.chunks.get_mut(&chunk.hash()).fill(chunk.id());
        file.size = file.size + chunk.size();
        chunk.transfer(file.id.to_address())
    }

    public fun receive_and_delete_chunk(
        file: &mut File,
        chunk_to_receive: Receiving<Chunk>,
    ) {
        let chunk = chunk::receive(&mut file.id, chunk_to_receive);
        file.chunks.remove(&chunk.hash());
        chunk.delete();
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