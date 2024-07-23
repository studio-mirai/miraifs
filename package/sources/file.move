// Copyright (c) Studio Mirai, Ltd.
// SPDX-License-Identifier: Apache-2.0

module miraifs::file {

    use std::string::{String};

    use sui::clock::{Clock};
    use sui::dynamic_field::{Self as df};
    use sui::event;
    use sui::transfer::{Receiving};
    use sui::vec_map::{Self, VecMap};

    use miraifs::chunk::{Self, Chunk, CreateChunkCap, RegisterChunkCap};
    use miraifs::utils::{calculate_hash};

    const MAX_CHUNK_SIZE_BYTES: u32 = 128_000;

    const EChunksNotDeleted: u64 = 1;
    const EInvalidHashLength: u64 = 2;
    const EInvalidVerifyFileCapForFile: u64 = 3;
    const EMaxChunkSizeExceeded: u64 = 4;
    const EVerificationHashMismatch: u64 = 5;
    
    public struct File has key, store {
        id: UID,
        chunk_size: u32,
        chunks: VecMap<vector<u8>, Option<ID>>,
        created_at: u64,
        extension: String,
        mime_type: String,
        size: u32,
    }

    public struct VerifyFileCap {
        file_id: ID,
        // Hash of all individual chunk identifier hashes combined into a single vector<u8>.
        verification_hash: vector<u8>,
    }

    public struct FileCreatedEvent has copy, drop {
        chunk_size: u32,
        created_at: u64,
        file_id: ID,
        mime_type: String,
        verification_hash: vector<u8>,
    }

    public fun add_chunk_hash(
        verify_file_cap: &VerifyFileCap,
        file: &mut File,
        hash: vector<u8>,
        ctx: &mut TxContext,
    ): CreateChunkCap {
        assert!(verify_file_cap.file_id == object::id(file), EInvalidVerifyFileCapForFile);
        assert!(hash.length() == 32, EInvalidHashLength);

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

        create_chunk_cap
    }

    public fun destroy_empty(
        file: File,
    ) {
        assert!(file.chunks.is_empty(), EChunksNotDeleted);

        let File {
            id,
            chunk_size: _,
            chunks,
            created_at: _,
            extension: _,
            mime_type: _,
            size: _,
        } = file;

        chunks.destroy_empty();
        id.delete();
    }

    public fun new(
        chunk_size: u32,
        extension: String,
        mime_type: String,
        verification_hash: vector<u8>,
        clock: &Clock,
        ctx: &mut TxContext,
    ): (File, VerifyFileCap) {
        assert!(chunk_size <= MAX_CHUNK_SIZE_BYTES, EMaxChunkSizeExceeded);
        assert!(verification_hash.length() == 32, EInvalidHashLength);

        let mut file = File {
            id: object::new(ctx),
            chunk_size: chunk_size,
            chunks: vec_map::empty(),
            created_at: clock.timestamp_ms(),
            extension: extension,
            mime_type: mime_type,
            size: 0,
        };

        df::add(&mut file.id, b"create_chunk_cap_ids", vector<ID>[]);

        let verify_file_cap = VerifyFileCap {
            file_id: file.id.to_inner(),
            verification_hash: verification_hash,
        };

        event::emit(
            FileCreatedEvent {
                chunk_size: chunk_size,
                created_at: file.created_at,
                file_id: file.id.to_inner(),
                mime_type: file.mime_type,
                verification_hash: verify_file_cap.verification_hash,
            }
        );

        (file, verify_file_cap)
    }

    public fun receive_and_register_chunk(
        file: &mut File,
        cap_to_receive: Receiving<RegisterChunkCap>,
    ) {
        let cap = transfer::public_receive(&mut file.id, cap_to_receive);
        let (chunk_id, chunk_hash, chunk_size) = chunk::register_chunk_cap_attrs(&cap);
        file.chunks.get_mut(&chunk_hash).fill(chunk_id);
        file.size = file.size + chunk_size;
        chunk::delete_register_chunk_cap(cap);
    }

    public fun receive_and_delete_chunk(
        file: &mut File,
        chunk_to_receive: Receiving<Chunk>,
    ) {
        let chunk = transfer::public_receive(&mut file.id, chunk_to_receive);
        file.chunks.remove(&chunk.hash());
        chunk.drop();
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
        
        assert!(calculate_hash(&concat_chunk_hashes_bytes) == cap.verification_hash, EVerificationHashMismatch);

        let VerifyFileCap {
            file_id: _,
            verification_hash: _,
        } = cap;
    }
}