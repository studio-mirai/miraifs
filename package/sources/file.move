// Copyright (c) Studio Mirai, Ltd.
// SPDX-License-Identifier: Apache-2.0

module miraifs::file {

    use std::string::{String};

    use sui::clock::{Clock};
    use sui::dynamic_field::{Self};
    use sui::event::{emit};
    use sui::package::{Self};
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
    
    public struct FILE has drop {}

    public struct File has key, store {
        id: UID,
        chunks: FileChunks,
        created_at: u64,
        extension: String,
        mime_type: String,
        size: u64,
    }

    public struct FileChunks has store {
        count: u32,
        hash: vector<u8>,
        manifest: VecMap<vector<u8>, Option<ID>>,
        size: u32
    }

    public struct VerifyFileCap {
        file_id: ID,
    }

    public struct FileCreatedEvent has copy, drop {
        chunk_size: u32,
        created_at: u64,
        file_id: ID,
        mime_type: String,
        chunks_hash: vector<u8>,
    }

    public struct ChunkRegisteredEvent has copy, drop {
        chunk_hash: vector<u8>,
        chunk_id: ID,
        chunk_index: u16,
        file_id: ID,
    }

    fun init(
        otw: FILE,
        ctx: &mut TxContext,
    ) {
        let publisher = package::claim(otw, ctx);
        transfer::public_transfer(publisher, ctx.sender());
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
            (file.chunks.manifest.size() as u16),
            ctx,
        );

        let create_chunk_cap_ids: &mut VecMap<vector<u8>, ID> = dynamic_field::borrow_mut(&mut file.id, b"create_chunk_cap_ids");
        create_chunk_cap_ids.insert(hash, object::id(&create_chunk_cap));
        
        file.chunks.manifest.insert(
            hash,
            option::none(),
        );

        create_chunk_cap
    }

    public fun destroy_empty(
        file: File,
    ) {
        assert!(file.chunks.manifest.is_empty(), EChunksNotDeleted);

        let File {
            id,
            chunks,
            ..
        } = file;

        id.delete();
        
        let FileChunks {manifest, ..} = chunks;
        manifest.destroy_empty();
    }

    public fun new(
        chunk_size: u32,
        extension: String,
        mime_type: String,
        chunks_hash: vector<u8>,
        clock: &Clock,
        ctx: &mut TxContext,
    ): (File, VerifyFileCap) {
        assert!(chunk_size <= MAX_CHUNK_SIZE_BYTES, EMaxChunkSizeExceeded);
        assert!(chunks_hash.length() == 32, EInvalidHashLength);

        let file_chunks = FileChunks {
            count: 0,
            hash: chunks_hash,
            manifest: vec_map::empty(),
            size: chunk_size,
        };

        let mut file = File {
            id: object::new(ctx),
            chunks: file_chunks,
            created_at: clock.timestamp_ms(),
            extension: extension,
            mime_type: mime_type,
            size: 0,
        };

        dynamic_field::add(&mut file.id, b"create_chunk_cap_ids", vec_map::empty<vector<u8>, ID>());

        let verify_file_cap = VerifyFileCap {
            file_id: file.id.to_inner(),
        };

        emit(
            FileCreatedEvent {
                chunk_size: chunk_size,
                created_at: file.created_at,
                file_id: file.id.to_inner(),
                mime_type: file.mime_type,
                chunks_hash: file.chunks.hash,
            }
        );

        (file, verify_file_cap)
    }

    public fun receive_and_register_chunk(
        file: &mut File,
        cap_to_receive: Receiving<RegisterChunkCap>,
    ) {
        // TODO: Implement create_chunk_cap_ids df cleanup.
        let cap = transfer::public_receive(&mut file.id, cap_to_receive);
        let chunk_id = cap.register_chunk_cap_id();
        let chunk_hash = cap.register_chunk_cap_hash();
        let chunk_size = cap.register_chunk_cap_size();
        
        let create_chunk_cap_ids_mut: &mut VecMap<vector<u8>, ID> = dynamic_field::borrow_mut(&mut file.id, b"create_chunk_cap_ids");
        create_chunk_cap_ids_mut.remove(&chunk_hash);

        file.chunks.manifest.get_mut(&chunk_hash).fill(chunk_id);
        file.size = file.size + (chunk_size as u64);

        if (create_chunk_cap_ids_mut.is_empty()) {
            let create_chunk_cap_ids: VecMap<vector<u8>, ID> = dynamic_field::remove(&mut file.id, b"create_chunk_cap_ids");
            create_chunk_cap_ids.destroy_empty();
        };
        
        emit(
            ChunkRegisteredEvent {
                chunk_id: chunk_id,
                chunk_index: cap.register_chunk_cap_index(),
                chunk_hash: chunk_hash,
                file_id: file.id(),
            }
        );

        cap.drop_register_chunk_cap();
    }

    public fun receive_and_drop_chunk(
        file: &mut File,
        chunk_to_receive: Receiving<Chunk>,
    ) {
        let chunk = transfer::public_receive(&mut file.id, chunk_to_receive);
        file.chunks.manifest.remove(&chunk.hash());
        chunk.drop();
    }

    public fun verify(
        cap: VerifyFileCap,
        file: &mut File,
    ) {
        assert!(cap.file_id == object::id(file), EInvalidVerifyFileCapForFile);

        let mut concat_chunk_hashes_bytes: vector<u8> = vector[];
        let mut i = 0;
        while (i < file.chunks.manifest.size()) {
            let (chunk_hash, _) = file.chunks.manifest.get_entry_by_idx(i);
            concat_chunk_hashes_bytes.append(*chunk_hash);
            i = i + 1;
        };
        
        assert!(calculate_hash(&concat_chunk_hashes_bytes) == file.chunks.hash, EVerificationHashMismatch);

        file.chunks.count = file.chunks.manifest.size() as u32;

        let VerifyFileCap {..} = cap;
    }

    public fun id(
        file: &File,
    ): ID {
        file.id.to_inner()
    }

    public fun chunks_count(
        file: &File,
    ): u32 {
        file.chunks.count
    }

    public fun chunks_hash(
        file: &File,
    ): vector<u8> {
        file.chunks.hash
    }

    public fun chunks_manifest(
        file: &File,
    ): VecMap<vector<u8>, Option<ID>> {
        file.chunks.manifest
    }

    public fun created_at(
        file: &File,
    ): u64 {
        file.created_at
    }

    public fun extension(
        file: &File,
    ): String {
        file.extension
    }

    public fun mime_type(
        file: &File,
    ): String {
        file.mime_type
    }

    public fun size(
        file: &File,
    ): u64 {
        file.size
    }
}