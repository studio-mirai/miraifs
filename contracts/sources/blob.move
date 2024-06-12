// Copyright (c) Studio Mirai, Ltd.
// SPDX-License-Identifier: Apache-2.0

module miraifs::blob {

    use std::string::{String, utf8};

    use sui::clock::{Clock};
    use sui::dynamic_field::{Self as df};
    use sui::event;
    use sui::hash::{blake2b256};
    use sui::hex::{Self};
    use sui::table::{Table};
    use sui::transfer::{Receiving};
    use sui::vec_map::{Self, VecMap};
    use sui::vec_set::{Self, VecSet};

    use miraifs::file::{Self, File};
    use miraifs::utils::{Self};

    public struct Blob has key, store {
        id: UID,
        chunks: VecMap<String, Option<ID>>,
        file_id: ID,
    }

    public struct BlobChunk has key {
        id: UID,
        data: vector<String>,
        hash: String,
        index: u64,
    }

    public struct CreateBlobChunkCap has key {
        id: UID,
        blob_id: ID,
        file_id: ID,
        hash: String,
        index: u64,
    }

    public struct DeleteBlobPromise {
        blob_id: ID,
    }

    public struct RegisterFileChunkCap has key {
        id: UID,
        blob_chunk_id: ID,
        blob_chunk_hash: String,
        blob_id: ID,
        create_blob_chunk_cap_id: ID,
        file_id: ID,
    }

    public struct BlobCreatedEvent has copy, drop {
        id: ID,
        file_id: ID,
    }

    public struct BlobChunkCreatedEvent has copy, drop {
        id: ID,
        blob_id: ID,
        file_id: ID,
        hash: String,
        index: u64,
    }

    public struct CreateBlobChunkCapCreatedEvent has copy, drop {
        id: ID,
        hash: String,
        index: u64,
    }

    public fun create_blob(
        mut chunk_hashes: vector<String>,
        file: &File,
        ctx: &mut TxContext,
    ) {
        let mut blob = Blob {
            id: object::new(ctx),
            file_id: file.file_id(),
            chunks: vec_map::empty(),
        };

        let mut create_blob_chunk_cap_ids = vector::empty<ID>();

        let mut index: u64 = 0;
        while (!chunk_hashes.is_empty()) {
            // Remove the first hash from the list of expected chunk hashes.
            let chunk_hash = chunk_hashes.remove(0);
            // Insert the hash into the file's chunks as a key.
            blob.chunks.insert(chunk_hash, option::none());
            // Create a cap to allow the creation of this file chunk.
            let create_blob_chunk_cap = CreateBlobChunkCap {
                id: object::new(ctx),
                blob_id: object::id(&blob),
                file_id: file.file_id(),
                hash: chunk_hash,
                index: index,
            };
            // Emit CreateFileChunkCapCreated event.
            event::emit(
                CreateBlobChunkCapCreatedEvent {
                    id: object::id(&create_blob_chunk_cap),
                    index: index,
                    hash: chunk_hash,
                }
            );

            create_blob_chunk_cap_ids.push_back(object::id(&create_blob_chunk_cap));
            transfer::transfer(create_blob_chunk_cap, object::uid_to_address(&blob.id));

            index = index + 1;
        };

        // Add a dynamic field to the blob object to store
        // the IDs of the associated CreateBlobChunkCap objects.
        df::add(
            &mut blob.id,
            utf8(b"create_blob_chunk_cap_ids"),
            create_blob_chunk_cap_ids,
        );

        event::emit(
            BlobCreatedEvent {
                id: object::id(&blob),
                file_id: file.file_id(),
            }
        );

        // Transfer the blob to the parent file.
        transfer::transfer(blob, file.file_address());
    }

    public fun create_blob_chunk(
        cap: CreateBlobChunkCap,
        data: vector<String>,
        verify_hash: bool,
        ctx: &mut TxContext,
    ) {
        // Initialize the file chunk hash variable with the hash within CreateFileChunkCap.
        let mut blob_chunk_hash = cap.hash;

        if (verify_hash == true) {
            // Concatenate the string vector into a single string.
            let file_data_concat = utils::concatenate_strings(data);
            // If verify_hash is set to True, recalculate the hash of the data onchain,
            // and assert that it matches the expected hash within CreateFileChunkCap.
            blob_chunk_hash = utils::calculate_hash_for_string(file_data_concat);
            assert!(blob_chunk_hash == cap.hash, 1);
        };
    
        let blob_chunk = BlobChunk { 
            id: object::new(ctx),
            index: cap.index,
            hash: blob_chunk_hash,
            data: data,
        };

        event::emit(
            BlobChunkCreatedEvent {
                id: object::id(&blob_chunk),
                blob_id: cap.blob_id,
                file_id: cap.file_id,
                index: cap.index,
                hash: blob_chunk_hash,
            }
        );

        let register_blob_chunk_cap = RegisterFileChunkCap {
            id: object::new(ctx),
            blob_chunk_id: object::id(&blob_chunk),
            blob_chunk_hash: blob_chunk_hash,
            blob_id: cap.blob_id,
            file_id: cap.file_id,
            create_blob_chunk_cap_id: object::id(&cap),
        };

        transfer::transfer(blob_chunk, object::id_to_address(&cap.blob_id));
        transfer::transfer(register_blob_chunk_cap, object::id_to_address(&cap.blob_id));

        let CreateBlobChunkCap {
            id,
            blob_id: _,
            file_id: _,
            hash: _,
            index: _,
        } = cap;
        
        id.delete();
    }

    public fun delete_blob(
        blob: Blob,
        promise: DeleteBlobPromise,
    ) {
        assert!(object::id(&blob) == promise.blob_id, 1);
        assert!(blob.chunks.is_empty(), 1);

        let Blob {
            id,
            chunks,
            file_id: _,
        } = blob;

        chunks.destroy_empty();
        id.delete();

        let DeleteBlobPromise { blob_id: _, } = promise;
    }
}