module miraifs::file {

    use std::string::{String, utf8};

    use sui::clock::{Clock};
    use sui::dynamic_field::{Self as df};
    use sui::event;
    use sui::hash::{blake2b256};
    use sui::hex::{Self};
    use sui::transfer::{Receiving};
    use sui::vec_map::{Self, VecMap};
    use sui::vec_set::{Self, VecSet};

    const EHashMismatch: u64 = 1;
    const EFilePromiseMismatch: u64 = 2;
    const EFileChunksNotDeleted: u64 = 3;

    public struct File has key, store {
        id: UID,
        name: Option<String>,
        encoding: String,
        mime_type: String,
        extension: String,
        size: u64,
        created_at_ts: u64,
        hash: String,
        config: FileConfig,
        chunks: VecMap<String, Option<ID>>,
    }

    public struct FileConfig has drop, store {
        chunk_size: u8,
        sublist_size: u16,
        compression_algorithm: Option<String>,
        compression_level: Option<u8>,
    }

    public struct FileChunk has key, store {
        id: UID,
        hash: String,
        data: vector<String>,
    }

    public struct CreateFileChunkCap has key {
        id: UID,
        hash: String,
        file_id: ID,
    }

    public struct RegisterFileChunkCap has key {
        id: UID,
        file_id: ID,
        chunk_id: ID,
        chunk_hash: String,
        // ID of the CreateFileChunkCap that was used to create this RegisterFileChunkCap.
        created_with: ID,
    }

    public struct FileCreatedEvent has copy, drop {
        id: ID,
    }
    
    public struct CreateFileChunkCapCreatedEvent has copy, drop {
        id: ID,
        hash: String,
    }

    public struct FileChunkCreatedEvent has copy, drop {
        id: ID,
        file_id: ID,
        hash: String,
    }

    public struct DeleteFilePromise {
        file_id: ID,
    }

    public fun create_file(
        encoding: String,
        mime_type: String,
        extension: String,
        size: u64,
        hash: String,
        config: FileConfig,
        mut file_chunk_hashes: vector<String>,
        clock: &Clock,
        ctx: &mut TxContext,
    ): File {
        let mut file = File {
            id: object::new(ctx),
            name: option::none(),
            encoding: encoding,
            mime_type: mime_type,
            extension: extension,
            size: size,
            created_at_ts: clock.timestamp_ms(),
            hash: hash,
            config: config,
            chunks: vec_map::empty(),
        };

        let mut create_file_chunk_cap_ids = vec_set::empty<ID>();

        while (!file_chunk_hashes.is_empty()) {
            // Remove the first hash from the list of expected chunk hashes.
            let file_chunk_hash = file_chunk_hashes.remove(0);
            // Insert the hash into the file's chunks as a key.
            file.chunks.insert(file_chunk_hash, option::none());
            // Create a cap to allow the creation of this file chunk.
            let create_file_chunk_cap = CreateFileChunkCap {
                id: object::new(ctx),
                hash: file_chunk_hash,
                file_id: object::id(&file),
            };
            // Emit CreateFileChunkCapCreated event.
            event::emit(
                CreateFileChunkCapCreatedEvent {
                    id: object::id(&create_file_chunk_cap),
                    hash: file_chunk_hash,
                }
            );

            create_file_chunk_cap_ids.insert(object::id(&create_file_chunk_cap));
            transfer::transfer(create_file_chunk_cap, object::uid_to_address(&file.id));
        };

        // Add a dynamic field to the file object to store
        // the IDs of the associated CreateFileChunkCap objects.
        df::add(
            &mut file.id,
            utf8(b"create_file_chunk_cap_ids"),
            create_file_chunk_cap_ids,
        );

        event::emit(
            FileCreatedEvent {
                id: object::id(&file),
            }
        );

        file
    }

    public fun create_file_config(
        chunk_size: u8,
        sublist_size: u16,
        compression_algorithm: Option<String>,
        compression_level: Option<u8>,
    ): FileConfig {
        let config = FileConfig {
            chunk_size: chunk_size,
            sublist_size: sublist_size,
            compression_algorithm: compression_algorithm,
            compression_level: compression_level,
        };

        config
    }

    public fun delete_file(
        file: File,
        promise: DeleteFilePromise,
    ) {
        assert!(object::id(&file) == promise.file_id, EFilePromiseMismatch);
        assert!(file.chunks.is_empty(), EFileChunksNotDeleted);

        let File {
            id,
            name: _,
            encoding: _,
            mime_type: _,
            extension: _,
            size: _,
            created_at_ts: _,
            hash: _,
            config: _,
            chunks,
        } = file;

        chunks.destroy_empty();
        id.delete();

        let DeleteFilePromise { file_id: _, } = promise;
    }

    public fun create_file_chunk(
        cap: CreateFileChunkCap,
        data: vector<String>,
        verify_hash: bool,
        ctx: &mut TxContext,
    ) {
        // Initialize the file chunk hash variable with the hash within CreateFileChunkCap.
        let mut file_chunk_hash = cap.hash;

        if (verify_hash == true) {
            // Concatenate the string vector into a single string.
            let file_data_concat = concatenate_string_vector(data);
            // If verify_hash is set to True, recalculate the hash of the data onchain,
            // and assert that it matches the expected hash within CreateFileChunkCap.
            file_chunk_hash = calculate_hash_for_string(file_data_concat);
            assert!(file_chunk_hash == cap.hash, EHashMismatch);
        };
    
        let file_chunk = FileChunk { 
            id: object::new(ctx),
            hash: file_chunk_hash,
            data: data,
        };

        event::emit(
            FileChunkCreatedEvent {
                id: object::id(&file_chunk),
                file_id: cap.file_id,
                hash: file_chunk_hash,
            }
        );

        let register_file_chunk_cap = RegisterFileChunkCap {
            id: object::new(ctx),
            file_id: cap.file_id,
            chunk_id: object::id(&file_chunk),
            chunk_hash: file_chunk_hash,
            created_with: object::id(&cap),
        };

        transfer::transfer(file_chunk, object::id_to_address(&cap.file_id));
        transfer::transfer(register_file_chunk_cap, object::id_to_address(&cap.file_id));

        let CreateFileChunkCap {
            id,
            hash: _,
            file_id: _,
        } = cap;
        
        id.delete();
    }

    public fun register_file_chunk(
        file: &mut File,
        cap_to_receive: Receiving<RegisterFileChunkCap>,
    ) {
        let cap = transfer::receive(
            &mut file.id,
            cap_to_receive,
        );

        let chunk_opt = file.chunks.get_mut(&cap.chunk_hash);
        chunk_opt.fill(cap.chunk_id);

        // Borrow a mutable reference to the image's "create_file_chunk_cap_ids" dynamic field.
        let create_file_chunk_cap_ids_for_file_mut: &mut VecSet<ID> = df::borrow_mut(
            &mut file.id,
            utf8(b"create_file_chunk_cap_ids"),
        );
        // Remove the ID of the CreateImageChunkCap associated with the RegisterImageChunkCap in question.
        create_file_chunk_cap_ids_for_file_mut.remove(&cap.created_with);

        // Remove the "create_file_chunk_cap_ids" dynamic field, and drop the vector set.
        if (create_file_chunk_cap_ids_for_file_mut.is_empty()) {
            let _: VecSet<ID> = df::remove(
                &mut file.id,
                utf8(b"create_file_chunk_cap_ids"),
            );
        };

        let RegisterFileChunkCap {
            id,
            file_id: _,
            chunk_id: _,
            chunk_hash: _,
            created_with: _,
        } = cap;
        id.delete()
    }

    public fun receive_create_file_chunk_cap(
        file: &mut File,
        cap_to_receive: Receiving<CreateFileChunkCap>,
        ctx: &mut TxContext,
    ) {
        let cap = transfer::receive(
            &mut file.id,
            cap_to_receive,
        );

        transfer::transfer(cap, ctx.sender());
    }

    fun calculate_hash_for_string(
        str: String,
    ): String {
        let hash_hex_bytes = hex::encode(blake2b256(str.bytes()));
        let hash_str = utf8(hash_hex_bytes);
        
        hash_str
    }

    fun concatenate_string_vector(
        mut data: vector<String>,
    ): String {
        let mut result_str = utf8(b"");

        while (!data.is_empty()) {
            result_str.append(data.remove(0));
        };

        result_str
    }
}
