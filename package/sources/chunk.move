module miraifs::chunk {

    use miraifs::utils::{calculate_chunk_identifier_hash, calculate_hash};

    public struct Chunk has key, store {
        id: UID,
        data: vector<u8>,
        hash: vector<u8>,
        index: u16,
        size: u32,
    }

    public struct CreateChunkCap has key, store {
        id: UID,
        file_id: ID,
        index: u16,
        hash: vector<u8>,
    }

    public struct RegisterChunkCap has key, store {
        id: UID,
        chunk_id: ID,
        hash: vector<u8>,
        size: u32,
    }

    public struct VerifyChunkCap {
        chunk_id: ID,
        file_id: ID,
    }

    const EChunkHashMismatch: u64 = 1;
    const EInvalidVerifyChunkCapForChunk: u64 = 2;

    public fun new(
        cap: CreateChunkCap,
        ctx: &mut TxContext,
    ): (Chunk, VerifyChunkCap) {
        let chunk = Chunk {
            id: object::new(ctx),
            data: vector::empty(),
            hash: cap.hash,
            index: cap.index,
            size: 0,
        };

        let verify_chunk_cap = VerifyChunkCap {
            chunk_id: object::id(&chunk),
            file_id: cap.file_id,
        };

        let CreateChunkCap {
            id,
            file_id: _,
            hash: _,
            index: _,
        } = cap;
        id.delete();

        (chunk, verify_chunk_cap)
    }

    public fun add_data(
        chunk: &mut Chunk,
        mut data: vector<vector<u8>>,
    ) {
        while (!data.is_empty()) {
            chunk.data.append(data.pop_back());
        };
    }

    public fun verify(
        cap: VerifyChunkCap,
        mut chunk: Chunk,
        ctx: &mut TxContext,
    ) {
        assert!(cap.chunk_id == object::id(&chunk), EInvalidVerifyChunkCapForChunk);

        let chunk_bytes_hash = calculate_hash(&chunk.data);
        let chunk_identifier_hash = calculate_chunk_identifier_hash(chunk.index, chunk_bytes_hash);
        assert!(chunk_identifier_hash == chunk.hash, EChunkHashMismatch);

        chunk.size = chunk.data.length() as u32;

        let register_chunk_cap = RegisterChunkCap {
            id: object::new(ctx),
            chunk_id: object::id(&chunk),
            hash: chunk.hash,
            size: chunk.size,
        }; 

        transfer::public_transfer(chunk, cap.file_id.to_address());
        transfer::public_transfer(register_chunk_cap, cap.file_id.to_address());

        let VerifyChunkCap {
            chunk_id: _,
            file_id: _,
        } = cap;
    }

    // === Public-View Functions ===

    public fun id(
        chunk: &Chunk,
    ): ID {
        chunk.id.to_inner()
    }

    public fun data(
        chunk: &Chunk,
    ): vector<u8> {
        chunk.data
    }

    public fun hash(
        chunk: &Chunk,
    ): vector<u8> {
        chunk.hash
    }

    public fun index(
        chunk: &Chunk,
    ): u16 {
        chunk.index
    }

    public fun size(
        chunk: &Chunk,
    ): u32 {
        chunk.size
    }

    public fun register_chunk_cap_id(
        cap: &RegisterChunkCap,
    ): ID {
        cap.chunk_id
    }

    public fun register_chunk_cap_hash(
        cap: &RegisterChunkCap,
    ): vector<u8> {
        cap.hash
    }

    public fun register_chunk_cap_size(
        cap: &RegisterChunkCap,
    ): u32 {
        cap.size
    }

    // === Public-Package Functions ===

    public(package) fun drop(
        chunk: Chunk,
    ) {
        let Chunk {
            id,
            data: _,
            hash: _,
            index: _,
            size: _,
        } = chunk;
        id.delete();
    }

    public(package) fun new_create_chunk_cap(
        file_id: ID,
        hash: vector<u8>,
        index: u16,
        ctx: &mut TxContext,
    ): CreateChunkCap {
        let cap = CreateChunkCap {
            id: object::new(ctx),
            file_id,
            hash: hash,
            index: index,
        };

        cap
    }

    public(package) fun drop_create_chunk_cap(
        cap: CreateChunkCap,
    ) {
        let CreateChunkCap {
            id,
            file_id: _,
            hash: _,
            index: _,
        } = cap;

        id.delete();
    }

    public(package) fun drop_register_chunk_cap(
        cap: RegisterChunkCap,
    ) {
        let RegisterChunkCap {
            id,
            chunk_id: _,
            hash: _,
            size: _,
        } = cap;
        
        id.delete();
    }
}