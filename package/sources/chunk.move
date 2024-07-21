module miraifs::chunk {

    use sui::transfer::{Receiving};
    use miraifs::utils::{calculate_chunk_identifier_hash, calculate_hash};

    public struct Chunk has key {
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

    public struct VerifyChunkCap {
        chunk_id: ID,
        file_id: ID,
    }

    const EChunkHashMismatch: u64 = 1;

    public fun new(
        cap: CreateChunkCap,
        hash: vector<u8>,
        index: u16,
        ctx: &mut TxContext,
    ): (Chunk, VerifyChunkCap) {
        let chunk = Chunk {
            id: object::new(ctx),
            data: vector::empty(),
            hash: hash,
            index: index,
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

    public(package) fun delete(
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

    public fun verify(
        cap: VerifyChunkCap,
        mut chunk: Chunk,
    ) {
        let chunk_bytes_hash = calculate_hash(&chunk.data);
        let chunk_identifier_hash = calculate_chunk_identifier_hash(chunk.index, chunk_bytes_hash);
        assert!(chunk_identifier_hash == chunk.hash, EChunkHashMismatch);

        chunk.size = chunk.data.length() as u32;

        transfer::transfer(chunk, cap.file_id.to_address());

        let VerifyChunkCap {
            chunk_id: _,
            file_id: _,
        } = cap;
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

    public(package) fun receive(
        file_id_mut: &mut UID,
        chunk_to_receive: Receiving<Chunk>,
    ): Chunk {
        transfer::receive(file_id_mut, chunk_to_receive)
    }

    public(package) fun id(
        chunk: &Chunk,
    ): ID {
        object::id(chunk)
    }

    public(package) fun hash(
        chunk: &Chunk,
    ): vector<u8> {
        chunk.hash
    }

    public(package) fun size(
        chunk: &Chunk,
    ): u32 {
        chunk.size
    }

    public(package) fun transfer(
        chunk: Chunk,
        recipeint: address,
    ) {
        transfer::transfer(chunk, recipeint);
    }

    public(package) fun create_chunk_cap_id(
        cap: &CreateChunkCap,
    ): ID {
        object::id(cap)
    }
}