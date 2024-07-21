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

    public struct RegisterChunkCap has key {
        id: UID,
        chunk_hash: vector<u8>,
        chunk_id: ID,
        file_id: ID,
        size: u32,
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

    // Verification function that's called after all partitions have been added
    // with `add_partition()`. This function performs two verification tasks.
    //
    // 1. Verifies the sum of the chunk partition sizes matches the expected size.
    // 2. Verifies the hash of the partition data matches the expected hash. Hash verification
    //    is optional (determined by the value of `verify_hash` passed to `new()`)
    //    because it's computationally expensive. Based on our local testing,
    //    enabling hash verification requires ~0.3 SUI more gas per chunk.
    public fun verify(
        cap: VerifyChunkCap,
        mut chunk: Chunk,
        ctx: &mut TxContext,
    ) {
        let chunk_bytes_hash = calculate_hash(&chunk.data);
        let chunk_identifier_hash = calculate_chunk_identifier_hash(chunk.index, chunk_bytes_hash);
        assert!(chunk_identifier_hash == chunk.hash, EChunkHashMismatch);

        chunk.size = chunk.data.length() as u32;
       
        let register_chunk_cap = RegisterChunkCap {
            id: object::new(ctx),
            chunk_hash: chunk.hash,
            chunk_id: object::id(&chunk),
            file_id: cap.file_id,
            size: chunk.size,
        };

        transfer::transfer(chunk, cap.file_id.to_address());
        transfer::transfer(register_chunk_cap, cap.file_id.to_address());

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

    public(package) fun delete_register_chunk_cap(
        cap: RegisterChunkCap,
    ) {
        let RegisterChunkCap {
            id,
            chunk_hash: _,
            chunk_id: _,
            file_id: _,
            size: _,
        } = cap;
        id.delete()
    }

    public(package) fun receive_chunk(
        file_id_mut: &mut UID,
        chunk_to_receive: Receiving<Chunk>,
    ): Chunk {
        transfer::receive(file_id_mut, chunk_to_receive)
    }

    public(package) fun receive_register_chunk_cap(
        file_id_mut: &mut UID,
        cap_to_receive: Receiving<RegisterChunkCap>,
    ): RegisterChunkCap {
        transfer::receive(file_id_mut, cap_to_receive)
    }

    public(package) fun chunk_hash(
        chunk: &Chunk,
    ): vector<u8> {
        chunk.hash
    }

    public(package) fun create_chunk_cap_id(
        cap: &CreateChunkCap,
    ): ID {
        object::id(cap)
    }

    public(package) fun register_chunk_cap_chunk_hash(
        cap: &RegisterChunkCap,
    ): vector<u8> {
        cap.chunk_hash
    }

    public(package) fun register_chunk_cap_chunk_id(
        cap: &RegisterChunkCap,
    ): ID {
        cap.chunk_id
    }

    public(package) fun register_chunk_cap_file_id(
        cap: &RegisterChunkCap,
    ): ID {
        cap.file_id
    }

    public(package) fun register_chunk_cap_size(
        cap: &RegisterChunkCap,
    ): u32 {
        cap.size
    }
}