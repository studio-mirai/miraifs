module miraifs::chunk {
    
    use sui::bcs::{to_bytes};

    use miraifs::utils::{calculate_hash};

    public struct Chunk has key, store {
        id: UID,
        partitions: vector<vector<u8>>,
        hash: u256,
        index: u64,
        is_verified: bool,
        size: u64,
    }

    public struct CreateChunkCap has key, store {
        id: UID,
        file_id: ID,
        index: u64,
        hash: u256,
    }

    public struct RegisterChunkCap has key, store {
        id: UID,
        chunk_hash: u256,
        chunk_id: ID,
        file_id: ID,
    }

    public struct VerifyChunkCap {
        chunk_id: ID,
        file_id: ID,
        verify_hash: bool,
    }

    const EChunkHashMismatch: u64 = 1;
    const EChunkLengthMismatch: u64 = 2;

    public fun new(
        cap: CreateChunkCap,
        hash: u256,
        index: u64,
        size: u64,
        verify_hash: bool,
        ctx: &mut TxContext,
    ): (Chunk, VerifyChunkCap) {
        let chunk = Chunk {
            id: object::new(ctx),
            partitions: vector::empty(),
            hash: hash,
            index: index,
            is_verified: false,
            size: size,
        };

        let verify_chunk_cap = VerifyChunkCap {
            chunk_id: object::id(&chunk),
            file_id: cap.file_id,
            verify_hash: verify_hash,
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

    public fun add_partition(
        chunk: &mut Chunk,
        partition: vector<u8>,
    ) {
        chunk.partitions.push_back(partition);
    }

    public fun delete(
        chunk: Chunk,
    ) {
        let Chunk {
            id,
            partitions,
            hash: _,
            index: _,
            is_verified: _,
            size: _,
        } = chunk;

        id.delete();
        partitions.destroy_empty();
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
        let mut chunk_size: u64 = 0;
        let mut concat_partitions: vector<u8> = vector[];
        
        let mut i: u64 = 0;
        while (i < chunk.partitions.length()) {
            let partition = chunk.partitions[i];
            chunk_size = chunk_size + partition.length();
            if (cap.verify_hash == true) {
                concat_partitions.append(partition);
            };
            i = i + 1;
        };
        assert!(chunk_size == chunk.size, EChunkLengthMismatch);

        if (!concat_partitions.is_empty()) {
            let chunk_bytes_hash = calculate_hash(&concat_partitions);
            let chunk_hash = calculate_chunk_identifier_hash(chunk.index, chunk_bytes_hash);
            assert!(chunk_hash == chunk.hash, EChunkHashMismatch);
            chunk.is_verified = true;
        };

        let register_chunk_cap = RegisterChunkCap {
            id: object::new(ctx),
            chunk_hash: chunk.hash,
            chunk_id: object::id(&chunk),
            file_id: cap.file_id,
        };

        transfer::public_transfer(chunk, cap.file_id.to_address());
        transfer::public_transfer(register_chunk_cap, cap.file_id.to_address());

        let VerifyChunkCap {
            chunk_id: _,
            file_id: _,
            verify_hash: _,
        } = cap;
    }

    public(package) fun new_create_chunk_cap(
        file_id: ID,
        hash: u256,
        index: u64,
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
        } = cap;
        id.delete()
    }

    public(package) fun register_chunk_cap_chunk_hash(
        cap: &RegisterChunkCap,
    ): u256 {
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

    // Create a unique identifier hash by combining chunk index and chunk bytes hash.
    fun calculate_chunk_identifier_hash(
        chunk_index: u64,
        chunk_hash: u256,
    ): u256 {
        let mut v: vector<u8> = vector[];
        v.append(to_bytes<u64>(&chunk_index));
        v.append(to_bytes<u256>(&chunk_hash));
        calculate_hash(&v)
    }
}