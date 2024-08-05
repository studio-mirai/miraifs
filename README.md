# MiraiFS

MiraiFS is an onchain file storage standard for Sui. It lets users upload files of any format directly to the Sui blockchain, where they can be stored permanently. Storing data on MiraiFS is exponentially more expensive than offchain options like IPFS and Amazon S3, so we recommend only using it for storing small files (less than 5MB) when there's a clear benefit to do so.

Suitable use cases for MiraiFS include:

* Storing images for high-end NFT collections like [Prime Machin](https://www.tradeport.xyz/sui/collection/prime-machin).
* Storing documents (unencrypted or encrypted) for proof-of-existence.
* Storing reference files for small JavaScript or CSS libraries like HTMX.

## How it Works

MiraiFS uses two primary object types in a parent-child relationship to store data onchain. A parent `File` object owns one or more `Chunk` child objects that contain up to 128,000 bytes (125KB) of data. The 125KB data limit was specifically chosen to allow for a chunk to be created and verified within a single programmable transaction block, which has a maximum size of 137KB.

```
public struct File has key, store {
    id: UID,
    manifest: Manifest,
    created_at: u64,
    mime_type: String,
    size: u64,
}

public struct Manifest has store {
    count: u32,
    hash: vector<u8>,
    chunks: VecMap<vector<u8>, Option<ID>>,
    size: u32
}

public struct Chunk has key, store {
    id: UID,
    data: vector<u8>,
    hash: vector<u8>,
    index: u16,
    size: u32,
}
```

As you can see from the object types above, `File` objects don't wrap `Chunk` objects directly. Instead, MiraiFS leverages Sui's "transfer to object", and transfer `Chunk` objects directly to their parent `File` object upon creation. Links between `File` and `Chunk` objects are maintained by the `File` object's `Manifest` object, which maintains a directory of associated chunks by storing a mapping between each chunk's blake2b hash (`vector<u8>`) and its object ID (`Option<ID>`). This architecture removes the bottleneck of having to insert `Chunk` objects into a `File` object sequentially, and allows for full parallelization where multiple `Chunk` objects can be created and transferred to its parent `File` object concurrently.