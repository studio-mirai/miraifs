module miraifs::file {

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

    public(package) fun file_address(
        file: &File,
    ): address {
        object::uid_to_address(&file.id)
    }

    public(package) fun file_id(
        file: &File,
    ): ID {
        object::id(file)
    }

}