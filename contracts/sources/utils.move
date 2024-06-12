// Copyright (c) Studio Mirai, Ltd.
// SPDX-License-Identifier: Apache-2.0

module miraifs::utils {

    use std::string::{String, utf8};
    
    use sui::hash::{blake2b256};
    use sui::hex::{Self};

    public(package) fun calculate_hash_for_string(
        str: String,
    ): String {
        let hash_hex_bytes = hex::encode(blake2b256(str.bytes()));
        let hash_str = utf8(hash_hex_bytes);
        
        hash_str
    }

    public(package) fun concatenate_strings(
        mut data: vector<String>,
    ): String {
        let mut result_str = utf8(b"");

        while (!data.is_empty()) {
            result_str.append(data.remove(0));
        };

        result_str
    }

}