module miraifs::utils {

    use std::debug;
    use std::string::{String};
    
    use sui::bcs::{to_bytes};
    use sui::hash::{blake2b256};

    use miraifs::base64::{Self};

    // Create a unique identifier hash by combining chunk index and chunk bytes hash.
    public(package) fun calculate_chunk_identifier_hash(
        chunk_index: u16,
        chunk_hash: vector<u8>,
    ): vector<u8> {
        let mut chunk_index_bytes = to_bytes<u16>(&chunk_index);
        chunk_index_bytes.reverse();
        let mut v: vector<u8> = vector[];
        v.append(chunk_index_bytes);
        v.append(chunk_hash);
        calculate_hash(&v)
    }

    public(package) fun calculate_hash(
        bytes: &vector<u8>,
    ): vector<u8> {
        blake2b256(bytes)
    }

    public fun transfer_objs<T: key + store>(
        mut objs: vector<T>,
        recipient: address,
    ) {
        while (!objs.is_empty()) {
            transfer::public_transfer(objs.pop_back(), recipient);
        };
        objs.destroy_empty();
    }

    public(package) fun u64_to_string_bytes(
        mut value: u64,
    ): vector<u8> {
        if (value == 0) {
            return b"0"
        };
        let mut buffer = vector[];
        while (value != 0) {
            buffer.push_back(((48 + value % 10) as u8));
            value = value / 10;
        };
        buffer.reverse();
        buffer
    }

    public(package) fun render_b64_svg_image(
        mime_type: String,
        size: u64,
    ): String {
        let mut svg = b"";
        svg.append(b"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 500 500'>");
        svg.append(b"<defs>");
        svg.append(b"<radialGradient id='a' cx='50%' cy='50%' r='50%'>");
        svg.append(b"<stop offset='0' stop-color='#374151'/>");
        svg.append(b"<stop offset='70%' stop-color='#111827'/>");
        svg.append(b"<stop offset='100%' stop-color='#000'/>");
        svg.append(b"</radialGradient>");
        svg.append(b"</defs>");
        svg.append(b"<rect width='500' height='500' fill='url(#a)'/>");
        svg.append(b"<g font-family='ui-monospace, monospace' fill='#fff' text-anchor='middle' dominant-baseline='middle'>");
        svg.append(b"<text x='250' y='250' font-size='50'> font-weight='bold'"); 
        svg.append(mime_type.into_bytes());
        svg.append(b"</text>");
        svg.append(b"<text x='250' y='310' font-size='25'>");
        svg.append(u64_to_string_bytes(size));
        svg.append(b" bytes");
        svg.append(b"</text>");
        svg.append(b"</g>");
        svg.append(b"</svg>");
        base64::encode(svg.to_string())
    }
}