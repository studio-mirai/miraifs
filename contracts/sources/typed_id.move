// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module miraifs::typed_id {
    public struct TypedID<phantom T: key> has copy, drop, store {
        id: ID,
    }

    public fun new<T: key>(
        obj: &T,
    ): TypedID<T> {
        TypedID { id: object::id(obj) }
    }

    public fun as_id<T: key>(
        typed_id: &TypedID<T>,
    ): &ID {
        &typed_id.id
    }

    public fun to_id<T: key>(
        typed_id: TypedID<T>,
    ): ID {
        let TypedID { id } = typed_id;
        id
    }

    public fun equals_object<T: key>(
        typed_id: &TypedID<T>,
        obj: &T,
    ): bool {
        typed_id.id == object::id(obj)
    }
}