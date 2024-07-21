from pydantic import BaseModel
from pysui import handle_result
from pysui.sui.sui_builders.get_builders import (
    GetDynamicFieldObject,
    GetMultipleObjects,
)
from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_txresults.single_tx import ObjectRead
from pysui.sui.sui_types import (
    ObjectID,
    SuiString,
    SuiU8,
    SuiU16,
    SuiU64,
    SuiU256,
)

from miraifs_sdk import PACKAGE_ID
from miraifs_sdk.sui import Sui


class Chunk(BaseModel):
    data: list[int]
    hash: list[int]
    index: int


class FileObj(BaseModel):
    id: str
    chunk_size: int
    chunks: list["ChunkMapping"]
    create_chunk_caps: list["CreateChunkCapObj"] = []
    mime_type: str


class Compression(BaseModel):
    algorithm: str | None = None
    level: int | None = None


class ChunkObj(BaseModel):
    id: str | None = None
    index: int
    hash: list[int]
    data: list[int]


class CreateChunkCapObj(BaseModel):
    id: str
    file_id: str
    hash: list[int]
    index: int
    owner: str


class ChunkMapping(BaseModel):
    key: list[int]
    value: str | None = None


class RegisterChunkCapObj(BaseModel):
    id: str
    file_id: str
    chunk_id: str
    chunk_hash: int
    created_with: str


def split_list(
    input_list: list[int],
) -> list[list[int]]:
    main_sublists = []
    for i in range(0, len(input_list), 10000):
        chunk = input_list[i : i + 10000]
        sublists = [chunk[j : j + 500] for j in range(0, len(chunk), 500)]
        main_sublists.append(sublists)
    return main_sublists


class MiraiFs(Sui):
    def __init__(self) -> None:
        super().__init__()

    def create_chunk(
        self,
        create_chunk_cap: CreateChunkCapObj,
        chunk: Chunk,
    ):
        txer = SuiTransaction(
            client=self.client,
            merge_gas_budget=True,
        )

        chunk_arg, verify_chunk_cap_arg = txer.move_call(
            target=f"{PACKAGE_ID}::chunk::new",
            arguments=[
                ObjectID(create_chunk_cap.id),
                [SuiU8(n) for n in create_chunk_cap.hash],
                SuiU16(create_chunk_cap.index),
            ],
        )

        for bucket in split_list(chunk.data):
            vec = [[SuiU8(n) for n in subbucket] for subbucket in bucket]
            # Reverse the chunks because the add_data() function in the smart contract uses pop_back() instead of remove(0).
            vec.reverse()
            txer.move_call(
                target=f"{PACKAGE_ID}::chunk::add_data",
                arguments=[
                    chunk_arg,
                    vec,
                ],
            )

        txer.move_call(
            target=f"{PACKAGE_ID}::chunk::verify",
            arguments=[
                verify_chunk_cap_arg,
                chunk_arg,
            ],
        )

        result = handle_result(txer.execute(gas_budget=10_000_000_000))

        return result

    def create_file(
        self,
        chunk_hashes: list[int],
        chunk_lengths: list[int],
        mime_type: str,
    ):
        txer = SuiTransaction(
            client=self.client,
            merge_gas_budget=True,
        )

        chunk_hashes_vector = txer.make_move_vector(
            items=[SuiU256(hash) for hash in chunk_hashes],
            item_type="u256",
        )

        chunk_lengths_vector = txer.make_move_vector(
            items=[SuiU64(length) for length in chunk_lengths],
            item_type="u64",
        )

        file = txer.move_call(
            target=f"{PACKAGE_ID}::file::new",
            arguments=[
                SuiString(mime_type),
                SuiU64(len(chunk_hashes)),
            ],
        )

        txer.move_call(
            target=f"{PACKAGE_ID}::file::add_chunk_hashes",
            arguments=[
                file,
                chunk_hashes_vector,
                chunk_lengths_vector,
            ],
        )

        txer.transfer_objects(
            transfers=[file],
            recipient=self.config.active_address,
        )

        result = handle_result(
            txer.execute(),
        )

        return result

    def get_file(
        self,
        file_id: str,
    ) -> FileObj:
        file_obj_raw = handle_result(self.client.get_object(ObjectID(file_id)))

        create_chunk_cap_df_obj = handle_result(
            self.client.execute(
                GetDynamicFieldObject(
                    parent_object_id=ObjectID(file_id),
                    name={
                        "type": "vector<u8>",
                        "value": [
                            99,
                            114,
                            101,
                            97,
                            116,
                            101,
                            95,
                            99,
                            104,
                            117,
                            110,
                            107,
                            95,
                            99,
                            97,
                            112,
                            95,
                            105,
                            100,
                            115,
                        ],
                    },
                )
            )
        )

        if isinstance(file_obj_raw, ObjectRead):
            chunks = [
                ChunkMapping(
                    key=chunk["fields"]["key"],
                    value=chunk["fields"]["value"],
                )
                for chunk in file_obj_raw.content.fields["chunks"]["fields"]["contents"]
            ]
            file_obj = FileObj(
                id=file_obj_raw.object_id,
                chunk_size=file_obj_raw.content.fields["chunk_size"],
                chunks=chunks,
                mime_type=file_obj_raw.content.fields["mime_type"],
            )

        if isinstance(create_chunk_cap_df_obj, ObjectRead):
            create_chunk_cap_ids = create_chunk_cap_df_obj.content.fields["value"]
            create_chunk_cap_objs_raw = handle_result(
                self.client.execute(
                    GetMultipleObjects(
                        object_ids=[ObjectID(id) for id in create_chunk_cap_ids]
                    )
                )
            )
            create_chunk_cap_objs: list[CreateChunkCapObj] = []
            for obj in create_chunk_cap_objs_raw:
                if isinstance(obj, ObjectRead):
                    create_chunk_cap = CreateChunkCapObj(
                        id=obj.object_id,
                        file_id=obj.content.fields["file_id"],
                        hash=obj.content.fields["hash"],
                        index=obj.content.fields["index"],
                        owner=obj.owner.address_owner,
                    )
                    create_chunk_cap_objs.append(create_chunk_cap)
            create_chunk_cap_objs.sort(key=lambda x: x.index)
            file_obj.create_chunk_caps = create_chunk_cap_objs

        return file_obj
