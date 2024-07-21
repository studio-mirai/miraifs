import logging

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
    SuiBoolean,
    SuiString,
    SuiU8,
    SuiU16,
    SuiU64,
    SuiU256,
)

from miraifs_sdk import PACKAGE_ID
from miraifs_sdk.sui import Sui
from miraifs_sdk.utils import calculate_hash_str, chunk_data

logging.basicConfig(level=logging.INFO)


class File(BaseModel):
    id: str
    chunks: list["ChunkMapping"]
    create_chunk_caps: list["CreateChunkCap"] = []
    mime_type: str


class Compression(BaseModel):
    algorithm: str | None = None
    level: int | None = None


class Chunk(BaseModel):
    id: str | None = None
    index: int
    hash: list[int]
    data: list[int]


class CreateChunkCap(BaseModel):
    id: str
    file_id: str
    hash: list[int]
    index: int
    owner: str


class ChunkMapping(BaseModel):
    key: list[int]
    value: str | None = None


class FileUploadData(BaseModel):
    mime_type: str
    extension: str
    size: int
    hash: int
    compression: Compression
    chunk_hashes: list[int]


class RegisterChunkCap(BaseModel):
    id: str
    file_id: str
    chunk_id: str
    chunk_hash: int
    created_with: str


class MiraiFs(Sui):
    def __init__(self) -> None:
        super().__init__()

    def create_chunk(
        self,
        create_chunk_cap: CreateChunkCap,
        chunk: Chunk,
    ):
        txer = SuiTransaction(
            client=self.client,
            compress_inputs=True,
        )

        chunk = txer.move_call(
            target=f"{PACKAGE_ID}::chunk::new",
            arguments=[
                ObjectID(create_chunk_cap.id),
                [SuiU8(n) for n in create_chunk_cap.hash],
                SuiU16(ObjectID(create_chunk_cap.index)),
            ],
        )

        for bucket in [chunk.data[i : i + 500] for i in range(0, len(chunk.data), 500)]:
            txer.move_call(
                target=f"{PACKAGE_ID}::chunk::add_data",
                arguments=[chunk, [SuiU8(n) for n in bucket]],
            )

        txer.move_call(
            target=f"{PACKAGE_ID}::chunk::add_data",
            arguments=[chunk],
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
            compress_inputs=True,
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
    ):
        file_obj = handle_result(self.client.get_object(ObjectID(file_id)))

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

        if isinstance(file_obj, ObjectRead):
            chunks = [
                ChunkMapping(
                    key=chunk["fields"]["key"],
                    value=chunk["fields"]["value"],
                )
                for chunk in file_obj.content.fields["chunks"]["fields"]["contents"]
            ]
            file = File(
                id=file_obj.object_id,
                chunks=chunks,
                mime_type=file_obj.content.fields["mime_type"],
            )

        if isinstance(create_chunk_cap_df_obj, ObjectRead):
            create_chunk_cap_ids = create_chunk_cap_df_obj.content.fields["value"]
            create_chunk_cap_objs = handle_result(
                self.client.execute(
                    GetMultipleObjects(
                        object_ids=[ObjectID(id) for id in create_chunk_cap_ids]
                    )
                )
            )
            create_chunk_caps: list[CreateChunkCap] = []
            for obj in create_chunk_cap_objs:
                if isinstance(obj, ObjectRead):
                    create_chunk_cap = CreateChunkCap(
                        id=obj.object_id,
                        file_id=obj.content.fields["file_id"],
                        hash=obj.content.fields["hash"],
                        index=obj.content.fields["index"],
                        owner=obj.owner.address_owner,
                    )
                    create_chunk_caps.append(create_chunk_cap)
            create_chunk_caps.sort(key=lambda x: x.index)
            file.create_chunk_caps = create_chunk_caps

        return file
