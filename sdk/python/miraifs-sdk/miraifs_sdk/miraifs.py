from miraifs_sdk import PACKAGE_ID
from miraifs_sdk.sui import Sui
from miraifs_sdk.utils import split_lists_into_sublists
from pydantic import BaseModel
from pysui import handle_result
from pysui.sui.sui_builders.get_builders import (
    GetDynamicFieldObject,
    GetMultipleObjects,
    GetObjectsOwnedByAddress,
)
from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_txresults.single_tx import ObjectRead, ObjectReadPage
from pysui.sui.sui_types import (
    ObjectID,
    SuiAddress,
    SuiString,
    SuiU8,
    SuiU16,
    SuiU64,
    SuiU256,
)


class Chunk(BaseModel):
    data: list[int]
    hash: list[int]
    index: int


class FileObj(BaseModel):
    id: str
    chunk_size: int
    chunks: list["ChunkMapping"]
    create_chunk_caps: list["CreateChunkCapObj"] = []
    extension: str
    mime_type: str
    size: int


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
    key: list[int] | str
    value: str | None = None


class RegisterChunkCapObj(BaseModel):
    id: str
    file_id: str
    chunk_id: str
    chunk_hash: list[int]
    size: int


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

    def register_chunks(
        self,
        file_obj: FileObj,
        chunk_objs: list[ChunkObj],
    ):
        txer = SuiTransaction(
            client=self.client,
            merge_gas_budget=True,
        )
        for obj in chunk_objs:
            txer.move_call(
                target=f"{PACKAGE_ID}::file::receive_and_register_chunk",
                arguments=[
                    ObjectID(file_obj.id),
                    ObjectID(obj.id),
                ],
            )
        result = handle_result(txer.execute(gas_budget=10_000_000_000))
        return result

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

    def delete_file(
        self,
        file: FileObj,
    ):
        txer = SuiTransaction(
            client=self.client,
            merge_gas_budget=True,
        )
        for chunk in file.chunks:
            txer.move_call(
                target=f"{PACKAGE_ID}::file::receive_and_delete_chunk",
                arguments=[
                    ObjectID(file.id),
                    ObjectID(chunk.value),
                ],
            )
        txer.move_call(
            target=f"{PACKAGE_ID}::file::delete",
            arguments=[
                ObjectID(file.id),
            ],
        )
        result = handle_result(
            txer.execute(),
        )
        return result

    def get_chunks_for_file(
        self,
        file: FileObj,
    ):
        chunk_ids = [chunk.value for chunk in file.chunks]
        builder = GetMultipleObjects(object_ids=[ObjectID(id) for id in chunk_ids])
        chunks_raw = handle_result(self.client.execute(builder))
        chunks: list[ChunkObj] = []
        for obj in chunks_raw:
            if isinstance(obj, ObjectRead):
                chunk = ChunkObj(
                    id=obj.object_id,
                    index=obj.content.fields["index"],
                    hash=obj.content.fields["hash"],
                    data=obj.content.fields["data"],
                )
                chunks.append(chunk)
        chunks.sort(key=lambda x: x.index)
        return chunks

    def get_file(
        self,
        file_id: str,
        convert_hashes: bool = True,
    ) -> FileObj:
        file_obj_raw = handle_result(self.client.get_object(ObjectID(file_id)))
        if isinstance(file_obj_raw, ObjectRead):
            chunks = [
                ChunkMapping(
                    key=chunk["fields"]["key"],
                    value=chunk["fields"]["value"],
                )
                for chunk in file_obj_raw.content.fields["chunks"]["fields"]["contents"]
            ]
            if convert_hashes:
                for chunk in chunks:
                    chunk.key = bytes(chunk.key).hex()
            file_obj = FileObj(
                id=file_obj_raw.object_id,
                chunk_size=file_obj_raw.content.fields["chunk_size"],
                chunks=chunks,
                extension=file_obj_raw.content.fields["extension"],
                mime_type=file_obj_raw.content.fields["mime_type"],
                size=file_obj_raw.content.fields["size"],
            )
        try:
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
            if isinstance(create_chunk_cap_df_obj, ObjectRead):
                create_chunk_cap_objs: list[CreateChunkCapObj] = []
                # Split create_chunk_cap_ids into lists of 50 IDs
                # because GetMultipleObjects accepts a maximum of 50 object IDs at a time.
                create_chunk_cap_id_buckets: list[list[str]] = (
                    split_lists_into_sublists(
                        create_chunk_cap_df_obj.content.fields["value"], 50
                    )
                )
                for bucket in create_chunk_cap_id_buckets:
                    create_chunk_cap_objs_raw = handle_result(
                        self.client.execute(
                            GetMultipleObjects(
                                object_ids=[ObjectID(id) for id in bucket]
                            )
                        )
                    )
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
        except Exception:
            pass
        return file_obj

    def get_chunk_objs(
        self,
        file_obj: FileObj,
    ):
        query = {
            "filter": {
                "StructType": f"{PACKAGE_ID}::chunk::Chunk",
            },
            "options": {
                "showType": False,
                "showOwner": False,
                "showPreviousTransaction": False,
                "showDisplay": False,
                "showContent": True,
                "showBcs": False,
                "showStorageRebate": False,
            },
        }
        builder = GetObjectsOwnedByAddress(
            address=SuiAddress(file_obj.id),
            query=query,
        )
        chunk_cap_objs_raw = handle_result(
            self.client.execute(builder),
        )
        if isinstance(chunk_cap_objs_raw, ObjectReadPage):
            chunk_cap_objs: list[ChunkObj] = []
            for obj in chunk_cap_objs_raw.data:
                if isinstance(obj, ObjectRead):
                    chunk_obj = ChunkObj(
                        id=obj.object_id,
                        data=obj.content.fields["data"],
                        index=obj.content.fields["index"],
                        hash=obj.content.fields["hash"],
                    )
                    chunk_cap_objs.append(chunk_obj)

            return chunk_cap_objs
