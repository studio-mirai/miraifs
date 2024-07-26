from miraifs_sdk import MIRAIFS_PACKAGE_ID
from miraifs_sdk.sui import Sui
from miraifs_sdk.utils import split_lists_into_sublists
from pysui import handle_result
from pysui.sui.sui_builders.get_builders import (
    GetDynamicFieldObject,
    GetMultipleObjects,
)
from datetime import datetime, UTC
from pysui.sui.sui_txn.sync_transaction import SuiTransaction, SuiRpcResult
from pysui.sui.sui_txresults.single_tx import ObjectRead
from pysui.sui.sui_txresults.complex_tx import TxResponse
from pysui.sui.sui_types import (
    ObjectID,
    SuiString,
    SuiU8,
    SuiU64,
    SuiU256,
)
from miraifs_sdk.models import (
    File,
    FileChunkManifestItem,
    FileChunks,
    Chunk,
    CreateChunkCap,
    RegisterChunkCap,
    ChunkRaw,
    GasCoin,
)


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
        file: File,
        register_chunk_caps: list[RegisterChunkCap],
    ) -> TxResponse:
        txer = SuiTransaction(
            client=self.client,
            merge_gas_budget=True,
        )
        for cap in register_chunk_caps:
            txer.move_call(
                target=f"{MIRAIFS_PACKAGE_ID}::file::receive_and_register_chunk",
                arguments=[
                    ObjectID(file.id),
                    ObjectID(cap.id),
                ],
            )
        result = handle_result(
            txer.execute(gas_budget=5_000_000_000),
        )
        return result

    def create_chunk(
        self,
        create_chunk_cap: CreateChunkCap,
        chunk: ChunkRaw,
        gas_coin: GasCoin,
    ) -> TxResponse:
        print(f"Creating chunk {create_chunk_cap.index} with Gas Coin {gas_coin.id}...")
        txer = SuiTransaction(
            client=self.client,
            merge_gas_budget=True,
        )
        chunk_arg, verify_chunk_cap_arg = txer.move_call(
            target=f"{MIRAIFS_PACKAGE_ID}::chunk::new",
            arguments=[ObjectID(create_chunk_cap.id)],
        )
        for bucket in split_list(chunk.data):
            vec = [[SuiU8(n) for n in subbucket] for subbucket in bucket]
            # Reverse the chunks because the add_data() function in the smart contract uses pop_back() instead of remove(0).
            vec.reverse()
            txer.move_call(
                target=f"{MIRAIFS_PACKAGE_ID}::chunk::add_data",
                arguments=[
                    chunk_arg,
                    vec,
                ],
            )
        txer.move_call(
            target=f"{MIRAIFS_PACKAGE_ID}::chunk::verify",
            arguments=[
                verify_chunk_cap_arg,
                chunk_arg,
            ],
        )
        result = handle_result(
            txer.execute(
                gas_budget=gas_coin.balance,
                use_gas_object=ObjectID(gas_coin.id),
            ),
        )
        return result

    def create_file(
        self,
        chunk_hashes: list[int],
        chunk_lengths: list[int],
        mime_type: str,
    ) -> TxResponse:
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
            target=f"{MIRAIFS_PACKAGE_ID}::file::new",
            arguments=[
                SuiString(mime_type),
                SuiU64(len(chunk_hashes)),
            ],
        )
        txer.move_call(
            target=f"{MIRAIFS_PACKAGE_ID}::file::add_chunk_hashes",
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
        file: File,
    ) -> TxResponse:
        txer = SuiTransaction(
            client=self.client,
            merge_gas_budget=True,
        )
        for item in file.chunks.manifest:
            txer.move_call(
                target=f"{MIRAIFS_PACKAGE_ID}::file::receive_and_drop_chunk",
                arguments=[
                    ObjectID(file.id),
                    ObjectID(item.id),
                ],
            )
        txer.move_call(
            target=f"{MIRAIFS_PACKAGE_ID}::file::destroy_empty",
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
        file: File,
    ):
        chunk_ids = [chunk.id for chunk in file.chunks.manifest]
        builder = GetMultipleObjects(object_ids=[ObjectID(id) for id in chunk_ids])
        chunk_objs = handle_result(self.client.execute(builder))
        chunks: list[Chunk] = []
        for obj in chunk_objs:
            if isinstance(obj, ObjectRead):
                chunk = Chunk(
                    id=obj.object_id,
                    index=obj.content.fields["index"],
                    hash=obj.content.fields["hash"],
                    data=obj.content.fields["data"],
                    size=obj.content.fields["size"],
                )
                chunks.append(chunk)
        chunks.sort(key=lambda x: x.index)
        return chunks

    def get_file(
        self,
        file_id: str,
    ) -> File:
        file_obj = handle_result(self.client.get_object(ObjectID(file_id)))
        print(file_obj)
        if isinstance(file_obj, ObjectRead):
            manifest: list[FileChunkManifestItem] = []
            for p in file_obj.content.fields["chunks"]["fields"]["manifest"]["fields"]["contents"]:  # fmt: skip
                partition = FileChunkManifestItem(
                    hash=p["fields"]["key"], id=p["fields"]["value"]
                )
                manifest.append(partition)
            file_chunks = FileChunks(
                count=file_obj.content.fields["chunks"]["fields"]["count"],
                hash=file_obj.content.fields["chunks"]["fields"]["hash"],
                manifest=manifest,
                size=file_obj.content.fields["chunks"]["fields"]["size"],
            )
            file = File(
                id=file_obj.object_id,
                chunks=file_chunks,
                created_at=datetime.fromtimestamp(int(file_obj.content.fields["created_at"]) / 1000, tz=UTC),
                extension=file_obj.content.fields["extension"],
                mime_type=file_obj.content.fields["mime_type"],
                size=file_obj.content.fields["size"],
            )  # fmt: skip
        return file

    def freeze_file(
        self,
        file: File,
    ):
        txer = SuiTransaction(
            client=self.client,
            merge_gas_budget=True,
        )
        txer.move_call(
            target="0x2::transfer::public_freeze_object",
            arguments=[ObjectID(file.id)],
            type_arguments=[f"{MIRAIFS_PACKAGE_ID}::file::File"],
        )
        result = handle_result(
            txer.execute(),
        )
        return result

    def get_create_chunk_caps(
        self,
        file_id: str,
    ) -> list[CreateChunkCap]:
        create_chunk_cap_objs: list[CreateChunkCap] = []
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
            create_chunk_cap_ids: list[str] = [
                obj["fields"]["value"]
                for obj in create_chunk_cap_df_obj.content.fields["value"]["fields"]["contents"] # fmt: skip
            ]  # fmt: skip
            # Split create_chunk_cap_ids into lists of 50 IDs
            # because GetMultipleObjects accepts a maximum of 50 object IDs at a time.
            create_chunk_cap_id_buckets: list[list[str]] = split_lists_into_sublists(
                create_chunk_cap_ids, 50
            )
            for bucket in create_chunk_cap_id_buckets:
                create_chunk_cap_objs_raw = handle_result(
                    self.client.execute(
                        GetMultipleObjects(object_ids=[ObjectID(id) for id in bucket])
                    )
                )
                for obj in create_chunk_cap_objs_raw:
                    if isinstance(obj, ObjectRead):
                        create_chunk_cap = CreateChunkCap(
                            id=obj.object_id,
                            file_id=obj.content.fields["file_id"],
                            hash=obj.content.fields["hash"],
                            index=obj.content.fields["index"],
                            owner=obj.owner.address_owner,
                        )
                        create_chunk_cap_objs.append(create_chunk_cap)
            create_chunk_cap_objs.sort(key=lambda x: x.index)
        return create_chunk_cap_objs

    def get_register_chunk_caps(
        self,
        file: File,
    ) -> list[RegisterChunkCap]:
        objs = self.get_owned_objects(
            address=SuiString(file.id),
            struct_type=f"{MIRAIFS_PACKAGE_ID}::chunk::RegisterChunkCap",
            show_content=True,
        )
        register_chunk_caps: list[RegisterChunkCap] = []
        for obj in objs:
            if isinstance(obj, ObjectRead):
                register_chunk_cap = RegisterChunkCap(
                    id=obj.object_id,
                    chunk_id=obj.content.fields["chunk_id"],
                    hash=obj.content.fields["hash"],
                    size=obj.content.fields["size"],
                )
                register_chunk_caps.append(register_chunk_cap)
        return register_chunk_caps
