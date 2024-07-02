import logging

from pydantic import BaseModel
from pysui import handle_result
from pysui.sui.sui_builders.get_builders import GetDynamicFieldObject, GetDynamicFields
from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_txresults.complex_tx import TxResponse
from pysui.sui.sui_txresults.single_tx import ObjectRead
from pysui.sui.sui_types import (
    ObjectID,
    SuiBoolean,
    SuiString,
    SuiU8,
    SuiU64,
    SuiU256,
)

from miraifs_sdk import PACKAGE_ID
from miraifs_sdk.sui import GasCoin, Sui
from miraifs_sdk.utils import to_mist

logging.basicConfig(level=logging.INFO)


class File(BaseModel):
    id: str
    chunks: list["ChunkMapping"]
    mime_type: str


class Compression(BaseModel):
    algorithm: str | None = None
    level: int | None = None


class Chunk(BaseModel):
    id: str
    index: int
    hash: int
    data: list[str]


class CreateChunkCap(BaseModel):
    id: str
    index: int
    hash: int
    file_id: str


class ChunkMapping(BaseModel):
    key: str
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

    def create_file(
        self,
        chunk_hashes: list[int],
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

        file = txer.move_call(
            target=f"{PACKAGE_ID}::file::create_file",
            arguments=[
                chunk_hashes_vector,
                SuiString(mime_type),
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
    ) -> File:
        result = handle_result(
            self.client.get_object(ObjectID(file_id)),
        )

        logging.debug(result)

        if isinstance(result, ObjectRead):
            fields = result.content.fields
            chunk_mappings = [
                ChunkMapping(
                    key=chunk["fields"]["key"],
                    value=chunk["fields"]["value"],
                )
                for chunk in fields["chunks"]["fields"]["contents"]
            ]
            file = File(
                id=result.object_id,
                mime_type=fields["mime_type"],
                chunks=chunk_mappings,
            )
            return file
        else:
            return None

    def get_file_chunks(
        self,
        df_id: str,
    ):
        builder = GetDynamicFields(parent_object_id=ObjectID(df_id))
        result = handle_result(self.client.execute(builder))
        return result

    def get_file_chunk(
        self,
        id: str,
    ):
        result = handle_result(
            self.client.get_object(ObjectID(id)),
        )

        if isinstance(result, ObjectRead):
            file_chunk = Chunk(
                id=result.object_id,
                index=result.content.fields["index"],
                hash=result.content.fields["hash"],
                data=result.content.fields["data"],
            )

        return file_chunk

    def create_file_chunk(
        self,
        create_chunk_cap: CreateChunkCap,
        chunk: list[str],
        verify_hash_onchain: bool,
        gas_coin: GasCoin | None = None,
    ) -> TxResponse:
        if verify_hash_onchain:
            gas_budget = to_mist(5)
        else:
            gas_budget = to_mist(1)

        txer = SuiTransaction(
            client=self.client,
            compress_inputs=True,
        )

        chunk_vector = txer.make_move_vector(
            items=[SuiString(subchunk) for subchunk in chunk],
            item_type="0x1::string::String",
        )

        txer.move_call(
            target=f"{PACKAGE_ID}::file::create_file_chunk",
            arguments=[
                ObjectID(create_chunk_cap.id),
                chunk_vector,
                SuiBoolean(verify_hash_onchain),
            ],
        )

        result = handle_result(
            txer.execute(
                gas_budget=gas_budget,
                use_gas_object=gas_coin.id if gas_coin else None,
            ),
        )

        if isinstance(result, TxResponse):
            return result

    def get_create_image_chunk_cap_ids_for_file(
        self,
        file_id: str,
    ) -> list[str]:
        builder = GetDynamicFieldObject(
            parent_object_id=ObjectID(file_id),
            name={"type": "0x1::string::String", "value": "create_chunk_cap_ids"},
        )
        result = handle_result(self.client.execute(builder))
        if isinstance(result, ObjectRead):
            return result.content.fields["value"]["fields"]["contents"]
        else:
            raise Exception("Failed to get create file chunk cap ids.")

    def get_create_image_chunk_cap(
        self,
        object_id: str,
    ) -> CreateChunkCap:
        result = handle_result(
            self.client.get_object(ObjectID(object_id)),
        )

        if isinstance(result, ObjectRead):
            return CreateChunkCap(
                id=result.object_id,
                index=result.content.fields["index"],
                hash=result.content.fields["hash"],
                file_id=result.content.fields["file_id"],
            )

    def get_register_chunk_caps_for_file(
        self,
        file_id: str,
    ):
        register_chunk_cap_objs = self.get_owned_objects(
            address=file_id,
            struct_type=f"{PACKAGE_ID}::file::RegisterChunkCap",
            show_content=True,
        )

        register_chunk_caps: list[RegisterChunkCap] = []

        for obj in register_chunk_cap_objs:
            if type(obj) == ObjectRead:
                register_chunk_caps.append(
                    RegisterChunkCap(**obj.content.fields),
                )

        return register_chunk_caps

    def receive_and_register_file_chunks(
        self,
        file: File,
        register_chunk_caps: list[RegisterChunkCap],
    ):
        txer = SuiTransaction(
            client=self.client,
            compress_inputs=True,
            merge_gas_budget=True,
        )

        for cap in register_chunk_caps:
            txer.move_call(
                target=f"{PACKAGE_ID}::file::register_file_chunk",
                arguments=[
                    ObjectID(file.id),
                    ObjectID(cap.id),
                ],
            )

        result = handle_result(
            txer.execute(
                gas_budget=2_500_000_000,
            ),
        )

        return result

    def receive_create_chunk_caps(
        self,
        file_id: str,
        create_chunk_caps: list[CreateChunkCap],
    ) -> TxResponse:
        txer = SuiTransaction(
            client=self.client,
            compress_inputs=True,
            merge_gas_budget=True,
        )

        for create_chunk_cap in create_chunk_caps:
            txer.move_call(
                target=f"{PACKAGE_ID}::file::receive_create_chunk_cap",
                arguments=[
                    ObjectID(file_id),
                    ObjectID(create_chunk_cap.id),
                ],
            )

        result = handle_result(
            txer.execute(),
        )

        if isinstance(result, TxResponse):
            return result
