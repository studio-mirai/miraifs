import asyncio
from miraifs_sdk.sui import Sui, GasCoin
from pydantic import BaseModel
from pysui import handle_result
from pysui.sui.sui_txn.async_transaction import SuiTransactionAsync
from pysui.sui.sui_types import (
    ObjectID,
    SuiString,
    SuiAddress,
    SuiU8,
    SuiU16,
    SuiBoolean,
)
from pysui.sui.sui_builders.get_builders import GetDynamicFieldObject
from miraifs_sdk import PACKAGE_ID
from pysui.sui.sui_txresults.single_tx import ObjectRead, ObjectNotExist
from pysui.sui.sui_txresults.complex_tx import TxResponse


class File(BaseModel):
    id: str
    name: str | None = None
    encoding: str
    mime_type: str
    extension: str
    hash: str
    config: "FileConfig"
    chunks: list["FileChunkMapping"]


class FileConfig(BaseModel):
    chunk_size: int
    sublist_size: int
    compression_algorithm: str | None = None
    compression_level: int | None = None


class FileChunk(BaseModel):
    id: str
    hash: str
    data: list[str]


class CreateFileChunkCap(BaseModel):
    id: str
    hash: str
    file_id: str


class FileChunkMapping(BaseModel):
    key: str
    value: str | None = None


class FileUploadData(BaseModel):
    encoding: str
    mime_type: str
    extension: str
    hash: str
    chunk_size: int
    sublist_size: int
    compression_algorithm: str | None = None
    compression_level: int | None = None
    chunk_hashes: list[str]


class RegisterFileChunkCap(BaseModel):
    id: str
    file_id: str
    chunk_id: str
    chunk_hash: str
    created_with: str


class MiraiFs(Sui):
    def __init__(self) -> None:
        super().__init__()

    async def get_file(
        self,
        file_id: str,
    ) -> File:
        result = handle_result(
            await self.client.get_object(ObjectID(file_id)),
        )
        if isinstance(result, ObjectRead):
            fields = result.content.fields
            file_chunk_mappings = [
                FileChunkMapping(
                    key=chunk["fields"]["key"],
                    value=chunk["fields"]["value"],
                )
                for chunk in fields["chunks"]["fields"]["contents"]
            ]
            file_config = FileConfig(
                chunk_size=fields["config"]["fields"]["chunk_size"],
                sublist_size=fields["config"]["fields"]["sublist_size"],
                compression_algorithm=fields["config"]["fields"][
                    "compression_algorithm"
                ],
                compression_level=fields["config"]["fields"]["compression_level"],
            )
            file = File(
                id=result.object_id,
                name=fields["name"],
                encoding=fields["encoding"],
                mime_type=fields["mime_type"],
                extension=fields["extension"],
                hash=fields["hash"],
                config=file_config,
                chunks=file_chunk_mappings,
            )
            return file
        else:
            return

    async def get_file_chunk(
        self,
        id: str,
    ):
        result = handle_result(
            await self.client.get_object(ObjectID(id)),
        )

        if isinstance(result, ObjectRead):
            file_chunk = FileChunk(
                id=result.object_id,
                hash=result.content.fields["hash"],
                data=result.content.fields["data"],
            )

        return file_chunk

    async def create_file_chunk(
        self,
        create_file_chunk_cap: CreateFileChunkCap,
        chunk: list[str],
        verify_hash_onchain: bool,
        gas_coin: GasCoin | None = None,
    ) -> TxResponse:
        txer = SuiTransactionAsync(
            client=self.client,
            compress_inputs=True,
        )

        chunk_vector = await txer.make_move_vector(
            items=[SuiString(subchunk) for subchunk in chunk],
            item_type="0x1::string::String",
        )

        await txer.move_call(
            target=f"{PACKAGE_ID}::file::create_file_chunk",
            arguments=[
                ObjectID(create_file_chunk_cap.id),
                chunk_vector,
                SuiBoolean(verify_hash_onchain),
            ],
        )

        result = handle_result(
            await txer.execute(
                gas_budget=10_000_000_000,
                use_gas_object=gas_coin.id if gas_coin else None,
            )
        )

        if isinstance(result, TxResponse):
            return result

    async def get_create_image_chunk_cap_ids_for_file(
        self,
        file_id: str,
    ) -> list[str]:
        builder = GetDynamicFieldObject(
            parent_object_id=ObjectID(file_id),
            name={"type": "0x1::string::String", "value": "create_file_chunk_cap_ids"},
        )
        result = handle_result(await self.client.execute(builder))
        if isinstance(result, ObjectRead):
            return result.content.fields["value"]["fields"]["contents"]
        else:
            raise Exception("Failed to get create file chunk cap ids.")

    async def get_create_image_chunk_cap(
        self,
        object_id: str,
    ) -> CreateFileChunkCap:
        result = handle_result(
            await self.client.get_object(ObjectID(object_id)),
        )

        if isinstance(result, ObjectRead):
            return CreateFileChunkCap(
                id=result.object_id,
                hash=result.content.fields["hash"],
                file_id=result.content.fields["file_id"],
            )

    async def get_register_file_chunk_caps_for_file(
        self,
        file_id: str,
    ):
        register_file_chunk_cap_objs = await self.get_owned_objects(
            address=file_id,
            struct_type=f"{PACKAGE_ID}::file::RegisterFileChunkCap",
            show_content=True,
        )

        register_file_chunk_caps: list[RegisterFileChunkCap] = []

        for obj in register_file_chunk_cap_objs:
            if type(obj) == ObjectRead:
                register_file_chunk_caps.append(
                    RegisterFileChunkCap(**obj.content.fields)
                )

        return register_file_chunk_caps

    async def receive_and_register_file_chunks(
        self,
        file: File,
        register_file_chunk_caps: list[RegisterFileChunkCap],
        gas_coin: GasCoin | None = None,
    ):
        txer = SuiTransactionAsync(
            client=self.client,
            compress_inputs=True,
        )

        for cap in register_file_chunk_caps:
            await txer.move_call(
                target=f"{PACKAGE_ID}::file::receive_and_register_file_chunk",
                arguments=[
                    ObjectID(file.id),
                    ObjectID(cap.id),
                ],
            )

        result = handle_result(
            await txer.execute(
                gas_budget=5_000_000_000,
                use_gas_object=ObjectID(gas_coin.id) if gas_coin else None,
            )
        )

        return result

    async def receive_create_file_chunk_caps(
        self,
        file_id: str,
        create_file_chunk_caps: list[CreateFileChunkCap],
        gas_coin: GasCoin | None = None,
    ) -> TxResponse:
        txer = SuiTransactionAsync(
            client=self.client,
            compress_inputs=True,
        )

        for create_file_chunk_cap in create_file_chunk_caps:
            await txer.move_call(
                target=f"{PACKAGE_ID}::file::receive_create_file_chunk_cap",
                arguments=[
                    ObjectID(file_id),
                    ObjectID(create_file_chunk_cap.id),
                ],
            )

        result = handle_result(
            await txer.execute(
                use_gas_object=gas_coin.id if gas_coin else None,
            )
        )

        if isinstance(result, TxResponse):
            return result

    async def initialize_file(
        self,
        upload_data: FileUploadData,
        gas_coin: GasCoin | None = None,
    ):
        txer = SuiTransactionAsync(
            client=self.client,
            compress_inputs=True,
        )

        file_chunk_hashes_vector = await txer.make_move_vector(
            items=[SuiString(hash) for hash in upload_data.chunk_hashes],
            item_type="0x1::string::String",
        )

        if upload_data.compression_algorithm:
            compression_algorithm_opt = await txer.move_call(
                target="0x1::option::some",
                arguments=[SuiString(upload_data.compression_algorithm)],
                type_arguments=["0x1::string::String"],
            )
        else:
            compression_algorithm_opt = await txer.move_call(
                target="0x1::option::none",
                arguments=[],
                type_arguments=["0x1::string::String"],
            )

        if upload_data.compression_level:
            compression_level_opt = await txer.move_call(
                target="0x1::option::some",
                arguments=[SuiU8(upload_data.compression_level)],
                type_arguments=["u8"],
            )
        else:
            compression_level_opt = await txer.move_call(
                target="0x1::option::none",
                arguments=[],
                type_arguments=["u8"],
            )

        file = await txer.move_call(
            target=f"{PACKAGE_ID}::file::initialize_file",
            arguments=[
                SuiString(upload_data.encoding),
                SuiString(upload_data.mime_type),
                SuiString(upload_data.extension),
                SuiString(upload_data.hash),
                SuiU8(upload_data.chunk_size),
                SuiU16(upload_data.sublist_size),
                compression_algorithm_opt,
                compression_level_opt,
                file_chunk_hashes_vector,
            ],
        )

        await txer.transfer_objects(
            transfers=[file],
            recipient=SuiAddress(
                "0x597996d64570b1ba7ec02376995fe126d4430d385a62970f33266d7eb711172f"
            ),
        )

        result = handle_result(
            await txer.execute(
                use_gas_object=gas_coin.id if gas_coin else None,
            )
        )

        return result
