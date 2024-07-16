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
from miraifs_sdk.utils import to_mist, calculate_hash_str

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

    def create_chunk(
        self,
        chunk_elements: list[list[int]],
    ):
        txer = SuiTransaction(
            client=self.client,
            compress_inputs=True,
        )

        chunk_element_hashes = txer.make_move_vector(
            items=[SuiString(calculate_hash_str(e)) for e in chunk_elements],
            item_type="u256",
        )

        chunk = txer.move_call(
            target=f"{PACKAGE_ID}::test::new",
            arguments=[chunk_element_hashes],
        )

        for e in chunk_elements:
            txer.move_call(
                target=f"{PACKAGE_ID}::test::insert_data",
                arguments=[chunk],
            )

        return

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
