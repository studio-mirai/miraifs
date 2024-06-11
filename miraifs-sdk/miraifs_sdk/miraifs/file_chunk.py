from typing_extensions import Unpack
from miraifs_sdk.txb import create_file
from miraifs_sdk.miraifs import MiraiFs
from pydantic import ConfigDict
from pysui import handle_result
from pysui.sui.sui_builders.get_builders import GetDynamicFieldObject
from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_txresults.complex_tx import TxResponse
from pysui.sui.sui_txresults.single_tx import ObjectRead
from pysui.sui.sui_types import (
    ObjectID,
    SuiAddress,
    SuiBoolean,
    SuiString,
    SuiU8,
    SuiU16,
    SuiU64,
)

from miraifs_sdk.exceptions import FileNotFoundError
from miraifs_sdk import PACKAGE_ID
from miraifs_sdk.sui import GasCoin, Sui
from miraifs_sdk.models import (
    FileUploadData,
    File,
    FileChunk,
    FileConfig,
    FileChunkMapping,
    CreateFileChunkCap,
    RegisterFileChunkCap,
)
from miraifs_sdk.miraifs.file import MfsFile


class MfsFileChunk(MfsFile):
    def __init__(
        self,
        file_id: str,
        file_chunk_id: str | None = None,
    ) -> None:
        super().__init__()
        self.file_id = file_id
        self.file_chunk_id = file_chunk_id

    def create(
        self,
        create_file_chunk_cap: CreateFileChunkCap,
        chunk: list[str],
        verify_hash_onchain: bool,
        gas_coin: GasCoin | None = None,
    ) -> TxResponse:
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
                ObjectID(create_file_chunk_cap.id),
                chunk_vector,
                SuiBoolean(verify_hash_onchain),
            ],
        )

        result = handle_result(
            txer.execute(
                gas_budget=5_000_000_000,
                use_gas_object=gas_coin.id if gas_coin else None,
            ),
        )

        if isinstance(result, TxResponse):
            return result

    def view(
        self,
    ) -> FileChunk:
        result = handle_result(
            self.client.get_object(
                ObjectID(self.file_chunk_id),
            ),
        )

        if not isinstance(result, ObjectRead):
            raise FileNotFoundError

        file_chunk = FileChunk(
            id=result.object_id,
            hash=result.content.fields["hash"],
            data=result.content.fields["data"],
        )

        return file_chunk

    def get_create_image_chunk_cap(
        self,
        object_id: str,
    ) -> CreateFileChunkCap:
        result = handle_result(
            self.client.get_object(ObjectID(object_id)),
        )

        if isinstance(result, ObjectRead):
            return CreateFileChunkCap(
                id=result.object_id,
                hash=result.content.fields["hash"],
                file_id=result.content.fields["file_id"],
            )
