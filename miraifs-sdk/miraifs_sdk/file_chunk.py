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
from miraifs_sdk.file import MfsFile


class MfsFileChunk(MfsFile):
    def __init__(
        self,
        file_id: str,
        file_chunk_id: str | None = None,
    ) -> None:
        super().__init__()
        self.file_id = file_id
        self.file_chunk_id = file_chunk_id

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
