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


class MiraiFs(Sui):
    def __init__(self) -> None:
        super().__init__()

    def get_create_image_chunk_cap_ids_for_file(
        self,
        file_id: str,
    ) -> list[str]:
        builder = GetDynamicFieldObject(
            parent_object_id=ObjectID(file_id),
            name={"type": "0x1::string::String", "value": "create_file_chunk_cap_ids"},
        )
        result = handle_result(self.client.execute(builder))
        if isinstance(result, ObjectRead):
            return result.content.fields["value"]["fields"]["contents"]
        else:
            raise Exception("Failed to get create file chunk cap ids.")

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
