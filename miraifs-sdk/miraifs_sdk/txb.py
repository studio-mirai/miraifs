from pydantic import BaseModel
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

from miraifs_sdk import PACKAGE_ID
from miraifs_sdk.sui import GasCoin, Sui
from pysui import SyncClient, SuiConfig, handle_result
from miraifs_sdk.models import (
    FileUploadData,
    File,
    FileConfig,
    FileChunkMapping,
    CreateFileChunkCap,
    RegisterFileChunkCap,
)


def create_file(
    client: SyncClient,
    upload_data: FileUploadData,
    recipient: SuiAddress,
    gas_coin: GasCoin | None = None,
):
    txer = SuiTransaction(
        client=client,
        compress_inputs=True,
    )

    if upload_data.compression_algorithm:
        compression_algorithm_opt = txer.move_call(
            target="0x1::option::some",
            arguments=[SuiString(upload_data.compression_algorithm)],
            type_arguments=["0x1::string::String"],
        )
    else:
        compression_algorithm_opt = txer.move_call(
            target="0x1::option::none",
            arguments=[],
            type_arguments=["0x1::string::String"],
        )

    if upload_data.compression_level:
        compression_level_opt = txer.move_call(
            target="0x1::option::some",
            arguments=[SuiU8(upload_data.compression_level)],
            type_arguments=["u8"],
        )
    else:
        compression_level_opt = txer.move_call(
            target="0x1::option::none",
            arguments=[],
            type_arguments=["u8"],
        )

    config = txer.move_call(
        target=f"{PACKAGE_ID}::file::create_file_config",
        arguments=[
            SuiU8(upload_data.chunk_size),
            SuiU16(upload_data.sublist_size),
            compression_algorithm_opt,
            compression_level_opt,
        ],
    )

    file_chunk_hashes_vector = txer.make_move_vector(
        items=[SuiString(hash) for hash in upload_data.chunk_hashes],
        item_type="0x1::string::String",
    )

    file = txer.move_call(
        target=f"{PACKAGE_ID}::file::create_file",
        arguments=[
            SuiString(upload_data.encoding),
            SuiString(upload_data.mime_type),
            SuiString(upload_data.extension),
            SuiU64(upload_data.size),
            SuiString(upload_data.hash),
            config,
            file_chunk_hashes_vector,
            ObjectID("0x6"),
        ],
    )

    txer.transfer_objects(
        transfers=[file],
        recipient=recipient,
    )

    result = handle_result(
        txer.execute(
            use_gas_object=gas_coin.id if gas_coin else None,
        ),
    )

    return result


def create_file_chunk(
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
