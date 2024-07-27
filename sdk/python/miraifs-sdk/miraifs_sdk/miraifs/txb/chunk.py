from miraifs_sdk import MIRAIFS_PACKAGE_ID
from pysui import SyncClient, handle_result
from miraifs_sdk.utils import split_list
from pysui.sui.sui_txn.sync_transaction import SuiTransaction
from pysui.sui.sui_txresults.complex_tx import TxResponse
from pysui.sui.sui_types import (
    ObjectID,
    SuiU8,
)
from miraifs_sdk.models import (
    File,
    CreateChunkCap,
    RegisterChunkCap,
    ChunkRaw,
    GasCoin,
)


def create_chunk_txb(
    create_chunk_cap: CreateChunkCap,
    chunk: ChunkRaw,
    client: SyncClient,
    gas_coin: GasCoin,
) -> TxResponse:
    """
    Create a MiraiFS chunk. This transaction requires an explicit gas coin to be provided
    because it's designed to be used in a multi-threaded environment where the same gas
    coin cannot be used for multiple transactions at the same time.

    Args:
        create_chunk_cap (CreateChunkCap): The capability object to create a chunk.
        chunk (ChunkRaw): The chunk to create.
        client (SyncClient): The Sui client.
        gas_coin (GasCoin): The gas coin to use for the transaction.
    """
    txer = SuiTransaction(
        client=client,
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


def register_chunks_txb(
    file: File,
    register_chunk_caps: list[RegisterChunkCap],
    client: SyncClient,
    gas_budget: int = 2_000_000_000,
) -> TxResponse:
    txer = SuiTransaction(
        client=client,
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
        txer.execute(gas_budget=gas_budget),
    )
    return result
