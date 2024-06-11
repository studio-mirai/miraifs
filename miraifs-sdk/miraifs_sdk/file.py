from miraifs_sdk.miraifs import MiraiFs
from pysui import handle_result
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
from miraifs_sdk.sui import GasCoin
from miraifs_sdk.models import (
    FileUploadData,
    File,
    FileConfig,
    FileChunkMapping,
    CreateFileChunkCap,
    RegisterFileChunkCap,
)


class MfsFile(MiraiFs):
    def __init__(
        self,
        file_id: str | None = None,
    ) -> None:
        super().__init__()
        self.file_id = file_id

    def create(
        self,
        upload_data: FileUploadData,
        recipient: SuiAddress,
        gas_coin: GasCoin | None = None,
    ):
        txer = SuiTransaction(
            client=self.client,
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
                gas_budget=3_00_000_000,
                use_gas_object=gas_coin if gas_coin else None,
            )
        )

        return result

    def delete(
        self,
    ):
        return

    def register_file_chunks(
        self,
        register_file_chunk_caps: list[RegisterFileChunkCap],
        gas_coin: GasCoin | None = None,
    ):
        txer = SuiTransaction(
            client=self.client,
            compress_inputs=True,
        )

        for cap in register_file_chunk_caps:
            txer.move_call(
                target=f"{PACKAGE_ID}::file::receive_and_register_file_chunk",
                arguments=[
                    ObjectID(self.file_id),
                    ObjectID(cap.id),
                ],
            )

        result = handle_result(
            txer.execute(
                gas_budget=5_000_000_000,
                use_gas_object=ObjectID(gas_coin.id) if gas_coin else None,
            ),
        )

        return result

    def receive_create_file_chunk_caps(
        self,
        create_file_chunk_caps: list[CreateFileChunkCap],
        gas_coin: GasCoin | None = None,
    ):
        txer = SuiTransaction(
            client=self.client,
            compress_inputs=True,
        )

        for create_file_chunk_cap in create_file_chunk_caps:
            txer.move_call(
                target=f"{PACKAGE_ID}::file::receive_create_file_chunk_cap",
                arguments=[
                    ObjectID(self.file_id),
                    ObjectID(create_file_chunk_cap.id),
                ],
            )

        result = handle_result(
            txer.execute(
                use_gas_object=gas_coin.id if gas_coin else None,
            ),
        )

        if isinstance(result, TxResponse):
            return result

    def upload(
        self,
    ):
        return

    def view(
        self,
    ):
        result = handle_result(
            self.client.get_object(ObjectID(self.file_id)),
        )

        if not isinstance(result, ObjectRead):
            raise FileNotFoundError(f"File {self.file_id} was not found.")

        file_chunk_mappings = [
            FileChunkMapping(
                key=chunk["fields"]["key"],
                value=chunk["fields"]["value"],
            )
            for chunk in result.content.fields["chunks"]["fields"]["contents"]
        ]

        file_config = FileConfig(
            chunk_size=result.content.fields["config"]["fields"]["chunk_size"],
            sublist_size=result.content.fields["config"]["fields"]["sublist_size"],
            compression_algorithm=result.content.fields["config"]["fields"][
                "compression_algorithm"
            ],
            compression_level=result.content.fields["config"]["fields"][
                "compression_level"
            ],
        )

        file = File(
            id=result.object_id,
            name=result.content.fields["name"],
            encoding=result.content.fields["encoding"],
            mime_type=result.content.fields["mime_type"],
            extension=result.content.fields["extension"],
            hash=result.content.fields["hash"],
            config=file_config,
            chunks=file_chunk_mappings,
        )

        return file

    def get_register_file_chunk_caps(
        self,
    ) -> list[RegisterFileChunkCap]:
        register_file_chunk_cap_objs = self.get_owned_objects(
            address=self.file_id,
            struct_type=f"{PACKAGE_ID}::file::RegisterFileChunkCap",
            show_content=True,
        )

        register_file_chunk_caps: list[RegisterFileChunkCap] = []

        for obj in register_file_chunk_cap_objs:
            if type(obj) == ObjectRead:
                register_file_chunk_caps.append(
                    RegisterFileChunkCap(**obj.content.fields),
                )

        return register_file_chunk_caps
