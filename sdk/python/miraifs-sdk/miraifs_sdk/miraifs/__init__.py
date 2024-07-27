from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

from miraifs_sdk import MIRAIFS_PACKAGE_ID
from miraifs_sdk.miraifs.txb.chunk import create_chunk_txb, register_chunks_txb
from miraifs_sdk.miraifs.txb.file import create_file_txb
from miraifs_sdk.models import (
    Chunk,
    CreateChunkCap,
    File,
    FileChunkManifestItem,
    FileChunks,
    GasCoin,
    RegisterChunkCap,
)
from miraifs_sdk.sui import Sui
from miraifs_sdk.utils import (
    calculate_chunks_manifest_hash,
    get_mime_type_for_file,
    load_chunks,
    parse_events,
    split_lists_into_sublists,
)
from pysui import handle_result
from pysui.sui.sui_builders.get_builders import (
    GetDynamicFieldObject,
    GetMultipleObjects,
)
from pysui.sui.sui_txresults.complex_tx import TxResponse
from pysui.sui.sui_txresults.single_tx import ObjectRead
from pysui.sui.sui_types import ObjectID, SuiAddress, SuiString


class MiraiFs(Sui):
    def __init__(self) -> None:
        super().__init__()

    # File Write Methods

    def create_file(
        self,
        path: Path,
        chunks: list[Chunk],
        chunk_size: int,
        recipient: SuiAddress,
        gas_coin: GasCoin,
    ) -> tuple[File, Path]:
        chunks_manifest_hash = calculate_chunks_manifest_hash(chunks)

        result = create_file_txb(
            chunk_size=chunk_size,
            chunks=chunks,
            chunks_manifest_hash=chunks_manifest_hash,
            mime_type=get_mime_type_for_file(path),
            recipient=recipient,
            client=self.client,
            gas_coin=gas_coin,
        )

        events = parse_events(result.events)

        if len(events) == 0:
            raise Exception(f"FAIL: {result.effects.transaction_digest}")

        for event in events:
            if event.event_type.endswith("FileCreatedEvent"):
                file_id = event.event_data["file_id"]
                file = self.get_file(file_id)
                return file, path

    def upload_chunks(
        self,
        file: File,
        path: Path,
        concurrency: int,
        gas_coins: list[GasCoin],
    ) -> File:
        """
        Uploads the chunks of a file to the MiraiFS network.

        Args:
            file (File): The file object to upload chunks for.
            path (Path): The path to the file on disk.
            concurrency (int, optional): The number of concurrent uploads to perform. Defaults to 8.
            gas_budget_per_chunk (int, optional): The gas budget per chunk in MIST. Defaults to 3_000_000_000.
        """
        chunks = load_chunks(path, file.chunks.size)
        chunks_by_hash = {bytes(chunk.hash): chunk for chunk in chunks}

        create_chunk_caps = self.get_create_chunk_caps(file.id)

        transaction_digests: list[str] = []
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for create_chunk_cap, gas_coin in zip(create_chunk_caps, gas_coins):
                print(f"Creating chunk {create_chunk_cap.index} with gas coin {gas_coin.id}")  # fmt: skip
                future = executor.submit(
                    create_chunk_txb,
                    create_chunk_cap,
                    chunks_by_hash[bytes(create_chunk_cap.hash)],
                    self.client,
                    gas_coin,
                )
                futures.append(future)
            for future in as_completed(futures):
                result = future.result()
                if isinstance(result, TxResponse):
                    transaction_digests.append(result.effects.transaction_digest)
                    events = parse_events(result.events)
                    for event in events:
                        if event.event_type.endswith("ChunkCreatedEvent"):
                            chunk_id = event.event_data["chunk_id"]
                            print(f"Created chunk {chunk_id}: {result.effects.transaction_digest}")  # fmt: skip

        return file

    def register_chunks(
        self,
        file: File,
        gas_coin: GasCoin,
    ):
        register_chunk_caps = self.get_register_chunk_caps(file)
        result = register_chunks_txb(
            file,
            register_chunk_caps,
            self.client,
            gas_coin,
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
                mime_type=file_obj.content.fields["mime_type"],
                size=file_obj.content.fields["size"],
            )  # fmt: skip
        return file

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
