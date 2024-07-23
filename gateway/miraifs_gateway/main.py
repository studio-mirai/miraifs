import asyncio
import base64
import httpx
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from miraifs_gateway import CACHE_DIR
import zstandard as zstd


@asynccontextmanager
async def lifespan(
    app: FastAPI,
):
    app.state.http_client = httpx.AsyncClient()
    yield
    await app.state.http_client.aclose()


app = FastAPI(
    lifespan=lifespan,
)


def get_rpc_url(
    network: str,
) -> str:
    if network == "localnet":
        return "http://localhost:9000"
    elif network == "testnet":
        return "https://fullnode.testnet.sui.io:443"
    elif network == "mainnet":
        return "https://fullnode.mainnet.sui.io:443"
    else:
        raise Exception("Invalid network!")


async def do_request(
    client: httpx.AsyncClient,
    payload: dict,
    network: str,
):
    r = await client.post(get_rpc_url(network), json=payload)
    return r


async def get_object(
    client: httpx.AsyncClient,
    object_id: str,
    network: str,
):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sui_getObject",
        "params": [
            object_id,
            {
                "showType": True,
                "showOwner": False,
                "showPreviousTransaction": False,
                "showDisplay": False,
                "showContent": True,
                "showBcs": False,
                "showStorageRebate": False,
            },
        ],
    }

    r = await do_request(client, payload, network)
    obj = r.json()["result"]

    if "error" in obj.keys():
        if obj["error"]["code"] == "notExists":
            return

    return r.json()["result"]


class FileChunk(BaseModel):
    id: str
    index: int
    hash: str
    data: list[str]


@app.get("/{file_id}/")
async def get_file(
    request: Request,
    file_id: str,
    network: str = "mainnet",
):
    file = await get_object(request.app.state.http_client, file_id, network)
    file_hash = file["data"]["content"]["fields"]["hash"]
    file_encoding = file["data"]["content"]["fields"]["encoding"]
    file_extension = file["data"]["content"]["fields"]["extension"]
    file_mime_type = file["data"]["content"]["fields"]["mime_type"]
    file_config = file["data"]["content"]["fields"]["config"]
    file_compression_algorithm = file_config["fields"]["compression_algorithm"]

    output_file_path = Path(CACHE_DIR / file_hash)

    if output_file_path.exists():
        print("File already exists in the cache directory!")
    else:
        print("File is not cached. Pulling from the network.")

        file_chunk_ids = [
            c["fields"]["value"]
            for c in file["data"]["content"]["fields"]["chunks"]["fields"]["contents"]
        ]

        async def _fetch_file_chunk(
            sem: asyncio.Semaphore,
            chunk_id: str,
            network: str,
        ) -> FileChunk:
            async with sem:
                obj = await get_object(request.app.state.http_client, chunk_id, network)
                file_chunk = FileChunk(
                    id=obj["data"]["objectId"],
                    index=obj["data"]["content"]["fields"]["index"],
                    hash=obj["data"]["content"]["fields"]["hash"],
                    data=obj["data"]["content"]["fields"]["data"],
                )
                return file_chunk

        sem = asyncio.Semaphore(8)
        file_chunks: list[FileChunk] = await asyncio.gather(
            *[_fetch_file_chunk(sem, chunk_id, network) for chunk_id in file_chunk_ids]
        )

        file_chunks.sort(key=lambda x: x.index)
        file_as_b85_str = "".join(["".join(chunk.data) for chunk in file_chunks])

        if file_encoding == "base64":
            data = base64.b64decode(file_as_b85_str)
        elif file_encoding == "base85":
            data = base64.b85decode(file_as_b85_str)

        if file_compression_algorithm == "zstd":
            dctx = zstd.ZstdDecompressor()
            data = dctx.decompress(data)

        with open(output_file_path, "wb") as f:
            f.write(data)

    return FileResponse(
        path=output_file_path,
        filename=f"{file_hash}.{file_extension}",
        media_type=file_mime_type,
    )
