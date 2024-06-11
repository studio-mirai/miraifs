from pydantic import BaseModel


class MfsFile(BaseModel):
    id: str
    name: str | None = None
    encoding: str
    mime_type: str
    extension: str
    hash: str
    config: "MfsFileConfig"
    chunks: list["MfsFileChunkMapping"]


class MfsFileConfig(BaseModel):
    chunk_size: int
    sublist_size: int
    compression_algorithm: str | None = None
    compression_level: int | None = None


class MfsFileChunk(BaseModel):
    id: str
    hash: str
    data: list[str]


class MfsCreateFileChunkCap(BaseModel):
    id: str
    hash: str
    file_id: str


class MfsFileChunkMapping(BaseModel):
    key: str
    value: str | None = None


class MfsFileUploadData(BaseModel):
    encoding: str
    mime_type: str
    extension: str
    size: int
    hash: str
    chunk_size: int
    sublist_size: int
    compression_algorithm: str | None = None
    compression_level: int | None = None
    chunk_hashes: list[str]


class MfsRegisterFileChunkCap(BaseModel):
    id: str
    file_id: str
    chunk_id: str
    chunk_hash: str
    created_with: str
