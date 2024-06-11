from pydantic import BaseModel


class File(BaseModel):
    id: str
    name: str | None = None
    encoding: str
    mime_type: str
    extension: str
    hash: str
    config: "FileConfig"
    chunks: list["FileChunkMapping"]


class FileConfig(BaseModel):
    chunk_size: int
    sublist_size: int
    compression_algorithm: str | None = None
    compression_level: int | None = None


class FileChunk(BaseModel):
    id: str
    hash: str
    data: list[str]


class CreateFileChunkCap(BaseModel):
    id: str
    hash: str
    file_id: str


class FileChunkMapping(BaseModel):
    key: str
    value: str | None = None


class FileUploadData(BaseModel):
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


class RegisterFileChunkCap(BaseModel):
    id: str
    file_id: str
    chunk_id: str
    chunk_hash: str
    created_with: str
