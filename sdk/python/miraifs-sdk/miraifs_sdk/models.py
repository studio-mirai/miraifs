from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class FileChunkManifestItem(BaseModel):
    hash: list[int]
    id: Optional[str]


class FileChunks(BaseModel):
    count: int
    hash: list[int]
    manifest: list[FileChunkManifestItem]
    size: int


class File(BaseModel):
    id: str
    chunks: FileChunks
    created_at: datetime
    extension: str
    mime_type: str
    size: int


class Chunk(BaseModel):
    id: str
    data: list[int]
    hash: list[int]
    index: int
    size: int


class ChunkRaw(BaseModel):
    data: list[int]
    hash: list[int]
    index: int


class CreateChunkCap(BaseModel):
    id: str
    file_id: str
    hash: list[int]
    index: int


class RegisterChunkCap(BaseModel):
    id: str
    chunk_id: str
    hash: list[int]
    size: int


class GasCoin(BaseModel):
    id: str
    balance: int
