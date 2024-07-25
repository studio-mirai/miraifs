from pydantic import BaseModel
from typing import Optional


class FileChunks(BaseModel):
    count: int
    hash: list[int]
    partitions: dict[list[int], Optional[str]]


class File(BaseModel):
    id: str
    chunks: FileChunks


class Chunk(BaseModel):
    id: str
    data: list[int]
    hash: list[int]
    index: int
    size: int


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
