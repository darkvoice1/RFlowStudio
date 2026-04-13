from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class DatasetRecord(BaseModel):
    """定义数据集元信息的持久化结构。"""

    id: str
    name: str
    file_name: str
    extension: str
    stored_path: str
    size_bytes: int
    status: Literal["draft", "ready", "failed"]
    created_at: datetime


class DatasetSummaryResponse(BaseModel):
    """定义数据集摘要信息的响应结构。"""

    id: str
    name: str
    file_name: str
    status: Literal["draft", "ready", "failed"]
    size_bytes: int
    created_at: datetime


class DatasetListResponse(BaseModel):
    """定义数据集列表接口的响应结构。"""

    items: list[DatasetSummaryResponse]
    total: int


class DatasetUploadCapabilitiesResponse(BaseModel):
    """定义上传能力说明接口的响应结构。"""

    supported_extensions: list[str]
    max_file_size_mb: int
    upload_strategy: Literal["single_file"]


class DatasetUploadResponse(BaseModel):
    """定义上传接口的响应结构。"""

    id: str
    name: str
    file_name: str
    stored_path: str
    size_bytes: int
    status: Literal["draft"]
    created_at: datetime


class DatasetDetailResponse(BaseModel):
    """定义数据集详情接口的响应结构。"""

    id: str
    name: str
    file_name: str
    extension: str
    stored_path: str
    size_bytes: int
    status: Literal["draft", "ready", "failed"]
    created_at: datetime
