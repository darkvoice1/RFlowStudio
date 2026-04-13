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


class DatasetPreviewResponse(BaseModel):
    """定义数据集预览接口的响应结构。"""

    dataset_id: str
    file_name: str
    columns: list[str]
    rows: list[dict[str, str | None]]
    preview_row_count: int
    offset: int
    limit: int
    has_more: bool
    preview_format: Literal["csv", "xlsx"]


class DatasetColumnProfile(BaseModel):
    """定义单个字段的基础元信息结构。"""

    name: str
    inferred_type: Literal["integer", "float", "boolean", "string", "empty"]
    nullable: bool
    missing_count: int
    unique_count: int
    sample_values: list[str]


class DatasetProfileResponse(BaseModel):
    """定义数据集字段元信息接口的响应结构。"""

    dataset_id: str
    file_name: str
    row_count: int
    column_count: int
    columns: list[DatasetColumnProfile]
    profile_format: Literal["csv", "xlsx"]
