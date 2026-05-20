from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

DatasetStatus = Literal["draft", "processing", "ready", "failed"]
DatasetCleaningStepType = Literal[
    "filter",
    "missing_value",
    "sort",
    "derive_variable",
    "recode",
]


class DatasetRecord(BaseModel):
    """定义数据集元信息的持久化结构。"""

    id: str
    name: str
    file_name: str
    extension: str
    stored_path: str
    size_bytes: int
    status: DatasetStatus
    created_at: datetime


class DatasetSummaryResponse(BaseModel):
    """定义数据集摘要信息的响应结构。"""

    id: str
    name: str
    file_name: str
    status: DatasetStatus
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
    status: DatasetStatus
    created_at: datetime


class DatasetDetailResponse(BaseModel):
    """定义数据集详情接口的响应结构。"""

    id: str
    name: str
    file_name: str
    extension: str
    stored_path: str
    size_bytes: int
    status: DatasetStatus
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


class DatasetCleaningStepCreateRequest(BaseModel):
    """定义创建数据清洗步骤时的请求结构。"""

    step_type: DatasetCleaningStepType
    name: str = Field(min_length=1)
    description: str | None = None
    enabled: bool = True
    parameters: dict[str, Any] = Field(default_factory=dict)


class DatasetCleaningStepRecord(BaseModel):
    """定义单个数据清洗步骤的持久化结构。"""

    id: str
    step_type: DatasetCleaningStepType
    name: str
    description: str | None
    enabled: bool
    order: int
    parameters: dict[str, Any]
    created_at: datetime


class DatasetCleaningStepResponse(BaseModel):
    """定义单个数据清洗步骤的响应结构。"""

    id: str
    step_type: DatasetCleaningStepType
    name: str
    description: str | None
    enabled: bool
    order: int
    parameters: dict[str, Any]
    created_at: datetime


class DatasetCleaningStepListResponse(BaseModel):
    """定义数据清洗步骤列表接口的响应结构。"""

    dataset_id: str
    items: list[DatasetCleaningStepResponse]
    total: int


class DatasetCleaningRScriptResponse(BaseModel):
    """定义数据清洗 R 代码草稿接口的响应结构。"""

    dataset_id: str
    file_name: str
    step_count: int
    script: str
