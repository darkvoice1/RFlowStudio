from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

DatasetWorkflowStatus = Literal["draft", "archived"]
DatasetWorkflowVersionStatus = Literal["draft", "published"]


class DatasetWorkflowCreateRequest(BaseModel):
    """定义创建数据集工作流时的请求结构。"""

    name: str = Field(min_length=1, max_length=255)
    description: str | None = None


class DatasetWorkflowVersionCreateRequest(BaseModel):
    """定义创建数据集工作流版本时的请求结构。"""

    description: str | None = None
    status: DatasetWorkflowVersionStatus = "draft"


class DatasetWorkflowRecord(BaseModel):
    """定义工作流持久化结构。"""

    id: str
    dataset_id: str
    name: str
    description: str | None
    status: DatasetWorkflowStatus
    created_at: datetime
    updated_at: datetime


class DatasetWorkflowVersionRecord(BaseModel):
    """定义工作流版本持久化结构。"""

    id: str
    workflow_id: str
    version_number: int
    description: str | None
    status: DatasetWorkflowVersionStatus
    created_at: datetime


class DatasetWorkflowResponse(BaseModel):
    """定义单个工作流响应结构。"""

    id: str
    dataset_id: str
    name: str
    description: str | None
    status: DatasetWorkflowStatus
    created_at: datetime
    updated_at: datetime


class DatasetWorkflowListResponse(BaseModel):
    """定义工作流列表接口响应结构。"""

    dataset_id: str
    items: list[DatasetWorkflowResponse]
    total: int


class DatasetWorkflowVersionResponse(BaseModel):
    """定义单个工作流版本响应结构。"""

    id: str
    workflow_id: str
    version_number: int
    description: str | None
    status: DatasetWorkflowVersionStatus
    created_at: datetime


class DatasetWorkflowVersionListResponse(BaseModel):
    """定义工作流版本列表接口响应结构。"""

    dataset_id: str
    workflow_id: str
    items: list[DatasetWorkflowVersionResponse]
    total: int


class DatasetWorkflowDetailResponse(BaseModel):
    """定义工作流详情接口响应结构。"""

    workflow: DatasetWorkflowResponse
    versions: list[DatasetWorkflowVersionResponse]
