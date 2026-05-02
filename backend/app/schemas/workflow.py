from datetime import datetime
from typing import Any, Literal

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


class DatasetWorkflowNodeCreateRequest(BaseModel):
    """定义创建工作流节点时的请求结构。"""

    node_key: str = Field(min_length=1, max_length=64)
    node_type: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    position_x: int = 0
    position_y: int = 0


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


class DatasetWorkflowNodeRecord(BaseModel):
    """定义工作流节点持久化结构。"""

    id: str
    workflow_version_id: str
    node_key: str
    node_type: str
    name: str
    description: str | None
    config: dict[str, Any]
    position_x: int
    position_y: int
    created_at: datetime
    updated_at: datetime


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


class DatasetWorkflowNodeResponse(BaseModel):
    """定义单个工作流节点响应结构。"""

    id: str
    workflow_version_id: str
    node_key: str
    node_type: str
    name: str
    description: str | None
    config: dict[str, Any]
    position_x: int
    position_y: int
    created_at: datetime
    updated_at: datetime


class DatasetWorkflowNodeListResponse(BaseModel):
    """定义工作流节点列表接口响应结构。"""

    dataset_id: str
    workflow_id: str
    workflow_version_id: str
    items: list[DatasetWorkflowNodeResponse]
    total: int


class DatasetWorkflowDetailResponse(BaseModel):
    """定义工作流详情接口响应结构。"""

    workflow: DatasetWorkflowResponse
    versions: list[DatasetWorkflowVersionResponse]
