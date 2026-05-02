from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

DatasetWorkflowStatus = Literal["draft", "archived"]


class DatasetWorkflowCreateRequest(BaseModel):
    """定义创建数据集工作流时的请求结构。"""

    name: str = Field(min_length=1, max_length=255)
    description: str | None = None


class DatasetWorkflowVersionCreateRequest(BaseModel):
    """定义保存工作流历史版本时的请求结构。"""

    description: str | None = None


class DatasetWorkflowNodeCreateRequest(BaseModel):
    """定义创建工作流节点时的请求结构。"""

    node_key: str = Field(min_length=1, max_length=64)
    node_type: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    position_x: int = 0
    position_y: int = 0


class DatasetWorkflowEdgeCreateRequest(BaseModel):
    """定义创建工作流连线时的请求结构。"""

    edge_key: str = Field(min_length=1, max_length=64)
    source_node_id: str = Field(min_length=1, max_length=32)
    target_node_id: str = Field(min_length=1, max_length=32)
    source_handle: str | None = Field(default=None, max_length=64)
    target_handle: str | None = Field(default=None, max_length=64)
    config: dict[str, Any] = Field(default_factory=dict)


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
    """定义工作流历史版本持久化结构。"""

    id: str
    workflow_id: str
    version_number: int
    description: str | None
    nodes_snapshot: list[dict[str, Any]]
    edges_snapshot: list[dict[str, Any]]
    created_at: datetime


class DatasetWorkflowNodeRecord(BaseModel):
    """定义工作流节点持久化结构。"""

    id: str
    workflow_id: str
    node_key: str
    node_type: str
    name: str
    description: str | None
    config: dict[str, Any]
    position_x: int
    position_y: int
    created_at: datetime
    updated_at: datetime


class DatasetWorkflowEdgeRecord(BaseModel):
    """定义工作流连线持久化结构。"""

    id: str
    workflow_id: str
    edge_key: str
    source_node_id: str
    target_node_id: str
    source_handle: str | None
    target_handle: str | None
    config: dict[str, Any]
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
    """定义单个工作流历史版本响应结构。"""

    id: str
    workflow_id: str
    version_number: int
    description: str | None
    nodes_snapshot: list[dict[str, Any]]
    edges_snapshot: list[dict[str, Any]]
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
    workflow_id: str
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
    items: list[DatasetWorkflowNodeResponse]
    total: int


class DatasetWorkflowEdgeResponse(BaseModel):
    """定义单个工作流连线响应结构。"""

    id: str
    workflow_id: str
    edge_key: str
    source_node_id: str
    target_node_id: str
    source_handle: str | None
    target_handle: str | None
    config: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class DatasetWorkflowEdgeListResponse(BaseModel):
    """定义工作流连线列表接口响应结构。"""

    dataset_id: str
    workflow_id: str
    items: list[DatasetWorkflowEdgeResponse]
    total: int


class DatasetWorkflowDetailResponse(BaseModel):
    """定义工作流详情接口响应结构。"""

    workflow: DatasetWorkflowResponse
    versions: list[DatasetWorkflowVersionResponse]
