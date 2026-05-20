from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

WorkflowDefinitionStatus = Literal["draft", "archived"]


class WorkflowDefinitionCreateRequest(BaseModel):
    """定义创建平台级工作流时的请求结构。"""

    name: str = Field(min_length=1, max_length=255)
    description: str | None = None


class WorkflowDefinitionNodePayload(BaseModel):
    """定义工作流图中的单个节点结构。"""

    id: str | None = Field(default=None, min_length=1, max_length=32)
    node_key: str = Field(min_length=1, max_length=64)
    node_type: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    position_x: int = 0
    position_y: int = 0


class WorkflowDefinitionEdgePayload(BaseModel):
    """定义工作流图中的单条边结构。"""

    id: str | None = Field(default=None, min_length=1, max_length=32)
    edge_key: str = Field(min_length=1, max_length=64)
    source_node_id: str = Field(min_length=1, max_length=32)
    target_node_id: str = Field(min_length=1, max_length=32)
    source_port: str | None = Field(default=None, max_length=64)
    target_port: str | None = Field(default=None, max_length=64)
    config: dict[str, Any] = Field(default_factory=dict)


class WorkflowDefinitionGraphUpdateRequest(BaseModel):
    """定义整张工作流图保存请求结构。"""

    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    nodes: list[WorkflowDefinitionNodePayload] = Field(default_factory=list)
    edges: list[WorkflowDefinitionEdgePayload] = Field(default_factory=list)


class WorkflowDefinitionRecord(BaseModel):
    """定义平台级工作流持久化记录。"""

    id: str
    name: str
    description: str | None
    status: WorkflowDefinitionStatus
    created_at: datetime
    updated_at: datetime


class WorkflowDefinitionNodeRecord(BaseModel):
    """定义平台级工作流节点持久化记录。"""

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


class WorkflowDefinitionEdgeRecord(BaseModel):
    """定义平台级工作流边持久化记录。"""

    id: str
    workflow_id: str
    edge_key: str
    source_node_id: str
    target_node_id: str
    source_port: str | None
    target_port: str | None
    config: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class WorkflowDefinitionResponse(BaseModel):
    """定义平台级工作流摘要响应结构。"""

    id: str
    name: str
    description: str | None
    status: WorkflowDefinitionStatus
    created_at: datetime
    updated_at: datetime


class WorkflowDefinitionListResponse(BaseModel):
    """定义平台级工作流列表响应结构。"""

    items: list[WorkflowDefinitionResponse]
    total: int


class WorkflowDefinitionNodeResponse(BaseModel):
    """定义平台级工作流节点响应结构。"""

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


class WorkflowDefinitionNodeListResponse(BaseModel):
    """定义平台级工作流节点列表响应结构。"""

    workflow_id: str
    items: list[WorkflowDefinitionNodeResponse]
    total: int


class WorkflowDefinitionEdgeResponse(BaseModel):
    """定义平台级工作流边响应结构。"""

    id: str
    workflow_id: str
    edge_key: str
    source_node_id: str
    target_node_id: str
    source_port: str | None
    target_port: str | None
    config: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class WorkflowDefinitionEdgeListResponse(BaseModel):
    """定义平台级工作流边列表响应结构。"""

    workflow_id: str
    items: list[WorkflowDefinitionEdgeResponse]
    total: int


class WorkflowDefinitionDetailResponse(BaseModel):
    """定义平台级工作流详情响应结构。"""

    workflow: WorkflowDefinitionResponse
    nodes: list[WorkflowDefinitionNodeResponse]
    edges: list[WorkflowDefinitionEdgeResponse]
