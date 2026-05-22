"""工作流定义层的协议模型。"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

WorkflowDefinitionStatus = Literal["draft", "archived"]


class WorkflowSchemaModel(BaseModel):
    """工作流协议模型基类。"""

    model_config = ConfigDict(str_strip_whitespace=True)


class WorkflowDefinitionCreateRequest(WorkflowSchemaModel):
    """创建工作流定义时的请求结构。"""

    name: str = Field(min_length=1, max_length=255)
    description: str | None = None


class WorkflowDefinitionNodePayload(WorkflowSchemaModel):
    """工作流图中单个节点的请求结构。"""

    id: str | None = Field(default=None, min_length=1, max_length=32)
    node_key: str = Field(min_length=1, max_length=64)
    node_type: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    position_x: int = 0
    position_y: int = 0


class WorkflowDefinitionEdgePayload(WorkflowSchemaModel):
    """工作流图中单条连线的请求结构。"""

    id: str | None = Field(default=None, min_length=1, max_length=32)
    edge_key: str = Field(min_length=1, max_length=64)
    source_node_id: str = Field(min_length=1, max_length=32)
    target_node_id: str = Field(min_length=1, max_length=32)
    source_port: str | None = Field(default=None, max_length=64)
    target_port: str | None = Field(default=None, max_length=64)
    config: dict[str, Any] = Field(default_factory=dict)


class WorkflowDefinitionGraphUpdateRequest(WorkflowSchemaModel):
    """整张工作流图的保存请求结构。"""

    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    nodes: list[WorkflowDefinitionNodePayload] = Field(default_factory=list)
    edges: list[WorkflowDefinitionEdgePayload] = Field(default_factory=list)


class WorkflowDefinitionRecord(WorkflowSchemaModel):
    """工作流定义的持久化记录结构。"""

    id: str = Field(min_length=1, max_length=32)
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    status: WorkflowDefinitionStatus
    created_at: datetime
    updated_at: datetime


class WorkflowDefinitionNodeRecord(WorkflowSchemaModel):
    """工作流节点的持久化记录结构。"""

    id: str = Field(min_length=1, max_length=32)
    workflow_id: str = Field(min_length=1, max_length=32)
    node_key: str = Field(min_length=1, max_length=64)
    node_type: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    position_x: int = 0
    position_y: int = 0
    created_at: datetime
    updated_at: datetime


class WorkflowDefinitionEdgeRecord(WorkflowSchemaModel):
    """工作流连线的持久化记录结构。"""

    id: str = Field(min_length=1, max_length=32)
    workflow_id: str = Field(min_length=1, max_length=32)
    edge_key: str = Field(min_length=1, max_length=64)
    source_node_id: str = Field(min_length=1, max_length=32)
    target_node_id: str = Field(min_length=1, max_length=32)
    source_port: str | None = Field(default=None, max_length=64)
    target_port: str | None = Field(default=None, max_length=64)
    config: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class WorkflowDefinitionResponse(WorkflowDefinitionRecord):
    """工作流摘要响应结构。"""


class WorkflowDefinitionListResponse(WorkflowSchemaModel):
    """工作流列表响应结构。"""

    items: list[WorkflowDefinitionResponse]
    total: int = Field(ge=0)


class WorkflowDefinitionNodeResponse(WorkflowDefinitionNodeRecord):
    """工作流节点响应结构。"""


class WorkflowDefinitionNodeListResponse(WorkflowSchemaModel):
    """工作流节点列表响应结构。"""

    workflow_id: str = Field(min_length=1, max_length=32)
    items: list[WorkflowDefinitionNodeResponse]
    total: int = Field(ge=0)


class WorkflowDefinitionEdgeResponse(WorkflowDefinitionEdgeRecord):
    """工作流连线响应结构。"""


class WorkflowDefinitionEdgeListResponse(WorkflowSchemaModel):
    """工作流连线列表响应结构。"""

    workflow_id: str = Field(min_length=1, max_length=32)
    items: list[WorkflowDefinitionEdgeResponse]
    total: int = Field(ge=0)


class WorkflowDefinitionDetailResponse(WorkflowSchemaModel):
    """工作流详情响应结构。"""

    workflow: WorkflowDefinitionResponse
    nodes: list[WorkflowDefinitionNodeResponse]
    edges: list[WorkflowDefinitionEdgeResponse]
