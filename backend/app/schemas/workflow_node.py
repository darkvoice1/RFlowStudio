from typing import Any, Literal

from pydantic import BaseModel, Field

WorkflowNodeCategory = Literal[
    "input",
    "inspection",
    "transform",
    "analysis",
    "script",
    "output",
]
WorkflowNodeExecutorKind = Literal["builtin", "script", "analysis", "report"]


class WorkflowNodePortSchema(BaseModel):
    """定义节点输入输出端口的统一结构。"""

    key: str
    name: str
    data_type: str = "any"
    required: bool = False
    description: str | None = None


class WorkflowNodeExecutionContext(BaseModel):
    """定义节点执行时共享的上下文。"""

    dataset_id: str
    workflow_id: str
    node_id: str
    node_key: str
    node_type: str
    workflow_run_id: str | None = None
    upstream_outputs: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkflowNodeExecutionInput(BaseModel):
    """定义节点执行器统一输入结构。"""

    context: WorkflowNodeExecutionContext
    input_values: dict[str, Any] = Field(default_factory=dict)


class WorkflowNodeExecutionRequest(BaseModel):
    """定义节点执行接口请求结构。"""

    input_values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkflowNodeExecutionOutput(BaseModel):
    """定义节点执行器统一输出结构。"""

    output_values: dict[str, Any] = Field(default_factory=dict)
    artifacts: dict[str, Any] = Field(default_factory=dict)
    summary: str | None = None


class WorkflowNodeExecutionResponse(BaseModel):
    """定义节点执行接口响应结构。"""

    workflow_id: str
    node_id: str
    node_type: str
    output_values: dict[str, Any] = Field(default_factory=dict)
    artifacts: dict[str, Any] = Field(default_factory=dict)
    summary: str | None = None


class WorkflowNodeDefinitionResponse(BaseModel):
    """定义节点注册中心返回的节点元数据。"""

    key: str
    name: str
    category: WorkflowNodeCategory
    description: str | None = None
    aliases: list[str] = Field(default_factory=list)
    executor_kind: WorkflowNodeExecutorKind
    config_schema: dict[str, Any] = Field(default_factory=dict)
    input_schema: list[WorkflowNodePortSchema] = Field(default_factory=list)
    output_schema: list[WorkflowNodePortSchema] = Field(default_factory=list)


class WorkflowNodeDefinitionListResponse(BaseModel):
    """定义节点注册中心列表响应。"""

    items: list[WorkflowNodeDefinitionResponse]
    total: int
