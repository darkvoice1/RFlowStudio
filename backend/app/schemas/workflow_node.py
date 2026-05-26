"""工作流节点目录协议模型。"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

WorkflowNodeCategory = Literal[
    "input",
    "inspection",
    "transform",
    "analysis",
    "script",
    "output",
]
WorkflowNodeExecutorKind = Literal["builtin", "script", "analysis", "report"]
WorkflowNodeSource = Literal["builtin", "plugin"]
WorkflowNodePortPayloadKind = Literal["value", "reference"]


class WorkflowNodeSchemaModel(BaseModel):
    """工作流节点协议模型基类。"""

    model_config = ConfigDict(str_strip_whitespace=True)


class WorkflowNodePortSchema(WorkflowNodeSchemaModel):
    """节点输入输出端口结构。"""

    key: str = Field(min_length=1)
    name: str = Field(min_length=1)
    data_type: str = "any"
    required: bool = False
    description: str | None = None


class WorkflowNodePortResult(WorkflowNodeSchemaModel):
    """节点运行时单个端口的结果结构。"""

    key: str = Field(min_length=1)
    name: str = Field(min_length=1)
    data_type: str = "any"
    payload_kind: WorkflowNodePortPayloadKind = "value"
    value: Any | None = None
    reference: dict[str, Any] | None = None


class WorkflowNodeDefinitionResponse(WorkflowNodeSchemaModel):
    """工作流节点目录中的单个节点定义。"""

    key: str = Field(min_length=1)
    name: str = Field(min_length=1)
    category: WorkflowNodeCategory
    description: str | None = None
    aliases: list[str] = Field(default_factory=list)
    executor_kind: WorkflowNodeExecutorKind
    config_schema: dict[str, Any] = Field(default_factory=dict)
    input_schema: list[WorkflowNodePortSchema] = Field(default_factory=list)
    output_schema: list[WorkflowNodePortSchema] = Field(default_factory=list)
    source: WorkflowNodeSource = "builtin"
    plugin_id: str | None = None


class WorkflowNodeDefinitionListResponse(WorkflowNodeSchemaModel):
    """工作流节点目录列表响应。"""

    items: list[WorkflowNodeDefinitionResponse]
    total: int = Field(ge=0)
