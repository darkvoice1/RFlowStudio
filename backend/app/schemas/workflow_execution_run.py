"""工作流执行结果协议模型。"""

from typing import Any, Literal

from pydantic import Field

from app.schemas.workflow_definition import WorkflowSchemaModel

WorkflowExecutionRunStatus = Literal["succeeded", "failed"]
WorkflowExecutionStepStatus = Literal["succeeded", "failed"]


class WorkflowExecutionRunRequest(WorkflowSchemaModel):
    """工作流执行请求结构。"""

    node_inputs: dict[str, dict[str, Any]] = Field(default_factory=dict)


class WorkflowExecutionStepResult(WorkflowSchemaModel):
    """单个节点的执行结果。"""

    node_id: str = Field(min_length=1, max_length=32)
    node_key: str = Field(min_length=1, max_length=64)
    node_type: str = Field(min_length=1, max_length=64)
    node_name: str = Field(min_length=1, max_length=255)
    sequence: int = Field(ge=1)
    status: WorkflowExecutionStepStatus
    inputs: dict[str, Any] = Field(default_factory=dict)
    outputs: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None


class WorkflowExecutionFailure(WorkflowSchemaModel):
    """整次执行失败时的节点错误信息。"""

    node_id: str = Field(min_length=1, max_length=32)
    node_key: str = Field(min_length=1, max_length=64)
    node_name: str = Field(min_length=1, max_length=255)
    message: str = Field(min_length=1)


class WorkflowExecutionRunResponse(WorkflowSchemaModel):
    """整张工作流图的一次执行结果。"""

    workflow_id: str = Field(min_length=1, max_length=32)
    workflow_name: str = Field(min_length=1, max_length=255)
    status: WorkflowExecutionRunStatus
    total: int = Field(ge=0)
    succeeded_steps: int = Field(ge=0)
    steps: list[WorkflowExecutionStepResult] = Field(default_factory=list)
    final_outputs: dict[str, dict[str, Any]] = Field(default_factory=dict)
    failure: WorkflowExecutionFailure | None = None
