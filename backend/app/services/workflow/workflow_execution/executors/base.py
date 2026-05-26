"""节点执行器基类协议。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from app.schemas.workflow_definition import (
    WorkflowDefinitionNodeRecord,
    WorkflowDefinitionRecord,
)
from app.schemas.workflow_node import WorkflowNodeDefinitionResponse
from app.schemas.workflow_plan import WorkflowExecutionPlanStep


@dataclass(frozen=True)
class WorkflowNodeExecutionRequest:
    """单个节点执行时的统一上下文。"""

    workflow: WorkflowDefinitionRecord
    node: WorkflowDefinitionNodeRecord
    definition: WorkflowNodeDefinitionResponse
    step: WorkflowExecutionPlanStep
    inputs: dict[str, Any]
    runtime_inputs: dict[str, Any]


class WorkflowNodeExecutor(Protocol):
    """所有节点执行器都要遵循的统一接口。"""

    def execute(self, request: WorkflowNodeExecutionRequest) -> dict[str, Any]:
        """执行当前节点，并返回输出端口结果。"""
