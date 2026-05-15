from __future__ import annotations

from abc import ABC, abstractmethod

from app.core.exceptions import DatasetWorkflowValidationError
from app.schemas.workflow_node import (
    WorkflowNodeDefinitionResponse,
    WorkflowNodeExecutionInput,
    WorkflowNodeExecutionOutput,
)


class WorkflowNodeExecutor(ABC):
    """定义所有工作流节点执行器都要遵守的统一接口。"""

    definition_key: str

    def __init__(self, definition: WorkflowNodeDefinitionResponse) -> None:
        self.definition = definition

    def validate_config(self, config: dict[str, object]) -> None:
        """校验节点配置是否满足当前执行器的最小要求。"""

    def validate_inputs(self, payload: WorkflowNodeExecutionInput) -> None:
        """校验节点输入是否满足当前执行器的最小要求。"""

    @abstractmethod
    def execute(self, payload: WorkflowNodeExecutionInput) -> WorkflowNodeExecutionOutput:
        """执行节点逻辑并返回统一输出结构。"""


def require_string_field(
    data: dict[str, object],
    field_name: str,
    error_prefix: str,
) -> str:
    """提取并校验字符串字段，避免各执行器重复写样板校验。"""
    value = data.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise DatasetWorkflowValidationError(f"{error_prefix}必须提供 {field_name}。")
    return value.strip()
