from __future__ import annotations

from abc import ABC, abstractmethod

from app.core.exceptions import WorkflowNodeValidationError
from app.schemas.workflow_node import (
    WorkflowNodeDefinitionResponse,
    WorkflowNodeExecutionInput,
    WorkflowNodeExecutionOutput,
)


class WorkflowNodeExecutor(ABC):
    """Define the shared interface for all workflow node executors."""

    definition_key: str

    def __init__(self, definition: WorkflowNodeDefinitionResponse) -> None:
        self.definition = definition

    def validate_config(self, config: dict[str, object]) -> None:
        """Validate whether node config satisfies executor requirements."""

    def validate_inputs(self, payload: WorkflowNodeExecutionInput) -> None:
        """Validate whether node inputs satisfy executor requirements."""

    @abstractmethod
    def execute(self, payload: WorkflowNodeExecutionInput) -> WorkflowNodeExecutionOutput:
        """Execute node logic and return a normalized output payload."""


def require_string_field(
    data: dict[str, object],
    field_name: str,
    error_prefix: str,
) -> str:
    """Read and validate a non-empty string field."""
    value = data.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise WorkflowNodeValidationError(f"{error_prefix}必须提供 {field_name}。")
    return value.strip()
