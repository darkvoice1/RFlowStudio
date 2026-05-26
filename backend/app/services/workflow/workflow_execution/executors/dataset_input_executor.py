"""数据输入节点执行器。"""

from __future__ import annotations

from typing import Any

from app.core.exceptions import ValidationError
from app.services.workflow.workflow_execution.executors.base import (
    WorkflowNodeExecutionRequest,
)


class DatasetInputExecutor:
    """负责生成工作流起点数据引用。"""

    def execute(self, request: WorkflowNodeExecutionRequest) -> dict[str, Any]:
        """返回当前输入节点产出的 dataset_ref。"""
        runtime_dataset_ref = request.runtime_inputs.get("dataset_ref")
        if runtime_dataset_ref is not None:
            return {"dataset_ref": runtime_dataset_ref}

        source = self._normalize_source(request.node.config.get("source"))
        dataset_ref: dict[str, Any] = {
            "source": source,
            "workflow_id": request.workflow.id,
            "node_id": request.node.id,
            "node_key": request.node.node_key,
        }
        return {"dataset_ref": dataset_ref}

    def _normalize_source(self, value: Any) -> str:
        """规范化数据来源配置。"""
        if not isinstance(value, str):
            raise ValidationError("数据输入节点缺少合法的 source 配置。")

        normalized = value.strip()
        if not normalized:
            raise ValidationError("数据输入节点缺少合法的 source 配置。")
        return normalized
