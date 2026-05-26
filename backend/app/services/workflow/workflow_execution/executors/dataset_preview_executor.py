"""数据预览节点执行器。"""

from __future__ import annotations

from app.core.exceptions import ValidationError
from app.services.workflow.workflow_execution.executors.base import (
    WorkflowNodeExecutionRequest,
)


class DatasetPreviewExecutor:
    """负责根据数据引用生成最小预览表格。"""

    def execute(self, request: WorkflowNodeExecutionRequest) -> dict[str, object]:
        """返回当前数据集的预览表格。"""
        dataset_ref = request.inputs.get("dataset_ref")
        if not isinstance(dataset_ref, dict):
            raise ValidationError("数据预览节点缺少合法的 dataset_ref 输入。")

        offset = self._normalize_non_negative_int(
            request.node.config.get("offset", 0),
            field_name="offset",
        )
        limit = self._normalize_positive_int(
            request.node.config.get("limit", 20),
            field_name="limit",
        )
        source = str(dataset_ref.get("source") or "unknown")
        rows = [
            {
                "row_index": offset + index,
                "dataset_source": source,
            }
            for index in range(limit)
        ]
        return {
            "preview_table": {
                "offset": offset,
                "limit": limit,
                "columns": ["row_index", "dataset_source"],
                "rows": rows,
            }
        }

    def _normalize_non_negative_int(self, value: object, *, field_name: str) -> int:
        """校验大于等于零的整数配置。"""
        if not isinstance(value, int):
            raise ValidationError(f"数据预览节点的 {field_name} 必须是整数。")
        if value < 0:
            raise ValidationError(f"数据预览节点的 {field_name} 不能小于 0。")
        return value

    def _normalize_positive_int(self, value: object, *, field_name: str) -> int:
        """校验大于零的整数配置。"""
        if not isinstance(value, int):
            raise ValidationError(f"数据预览节点的 {field_name} 必须是整数。")
        if value <= 0:
            raise ValidationError(f"数据预览节点的 {field_name} 必须大于 0。")
        return value
