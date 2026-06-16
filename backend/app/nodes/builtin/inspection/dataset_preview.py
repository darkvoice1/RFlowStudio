"""数据预览内置节点。"""

from __future__ import annotations

from typing import Any

NODE_DEFINITION = {
    "key": "dataset_preview",
    "name": "数据预览",
    "category": "inspection",
    "description": "查看当前数据集的分页预览内容。",
    "aliases": ["preview", "data_preview"],
    "executor_kind": "builtin",
    "config_schema": {
        "type": "object",
        "properties": {
            "offset": {
                "type": "integer",
                "minimum": 0,
                "default": 0,
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "default": 20,
            },
        },
        "additionalProperties": True,
    },
    "input_schema": [
        {
            "key": "dataset_ref",
            "name": "数据引用",
            "data_type": "dataset",
            "required": True,
        }
    ],
    "output_schema": [
        {
            "key": "preview_table",
            "name": "预览表格",
            "data_type": "table",
            "required": True,
        }
    ],
}


def execute(
    *,
    inputs: dict[str, Any],
    runtime_inputs: dict[str, Any],
    config: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, object]:
    """执行数据预览节点，返回最小预览表格。"""
    del runtime_inputs
    del context
    dataset_ref = inputs.get("dataset_ref")
    if not isinstance(dataset_ref, dict):
        raise ValueError("数据预览节点缺少合法的 dataset_ref 输入。")

    offset = _normalize_non_negative_int(config.get("offset", 0), field_name="offset")
    limit = _normalize_positive_int(config.get("limit", 20), field_name="limit")
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


def _normalize_non_negative_int(value: object, *, field_name: str) -> int:
    """校验大于等于零的整数配置。"""
    if not isinstance(value, int):
        raise ValueError(f"数据预览节点的 {field_name} 必须是整数。")
    if value < 0:
        raise ValueError(f"数据预览节点的 {field_name} 不能小于 0。")
    return value


def _normalize_positive_int(value: object, *, field_name: str) -> int:
    """校验大于零的整数配置。"""
    if not isinstance(value, int):
        raise ValueError(f"数据预览节点的 {field_name} 必须是整数。")
    if value <= 0:
        raise ValueError(f"数据预览节点的 {field_name} 必须大于 0。")
    return value
