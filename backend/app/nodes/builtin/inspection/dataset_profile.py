"""数据概览内置节点。"""

from __future__ import annotations

from typing import Any

NODE_DEFINITION = {
    "key": "dataset_profile",
    "name": "数据概览",
    "category": "inspection",
    "description": "查看当前数据集的字段统计和基础画像信息。",
    "aliases": ["profile", "data_profile"],
    "executor_kind": "builtin",
    "config_schema": {
        "type": "object",
        "properties": {},
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
            "key": "profile_result",
            "name": "数据概览结果",
            "data_type": "profile",
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
    """执行数据概览节点，返回最小占位画像结果。"""
    del runtime_inputs
    del config
    del context
    dataset_ref = inputs.get("dataset_ref")
    if not isinstance(dataset_ref, dict):
        raise ValueError("数据概览节点缺少合法的 dataset_ref 输入。")

    return {
        "profile_result": {
            "dataset_source": str(dataset_ref.get("source") or "unknown"),
            "column_count": 0,
            "row_count": 0,
            "columns": [],
        }
    }
