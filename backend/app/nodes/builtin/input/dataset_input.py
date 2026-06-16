"""数据输入内置节点。"""

from __future__ import annotations

from typing import Any

NODE_DEFINITION = {
    "key": "dataset_input",
    "name": "数据输入",
    "category": "input",
    "description": "从数据集资源或其他数据源引入数据，作为工作流起点。",
    "aliases": ["input", "source_dataset"],
    "executor_kind": "builtin",
    "config_schema": {
        "type": "object",
        "properties": {
            "source": {
                "type": "string",
                "title": "数据来源",
            }
        },
        "required": ["source"],
        "additionalProperties": True,
    },
    "input_schema": [],
    "output_schema": [
        {
            "key": "dataset_ref",
            "name": "数据引用",
            "data_type": "dataset",
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
) -> dict[str, Any]:
    """执行数据输入节点，产出最小数据引用。"""
    del inputs
    runtime_dataset_ref = runtime_inputs.get("dataset_ref")
    if runtime_dataset_ref is not None:
        return {"dataset_ref": runtime_dataset_ref}

    source = _normalize_source(config.get("source"))
    return {
        "dataset_ref": {
            "source": source,
            "workflow_id": context["workflow_id"],
            "node_id": context["node_id"],
            "node_key": context["node_key"],
        }
    }


def _normalize_source(value: Any) -> str:
    """规范化数据来源配置。"""
    if not isinstance(value, str):
        raise ValueError("数据输入节点缺少合法的 source 配置。")

    normalized = value.strip()
    if not normalized:
        raise ValueError("数据输入节点缺少合法的 source 配置。")
    return normalized
