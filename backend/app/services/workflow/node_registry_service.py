"""工作流节点注册中心。"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from app.core.config import BACKEND_DIR
from app.core.exceptions import ResourceNotFoundError
from app.schemas.workflow_node import (
    WorkflowNodeDefinitionListResponse,
    WorkflowNodeDefinitionResponse,
)
from app.services.platform import PluginRegistryService


class WorkflowNodeRegistryService:
    """维护统一节点目录，并接入已启用插件节点。"""

    def __init__(self, plugin_registry_service: PluginRegistryService | None = None) -> None:
        self.plugin_registry_service = plugin_registry_service or PluginRegistryService()
        self.builtin_nodes_root = BACKEND_DIR / "app" / "nodes" / "builtin"

    def list_node_definitions(self) -> WorkflowNodeDefinitionListResponse:
        """返回当前可见的全部节点定义。"""
        items = list(self._build_visible_definitions().values())
        return WorkflowNodeDefinitionListResponse(items=items, total=len(items))

    def get_node_definition(self, node_type: str) -> WorkflowNodeDefinitionResponse:
        """按 key 或别名返回单个节点定义。"""
        normalized_node_type = node_type.strip()
        definitions_by_key = self._build_visible_definitions()
        alias_to_key = {
            alias: definition.key
            for definition in definitions_by_key.values()
            for alias in definition.aliases
        }
        canonical_key = alias_to_key.get(normalized_node_type, normalized_node_type)
        definition = definitions_by_key.get(canonical_key)
        if definition is None:
            raise ResourceNotFoundError(f"节点类型 {normalized_node_type} 未注册。")
        return definition.model_copy(deep=True)

    def _build_visible_definitions(self) -> dict[str, WorkflowNodeDefinitionResponse]:
        """构造当前对前端可见的统一节点目录。"""
        definitions = {
            key: definition.model_copy(deep=True)
            for key, definition in self._load_builtin_definitions().items()
        }

        for plugin in self.plugin_registry_service.list_enabled_plugins():
            builtin_definition = definitions.get(plugin.key)
            if builtin_definition is not None:
                definitions[plugin.key] = builtin_definition.model_copy(
                    deep=True,
                    update={
                        "source": "plugin",
                        "plugin_id": plugin.id,
                    },
                )
                continue

            definitions[plugin.key] = WorkflowNodeDefinitionResponse(
                key=plugin.key,
                name=plugin.name,
                category=self._map_plugin_category(plugin.category),
                description=plugin.description,
                aliases=plugin.aliases,
                executor_kind=plugin.executor_kind,
                config_schema=plugin.config_schema,
                input_schema=plugin.input_schema,
                output_schema=plugin.output_schema,
                source="plugin",
                plugin_id=plugin.id,
            )

        return definitions

    def _load_builtin_definitions(self) -> dict[str, WorkflowNodeDefinitionResponse]:
        """从 nodes 目录读取全部内置节点定义。"""
        if not self.builtin_nodes_root.exists():
            return {}

        definitions: dict[str, WorkflowNodeDefinitionResponse] = {}
        for node_file_path in sorted(self.builtin_nodes_root.rglob("*.py")):
            if node_file_path.name == "__init__.py":
                continue
            payload = self._load_builtin_node_definition(node_file_path)
            definition = WorkflowNodeDefinitionResponse.model_validate(payload)
            definitions[definition.key] = definition

        return definitions

    def _load_builtin_node_definition(self, node_file_path: Path) -> dict[str, object]:
        """从单文件节点中读取节点定义。"""
        module = self._load_module_from_path(
            file_path=node_file_path,
            module_name_prefix="builtin_node_definition",
        )
        payload = getattr(module, "NODE_DEFINITION", None)
        if not isinstance(payload, dict):
            raise ValueError(f"内置节点文件缺少合法的 NODE_DEFINITION：{node_file_path}")
        return payload

    def _load_module_from_path(self, *, file_path: Path, module_name_prefix: str):
        """按文件路径动态加载 Python 模块。"""
        module_name = f"{module_name_prefix}_{file_path.stem}_{abs(hash(file_path.as_posix()))}"
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise ValueError(f"无法加载节点文件：{file_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _map_plugin_category(self, category: str) -> str:
        """把插件清单分类映射为统一节点分类。"""
        normalized_category = category.strip().lower()
        category_map = {
            "dataset": "inspection",
            "input": "input",
            "inspection": "inspection",
            "cleaning": "transform",
            "transform": "transform",
            "analysis": "analysis",
            "script": "script",
            "report": "output",
            "output": "output",
        }
        return category_map.get(normalized_category, "transform")


workflow_node_registry_service = WorkflowNodeRegistryService()
