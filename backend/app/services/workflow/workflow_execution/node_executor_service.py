"""节点执行器解析服务。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable

from app.core.exceptions import ValidationError
from app.schemas.plugin import PluginManifestRecord
from app.schemas.workflow_node import WorkflowNodeDefinitionResponse
from app.services.platform import PluginRegistryService
from app.services.workflow.workflow_execution.executors import (
    DatasetInputExecutor,
    DatasetPreviewExecutor,
)
from app.services.workflow.workflow_execution.executors.base import (
    WorkflowNodeExecutionRequest,
    WorkflowNodeExecutor,
)


class PythonFunctionExecutorAdapter:
    """把插件脚本里的 execute 函数适配成统一执行器。"""

    def __init__(self, execute_fn: Callable[..., Any]) -> None:
        self.execute_fn = execute_fn

    def execute(self, request: WorkflowNodeExecutionRequest) -> dict[str, Any]:
        """调用插件执行函数，并验证返回结果。"""
        try:
            result = self.execute_fn(
                inputs=request.inputs,
                runtime_inputs=request.runtime_inputs,
                config=request.node.config,
                context={
                    "workflow_id": request.workflow.id,
                    "workflow_name": request.workflow.name,
                    "node_id": request.node.id,
                    "node_key": request.node.node_key,
                    "node_type": request.node.node_type,
                },
            )
        except TypeError as exc:
            raise ValidationError("插件执行器的 execute 函数签名不符合平台约定。") from exc

        if not isinstance(result, dict):
            raise ValidationError("插件执行器必须返回 dict 结构的输出结果。")
        return result


class WorkflowNodeExecutorService:
    """统一解析并调用节点执行器。"""

    def __init__(self, plugin_registry_service: PluginRegistryService | None = None) -> None:
        self.plugin_registry_service = plugin_registry_service or PluginRegistryService()
        self._builtin_executors: dict[str, WorkflowNodeExecutor] = {
            "dataset_input": DatasetInputExecutor(),
            "dataset_preview": DatasetPreviewExecutor(),
        }
        self._plugin_executor_cache: dict[str, WorkflowNodeExecutor] = {}

    def execute_node(self, request: WorkflowNodeExecutionRequest) -> dict[str, Any]:
        """解析当前节点执行器并执行。"""
        executor = self._resolve_executor(request.definition)
        return executor.execute(request)

    def _resolve_executor(
        self,
        definition: WorkflowNodeDefinitionResponse,
    ) -> WorkflowNodeExecutor:
        """根据节点来源解析执行器对象。"""
        if definition.source == "builtin":
            executor = self._builtin_executors.get(definition.key)
            if executor is None:
                raise ValidationError(f"内置节点 {definition.key} 尚未接入执行器。")
            return executor

        plugin = self._find_plugin_manifest(definition.key)
        cached_executor = self._plugin_executor_cache.get(plugin.id)
        if cached_executor is not None:
            return cached_executor

        executor = self._load_plugin_executor(plugin)
        self._plugin_executor_cache[plugin.id] = executor
        return executor

    def _find_plugin_manifest(self, node_type: str) -> PluginManifestRecord:
        """按 key 或别名查询插件节点清单。"""
        normalized_node_type = node_type.strip()
        for plugin in self.plugin_registry_service.list_enabled_plugins():
            if plugin.key == normalized_node_type:
                return plugin
            if normalized_node_type in plugin.aliases:
                return plugin
        raise ValidationError(f"插件节点 {normalized_node_type} 的 manifest 不存在。")

    def _load_plugin_executor(self, plugin: PluginManifestRecord) -> WorkflowNodeExecutor:
        """从插件 manifest 指向的执行器文件中加载执行器。"""
        if plugin.executor_kind != "script":
            raise ValidationError(
                f"插件节点 {plugin.key} 的执行器类型 {plugin.executor_kind} 暂不支持。"
            )

        executor_path = Path(plugin.plugin_path) / plugin.executor
        if not executor_path.exists():
            raise ValidationError(
                f"插件节点 {plugin.key} 的执行器文件不存在：{plugin.executor}。"
            )

        module_name = f"workflow_plugin_executor_{plugin.id.replace('-', '_')}"
        spec = importlib.util.spec_from_file_location(module_name, executor_path)
        if spec is None or spec.loader is None:
            raise ValidationError(f"插件节点 {plugin.key} 的执行器无法加载。")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        execute_fn = getattr(module, "execute", None)
        if not callable(execute_fn):
            raise ValidationError(
                f"插件节点 {plugin.key} 的执行器缺少 execute 函数入口。"
            )

        return PythonFunctionExecutorAdapter(execute_fn)
