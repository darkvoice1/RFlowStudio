from app.core.exceptions import WorkflowNodeNotFoundError, WorkflowNodeValidationError
from app.schemas.plugin import PluginResponse
from app.schemas.workflow_node import (
    WorkflowNodeDefinitionListResponse,
    WorkflowNodeDefinitionResponse,
    WorkflowNodePortSchema,
)
from app.services.platform.plugin_registry_service import PluginRegistryService


class WorkflowNodeRegistryService:
    """维护平台工作流节点目录，并接入已启用插件节点。"""

    def __init__(self) -> None:
        """初始化核心节点协议模板和插件注册表。"""
        self.plugin_registry_service = PluginRegistryService()
        self._compatibility_definition_keys = {
            "cleaning_step",
            "analysis_step",
            "report_step",
        }
        self._definitions_by_key = self._build_definition_templates()
        self._plugin_backed_keys = {
            "dataset_input",
            "dataset_preview",
            "dataset_profile",
            "filter_rows",
            "fill_missing",
            "sort_rows",
            "recode_values",
            "descriptive_statistics",
            "correlation_analysis",
            "t_test",
            "anova",
            "r_script",
            "html_report",
        }
        self._base_alias_to_key = {
            alias: definition.key
            for definition in self._definitions_by_key.values()
            for alias in definition.aliases
        }

    def _build_definition_templates(self) -> dict[str, WorkflowNodeDefinitionResponse]:
        """构建平台当前支持的节点协议模板。"""
        definitions = [
            WorkflowNodeDefinitionResponse(
                key="dataset_input",
                name="数据输入",
                category="input",
                description="从当前数据集或外部来源引入数据，作为流程起点。",
                aliases=["input", "source_dataset"],
                executor_kind="builtin",
                config_schema={
                    "type": "object",
                    "properties": {
                        "source": {
                            "type": "string",
                            "title": "数据来源",
                            "description": "例如 current_dataset。",
                        }
                    },
                    "required": ["source"],
                    "additionalProperties": True,
                },
                input_schema=[],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                        description="向下游节点传递的数据集引用。",
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="dataset_preview",
                name="数据预览",
                category="inspection",
                description="查看当前数据的分页预览和基本内容。",
                aliases=["preview", "data_preview"],
                executor_kind="builtin",
                config_schema={
                    "type": "object",
                    "properties": {
                        "offset": {"type": "integer", "minimum": 0, "default": 0},
                        "limit": {"type": "integer", "minimum": 1, "default": 20},
                    },
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="preview_table",
                        name="预览表格",
                        data_type="table",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="dataset_profile",
                name="数据概览",
                category="inspection",
                description="查看当前数据集的字段统计和基础画像信息。",
                aliases=["profile", "data_profile"],
                executor_kind="builtin",
                config_schema={
                    "type": "object",
                    "properties": {},
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="profile_result",
                        name="数据概览结果",
                        data_type="profile",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="cleaning_step",
                name="清洗步骤",
                category="transform",
                description="执行过滤、缺失值处理、排序、重编码或派生变量。",
                aliases=["cleaning", "clean_step"],
                executor_kind="builtin",
                config_schema={
                    "type": "object",
                    "properties": {
                        "step_type": {
                            "type": "string",
                            "enum": [
                                "filter",
                                "missing_value",
                                "sort",
                                "recode",
                                "derive_variable",
                            ],
                        },
                        "parameters": {"type": "object"},
                    },
                    "required": ["step_type", "parameters"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="cleaned_dataset_ref",
                        name="清洗后数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="filter_rows",
                name="筛选行",
                category="transform",
                description="根据条件筛选数据行，输出筛选后的数据集引用。",
                aliases=["cleaning_filter"],
                executor_kind="builtin",
                config_schema={
                    "type": "object",
                    "properties": {
                        "column": {"type": "string"},
                        "operator": {"type": "string"},
                        "value": {},
                    },
                    "required": ["column", "operator"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="筛选后数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="fill_missing",
                name="缺失值处理",
                category="transform",
                description="对缺失值执行删除、填充或标记处理。",
                aliases=["missing_value"],
                executor_kind="builtin",
                config_schema={
                    "type": "object",
                    "properties": {
                        "method": {"type": "string"},
                        "columns": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["method"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="处理后数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="sort_rows",
                name="排序",
                category="transform",
                description="按指定字段对数据集进行排序。",
                aliases=["dataset_sort"],
                executor_kind="builtin",
                config_schema={
                    "type": "object",
                    "properties": {
                        "column": {"type": "string"},
                        "direction": {
                            "type": "string",
                            "enum": ["asc", "desc"],
                        },
                    },
                    "required": ["column"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="排序后数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="recode_values",
                name="重编码",
                category="transform",
                description="按映射规则批量重编码字段值。",
                aliases=["dataset_recode"],
                executor_kind="builtin",
                config_schema={
                    "type": "object",
                    "properties": {
                        "column": {"type": "string"},
                        "mapping": {"type": "object"},
                    },
                    "required": ["column", "mapping"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="重编码后数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="analysis_step",
                name="统计分析",
                category="analysis",
                description="执行描述统计、相关分析、t 检验、方差分析或卡方检验。",
                aliases=["analysis", "analysis_node"],
                executor_kind="analysis",
                config_schema={
                    "type": "object",
                    "properties": {
                        "analysis_type": {
                            "type": "string",
                            "enum": [
                                "descriptive_statistics",
                                "correlation_analysis",
                                "independent_samples_t_test",
                                "one_way_anova",
                                "chi_square_test",
                            ],
                        },
                        "variables": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "group_variable": {"type": ["string", "null"]},
                        "options": {"type": "object"},
                    },
                    "required": ["analysis_type", "variables"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="cleaned_dataset_ref",
                        name="清洗后数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="analysis_result",
                        name="分析结果",
                        data_type="analysis_result",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="descriptive_statistics",
                name="描述统计",
                category="analysis",
                description="对指定变量执行描述统计分析。",
                aliases=["stats_descriptive"],
                executor_kind="analysis",
                config_schema={
                    "type": "object",
                    "properties": {
                        "variables": {
                            "type": "array",
                            "items": {"type": "string"},
                        }
                    },
                    "required": ["variables"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="analysis_result",
                        name="分析结果",
                        data_type="analysis_result",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="correlation_analysis",
                name="相关分析",
                category="analysis",
                description="对多个变量执行相关分析。",
                aliases=["stats_correlation"],
                executor_kind="analysis",
                config_schema={
                    "type": "object",
                    "properties": {
                        "variables": {
                            "type": "array",
                            "items": {"type": "string"},
                        }
                    },
                    "required": ["variables"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="analysis_result",
                        name="分析结果",
                        data_type="analysis_result",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="t_test",
                name="独立样本 t 检验",
                category="analysis",
                description="对目标变量和分组变量执行独立样本 t 检验。",
                aliases=["independent_samples_t_test"],
                executor_kind="analysis",
                config_schema={
                    "type": "object",
                    "properties": {
                        "target_variable": {"type": "string"},
                        "group_variable": {"type": "string"},
                    },
                    "required": ["target_variable", "group_variable"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="analysis_result",
                        name="分析结果",
                        data_type="analysis_result",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="anova",
                name="单因素方差分析",
                category="analysis",
                description="对目标变量和分组变量执行单因素方差分析。",
                aliases=["one_way_anova"],
                executor_kind="analysis",
                config_schema={
                    "type": "object",
                    "properties": {
                        "target_variable": {"type": "string"},
                        "group_variable": {"type": "string"},
                    },
                    "required": ["target_variable", "group_variable"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="dataset_ref",
                        name="数据引用",
                        data_type="dataset",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="analysis_result",
                        name="分析结果",
                        data_type="analysis_result",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="r_script",
                name="R 脚本",
                category="script",
                description="生成或承接 R 脚本文本，供执行或展示使用。",
                aliases=["script", "r_script_node"],
                executor_kind="script",
                config_schema={
                    "type": "object",
                    "properties": {
                        "script": {"type": "string", "minLength": 1},
                    },
                    "required": ["script"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="workflow_context",
                        name="流程上下文",
                        data_type="context",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="script_text",
                        name="脚本文本",
                        data_type="text",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="report_step",
                name="报告输出",
                category="output",
                description="将分析结果渲染为 HTML 报告或后续导出产物。",
                aliases=["report", "report_node"],
                executor_kind="report",
                config_schema={
                    "type": "object",
                    "properties": {
                        "template_key": {
                            "type": "string",
                            "enum": ["general", "teaching", "research"],
                        },
                        "export_format": {
                            "type": "string",
                            "enum": ["html", "pdf"],
                        },
                    },
                    "required": ["template_key"],
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="analysis_result",
                        name="分析结果",
                        data_type="analysis_result",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="report_html",
                        name="HTML 报告",
                        data_type="html",
                        required=True,
                    )
                ],
            ),
            WorkflowNodeDefinitionResponse(
                key="html_report",
                name="HTML 报告",
                category="output",
                description="把分析结果渲染成 HTML 报告。",
                aliases=["analysis_report"],
                executor_kind="report",
                config_schema={
                    "type": "object",
                    "properties": {
                        "template_key": {"type": "string"},
                    },
                    "additionalProperties": True,
                },
                input_schema=[
                    WorkflowNodePortSchema(
                        key="analysis_result",
                        name="分析结果",
                        data_type="analysis_result",
                        required=True,
                    )
                ],
                output_schema=[
                    WorkflowNodePortSchema(
                        key="report_html",
                        name="HTML 报告",
                        data_type="html",
                        required=True,
                    )
                ],
            ),
        ]
        return {definition.key: definition for definition in definitions}

    def list_node_definitions(self) -> WorkflowNodeDefinitionListResponse:
        """返回当前对前端可见的工作流节点目录。"""
        items = list(self._build_visible_definitions().values())
        return WorkflowNodeDefinitionListResponse(items=items, total=len(items))

    def get_node_definition(self, node_type: str) -> WorkflowNodeDefinitionResponse:
        """按节点类型或别名返回单个节点定义。"""
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
            raise WorkflowNodeNotFoundError(f"节点类型 {normalized_node_type} 未注册。")

        return definition.model_copy(deep=True)

    def validate_node_type(self, node_type: str) -> WorkflowNodeDefinitionResponse:
        """验证节点类型是否已经注册到目录中。"""
        try:
            return self.get_node_definition(node_type)
        except WorkflowNodeNotFoundError as exc:
            normalized_node_type = node_type.strip()
            raise WorkflowNodeValidationError(
                f"节点类型 {normalized_node_type} 未注册。"
            ) from exc

    def _build_visible_definitions(self) -> dict[str, WorkflowNodeDefinitionResponse]:
        """按插件状态过滤并输出当前可见节点目录。"""
        plugin_index = self._build_plugin_index()
        has_registered_plugins = bool(plugin_index)
        visible_definitions: dict[str, WorkflowNodeDefinitionResponse] = {}

        for key, definition in self._definitions_by_key.items():
            plugin = plugin_index.get(key)

            if key in self._plugin_backed_keys:
                # 还没同步插件时先保留兼容行为，避免旧接口和测试立刻失效。
                if not has_registered_plugins:
                    visible_definitions[key] = definition.model_copy(deep=True)
                    continue
                if plugin is None:
                    continue
                visible_definitions[key] = definition.model_copy(
                    deep=True,
                    update={
                        "source": "plugin",
                        "plugin_id": plugin.id,
                    },
                )
                continue

            visible_definitions[key] = definition.model_copy(deep=True)

        return visible_definitions

    def _build_plugin_index(self) -> dict[str, PluginResponse]:
        """把启用插件列表整理成 node_key -> plugin 的索引。"""
        plugin_index: dict[str, PluginResponse] = {}
        for plugin in self.plugin_registry_service.list_enabled_plugins():
            plugin_index[self._extract_node_key(plugin.id)] = plugin
        return plugin_index

    def _extract_node_key(self, plugin_id: str) -> str:
        """从插件 id 中提取节点 key。"""
        _, _, node_key = plugin_id.partition("-")
        return node_key or plugin_id


workflow_node_registry_service = WorkflowNodeRegistryService()
