from app.core.exceptions import (
    DatasetWorkflowValidationError,
    WorkflowNodeNotFoundError,
)
from app.schemas.workflow_node import (
    WorkflowNodeDefinitionListResponse,
    WorkflowNodeDefinitionResponse,
    WorkflowNodePortSchema,
)


class WorkflowNodeRegistryService:
    """维护平台内置节点目录，并负责节点类型归一化。"""

    def __init__(self) -> None:
        self._definitions = [
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
        ]
        self._definitions_by_key = {
            definition.key: definition for definition in self._definitions
        }
        self._alias_to_key = {
            alias: definition.key
            for definition in self._definitions
            for alias in definition.aliases
        }

    def list_node_definitions(self) -> WorkflowNodeDefinitionListResponse:
        """返回全部节点定义。"""
        items = [definition.model_copy(deep=True) for definition in self._definitions]
        return WorkflowNodeDefinitionListResponse(items=items, total=len(items))

    def get_node_definition(self, node_type: str) -> WorkflowNodeDefinitionResponse:
        """按节点类型或别名读取节点定义。"""
        normalized_node_type = node_type.strip()
        canonical_key = self._alias_to_key.get(normalized_node_type, normalized_node_type)
        definition = self._definitions_by_key.get(canonical_key)
        if definition is None:
            raise WorkflowNodeNotFoundError(f"节点类型 {normalized_node_type} 未注册。")

        return definition.model_copy(deep=True)

    def validate_node_type(self, node_type: str) -> WorkflowNodeDefinitionResponse:
        """校验节点类型是否已注册，并返回规范定义。"""
        try:
            return self.get_node_definition(node_type)
        except WorkflowNodeNotFoundError as exc:
            normalized_node_type = node_type.strip()
            raise DatasetWorkflowValidationError(
                f"节点类型 {normalized_node_type} 未注册。"
            ) from exc


workflow_node_registry_service = WorkflowNodeRegistryService()
