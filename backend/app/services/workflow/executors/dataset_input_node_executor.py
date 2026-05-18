from app.core.exceptions import WorkflowNodeValidationError
from app.schemas.dataset import DatasetDetailResponse
from app.schemas.workflow_node import (
    WorkflowNodeExecutionInput,
    WorkflowNodeExecutionOutput,
)
from app.services.workflow.node_executor_service import (
    WorkflowNodeExecutor,
    require_string_field,
)


class DatasetInputNodeExecutor(WorkflowNodeExecutor):
    """Wrap the current dataset as a workflow input node."""

    definition_key = "dataset_input"

    def __init__(self, definition, dataset_detail_loader) -> None:
        super().__init__(definition)
        self.dataset_detail_loader = dataset_detail_loader

    def validate_config(self, config: dict[str, object]) -> None:
        source = require_string_field(config, "source", "数据输入节点配置")
        if source != "current_dataset":
            raise WorkflowNodeValidationError(
                "数据输入节点当前仅支持 source=current_dataset。"
            )

    def validate_inputs(self, payload: WorkflowNodeExecutionInput) -> None:
        if payload.input_values:
            raise WorkflowNodeValidationError(
                "数据输入节点作为起点节点，当前不接受上游输入。"
            )

    def execute(self, payload: WorkflowNodeExecutionInput) -> WorkflowNodeExecutionOutput:
        self.validate_config(payload.context.config)
        self.validate_inputs(payload)

        dataset_detail = self._load_dataset_detail(payload.context.dataset_id)
        dataset_ref = {
            "dataset_id": dataset_detail.id,
            "name": dataset_detail.name,
            "file_name": dataset_detail.file_name,
            "extension": dataset_detail.extension,
            "stored_path": dataset_detail.stored_path,
            "status": dataset_detail.status,
        }
        return WorkflowNodeExecutionOutput(
            output_values={"dataset_ref": dataset_ref},
            artifacts={"dataset_detail": dataset_detail.model_dump(mode="json")},
            summary=f"已加载数据集 {dataset_detail.name} 作为流程输入。",
        )

    def _load_dataset_detail(self, dataset_id: str) -> DatasetDetailResponse:
        detail = self.dataset_detail_loader(dataset_id)
        if not isinstance(detail, DatasetDetailResponse):
            raise WorkflowNodeValidationError("数据输入节点未能读取到合法的数据集详情。")
        return detail
