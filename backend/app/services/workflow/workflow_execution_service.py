from app.core.exceptions import (
    DatasetWorkflowNotFoundError,
    WorkflowNodeExecutionError,
)
from app.schemas.dataset import DatasetDetailResponse
from app.schemas.workflow import (
    DatasetWorkflowNodeExecuteResponse,
)
from app.schemas.workflow_node import (
    WorkflowNodeExecutionContext,
    WorkflowNodeExecutionInput,
)
from app.services.workflow.executors import DatasetInputNodeExecutor
from app.services.workflow.node_registry_service import workflow_node_registry_service


class WorkflowExecutionService:
    """负责按统一协议执行单个工作流节点。"""

    def __init__(self, dataset_detail_loader) -> None:
        self.node_registry_service = workflow_node_registry_service
        self.dataset_detail_loader = dataset_detail_loader

    def execute_node(
        self,
        dataset_id: str,
        workflow_id: str,
        node_record,
        input_values: dict[str, object],
        metadata: dict[str, object],
    ) -> DatasetWorkflowNodeExecuteResponse:
        definition = self.node_registry_service.get_node_definition(node_record.node_type)
        executor = self._build_executor(definition.key)
        payload = WorkflowNodeExecutionInput(
            context=WorkflowNodeExecutionContext(
                dataset_id=dataset_id,
                workflow_id=workflow_id,
                node_id=node_record.id,
                node_key=node_record.node_key,
                node_type=node_record.node_type,
                config=dict(node_record.config),
                metadata=dict(metadata),
            ),
            input_values=dict(input_values),
        )
        output = executor.execute(payload)
        return DatasetWorkflowNodeExecuteResponse(
            workflow_id=workflow_id,
            node_id=node_record.id,
            node_type=node_record.node_type,
            output_values=output.output_values,
            artifacts=output.artifacts,
            summary=output.summary,
        )

    def _build_executor(self, definition_key: str):
        definition = self.node_registry_service.get_node_definition(definition_key)
        if definition_key == DatasetInputNodeExecutor.definition_key:
            return DatasetInputNodeExecutor(
                definition=definition,
                dataset_detail_loader=self._load_dataset_detail,
            )

        raise WorkflowNodeExecutionError(f"节点类型 {definition_key} 暂未接入执行器。")

    def _load_dataset_detail(self, dataset_id: str) -> DatasetDetailResponse:
        detail = self.dataset_detail_loader(dataset_id)
        if not isinstance(detail, DatasetDetailResponse):
            raise DatasetWorkflowNotFoundError("未能读取到数据集详情。")
        return detail
