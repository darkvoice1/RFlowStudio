from fastapi import APIRouter, HTTPException, status

from app.core.exceptions import (
    DatasetNotFoundError,
    WorkflowDefinitionNotFoundError,
    WorkflowDefinitionValidationError,
    WorkflowNodeNotFoundError,
    WorkflowNodeValidationError,
)
from app.schemas.workflow_definition import (
    WorkflowDefinitionCreateRequest,
    WorkflowDefinitionNodePayload,
    WorkflowDefinitionNodeResponse,
    WorkflowDefinitionResponse,
)
from app.schemas.workflow_node import (
    WorkflowNodeExecutionContext,
    WorkflowNodeExecutionInput,
    WorkflowNodeExecutionRequest,
    WorkflowNodeExecutionResponse,
)
from app.services.resources.datasets.dataset_resource_service import dataset_service
from app.services.workflow.executors import DatasetInputNodeExecutor
from app.services.workflow.node_registry_service import workflow_node_registry_service
from app.services.workflow.workflow_definition_service import WorkflowDefinitionService

router = APIRouter()
workflow_definition_service = WorkflowDefinitionService()


@router.post(
    "/{dataset_id}/workflows",
    response_model=WorkflowDefinitionResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建数据集工作流",
)
def create_dataset_workflow(
    dataset_id: str,
    payload: WorkflowDefinitionCreateRequest,
) -> WorkflowDefinitionResponse:
    """兼容数据集作用域的工作流创建入口。"""
    try:
        dataset_service.get_dataset_detail(dataset_id)
        return workflow_definition_service.create_workflow(payload)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/workflows/{workflow_id}/nodes",
    response_model=WorkflowDefinitionNodeResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建数据集工作流节点",
)
def create_dataset_workflow_node(
    dataset_id: str,
    workflow_id: str,
    payload: WorkflowDefinitionNodePayload,
) -> WorkflowDefinitionNodeResponse:
    """兼容数据集作用域的工作流节点创建入口。"""
    try:
        dataset_service.get_dataset_detail(dataset_id)
        return workflow_definition_service.create_workflow_node(workflow_id, payload)
    except (
        DatasetNotFoundError,
        WorkflowDefinitionNotFoundError,
        WorkflowNodeNotFoundError,
    ) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except (WorkflowDefinitionValidationError, WorkflowNodeValidationError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/workflows/{workflow_id}/nodes/{node_id}/execute",
    response_model=WorkflowNodeExecutionResponse,
    summary="执行数据集工作流节点",
)
def execute_dataset_workflow_node(
    dataset_id: str,
    workflow_id: str,
    node_id: str,
    payload: WorkflowNodeExecutionRequest,
) -> WorkflowNodeExecutionResponse:
    """兼容数据集作用域的单节点执行调试入口。"""
    try:
        dataset_service.get_dataset_detail(dataset_id)
        node = workflow_definition_service.get_workflow_node(workflow_id, node_id)
        definition = workflow_node_registry_service.get_node_definition(node.node_type)
        executor = _build_executor(definition)
        execution_input = WorkflowNodeExecutionInput(
            context=WorkflowNodeExecutionContext(
                dataset_id=dataset_id,
                workflow_id=workflow_id,
                node_id=node.id,
                node_key=node.node_key,
                node_type=node.node_type,
                config=dict(node.config),
                metadata=dict(payload.metadata),
            ),
            input_values=dict(payload.input_values),
        )
        result = executor.execute(execution_input)
        return WorkflowNodeExecutionResponse(
            workflow_id=workflow_id,
            node_id=node.id,
            node_type=node.node_type,
            output_values=result.output_values,
            artifacts=result.artifacts,
            summary=result.summary,
        )
    except (
        DatasetNotFoundError,
        WorkflowDefinitionNotFoundError,
        WorkflowNodeNotFoundError,
    ) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except (WorkflowDefinitionValidationError, WorkflowNodeValidationError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


def _build_executor(definition):
    if definition.key == "dataset_input":
        return DatasetInputNodeExecutor(definition, dataset_service.get_dataset_detail)
    raise WorkflowNodeValidationError(f"节点类型 {definition.key} 当前还未接入执行器。")
