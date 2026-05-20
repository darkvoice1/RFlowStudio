from fastapi import APIRouter, HTTPException, status

from app.core.exceptions import (
    WorkflowDefinitionNotFoundError,
    WorkflowDefinitionValidationError,
    WorkflowNodeNotFoundError,
)
from app.schemas.workflow_definition import (
    WorkflowDefinitionCreateRequest,
    WorkflowDefinitionDetailResponse,
    WorkflowDefinitionEdgeListResponse,
    WorkflowDefinitionEdgePayload,
    WorkflowDefinitionEdgeResponse,
    WorkflowDefinitionGraphUpdateRequest,
    WorkflowDefinitionListResponse,
    WorkflowDefinitionNodeListResponse,
    WorkflowDefinitionNodePayload,
    WorkflowDefinitionNodeResponse,
    WorkflowDefinitionResponse,
)
from app.services.workflow.workflow_definition_service import (
    WorkflowDefinitionService,
)

router = APIRouter()
workflow_definition_service = WorkflowDefinitionService()


@router.get(
    "",
    response_model=WorkflowDefinitionListResponse,
    summary="列出工作流定义",
)
def list_workflow_definitions() -> WorkflowDefinitionListResponse:
    """返回平台当前注册的全部工作流定义。"""
    return workflow_definition_service.list_workflows()


@router.post(
    "",
    response_model=WorkflowDefinitionResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建工作流定义",
)
def create_workflow_definition(
    payload: WorkflowDefinitionCreateRequest,
) -> WorkflowDefinitionResponse:
    """创建一条平台级工作流定义。"""
    return workflow_definition_service.create_workflow(payload)


@router.put(
    "/{workflow_id}/graph",
    response_model=WorkflowDefinitionDetailResponse,
    summary="保存工作流整图",
)
def update_workflow_definition_graph(
    workflow_id: str,
    payload: WorkflowDefinitionGraphUpdateRequest,
) -> WorkflowDefinitionDetailResponse:
    """一次性保存工作流名称、节点和连线。"""
    try:
        return workflow_definition_service.update_workflow_graph(workflow_id, payload)
    except WorkflowDefinitionNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except WorkflowDefinitionValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get(
    "/{workflow_id}",
    response_model=WorkflowDefinitionDetailResponse,
    summary="获取工作流定义详情",
)
def get_workflow_definition_detail(workflow_id: str) -> WorkflowDefinitionDetailResponse:
    """返回工作流定义及其节点和边。"""
    try:
        return workflow_definition_service.get_workflow_detail(workflow_id)
    except WorkflowDefinitionNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{workflow_id}/nodes",
    response_model=WorkflowDefinitionNodeListResponse,
    summary="列出工作流节点",
)
def list_workflow_definition_nodes(
    workflow_id: str,
) -> WorkflowDefinitionNodeListResponse:
    """返回工作流节点列表。"""
    try:
        return workflow_definition_service.list_workflow_nodes(workflow_id)
    except WorkflowDefinitionNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{workflow_id}/nodes",
    response_model=WorkflowDefinitionNodeResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建工作流节点",
)
def create_workflow_definition_node(
    workflow_id: str,
    payload: WorkflowDefinitionNodePayload,
) -> WorkflowDefinitionNodeResponse:
    """为工作流定义创建一个节点。"""
    try:
        return workflow_definition_service.create_workflow_node(workflow_id, payload)
    except (WorkflowDefinitionNotFoundError, WorkflowNodeNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except WorkflowDefinitionValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get(
    "/{workflow_id}/edges",
    response_model=WorkflowDefinitionEdgeListResponse,
    summary="列出工作流边",
)
def list_workflow_definition_edges(
    workflow_id: str,
) -> WorkflowDefinitionEdgeListResponse:
    """返回工作流边列表。"""
    try:
        return workflow_definition_service.list_workflow_edges(workflow_id)
    except WorkflowDefinitionNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{workflow_id}/edges",
    response_model=WorkflowDefinitionEdgeResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建工作流边",
)
def create_workflow_definition_edge(
    workflow_id: str,
    payload: WorkflowDefinitionEdgePayload,
) -> WorkflowDefinitionEdgeResponse:
    """为工作流定义创建一条边。"""
    try:
        return workflow_definition_service.create_workflow_edge(workflow_id, payload)
    except (WorkflowDefinitionNotFoundError, WorkflowNodeNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except WorkflowDefinitionValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
