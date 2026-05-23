"""工作流定义层接口。"""

from fastapi import APIRouter, HTTPException, status

from app.core.exceptions import ResourceNotFoundError, ValidationError
from app.schemas.workflow_definition import (
    WorkflowDefinitionCreateRequest,
    WorkflowDefinitionDetailResponse,
    WorkflowDefinitionGraphUpdateRequest,
    WorkflowDefinitionListResponse,
    WorkflowDefinitionResponse,
)
from app.services.workflow import WorkflowDefinitionService

router = APIRouter(prefix="/workflows", tags=["workflow-definitions"])
service = WorkflowDefinitionService()


@router.get("", response_model=WorkflowDefinitionListResponse, summary="查询工作流列表")
def list_workflows() -> WorkflowDefinitionListResponse:
    """返回当前所有工作流定义。"""
    return service.list_workflows()


@router.post(
    "",
    response_model=WorkflowDefinitionResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建工作流",
)
def create_workflow(payload: WorkflowDefinitionCreateRequest) -> WorkflowDefinitionResponse:
    """创建一条新的工作流定义。"""
    try:
        return service.create_workflow(payload)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get(
    "/{workflow_id}",
    response_model=WorkflowDefinitionDetailResponse,
    summary="查询工作流详情",
)
def get_workflow_detail(workflow_id: str) -> WorkflowDefinitionDetailResponse:
    """返回工作流定义及整张图详情。"""
    try:
        return service.get_workflow_detail(workflow_id)
    except ResourceNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.put(
    "/{workflow_id}/graph",
    response_model=WorkflowDefinitionDetailResponse,
    summary="保存整张工作流图",
)
def save_workflow_graph(
    workflow_id: str,
    payload: WorkflowDefinitionGraphUpdateRequest,
) -> WorkflowDefinitionDetailResponse:
    """保存整张工作流图并返回最新详情。"""
    try:
        return service.save_workflow_graph(workflow_id, payload)
    except ResourceNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
