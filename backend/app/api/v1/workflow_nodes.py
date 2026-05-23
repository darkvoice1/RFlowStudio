"""工作流节点目录接口。"""

from fastapi import APIRouter, HTTPException, status

from app.core.exceptions import ResourceNotFoundError
from app.schemas.workflow_node import (
    WorkflowNodeDefinitionListResponse,
    WorkflowNodeDefinitionResponse,
)
from app.services.workflow import workflow_node_registry_service

router = APIRouter(prefix="/workflow-nodes", tags=["workflow-nodes"])


@router.get(
    "",
    response_model=WorkflowNodeDefinitionListResponse,
    summary="查询工作流节点目录",
)
def list_workflow_node_definitions() -> WorkflowNodeDefinitionListResponse:
    """返回当前对前端可见的全部工作流节点。"""
    return workflow_node_registry_service.list_node_definitions()


@router.get(
    "/{node_type}",
    response_model=WorkflowNodeDefinitionResponse,
    summary="查询单个工作流节点定义",
)
def get_workflow_node_definition(node_type: str) -> WorkflowNodeDefinitionResponse:
    """按节点 key 或别名查询单个节点定义。"""
    try:
        return workflow_node_registry_service.get_node_definition(node_type)
    except ResourceNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
