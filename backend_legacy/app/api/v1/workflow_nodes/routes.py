from fastapi import APIRouter, HTTPException, status

from app.core.exceptions import WorkflowNodeNotFoundError
from app.schemas.workflow_node import (
    WorkflowNodeDefinitionListResponse,
    WorkflowNodeDefinitionResponse,
)
from app.services.workflow.node_registry_service import workflow_node_registry_service

router = APIRouter(prefix="/workflow-nodes")


@router.get(
    "",
    response_model=WorkflowNodeDefinitionListResponse,
    summary="获取已注册的工作流节点列表",
)
def list_workflow_node_definitions() -> WorkflowNodeDefinitionListResponse:
    """返回平台当前已注册的全部工作流节点。"""
    return workflow_node_registry_service.list_node_definitions()


@router.get(
    "/{node_type}",
    response_model=WorkflowNodeDefinitionResponse,
    summary="获取单个工作流节点定义",
)
def get_workflow_node_definition(node_type: str) -> WorkflowNodeDefinitionResponse:
    """返回指定节点类型或别名对应的节点定义。"""
    try:
        return workflow_node_registry_service.get_node_definition(node_type)
    except WorkflowNodeNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
