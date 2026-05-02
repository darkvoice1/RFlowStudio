from fastapi import APIRouter, HTTPException, status

from app.core.exceptions import DatasetNotFoundError, DatasetWorkflowNotFoundError
from app.schemas.workflow import (
    DatasetWorkflowCreateRequest,
    DatasetWorkflowDetailResponse,
    DatasetWorkflowEdgeCreateRequest,
    DatasetWorkflowEdgeListResponse,
    DatasetWorkflowEdgeResponse,
    DatasetWorkflowListResponse,
    DatasetWorkflowNodeCreateRequest,
    DatasetWorkflowNodeListResponse,
    DatasetWorkflowNodeResponse,
    DatasetWorkflowResponse,
    DatasetWorkflowVersionCreateRequest,
    DatasetWorkflowVersionListResponse,
    DatasetWorkflowVersionResponse,
)
from app.services.dataset.dataset_service import dataset_service

router = APIRouter()


@router.get(
    "/{dataset_id}/workflows",
    response_model=DatasetWorkflowListResponse,
    summary="List dataset workflows",
)
def list_dataset_workflows(dataset_id: str) -> DatasetWorkflowListResponse:
    """返回指定数据集下的工作流列表。"""
    try:
        return dataset_service.list_dataset_workflows(dataset_id)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/workflows",
    response_model=DatasetWorkflowResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create dataset workflow",
)
def create_dataset_workflow(
    dataset_id: str,
    payload: DatasetWorkflowCreateRequest,
) -> DatasetWorkflowResponse:
    """为指定数据集创建一条新的工作流。"""
    try:
        return dataset_service.create_dataset_workflow(dataset_id, payload)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/workflows/{workflow_id}",
    response_model=DatasetWorkflowDetailResponse,
    summary="Get dataset workflow detail",
)
def get_dataset_workflow_detail(
    dataset_id: str,
    workflow_id: str,
) -> DatasetWorkflowDetailResponse:
    """返回指定工作流详情及其历史版本列表。"""
    try:
        return dataset_service.get_dataset_workflow_detail(dataset_id, workflow_id)
    except (DatasetNotFoundError, DatasetWorkflowNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/workflows/{workflow_id}/versions",
    response_model=DatasetWorkflowVersionListResponse,
    summary="List dataset workflow versions",
)
def list_dataset_workflow_versions(
    dataset_id: str,
    workflow_id: str,
) -> DatasetWorkflowVersionListResponse:
    """返回指定工作流下的历史版本列表。"""
    try:
        return dataset_service.list_dataset_workflow_versions(dataset_id, workflow_id)
    except (DatasetNotFoundError, DatasetWorkflowNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/workflows/{workflow_id}/versions",
    response_model=DatasetWorkflowVersionResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Save dataset workflow version",
)
def create_dataset_workflow_version(
    dataset_id: str,
    workflow_id: str,
    payload: DatasetWorkflowVersionCreateRequest,
) -> DatasetWorkflowVersionResponse:
    """把当前工作流保存为不可变历史版本。"""
    try:
        return dataset_service.create_dataset_workflow_version(dataset_id, workflow_id, payload)
    except (DatasetNotFoundError, DatasetWorkflowNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/workflows/{workflow_id}/nodes",
    response_model=DatasetWorkflowNodeListResponse,
    summary="List dataset workflow nodes",
)
def list_dataset_workflow_nodes(
    dataset_id: str,
    workflow_id: str,
) -> DatasetWorkflowNodeListResponse:
    """返回指定工作流当前编辑态的节点列表。"""
    try:
        return dataset_service.list_dataset_workflow_nodes(dataset_id, workflow_id)
    except (DatasetNotFoundError, DatasetWorkflowNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/workflows/{workflow_id}/nodes",
    response_model=DatasetWorkflowNodeResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create dataset workflow node",
)
def create_dataset_workflow_node(
    dataset_id: str,
    workflow_id: str,
    payload: DatasetWorkflowNodeCreateRequest,
) -> DatasetWorkflowNodeResponse:
    """为指定工作流当前编辑态创建一个节点。"""
    try:
        return dataset_service.create_dataset_workflow_node(
            dataset_id,
            workflow_id,
            payload,
        )
    except (DatasetNotFoundError, DatasetWorkflowNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/workflows/{workflow_id}/edges",
    response_model=DatasetWorkflowEdgeListResponse,
    summary="List dataset workflow edges",
)
def list_dataset_workflow_edges(
    dataset_id: str,
    workflow_id: str,
) -> DatasetWorkflowEdgeListResponse:
    """返回指定工作流当前编辑态的连线列表。"""
    try:
        return dataset_service.list_dataset_workflow_edges(dataset_id, workflow_id)
    except (DatasetNotFoundError, DatasetWorkflowNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/workflows/{workflow_id}/edges",
    response_model=DatasetWorkflowEdgeResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create dataset workflow edge",
)
def create_dataset_workflow_edge(
    dataset_id: str,
    workflow_id: str,
    payload: DatasetWorkflowEdgeCreateRequest,
) -> DatasetWorkflowEdgeResponse:
    """为指定工作流当前编辑态创建一条连线。"""
    try:
        return dataset_service.create_dataset_workflow_edge(
            dataset_id,
            workflow_id,
            payload,
        )
    except (DatasetNotFoundError, DatasetWorkflowNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
