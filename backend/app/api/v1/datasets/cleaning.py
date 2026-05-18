from fastapi import APIRouter, HTTPException, status

from app.core.exceptions import DatasetCleaningError, DatasetNotFoundError
from app.schemas.dataset import (
    DatasetCleaningRScriptResponse,
    DatasetCleaningStepCreateRequest,
    DatasetCleaningStepListResponse,
    DatasetCleaningStepResponse,
)
from app.services.resources.datasets import dataset_service

router = APIRouter()


@router.get(
    "/{dataset_id}/cleaning-steps",
    response_model=DatasetCleaningStepListResponse,
    summary="List dataset cleaning steps",
)
def list_dataset_cleaning_steps(dataset_id: str) -> DatasetCleaningStepListResponse:
    """返回指定数据集当前已记录的清洗步骤。"""
    try:
        return dataset_service.list_dataset_cleaning_steps(dataset_id)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/{dataset_id}/cleaning-steps",
    response_model=DatasetCleaningStepResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create dataset cleaning step",
)
def create_dataset_cleaning_step(
    dataset_id: str,
    payload: DatasetCleaningStepCreateRequest,
) -> DatasetCleaningStepResponse:
    """为指定数据集记录一条新的清洗步骤。"""
    try:
        return dataset_service.create_dataset_cleaning_step(dataset_id, payload)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except DatasetCleaningError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/cleaning-r-script",
    response_model=DatasetCleaningRScriptResponse,
    summary="Get dataset cleaning R script draft",
)
def get_dataset_cleaning_r_script(dataset_id: str) -> DatasetCleaningRScriptResponse:
    """返回指定数据集当前清洗步骤对应的 R 代码草稿。"""
    try:
        return dataset_service.get_dataset_cleaning_r_script(dataset_id)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
