from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.core.config import settings
from app.core.exceptions import DatasetNotFoundError, DatasetPreviewError, DatasetUploadError
from app.schemas.dataset import (
    DatasetDetailResponse,
    DatasetListResponse,
    DatasetPreviewResponse,
    DatasetUploadCapabilitiesResponse,
    DatasetUploadResponse,
)
from app.services.dataset_service import dataset_service

router = APIRouter(prefix="/datasets")


@router.get("", response_model=DatasetListResponse, summary="List datasets")
def list_datasets() -> DatasetListResponse:
    """返回数据集列表，用于承接后续的数据集首页。"""
    # 路由层只负责调用服务，不直接编排业务细节。
    return dataset_service.list_datasets()


@router.get(
    "/upload-capabilities",
    response_model=DatasetUploadCapabilitiesResponse,
    summary="Get upload capabilities",
)
def get_upload_capabilities() -> DatasetUploadCapabilitiesResponse:
    """返回当前阶段允许的上传类型和限制。"""
    return dataset_service.get_upload_capabilities()


@router.post("/upload", response_model=DatasetUploadResponse, summary="Upload dataset file")
def upload_dataset(file: UploadFile = File(...)) -> DatasetUploadResponse:
    """接收单个数据文件上传，并返回保存结果。"""
    try:
        return dataset_service.save_uploaded_file(file)
    except DatasetUploadError as exc:
        # 业务校验失败时统一转成 400，方便前端给用户展示提示。
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get(
    "/{dataset_id}/preview",
    response_model=DatasetPreviewResponse,
    summary="Get dataset preview",
)
def get_dataset_preview(
    dataset_id: str,
    limit: int = Query(
        default=settings.default_preview_rows,
        ge=1,
        le=settings.max_preview_rows,
    ),
) -> DatasetPreviewResponse:
    """按数据集 ID 返回当前支持格式的预览结果。"""
    try:
        return dataset_service.get_dataset_preview(dataset_id=dataset_id, limit=limit)
    except DatasetNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except DatasetPreviewError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get("/{dataset_id}", response_model=DatasetDetailResponse, summary="Get dataset detail")
def get_dataset_detail(dataset_id: str) -> DatasetDetailResponse:
    """按数据集 ID 返回元信息详情。"""
    try:
        return dataset_service.get_dataset_detail(dataset_id)
    except DatasetNotFoundError as exc:
        # 数据集不存在时返回 404，便于前端区分参数错误和系统错误。
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
