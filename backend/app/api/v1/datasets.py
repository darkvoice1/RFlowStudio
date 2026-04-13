from fastapi import APIRouter, File, HTTPException, UploadFile, status

from app.core.exceptions import DatasetUploadError
from app.schemas.dataset import (
    DatasetListResponse,
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
