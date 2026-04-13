from pathlib import Path
from uuid import uuid4

from fastapi import UploadFile

from app.core.config import settings
from app.core.exceptions import DatasetUploadError
from app.schemas.dataset import (
    DatasetListResponse,
    DatasetUploadCapabilitiesResponse,
    DatasetUploadResponse,
)


class DatasetService:
    """封装数据集模块的业务逻辑。"""

    def __init__(self) -> None:
        """初始化数据集服务。"""
        self.supported_extensions = [".csv", ".xlsx", ".sav"]

    def list_datasets(self) -> DatasetListResponse:
        """返回当前数据集列表占位结果。"""
        # 当前阶段先返回空列表，后续会接入真实存储和数据库。
        return DatasetListResponse(items=[], total=0)

    def get_upload_capabilities(self) -> DatasetUploadCapabilitiesResponse:
        """返回当前阶段支持的上传能力说明。"""
        # 先把前后端都需要知道的上传约束收口到这里，避免散落在多个文件中。
        return DatasetUploadCapabilitiesResponse(
            supported_extensions=self.supported_extensions,
            max_file_size_mb=settings.max_upload_size_mb,
            upload_strategy="single_file",
        )

    def save_uploaded_file(self, upload_file: UploadFile) -> DatasetUploadResponse:
        """保存上传文件并返回上传结果摘要。"""
        file_name = upload_file.filename or ""
        extension = Path(file_name).suffix.lower()

        # 先校验文件名和扩展名，避免无意义文件进入存储目录。
        if not file_name:
            raise DatasetUploadError("上传文件缺少文件名。")
        if extension not in self.supported_extensions:
            raise DatasetUploadError("当前仅支持 csv、xlsx 和 sav 文件。")

        content = upload_file.file.read()
        size_bytes = len(content)
        max_bytes = settings.max_upload_size_mb * 1024 * 1024

        # 在真正落盘前先做大小校验，避免超限文件写入本地磁盘。
        if size_bytes == 0:
            raise DatasetUploadError("上传文件不能为空。")
        if size_bytes > max_bytes:
            raise DatasetUploadError(
                f"上传文件超过大小限制，当前最大支持 {settings.max_upload_size_mb} MB。"
            )

        dataset_id = uuid4().hex
        stored_name = f"{dataset_id}{extension}"
        stored_path = settings.upload_root / stored_name

        # 当前阶段先把原始文件落盘，为后续解析和元数据入库做准备。
        stored_path.write_bytes(content)

        return DatasetUploadResponse(
            id=dataset_id,
            file_name=file_name,
            stored_path=stored_path.relative_to(settings.storage_root).as_posix(),
            size_bytes=size_bytes,
            status="draft",
        )


# 提供默认服务实例，后续接入依赖注入时可以平滑替换。
dataset_service = DatasetService()
