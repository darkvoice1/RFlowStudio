from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import UploadFile
from sqlalchemy import desc, select
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.core.exceptions import DatasetNotFoundError, DatasetPreviewError, DatasetUploadError
from app.db.session import session_scope
from app.models.dataset import DatasetRecordModel
from app.schemas.dataset import (
    DatasetDetailResponse,
    DatasetListResponse,
    DatasetRecord,
    DatasetStatus,
    DatasetSummaryResponse,
    DatasetUploadCapabilitiesResponse,
    DatasetUploadResponse,
)


class DatasetUploadService:
    """封装数据集上传及其相关管理逻辑。"""

    def __init__(self) -> None:
        """初始化数据集存储服务。"""
        self.supported_extensions = [".csv", ".xlsx", ".sav"]

    def list_datasets(self) -> DatasetListResponse:
        """返回当前已保存的数据集列表。"""
        with session_scope() as session:
            records = session.scalars(
                select(DatasetRecordModel).order_by(desc(DatasetRecordModel.created_at))
            ).all()

        items = [
            DatasetSummaryResponse(
                id=record.id,
                name=record.name,
                file_name=record.file_name,
                status=record.status,  # type: ignore[arg-type]
                size_bytes=record.size_bytes,
                created_at=record.created_at,
            )
            for record in records
        ]
        return DatasetListResponse(items=items, total=len(items))

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
        dataset_name = Path(file_name).stem
        created_at = datetime.now(UTC)

        # 当前阶段先把原始文件落盘，为后续解析和元数据入库做准备。
        stored_path.write_bytes(content)
        record = DatasetRecord(
            id=dataset_id,
            name=dataset_name,
            file_name=file_name,
            extension=extension,
            stored_path=stored_path.relative_to(settings.storage_root).as_posix(),
            size_bytes=size_bytes,
            status="draft",
            created_at=created_at,
        )
        try:
            self._save_record(record)
        except SQLAlchemyError as exc:
            # 如果数据库写入失败，需要回收刚刚落盘的原始文件，避免留下孤儿文件。
            stored_path.unlink(missing_ok=True)
            raise DatasetUploadError("数据集元信息写入数据库失败。") from exc

        return DatasetUploadResponse(
            id=record.id,
            name=record.name,
            file_name=file_name,
            stored_path=record.stored_path,
            size_bytes=record.size_bytes,
            status="draft",
            created_at=record.created_at,
        )

    def get_dataset_detail(self, dataset_id: str) -> DatasetDetailResponse:
        """按数据集 ID 返回详情信息。"""
        record = self.load_record(dataset_id)
        return DatasetDetailResponse(**record.model_dump())

    def update_dataset_status(self, dataset_id: str, status: DatasetStatus) -> DatasetRecord:
        """更新数据集状态并返回最新记录。"""
        record = self.load_record(dataset_id)
        record.status = status
        self._save_record(record)
        return record

    def load_record(self, dataset_id: str) -> DatasetRecord:
        """读取单个数据集元信息记录。"""
        with session_scope() as session:
            model = session.get(DatasetRecordModel, dataset_id)
            if model is None:
                raise DatasetNotFoundError("请求的数据集不存在。")

        return self._to_record(model)

    def resolve_data_file(
        self,
        record: DatasetRecord,
        supported_extensions: set[str],
        unsupported_message: str,
        missing_file_message: str,
    ) -> Path:
        """校验数据集格式并返回对应的数据文件路径。"""
        if record.extension not in supported_extensions:
            raise DatasetPreviewError(unsupported_message)

        data_file_path = settings.storage_root / record.stored_path
        if not data_file_path.exists():
            raise DatasetPreviewError(missing_file_message)

        return data_file_path

    def _save_record(self, record: DatasetRecord) -> None:
        """把数据集元信息写入数据库。"""
        with session_scope() as session:
            existing_model = session.get(DatasetRecordModel, record.id)
            if existing_model is None:
                session.add(self._to_model(record))
                return

            # 复用同一条记录做状态更新，避免多份元信息出现分叉。
            existing_model.name = record.name
            existing_model.file_name = record.file_name
            existing_model.extension = record.extension
            existing_model.stored_path = record.stored_path
            existing_model.size_bytes = record.size_bytes
            existing_model.status = record.status
            existing_model.created_at = record.created_at

    def _to_record(self, model: DatasetRecordModel) -> DatasetRecord:
        """把数据库模型转换为领域内统一的数据集记录。"""
        return DatasetRecord(
            id=model.id,
            name=model.name,
            file_name=model.file_name,
            extension=model.extension,
            stored_path=model.stored_path,
            size_bytes=model.size_bytes,
            status=model.status,  # type: ignore[arg-type]
            created_at=model.created_at,
        )

    def _to_model(self, record: DatasetRecord) -> DatasetRecordModel:
        """把领域记录转换为数据库模型对象。"""
        return DatasetRecordModel(
            id=record.id,
            name=record.name,
            file_name=record.file_name,
            extension=record.extension,
            stored_path=record.stored_path,
            size_bytes=record.size_bytes,
            status=record.status,
            created_at=record.created_at,
        )
