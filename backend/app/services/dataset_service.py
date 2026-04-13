import json
from csv import DictReader
from csv import Error as CsvError
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import UploadFile

from app.core.config import settings
from app.core.exceptions import DatasetNotFoundError, DatasetPreviewError, DatasetUploadError
from app.schemas.dataset import (
    DatasetDetailResponse,
    DatasetListResponse,
    DatasetPreviewResponse,
    DatasetRecord,
    DatasetSummaryResponse,
    DatasetUploadCapabilitiesResponse,
    DatasetUploadResponse,
)


class DatasetService:
    """封装数据集模块的业务逻辑。"""

    def __init__(self) -> None:
        """初始化数据集服务。"""
        self.supported_extensions = [".csv", ".xlsx", ".sav"]

    def list_datasets(self) -> DatasetListResponse:
        """返回当前已保存的数据集列表。"""
        items = [
            DatasetSummaryResponse(
                id=record.id,
                name=record.name,
                file_name=record.file_name,
                status=record.status,
                size_bytes=record.size_bytes,
                created_at=record.created_at,
            )
            for record in self._list_records()
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
        self._save_record(record)

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
        record = self._load_record(dataset_id)
        return DatasetDetailResponse(**record.model_dump())

    def get_dataset_preview(self, dataset_id: str, limit: int) -> DatasetPreviewResponse:
        """按数据集 ID 返回 CSV 预览结果。"""
        record = self._load_record(dataset_id)
        if record.extension != ".csv":
            raise DatasetPreviewError("当前预览接口暂仅支持 CSV 文件。")

        data_file_path = settings.storage_root / record.stored_path
        if not data_file_path.exists():
            raise DatasetPreviewError("原始数据文件不存在，暂时无法预览。")

        try:
            with data_file_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                reader = DictReader(file_obj)
                columns = reader.fieldnames or []
                if not columns:
                    raise DatasetPreviewError("CSV 文件缺少表头，暂时无法预览。")

                rows: list[dict[str, str | None]] = []
                has_more = False

                # 只读取前 limit 行，避免预览接口一次性把大文件全部拉进内存。
                for row in reader:
                    if len(rows) >= limit:
                        has_more = True
                        break

                    cleaned_row = {column: row.get(column) for column in columns}
                    rows.append(cleaned_row)
        except CsvError as exc:
            raise DatasetPreviewError("CSV 文件格式异常，暂时无法预览。") from exc

        return DatasetPreviewResponse(
            dataset_id=record.id,
            file_name=record.file_name,
            columns=columns,
            rows=rows,
            preview_row_count=len(rows),
            limit=limit,
            has_more=has_more,
            preview_format="csv",
        )

    def _list_records(self) -> list[DatasetRecord]:
        """读取并按创建时间倒序返回所有数据集记录。"""
        records = [
            self._read_record(record_path)
            for record_path in settings.dataset_metadata_root.glob("*.json")
        ]

        # 列表接口优先展示最新上传的数据集，符合用户直觉。
        return sorted(records, key=lambda item: item.created_at, reverse=True)

    def _load_record(self, dataset_id: str) -> DatasetRecord:
        """读取单个数据集元信息记录。"""
        record_path = self._build_record_path(dataset_id)
        if not record_path.exists():
            raise DatasetNotFoundError("请求的数据集不存在。")

        return self._read_record(record_path)

    def _save_record(self, record: DatasetRecord) -> None:
        """把数据集元信息写入本地 JSON 文件。"""
        record_path = self._build_record_path(record.id)
        record_path.write_text(
            json.dumps(record.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _read_record(self, record_path: Path) -> DatasetRecord:
        """从本地 JSON 文件读取数据集元信息。"""
        return DatasetRecord.model_validate_json(record_path.read_text(encoding="utf-8"))

    def _build_record_path(self, dataset_id: str) -> Path:
        """构造数据集元信息文件路径。"""
        return settings.dataset_metadata_root / f"{dataset_id}.json"


# 提供默认服务实例，后续接入依赖注入时可以平滑替换。
dataset_service = DatasetService()
