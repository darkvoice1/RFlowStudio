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
    DatasetColumnProfile,
    DatasetDetailResponse,
    DatasetListResponse,
    DatasetPreviewResponse,
    DatasetProfileResponse,
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
        data_file_path = self._resolve_csv_data_file(
            record=record,
            unsupported_message="当前预览接口暂仅支持 CSV 文件。",
            missing_file_message="原始数据文件不存在，暂时无法预览。",
        )
        columns, rows, has_more = self._read_csv_preview(
            data_file_path=data_file_path,
            limit=limit,
        )

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

    def get_dataset_profile(self, dataset_id: str) -> DatasetProfileResponse:
        """按数据集 ID 返回字段元信息统计结果。"""
        record = self._load_record(dataset_id)
        data_file_path = self._resolve_csv_data_file(
            record=record,
            unsupported_message="当前字段分析接口暂仅支持 CSV 文件。",
            missing_file_message="原始数据文件不存在，暂时无法分析字段信息。",
        )

        try:
            with data_file_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                reader = DictReader(file_obj)
                columns = reader.fieldnames or []
                if not columns:
                    raise DatasetPreviewError("CSV 文件缺少表头，暂时无法分析字段信息。")

                column_values: dict[str, list[str | None]] = {column: [] for column in columns}
                row_count = 0

                # 逐行累积列值，为字段类型和缺失值统计提供输入。
                for row in reader:
                    row_count += 1
                    for column in columns:
                        column_values[column].append(row.get(column))
        except CsvError as exc:
            raise DatasetPreviewError("CSV 文件格式异常，暂时无法分析字段信息。") from exc

        profiles = [
            self._build_column_profile(name=column, values=column_values[column])
            for column in columns
        ]

        return DatasetProfileResponse(
            dataset_id=record.id,
            file_name=record.file_name,
            row_count=row_count,
            column_count=len(columns),
            columns=profiles,
            profile_format="csv",
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

    def _read_csv_preview(
        self,
        data_file_path: Path,
        limit: int,
    ) -> tuple[list[str], list[dict[str, str | None]], bool]:
        """读取 CSV 预览数据，返回列名、行数据和是否还有更多行。"""
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

        return columns, rows, has_more

    def _resolve_csv_data_file(
        self,
        record: DatasetRecord,
        unsupported_message: str,
        missing_file_message: str,
    ) -> Path:
        """校验数据集格式并返回对应的 CSV 文件路径。"""
        if record.extension != ".csv":
            raise DatasetPreviewError(unsupported_message)

        data_file_path = settings.storage_root / record.stored_path
        if not data_file_path.exists():
            raise DatasetPreviewError(missing_file_message)

        return data_file_path

    def _build_column_profile(
        self,
        name: str,
        values: list[str | None],
    ) -> DatasetColumnProfile:
        """根据列值列表构造字段元信息。"""
        normalized_values = [self._normalize_cell_value(value) for value in values]
        non_empty_values = [value for value in normalized_values if value is not None]
        inferred_type = self._infer_column_type(non_empty_values)

        # 只保留前几个非空样例，方便前端快速展示字段内容特征。
        sample_values: list[str] = []
        for value in non_empty_values:
            if value not in sample_values:
                sample_values.append(value)
            if len(sample_values) >= 3:
                break

        return DatasetColumnProfile(
            name=name,
            inferred_type=inferred_type,
            nullable=len(non_empty_values) != len(normalized_values),
            missing_count=len(normalized_values) - len(non_empty_values),
            unique_count=len(set(non_empty_values)),
            sample_values=sample_values,
        )

    def _normalize_cell_value(self, value: str | None) -> str | None:
        """把原始单元格值归一化为空值或去除首尾空格的文本。"""
        if value is None:
            return None

        normalized = value.strip()
        return normalized or None

    def _infer_column_type(
        self,
        values: list[str],
    ) -> str:
        """根据当前列的非空值推断字段类型。"""
        if not values:
            return "empty"
        if all(self._is_integer(value) for value in values):
            return "integer"
        if all(self._is_float(value) for value in values):
            return "float"
        if all(self._is_boolean(value) for value in values):
            return "boolean"
        return "string"

    def _is_integer(self, value: str) -> bool:
        """判断字符串是否可以解析为整数。"""
        try:
            int(value)
        except ValueError:
            return False

        return True

    def _is_float(self, value: str) -> bool:
        """判断字符串是否可以解析为浮点数。"""
        try:
            float(value)
        except ValueError:
            return False

        return True

    def _is_boolean(self, value: str) -> bool:
        """判断字符串是否属于常见布尔值文本。"""
        return value.lower() in {"true", "false", "yes", "no", "0", "1"}


# 提供默认服务实例，后续接入依赖注入时可以平滑替换。
dataset_service = DatasetService()
