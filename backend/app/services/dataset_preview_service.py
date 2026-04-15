from pathlib import Path
from typing import Any

from app.schemas.dataset import (
    DatasetCleaningStepRecord,
    DatasetColumnProfile,
    DatasetPreviewResponse,
    DatasetProfileResponse,
    DatasetRecord,
)
from app.services.dataset_cleaning_execute_service import DatasetCleaningExecuteService
from app.services.dataset_reader_service import DatasetReaderService


class DatasetPreviewService:
    """封装数据集预览与字段分析逻辑。"""

    def __init__(self) -> None:
        """初始化预览服务依赖的读取器和清洗执行器。"""
        self.reader_service = DatasetReaderService()
        self.cleaning_execute_service = DatasetCleaningExecuteService()

    def get_dataset_preview(
        self,
        record: DatasetRecord,
        data_file_path: Path,
        offset: int,
        limit: int,
        cleaning_steps: list[DatasetCleaningStepRecord] | None = None,
    ) -> DatasetPreviewResponse:
        """按数据集记录返回当前支持格式的预览结果。"""
        columns, rows = self.reader_service.read_all_rows(
            record=record,
            data_file_path=data_file_path,
            empty_message="当前预览接口暂不支持该文件格式。",
            csv_header_message="CSV 文件缺少表头，暂时无法预览。",
            xlsx_header_message="XLSX 文件缺少表头，暂时无法预览。",
            xlsx_invalid_message="XLSX 文件格式异常，暂时无法预览。",
        )
        columns, filtered_rows = self.cleaning_execute_service.apply_cleaning_steps(
            columns=columns,
            rows=rows,
            cleaning_steps=cleaning_steps or [],
        )
        paged_rows = filtered_rows[offset : offset + limit]
        has_more = offset + limit < len(filtered_rows)

        return DatasetPreviewResponse(
            dataset_id=record.id,
            file_name=record.file_name,
            columns=columns,
            rows=paged_rows,
            preview_row_count=len(paged_rows),
            offset=offset,
            limit=limit,
            has_more=has_more,
            preview_format=record.extension.lstrip("."),
        )

    def get_dataset_profile(
        self,
        record: DatasetRecord,
        data_file_path: Path,
        cleaning_steps: list[DatasetCleaningStepRecord] | None = None,
    ) -> DatasetProfileResponse:
        """按数据集记录返回当前支持格式的字段元信息统计结果。"""
        columns, rows = self.reader_service.read_all_rows(
            record=record,
            data_file_path=data_file_path,
            empty_message="当前字段分析接口暂不支持该文件格式。",
            csv_header_message="CSV 文件缺少表头，暂时无法分析字段信息。",
            xlsx_header_message="XLSX 文件缺少表头，暂时无法分析字段信息。",
            xlsx_invalid_message="XLSX 文件格式异常，暂时无法分析字段信息。",
        )
        columns, filtered_rows = self.cleaning_execute_service.apply_cleaning_steps(
            columns=columns,
            rows=rows,
            cleaning_steps=cleaning_steps or [],
        )
        column_values, row_count = self._build_profile_inputs(columns, filtered_rows)
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
            profile_format=record.extension.lstrip("."),
        )

    def _build_profile_inputs(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
    ) -> tuple[dict[str, list[str | None]], int]:
        """根据行数据构造字段分析需要的列值结构。"""
        column_values: dict[str, list[str | None]] = {column: [] for column in columns}
        for row in rows:
            for column in columns:
                column_values[column].append(row.get(column))

        return column_values, len(rows)

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

    def _normalize_cell_value(self, value: Any) -> str | None:
        """把原始单元格值归一化为空值或去除首尾空格的文本。"""
        if value is None:
            return None

        normalized = str(value).strip()
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
