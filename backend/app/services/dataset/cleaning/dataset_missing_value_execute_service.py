from typing import Any

from app.core.exceptions import DatasetPreviewError
from app.schemas.dataset import DatasetCleaningStepRecord


class DatasetMissingValueExecuteService:
    """执行缺失值处理步骤。"""

    def apply_step(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
        step: DatasetCleaningStepRecord,
    ) -> list[dict[str, str | None]]:
        """执行单个缺失值处理步骤并返回处理后的行数据。"""
        parameters = step.parameters
        method = str(parameters["method"])

        if method == "drop_rows":
            # 第一版先按整行处理，只要任意字段缺失就移除该记录。
            return [row for row in rows if not self._row_has_missing_value(columns, row)]

        if method == "fill_value":
            column = str(parameters["column"])
            if column not in columns:
                raise DatasetPreviewError(
                    f"缺失值处理字段 {column} 不存在，暂时无法执行当前步骤。"
                )

            fill_value = self._normalize_value(parameters.get("value"))
            if fill_value is None:
                raise DatasetPreviewError("缺失值替换步骤缺少有效的替换值。")

            filled_rows: list[dict[str, str | None]] = []
            for row in rows:
                updated_row = dict(row)
                if updated_row.get(column) is None:
                    updated_row[column] = fill_value
                filled_rows.append(updated_row)

            return filled_rows

        if method == "mark_values":
            column = str(parameters["column"])
            if column not in columns:
                raise DatasetPreviewError(
                    f"缺失值处理字段 {column} 不存在，暂时无法执行当前步骤。"
                )

            marker_values = {
                self._normalize_value(item)
                for item in parameters.get("values", [])
            }
            marker_values.discard(None)

            marked_rows: list[dict[str, str | None]] = []
            for row in rows:
                updated_row = dict(row)
                if updated_row.get(column) in marker_values:
                    updated_row[column] = None
                marked_rows.append(updated_row)

            return marked_rows

        raise DatasetPreviewError("当前缺失值处理步骤包含不受支持的 method。")

    def _row_has_missing_value(
        self,
        columns: list[str],
        row: dict[str, str | None],
    ) -> bool:
        """判断当前整行是否至少包含一个缺失值。"""
        for column in columns:
            if row.get(column) is None:
                return True

        return False

    def _normalize_value(self, value: Any) -> str | None:
        """把输入值统一转换为空值或去除首尾空格的文本。"""
        if value is None:
            return None

        normalized = str(value).strip()
        return normalized or None
