from typing import Any

from app.core.exceptions import DatasetPreviewError
from app.schemas.dataset import DatasetCleaningStepRecord


class DatasetMissingValueExecuteService:
    """Execute missing-value cleaning steps."""

    def apply_step(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
        step: DatasetCleaningStepRecord,
    ) -> list[dict[str, str | None]]:
        parameters = step.parameters
        method = str(parameters["method"])

        if method == "drop_rows":
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
        for column in columns:
            if row.get(column) is None:
                return True

        return False

    def _normalize_value(self, value: Any) -> str | None:
        if value is None:
            return None

        normalized = str(value).strip()
        return normalized or None
