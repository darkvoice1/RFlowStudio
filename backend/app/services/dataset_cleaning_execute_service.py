from typing import Any

from app.core.exceptions import DatasetPreviewError
from app.schemas.dataset import DatasetCleaningStepRecord


class DatasetCleaningExecuteService:
    """执行已启用的数据清洗步骤并返回处理后的行数据。"""

    def apply_cleaning_steps(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
        cleaning_steps: list[DatasetCleaningStepRecord],
    ) -> list[dict[str, str | None]]:
        """按顺序把已启用的清洗步骤作用到当前行数据上。"""
        filtered_rows = rows
        for step in cleaning_steps:
            if step.step_type != "filter":
                continue

            filtered_rows = self._apply_filter_step(columns, filtered_rows, step)

        return filtered_rows

    def _apply_filter_step(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
        step: DatasetCleaningStepRecord,
    ) -> list[dict[str, str | None]]:
        """执行单个筛选步骤并返回筛选后的行数据。"""
        parameters = step.parameters
        column = str(parameters["column"])
        operator = str(parameters["operator"])

        if column not in columns:
            raise DatasetPreviewError(f"筛选字段 {column} 不存在，暂时无法执行当前筛选步骤。")

        matched_rows: list[dict[str, str | None]] = []
        for row in rows:
            if self._matches_filter(row.get(column), operator, parameters):
                matched_rows.append(row)

        return matched_rows

    def _matches_filter(
        self,
        value: str | None,
        operator: str,
        parameters: dict[str, Any],
    ) -> bool:
        """判断单个单元格值是否满足筛选条件。"""
        if operator == "is_empty":
            return value is None
        if operator == "is_not_empty":
            return value is not None
        if operator == "contains":
            expected = self._normalize_value(parameters.get("value"))
            if value is None or expected is None:
                return False
            return expected.lower() in value.lower()
        if operator == "eq":
            return value == self._normalize_value(parameters.get("value"))
        if operator == "neq":
            return value != self._normalize_value(parameters.get("value"))
        if operator in {"gt", "gte", "lt", "lte"}:
            return self._compare_numeric_value(
                value=value,
                operator=operator,
                target=self._normalize_value(parameters.get("value")),
            )
        if operator == "between":
            return self._between_numeric_value(
                value=value,
                start=self._normalize_value(parameters.get("start")),
                end=self._normalize_value(parameters.get("end")),
            )

        raise DatasetPreviewError("当前筛选步骤包含不受支持的操作符。")

    def _compare_numeric_value(
        self,
        value: str | None,
        operator: str,
        target: str | None,
    ) -> bool:
        """按数值比较操作符判断单元格是否命中筛选条件。"""
        if value is None or target is None:
            return False

        try:
            current_number = float(value)
            target_number = float(target)
        except ValueError:
            return False

        if operator == "gt":
            return current_number > target_number
        if operator == "gte":
            return current_number >= target_number
        if operator == "lt":
            return current_number < target_number
        return current_number <= target_number

    def _between_numeric_value(
        self,
        value: str | None,
        start: str | None,
        end: str | None,
    ) -> bool:
        """判断单元格值是否落在给定数值区间内。"""
        if value is None or start is None or end is None:
            return False

        try:
            current_number = float(value)
            start_number = float(start)
            end_number = float(end)
        except ValueError:
            return False

        return start_number <= current_number <= end_number

    def _normalize_value(self, value: Any) -> str | None:
        """把筛选参数中的值统一转换为空值或去除首尾空格的文本。"""
        if value is None:
            return None

        normalized = str(value).strip()
        return normalized or None
