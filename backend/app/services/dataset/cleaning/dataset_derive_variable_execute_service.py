from typing import Any

from app.core.exceptions import DatasetPreviewError
from app.schemas.dataset import DatasetCleaningStepRecord


class DatasetDeriveVariableExecuteService:
    """执行新变量生成步骤。"""

    def apply_step(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
        step: DatasetCleaningStepRecord,
    ) -> tuple[list[str], list[dict[str, str | None]]]:
        """执行单个新变量生成步骤并返回更新后的列结构和行数据。"""
        parameters = step.parameters
        method = str(parameters["method"])
        new_column = str(parameters["new_column"])

        if new_column in columns:
            raise DatasetPreviewError(
                f"新变量字段 {new_column} 已存在，暂时无法重复创建同名字段。"
            )

        if method == "binary_operation":
            return self._apply_binary_operation_step(columns, rows, parameters)
        if method == "concat":
            return self._apply_concat_step(columns, rows, parameters)

        raise DatasetPreviewError("当前新变量生成步骤包含不受支持的 method。")

    def _apply_binary_operation_step(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
        parameters: dict[str, Any],
    ) -> tuple[list[str], list[dict[str, str | None]]]:
        """按双字段数值运算生成新变量。"""
        left_column = str(parameters["left_column"])
        right_column = str(parameters["right_column"])
        operator = str(parameters["operator"])
        new_column = str(parameters["new_column"])

        self._ensure_columns_exist(
            columns=columns,
            required_columns=[left_column, right_column],
            error_prefix="新变量计算字段",
        )

        derived_rows: list[dict[str, str | None]] = []
        for row in rows:
            updated_row = dict(row)
            updated_row[new_column] = self._calculate_binary_result(
                left_value=row.get(left_column),
                right_value=row.get(right_column),
                operator=operator,
            )
            derived_rows.append(updated_row)

        return columns + [new_column], derived_rows

    def _apply_concat_step(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
        parameters: dict[str, Any],
    ) -> tuple[list[str], list[dict[str, str | None]]]:
        """按多个字段拼接文本生成新变量。"""
        source_columns = [str(column) for column in parameters["source_columns"]]
        separator = str(parameters["separator"])
        new_column = str(parameters["new_column"])

        self._ensure_columns_exist(
            columns=columns,
            required_columns=source_columns,
            error_prefix="字段拼接来源字段",
        )

        derived_rows: list[dict[str, str | None]] = []
        for row in rows:
            updated_row = dict(row)
            updated_row[new_column] = self._build_concat_value(
                row=row,
                source_columns=source_columns,
                separator=separator,
            )
            derived_rows.append(updated_row)

        return columns + [new_column], derived_rows

    def _ensure_columns_exist(
        self,
        columns: list[str],
        required_columns: list[str],
        error_prefix: str,
    ) -> None:
        """校验当前步骤依赖的字段都存在于列结构中。"""
        for column in required_columns:
            if column not in columns:
                raise DatasetPreviewError(
                    f"{error_prefix} {column} 不存在，暂时无法执行当前步骤。"
                )

    def _calculate_binary_result(
        self,
        left_value: str | None,
        right_value: str | None,
        operator: str,
    ) -> str | None:
        """根据双字段运算规则计算新变量值。"""
        if left_value is None or right_value is None:
            return None

        try:
            left_number = float(left_value)
            right_number = float(right_value)
        except ValueError:
            return None

        if operator == "add":
            result = left_number + right_number
        elif operator == "subtract":
            result = left_number - right_number
        elif operator == "multiply":
            result = left_number * right_number
        else:
            if right_number == 0:
                return None
            result = left_number / right_number

        if float(result).is_integer():
            return str(int(result))
        return str(result)

    def _build_concat_value(
        self,
        row: dict[str, str | None],
        source_columns: list[str],
        separator: str,
    ) -> str | None:
        """根据多个来源字段生成拼接文本。"""
        parts: list[str] = []
        for column in source_columns:
            value = row.get(column)
            if value is None:
                continue
            parts.append(value)

        if not parts:
            return None

        return separator.join(parts)
