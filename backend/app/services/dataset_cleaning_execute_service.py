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
    ) -> tuple[list[str], list[dict[str, str | None]]]:
        """按顺序把已启用的清洗步骤作用到当前列结构和行数据上。"""
        current_columns = list(columns)
        filtered_rows = rows
        for step in cleaning_steps:
            if step.step_type == "filter":
                filtered_rows = self._apply_filter_step(current_columns, filtered_rows, step)
                continue
            if step.step_type == "missing_value":
                filtered_rows = self._apply_missing_value_step(
                    current_columns,
                    filtered_rows,
                    step,
                )
                continue
            if step.step_type == "sort":
                filtered_rows = self._apply_sort_step(current_columns, filtered_rows, step)
                continue
            if step.step_type == "recode":
                filtered_rows = self._apply_recode_step(current_columns, filtered_rows, step)
                continue
            if step.step_type == "derive_variable":
                current_columns, filtered_rows = self._apply_derive_variable_step(
                    current_columns,
                    filtered_rows,
                    step,
                )

        return current_columns, filtered_rows

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

    def _apply_missing_value_step(
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

    def _apply_sort_step(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
        step: DatasetCleaningStepRecord,
    ) -> list[dict[str, str | None]]:
        """执行单个排序步骤并返回排序后的行数据。"""
        parameters = step.parameters
        column = str(parameters["column"])
        direction = str(parameters["direction"])

        if column not in columns:
            raise DatasetPreviewError(f"排序字段 {column} 不存在，暂时无法执行当前排序步骤。")

        # 先拆分非空和空值，保证空值始终排在最后，结果更符合用户直觉。
        non_missing_rows = [row for row in rows if row.get(column) is not None]
        missing_rows = [row for row in rows if row.get(column) is None]
        reverse = direction == "desc"

        if self._should_sort_as_number(non_missing_rows, column):
            sorted_rows = sorted(
                non_missing_rows,
                key=lambda row: float(row[column] or 0),
                reverse=reverse,
            )
        else:
            sorted_rows = sorted(
                non_missing_rows,
                key=lambda row: (row.get(column) or "").lower(),
                reverse=reverse,
            )

        return sorted_rows + missing_rows

    def _apply_recode_step(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
        step: DatasetCleaningStepRecord,
    ) -> list[dict[str, str | None]]:
        """执行单个字段重编码步骤并返回处理后的行数据。"""
        parameters = step.parameters
        column = str(parameters["column"])
        mapping = {
            str(source): str(target)
            for source, target in dict(parameters["mapping"]).items()
        }

        if column not in columns:
            raise DatasetPreviewError(
                f"重编码字段 {column} 不存在，暂时无法执行当前重编码步骤。"
            )

        recoded_rows: list[dict[str, str | None]] = []
        for row in rows:
            updated_row = dict(row)
            current_value = updated_row.get(column)
            if current_value is not None and current_value in mapping:
                updated_row[column] = mapping[current_value]
            recoded_rows.append(updated_row)

        return recoded_rows

    def _apply_derive_variable_step(
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

    def _should_sort_as_number(
        self,
        rows: list[dict[str, str | None]],
        column: str,
    ) -> bool:
        """判断当前排序列是否适合按数值大小排序。"""
        if not rows:
            return False

        for row in rows:
            value = row.get(column)
            if value is None:
                continue
            try:
                float(value)
            except ValueError:
                return False

        return True

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
