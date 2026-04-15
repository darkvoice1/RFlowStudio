from app.core.exceptions import DatasetPreviewError
from app.schemas.dataset import DatasetCleaningStepRecord


class DatasetSortExecuteService:
    """执行排序步骤。"""

    def apply_step(
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
