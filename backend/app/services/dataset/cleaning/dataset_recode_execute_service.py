from app.core.exceptions import DatasetPreviewError
from app.schemas.dataset import DatasetCleaningStepRecord


class DatasetRecodeExecuteService:
    """执行字段重编码步骤。"""

    def apply_step(
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
