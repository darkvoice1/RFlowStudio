from app.schemas.dataset import DatasetCleaningStepRecord
from app.services.resources.datasets.cleaning.dataset_derive_variable_execute_service import (
    DatasetDeriveVariableExecuteService,
)
from app.services.resources.datasets.cleaning.dataset_filter_execute_service import (
    DatasetFilterExecuteService,
)
from app.services.resources.datasets.cleaning.dataset_missing_value_execute_service import (
    DatasetMissingValueExecuteService,
)
from app.services.resources.datasets.cleaning.dataset_recode_execute_service import (
    DatasetRecodeExecuteService,
)
from app.services.resources.datasets.cleaning.dataset_sort_execute_service import (
    DatasetSortExecuteService,
)


class DatasetCleaningExecuteService:
    """Dispatch dataset cleaning steps by step type."""

    def __init__(self) -> None:
        self.filter_execute_service = DatasetFilterExecuteService()
        self.missing_value_execute_service = DatasetMissingValueExecuteService()
        self.sort_execute_service = DatasetSortExecuteService()
        self.recode_execute_service = DatasetRecodeExecuteService()
        self.derive_variable_execute_service = DatasetDeriveVariableExecuteService()

    def apply_cleaning_steps(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
        cleaning_steps: list[DatasetCleaningStepRecord],
    ) -> tuple[list[str], list[dict[str, str | None]]]:
        current_columns = list(columns)
        filtered_rows = rows
        for step in cleaning_steps:
            if step.step_type == "filter":
                filtered_rows = self.filter_execute_service.apply_step(
                    current_columns,
                    filtered_rows,
                    step,
                )
                continue
            if step.step_type == "missing_value":
                filtered_rows = self.missing_value_execute_service.apply_step(
                    current_columns,
                    filtered_rows,
                    step,
                )
                continue
            if step.step_type == "sort":
                filtered_rows = self.sort_execute_service.apply_step(
                    current_columns,
                    filtered_rows,
                    step,
                )
                continue
            if step.step_type == "recode":
                filtered_rows = self.recode_execute_service.apply_step(
                    current_columns,
                    filtered_rows,
                    step,
                )
                continue
            if step.step_type == "derive_variable":
                current_columns, filtered_rows = self.derive_variable_execute_service.apply_step(
                    current_columns,
                    filtered_rows,
                    step,
                )

        return current_columns, filtered_rows
