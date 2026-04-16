from pathlib import Path

from app.core.exceptions import DatasetAnalysisError
from app.schemas.analysis import (
    DatasetAnalysisCreateRequest,
    DatasetAnalysisPreparedRequest,
    DatasetAnalysisResult,
)
from app.schemas.dataset import DatasetCleaningStepRecord, DatasetRecord
from app.services.dataset.analysis.dataset_analysis_execution_service import (
    DatasetAnalysisExecutionService,
)
from app.services.dataset.cleaning.dataset_cleaning_execute_service import (
    DatasetCleaningExecuteService,
)
from app.services.dataset.dataset_reader_service import DatasetReaderService


class DatasetAnalysisService:
    """负责统计分析任务的统一校验和结果骨架构建。"""

    def __init__(self) -> None:
        """初始化统计分析服务依赖的读取器。"""
        self.reader_service = DatasetReaderService()
        self.cleaning_execute_service = DatasetCleaningExecuteService()
        self.execution_service = DatasetAnalysisExecutionService()

    def prepare_request(
        self,
        record: DatasetRecord,
        data_file_path: Path,
        payload: DatasetAnalysisCreateRequest,
    ) -> DatasetAnalysisPreparedRequest:
        """校验统计分析任务请求，并整理成统一内部结构。"""
        columns = self.reader_service.read_columns(
            record=record,
            data_file_path=data_file_path,
            empty_message="当前统计分析接口暂不支持该文件格式。",
            csv_header_message="CSV 文件缺少表头，暂时无法发起统计分析。",
            xlsx_header_message="XLSX 文件缺少表头，暂时无法发起统计分析。",
            xlsx_invalid_message="XLSX 文件格式异常，暂时无法发起统计分析。",
        )
        normalized_variables = self._normalize_variables(payload.variables)
        normalized_group_variable = self._normalize_group_variable(payload.group_variable)
        self._validate_analysis_request(
            analysis_type=payload.analysis_type,
            variables=normalized_variables,
            group_variable=normalized_group_variable,
            columns=columns,
        )

        return DatasetAnalysisPreparedRequest(
            dataset_id=record.id,
            dataset_name=record.name,
            file_name=record.file_name,
            analysis_type=payload.analysis_type,
            variables=normalized_variables,
            group_variable=normalized_group_variable,
            options=dict(payload.options),
        )

    def build_result(
        self,
        record: DatasetRecord,
        data_file_path: Path,
        prepared_request: DatasetAnalysisPreparedRequest,
        cleaning_steps: list[DatasetCleaningStepRecord],
    ) -> DatasetAnalysisResult:
        """根据分析类型生成统计分析结果。"""
        columns, raw_rows = self.reader_service.read_all_rows(
            record=record,
            data_file_path=data_file_path,
            empty_message="当前统计分析接口暂不支持该文件格式。",
            csv_header_message="CSV 文件缺少表头，暂时无法发起统计分析。",
            xlsx_header_message="XLSX 文件缺少表头，暂时无法发起统计分析。",
            xlsx_invalid_message="XLSX 文件格式异常，暂时无法发起统计分析。",
        )
        columns, rows = self.cleaning_execute_service.apply_cleaning_steps(
            columns=columns,
            rows=raw_rows,
            cleaning_steps=cleaning_steps,
        )

        return self.execution_service.build_result(
            prepared_request=prepared_request,
            columns=columns,
            rows=rows,
            raw_row_count=len(raw_rows),
        )

    def _normalize_variables(self, raw_variables: list[str]) -> list[str]:
        """整理变量列表，去掉空值并保留用户给出的顺序。"""
        normalized_variables: list[str] = []
        for raw_variable in raw_variables:
            variable = raw_variable.strip()
            if not variable:
                continue
            if variable not in normalized_variables:
                normalized_variables.append(variable)

        return normalized_variables

    def _normalize_group_variable(self, group_variable: str | None) -> str | None:
        """整理分组变量文本。"""
        if group_variable is None:
            return None

        normalized_group_variable = group_variable.strip()
        return normalized_group_variable or None

    def _validate_analysis_request(
        self,
        analysis_type: str,
        variables: list[str],
        group_variable: str | None,
        columns: list[str],
    ) -> None:
        """校验不同统计分析方法的最小入参要求。"""
        self._validate_columns_exist(
            variables=variables,
            group_variable=group_variable,
            columns=columns,
        )

        if analysis_type == "descriptive_statistics":
            if not variables:
                raise DatasetAnalysisError("描述统计至少需要选择一个字段。")
            return

        if analysis_type == "correlation_analysis":
            if len(variables) < 2:
                raise DatasetAnalysisError("相关分析至少需要选择两个字段。")
            return

        if analysis_type == "chi_square_test":
            if len(variables) != 2:
                raise DatasetAnalysisError("卡方检验当前要求选择且仅选择两个字段。")
            return

        if analysis_type in {"independent_samples_t_test", "one_way_anova"}:
            if len(variables) != 1:
                raise DatasetAnalysisError("当前分析方法需要且仅需要选择一个目标字段。")
            if group_variable is None:
                raise DatasetAnalysisError("当前分析方法必须提供分组字段。")
            if group_variable == variables[0]:
                raise DatasetAnalysisError("分组字段不能和目标字段相同。")
            return

        raise DatasetAnalysisError("当前分析类型暂不受支持。")

    def _validate_columns_exist(
        self,
        variables: list[str],
        group_variable: str | None,
        columns: list[str],
    ) -> None:
        """校验用户选择的字段是否真实存在于当前数据集。"""
        missing_columns = [variable for variable in variables if variable not in columns]
        if group_variable is not None and group_variable not in columns:
            missing_columns.append(group_variable)

        if missing_columns:
            joined_columns = "、".join(missing_columns)
            raise DatasetAnalysisError(f"统计分析请求包含不存在的字段：{joined_columns}。")
