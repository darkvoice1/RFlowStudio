from app.core.exceptions import DatasetAnalysisError
from app.schemas.analysis import (
    DatasetAnalysisPreparedRequest,
    DatasetAnalysisResult,
)
from app.services.resources.analysis.analysis_r_execution_service import (
    DatasetAnalysisRExecutionService,
)


class DatasetAnalysisExecutionService:
    """Dispatch analysis requests to the R execution layer."""

    def __init__(self) -> None:
        self.r_execution_service = DatasetAnalysisRExecutionService()

    def build_result(
        self,
        prepared_request: DatasetAnalysisPreparedRequest,
        columns: list[str],
        rows: list[dict[str, str | None]],
        raw_row_count: int,
    ) -> DatasetAnalysisResult:
        if not self.r_execution_service.is_available():
            raise DatasetAnalysisError(
                "当前无法连接到 R 统计服务，请确认 r-analysis 容器已经启动。"
            )

        return self.r_execution_service.build_result(
            prepared_request=prepared_request,
            columns=columns,
            rows=rows,
            raw_row_count=raw_row_count,
        )
