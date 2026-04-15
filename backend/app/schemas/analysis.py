from typing import Any, Literal

from pydantic import BaseModel, Field

DatasetAnalysisType = Literal[
    "descriptive_statistics",
    "independent_samples_t_test",
    "one_way_anova",
    "chi_square_test",
    "correlation_analysis",
]


class DatasetAnalysisCreateRequest(BaseModel):
    """定义创建统计分析任务时的请求结构。"""

    analysis_type: DatasetAnalysisType
    variables: list[str] = Field(default_factory=list)
    group_variable: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class DatasetAnalysisPreparedRequest(BaseModel):
    """定义通过校验后的统计分析任务内部结构。"""

    dataset_id: str
    dataset_name: str
    file_name: str
    analysis_type: DatasetAnalysisType
    variables: list[str]
    group_variable: str | None
    options: dict[str, Any]


class DatasetAnalysisTable(BaseModel):
    """定义统一统计分析结果中的表格结构。"""

    key: str
    title: str
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)


class DatasetAnalysisPlot(BaseModel):
    """定义统一统计分析结果中的图形结构。"""

    key: str
    title: str
    plot_type: str
    spec: dict[str, Any] = Field(default_factory=dict)


class DatasetAnalysisSummary(BaseModel):
    """定义统一统计分析结果中的摘要结构。"""

    title: str
    analysis_type: DatasetAnalysisType
    effective_row_count: int | None = None
    excluded_row_count: int | None = None
    missing_value_strategy: str
    note: str | None = None


class DatasetAnalysisResult(BaseModel):
    """定义统一统计分析结果结构。"""

    dataset_id: str
    dataset_name: str
    file_name: str
    analysis_type: DatasetAnalysisType
    variables: list[str]
    group_variable: str | None
    status: Literal["skeleton_ready"]
    summary: DatasetAnalysisSummary
    tables: list[DatasetAnalysisTable] = Field(default_factory=list)
    plots: list[DatasetAnalysisPlot] = Field(default_factory=list)
    interpretations: list[str] = Field(default_factory=list)
