from itertools import combinations
from math import sqrt

from app.core.exceptions import DatasetAnalysisError
from app.schemas.analysis import (
    DatasetAnalysisPlot,
    DatasetAnalysisPreparedRequest,
    DatasetAnalysisResult,
    DatasetAnalysisSummary,
    DatasetAnalysisTable,
)


class DatasetCorrelationAnalysisService:
    """负责生成相关分析结果。"""

    def build_result(
        self,
        prepared_request: DatasetAnalysisPreparedRequest,
        rows: list[dict[str, str | None]],
        raw_row_count: int,
    ) -> DatasetAnalysisResult:
        """基于清洗后的数据生成相关分析结果。"""
        method = self._normalize_method(prepared_request.options.get("method"))
        numeric_rows = self._build_numeric_rows(prepared_request.variables, rows)
        pair_rows: list[dict[str, object | None]] = []
        heatmap_cells: list[dict[str, object]] = []
        pair_counts: dict[tuple[str, str], int] = {}
        correlations: dict[tuple[str, str], float | None] = {}
        scatter_plots: list[DatasetAnalysisPlot] = []
        interpretations: list[str] = []

        for left, right in combinations(prepared_request.variables, 2):
            paired_values = self._collect_paired_values(numeric_rows, left, right)
            pair_count = len(paired_values)
            if pair_count < 2:
                raise DatasetAnalysisError(
                    f"字段 {left} 和 {right} 的有效成对样本不足 2 条，暂时无法进行相关分析。"
                )

            correlation = self._compute_pearson_correlation(paired_values)
            pair_counts[(left, right)] = pair_count
            correlations[(left, right)] = correlation
            pair_rows.append(
                {
                    "variable_x": left,
                    "variable_y": right,
                    "method": method,
                    "pair_count": pair_count,
                    "correlation": (
                        self._round_number(correlation) if correlation is not None else None
                    ),
                }
            )
            heatmap_cells.append(
                {
                    "x": left,
                    "y": right,
                    "value": self._round_number(correlation) if correlation is not None else None,
                }
            )
            heatmap_cells.append(
                {
                    "x": right,
                    "y": left,
                    "value": self._round_number(correlation) if correlation is not None else None,
                }
            )
            scatter_plots.append(
                DatasetAnalysisPlot(
                    key=f"{left}_{right}_scatter",
                    title=f"{left} 与 {right} 散点图",
                    plot_type="scatter_plot",
                    spec={
                        "x": left,
                        "y": right,
                        "points": [
                            {
                                "x": self._round_number(x_value),
                                "y": self._round_number(y_value),
                            }
                            for x_value, y_value in paired_values
                        ],
                    },
                )
            )
            interpretations.append(
                self._build_pair_interpretation(left, right, correlation, pair_count)
            )

        correlation_matrix = self._build_correlation_matrix(
            prepared_request.variables,
            correlations,
        )
        pair_count_matrix = self._build_pair_count_matrix(prepared_request.variables, pair_counts)
        cleaned_row_count = len(rows)
        excluded_row_count = max(raw_row_count - cleaned_row_count, 0)
        note = "当前第一版相关分析默认采用皮尔逊相关，并按成对非缺失样本计算。"
        if excluded_row_count > 0:
            note = (
                f"当前数据清洗步骤已先剔除 {excluded_row_count} 行记录，"
                "随后按成对非缺失样本执行皮尔逊相关分析。"
            )

        return DatasetAnalysisResult(
            dataset_id=prepared_request.dataset_id,
            dataset_name=prepared_request.dataset_name,
            file_name=prepared_request.file_name,
            analysis_type=prepared_request.analysis_type,
            variables=prepared_request.variables,
            group_variable=prepared_request.group_variable,
            status="completed",
            summary=DatasetAnalysisSummary(
                title="相关分析",
                analysis_type=prepared_request.analysis_type,
                effective_row_count=cleaned_row_count,
                excluded_row_count=excluded_row_count,
                missing_value_strategy="先应用当前清洗步骤，再按字段对的成对非缺失样本计算皮尔逊相关。",
                note=note,
            ),
            tables=[
                correlation_matrix,
                pair_count_matrix,
                DatasetAnalysisTable(
                    key="correlation_pairs",
                    title="字段对相关结果",
                    columns=["variable_x", "variable_y", "method", "pair_count", "correlation"],
                    rows=pair_rows,
                ),
            ],
            plots=[
                DatasetAnalysisPlot(
                    key="correlation_heatmap",
                    title="相关系数热力图",
                    plot_type="heatmap",
                    spec={
                        "variables": prepared_request.variables,
                        "cells": [
                            *[
                                {"x": variable, "y": variable, "value": 1}
                                for variable in prepared_request.variables
                            ],
                            *heatmap_cells,
                        ],
                    },
                ),
                *scatter_plots,
            ],
            interpretations=interpretations,
        )

    def _normalize_method(self, raw_method: object) -> str:
        """整理相关分析方法配置。"""
        if raw_method is None:
            return "pearson"

        method = str(raw_method).strip().lower()
        if method != "pearson":
            raise DatasetAnalysisError("当前相关分析仅支持 pearson 方法。")

        return method

    def _build_numeric_rows(
        self,
        variables: list[str],
        rows: list[dict[str, str | None]],
    ) -> list[dict[str, float | None]]:
        """把选中字段整理成可用于相关分析的数值行。"""
        numeric_rows: list[dict[str, float | None]] = []
        invalid_columns: set[str] = set()

        for row in rows:
            numeric_row: dict[str, float | None] = {}
            for variable in variables:
                raw_value = row.get(variable)
                if raw_value is None or not raw_value.strip():
                    numeric_row[variable] = None
                    continue

                try:
                    numeric_row[variable] = float(raw_value.strip())
                except ValueError:
                    invalid_columns.add(variable)
                    numeric_row[variable] = None

            numeric_rows.append(numeric_row)

        if invalid_columns:
            joined_columns = "、".join(sorted(invalid_columns))
            raise DatasetAnalysisError(f"相关分析当前仅支持数值型字段：{joined_columns}。")

        return numeric_rows

    def _collect_paired_values(
        self,
        numeric_rows: list[dict[str, float | None]],
        left: str,
        right: str,
    ) -> list[tuple[float, float]]:
        """收集两个字段都存在有效数值的成对样本。"""
        paired_values: list[tuple[float, float]] = []
        for row in numeric_rows:
            left_value = row.get(left)
            right_value = row.get(right)
            if left_value is None or right_value is None:
                continue

            paired_values.append((left_value, right_value))

        return paired_values

    def _compute_pearson_correlation(
        self,
        paired_values: list[tuple[float, float]],
    ) -> float | None:
        """计算皮尔逊相关系数。"""
        sample_size = len(paired_values)
        mean_x = sum(x_value for x_value, _ in paired_values) / sample_size
        mean_y = sum(y_value for _, y_value in paired_values) / sample_size
        covariance = sum(
            (x_value - mean_x) * (y_value - mean_y)
            for x_value, y_value in paired_values
        )
        variance_x = sum((x_value - mean_x) ** 2 for x_value, _ in paired_values)
        variance_y = sum((y_value - mean_y) ** 2 for _, y_value in paired_values)
        denominator = sqrt(variance_x * variance_y)

        if denominator == 0:
            return None

        return covariance / denominator

    def _build_correlation_matrix(
        self,
        variables: list[str],
        correlations: dict[tuple[str, str], float | None],
    ) -> DatasetAnalysisTable:
        """生成相关系数矩阵表。"""
        rows: list[dict[str, object | None]] = []
        for left in variables:
            row: dict[str, object | None] = {"variable": left}
            for right in variables:
                if left == right:
                    row[right] = 1
                    continue

                pair_key = (left, right)
                reverse_key = (right, left)
                correlation = correlations.get(pair_key, correlations.get(reverse_key))
                row[right] = self._round_number(correlation) if correlation is not None else None

            rows.append(row)

        return DatasetAnalysisTable(
            key="correlation_matrix",
            title="相关系数矩阵",
            columns=["variable", *variables],
            rows=rows,
        )

    def _build_pair_count_matrix(
        self,
        variables: list[str],
        pair_counts: dict[tuple[str, str], int],
    ) -> DatasetAnalysisTable:
        """生成成对样本量矩阵表。"""
        rows: list[dict[str, object]] = []
        for left in variables:
            row: dict[str, object] = {"variable": left}
            for right in variables:
                if left == right:
                    row[right] = None
                    continue

                pair_key = (left, right)
                reverse_key = (right, left)
                row[right] = pair_counts.get(pair_key, pair_counts.get(reverse_key))

            rows.append(row)

        return DatasetAnalysisTable(
            key="correlation_pair_counts",
            title="成对样本量矩阵",
            columns=["variable", *variables],
            rows=rows,
        )

    def _build_pair_interpretation(
        self,
        left: str,
        right: str,
        correlation: float | None,
        pair_count: int,
    ) -> str:
        """生成人类可读的字段对解释。"""
        if correlation is None:
            return (
                f"字段 {left} 与 {right} 共有 {pair_count} 条成对有效样本，"
                "但至少有一个字段方差为 0，当前无法计算有效相关系数。"
            )

        rounded = self._round_number(correlation)
        direction = "正相关" if correlation > 0 else "负相关"
        if correlation == 0:
            direction = "线性相关性较弱"

        return (
            f"字段 {left} 与 {right} 基于 {pair_count} 条成对有效样本"
            f"计算得到皮尔逊相关系数 {rounded}，"
            f"表现为{direction}。"
        )

    def _round_number(self, value: float) -> float | int:
        """把结果整理成更适合展示的数值。"""
        rounded_value = round(value, 4)
        if rounded_value.is_integer():
            return int(rounded_value)

        return rounded_value
