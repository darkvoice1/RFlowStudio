from app.core.exceptions import DatasetAnalysisError
from app.schemas.analysis import (
    DatasetAnalysisPlot,
    DatasetAnalysisPreparedRequest,
    DatasetAnalysisResult,
    DatasetAnalysisSummary,
    DatasetAnalysisTable,
)
from app.services.dataset.analysis.dataset_analysis_stats_utils import (
    DatasetAnalysisStatsUtils,
)


class DatasetChiSquareAnalysisService:
    """负责生成卡方检验结果。"""

    def build_result(
        self,
        prepared_request: DatasetAnalysisPreparedRequest,
        rows: list[dict[str, str | None]],
        raw_row_count: int,
    ) -> DatasetAnalysisResult:
        """基于清洗后的数据生成卡方检验结果。"""
        left_variable, right_variable = prepared_request.variables
        paired_rows = self._collect_valid_pairs(rows, left_variable, right_variable)
        if len(paired_rows) < 2:
            raise DatasetAnalysisError("卡方检验的有效成对样本不足 2 条。")

        left_levels = sorted({left for left, _ in paired_rows})
        right_levels = sorted({right for _, right in paired_rows})
        if len(left_levels) < 2 or len(right_levels) < 2:
            raise DatasetAnalysisError("卡方检验要求两个字段在有效样本中都至少包含两个取值。")

        observed_counts = self._build_observed_counts(paired_rows, left_levels, right_levels)
        row_totals = {level: sum(observed_counts[level].values()) for level in left_levels}
        column_totals = {
            level: sum(observed_counts[row_level][level] for row_level in left_levels)
            for level in right_levels
        }
        total_count = len(paired_rows)
        expected_counts = self._build_expected_counts(
            left_levels,
            right_levels,
            row_totals,
            column_totals,
            total_count,
        )
        chi_square_value = self._compute_chi_square(
            left_levels,
            right_levels,
            observed_counts,
            expected_counts,
        )
        degrees_of_freedom = (len(left_levels) - 1) * (len(right_levels) - 1)
        p_value = DatasetAnalysisStatsUtils.chi_square_survival_p_value(
            chi_square_value,
            degrees_of_freedom,
        )
        cleaned_row_count = len(rows)
        excluded_row_count = max(raw_row_count - cleaned_row_count, 0)

        return DatasetAnalysisResult(
            dataset_id=prepared_request.dataset_id,
            dataset_name=prepared_request.dataset_name,
            file_name=prepared_request.file_name,
            analysis_type=prepared_request.analysis_type,
            variables=prepared_request.variables,
            group_variable=prepared_request.group_variable,
            status="completed",
            summary=DatasetAnalysisSummary(
                title="卡方检验",
                analysis_type=prepared_request.analysis_type,
                effective_row_count=cleaned_row_count,
                excluded_row_count=excluded_row_count,
                missing_value_strategy="先应用当前清洗步骤，再按两个字段都非缺失的成对样本执行卡方检验。",
                note=(
                    f"卡方检验基于 {total_count} 条有效成对样本，"
                    f"比较字段 {left_variable} 与 {right_variable} 的分类分布差异。"
                ),
            ),
            tables=[
                self._build_count_table(
                    "chi_square_observed",
                    "列联表（观测频数）",
                    left_levels,
                    right_levels,
                    observed_counts,
                ),
                self._build_expected_table(left_levels, right_levels, expected_counts),
                DatasetAnalysisTable(
                    key="chi_square_summary",
                    title="卡方检验汇总",
                    columns=[
                        "variable_x",
                        "variable_y",
                        "sample_count",
                        "degrees_of_freedom",
                        "chi_square",
                        "p_value",
                    ],
                    rows=[
                        {
                            "variable_x": left_variable,
                            "variable_y": right_variable,
                            "sample_count": total_count,
                            "degrees_of_freedom": degrees_of_freedom,
                            "chi_square": DatasetAnalysisStatsUtils.round_number(
                                chi_square_value
                            ),
                            "p_value": DatasetAnalysisStatsUtils.round_number(p_value),
                        }
                    ],
                ),
            ],
            plots=[
                DatasetAnalysisPlot(
                    key="chi_square_grouped_bar",
                    title=f"{left_variable} 与 {right_variable} 分组条形图",
                    plot_type="grouped_bar_chart",
                    spec={
                        "x_categories": left_levels,
                        "series": [
                            {
                                "name": level,
                                "counts": [
                                    observed_counts[row_level][level]
                                    for row_level in left_levels
                                ],
                            }
                            for level in right_levels
                        ],
                    },
                ),
                DatasetAnalysisPlot(
                    key="chi_square_heatmap",
                    title=f"{left_variable} 与 {right_variable} 列联热力图",
                    plot_type="heatmap",
                    spec={
                        "x_categories": right_levels,
                        "y_categories": left_levels,
                        "cells": [
                            {
                                "x": right_level,
                                "y": left_level,
                                "value": observed_counts[left_level][right_level],
                            }
                            for left_level in left_levels
                            for right_level in right_levels
                        ],
                    },
                ),
            ],
            interpretations=[
                (
                    f"字段 {left_variable} 与 {right_variable} 的卡方检验统计量为 "
                    f"{DatasetAnalysisStatsUtils.round_number(chi_square_value)}，"
                    f"自由度为 {degrees_of_freedom}，p 值为 "
                    f"{DatasetAnalysisStatsUtils.round_number(p_value)}。"
                )
            ],
        )

    def _collect_valid_pairs(
        self,
        rows: list[dict[str, str | None]],
        left_variable: str,
        right_variable: str,
    ) -> list[tuple[str, str]]:
        """收集两个分类字段都存在有效值的样本。"""
        paired_rows: list[tuple[str, str]] = []
        for row in rows:
            left_value = self._normalize_value(row.get(left_variable))
            right_value = self._normalize_value(row.get(right_variable))
            if left_value is None or right_value is None:
                continue

            paired_rows.append((left_value, right_value))

        return paired_rows

    def _build_observed_counts(
        self,
        paired_rows: list[tuple[str, str]],
        left_levels: list[str],
        right_levels: list[str],
    ) -> dict[str, dict[str, int]]:
        """生成观测频数列联表。"""
        observed_counts = {
            left_level: {right_level: 0 for right_level in right_levels}
            for left_level in left_levels
        }
        for left_value, right_value in paired_rows:
            observed_counts[left_value][right_value] += 1

        return observed_counts

    def _build_expected_counts(
        self,
        left_levels: list[str],
        right_levels: list[str],
        row_totals: dict[str, int],
        column_totals: dict[str, int],
        total_count: int,
    ) -> dict[str, dict[str, float]]:
        """根据边际频数生成期望频数表。"""
        expected_counts = {
            left_level: {right_level: 0.0 for right_level in right_levels}
            for left_level in left_levels
        }
        for left_level in left_levels:
            for right_level in right_levels:
                expected_counts[left_level][right_level] = (
                    row_totals[left_level] * column_totals[right_level] / total_count
                )

        return expected_counts

    def _compute_chi_square(
        self,
        left_levels: list[str],
        right_levels: list[str],
        observed_counts: dict[str, dict[str, int]],
        expected_counts: dict[str, dict[str, float]],
    ) -> float:
        """计算卡方统计量。"""
        chi_square_value = 0.0
        for left_level in left_levels:
            for right_level in right_levels:
                expected_value = expected_counts[left_level][right_level]
                if expected_value == 0:
                    continue

                observed_value = observed_counts[left_level][right_level]
                chi_square_value += (observed_value - expected_value) ** 2 / expected_value

        return chi_square_value

    def _build_count_table(
        self,
        key: str,
        title: str,
        left_levels: list[str],
        right_levels: list[str],
        count_matrix: dict[str, dict[str, int]],
    ) -> DatasetAnalysisTable:
        """生成列联表。"""
        rows = []
        for left_level in left_levels:
            row: dict[str, int | str] = {"variable": left_level}
            row.update(
                {
                    right_level: count_matrix[left_level][right_level]
                    for right_level in right_levels
                }
            )
            rows.append(row)

        return DatasetAnalysisTable(
            key=key,
            title=title,
            columns=["variable", *right_levels],
            rows=rows,
        )

    def _build_expected_table(
        self,
        left_levels: list[str],
        right_levels: list[str],
        expected_counts: dict[str, dict[str, float]],
    ) -> DatasetAnalysisTable:
        """生成期望频数表。"""
        rows = []
        for left_level in left_levels:
            row: dict[str, object] = {"variable": left_level}
            row.update(
                {
                    right_level: DatasetAnalysisStatsUtils.round_number(
                        expected_counts[left_level][right_level]
                    )
                    for right_level in right_levels
                }
            )
            rows.append(row)

        return DatasetAnalysisTable(
            key="chi_square_expected",
            title="列联表（期望频数）",
            columns=["variable", *right_levels],
            rows=rows,
        )

    def _normalize_value(self, value: str | None) -> str | None:
        """把分类字段值整理成去除首尾空格的文本。"""
        if value is None:
            return None

        normalized_value = value.strip()
        return normalized_value or None
