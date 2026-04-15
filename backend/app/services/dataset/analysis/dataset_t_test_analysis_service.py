from statistics import mean

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


class DatasetTTestAnalysisService:
    """负责生成独立样本 t 检验结果。"""

    def build_result(
        self,
        prepared_request: DatasetAnalysisPreparedRequest,
        rows: list[dict[str, str | None]],
        raw_row_count: int,
    ) -> DatasetAnalysisResult:
        """基于清洗后的数据生成独立样本 t 检验结果。"""
        target_variable = prepared_request.variables[0]
        group_variable = prepared_request.group_variable
        if group_variable is None:
            raise DatasetAnalysisError("独立样本 t 检验必须提供分组字段。")

        grouped_values = self._collect_grouped_numeric_values(rows, target_variable, group_variable)
        if len(grouped_values) != 2:
            raise DatasetAnalysisError("独立样本 t 检验要求分组字段在有效样本中恰好包含两个组别。")

        group_names = sorted(grouped_values)
        left_group = group_names[0]
        right_group = group_names[1]
        left_values = grouped_values[left_group]
        right_values = grouped_values[right_group]
        if len(left_values) < 2 or len(right_values) < 2:
            raise DatasetAnalysisError("独立样本 t 检验要求两个组别都至少包含 2 条有效样本。")

        left_mean = mean(left_values)
        right_mean = mean(right_values)
        left_variance = self._sample_variance(left_values)
        right_variance = self._sample_variance(right_values)
        pooled_variance = (
            ((len(left_values) - 1) * left_variance)
            + ((len(right_values) - 1) * right_variance)
        ) / (len(left_values) + len(right_values) - 2)
        standard_error = (
            pooled_variance * (1 / len(left_values) + 1 / len(right_values))
        ) ** 0.5
        if standard_error == 0:
            raise DatasetAnalysisError("独立样本 t 检验当前无法处理组内方差为 0 的情况。")

        t_statistic = (left_mean - right_mean) / standard_error
        degrees_of_freedom = len(left_values) + len(right_values) - 2
        p_value = DatasetAnalysisStatsUtils.student_t_two_tailed_p_value(
            t_statistic,
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
            group_variable=group_variable,
            status="completed",
            summary=DatasetAnalysisSummary(
                title="独立样本 t 检验",
                analysis_type=prepared_request.analysis_type,
                effective_row_count=cleaned_row_count,
                excluded_row_count=excluded_row_count,
                missing_value_strategy=(
                    "先应用当前清洗步骤，再按目标字段与分组字段都非缺失的样本"
                    "执行独立样本 t 检验。"
                ),
                note=(
                    f"目标字段 {target_variable} 在组别 {left_group} 与 "
                    f"{right_group} 之间进行均值比较。"
                ),
            ),
            tables=[
                DatasetAnalysisTable(
                    key="t_test_group_summary",
                    title="分组样本汇总",
                    columns=["group", "count", "mean", "variance", "std_dev", "min", "max"],
                    rows=[
                        self._build_group_summary_row(left_group, left_values),
                        self._build_group_summary_row(right_group, right_values),
                    ],
                ),
                DatasetAnalysisTable(
                    key="t_test_result",
                    title="独立样本 t 检验结果",
                    columns=[
                        "target_variable",
                        "group_variable",
                        "group_a",
                        "group_b",
                        "mean_difference",
                        "degrees_of_freedom",
                        "t_statistic",
                        "p_value",
                    ],
                    rows=[
                        {
                            "target_variable": target_variable,
                            "group_variable": group_variable,
                            "group_a": left_group,
                            "group_b": right_group,
                            "mean_difference": DatasetAnalysisStatsUtils.round_number(
                                left_mean - right_mean
                            ),
                            "degrees_of_freedom": degrees_of_freedom,
                            "t_statistic": DatasetAnalysisStatsUtils.round_number(t_statistic),
                            "p_value": DatasetAnalysisStatsUtils.round_number(p_value),
                        }
                    ],
                ),
            ],
            plots=[
                DatasetAnalysisPlot(
                    key="t_test_boxplot",
                    title=f"{target_variable} 按 {group_variable} 分组箱线图",
                    plot_type="grouped_boxplot",
                    spec={
                        "target_variable": target_variable,
                        "group_variable": group_variable,
                        "groups": [
                            {
                                "name": left_group,
                                "values": [
                                    DatasetAnalysisStatsUtils.round_number(value)
                                    for value in left_values
                                ],
                            },
                            {
                                "name": right_group,
                                "values": [
                                    DatasetAnalysisStatsUtils.round_number(value)
                                    for value in right_values
                                ],
                            },
                        ],
                    },
                ),
                DatasetAnalysisPlot(
                    key="t_test_mean_bar",
                    title=f"{target_variable} 分组均值对比图",
                    plot_type="bar_chart",
                    spec={
                        "categories": [left_group, right_group],
                        "values": [
                            DatasetAnalysisStatsUtils.round_number(left_mean),
                            DatasetAnalysisStatsUtils.round_number(right_mean),
                        ],
                    },
                ),
            ],
            interpretations=[
                (
                    f"字段 {target_variable} 在组别 {left_group} 与 {right_group} 之间的 "
                    f"t 统计量为 {DatasetAnalysisStatsUtils.round_number(t_statistic)}，"
                    f"自由度为 {degrees_of_freedom}，p 值为 "
                    f"{DatasetAnalysisStatsUtils.round_number(p_value)}。"
                )
            ],
        )

    def _collect_grouped_numeric_values(
        self,
        rows: list[dict[str, str | None]],
        target_variable: str,
        group_variable: str,
    ) -> dict[str, list[float]]:
        """收集独立样本 t 检验所需的分组数值。"""
        grouped_values: dict[str, list[float]] = {}
        invalid_values_found = False

        for row in rows:
            group_value = self._normalize_value(row.get(group_variable))
            raw_target_value = row.get(target_variable)
            if group_value is None or raw_target_value is None or not raw_target_value.strip():
                continue

            try:
                numeric_value = float(raw_target_value.strip())
            except ValueError:
                invalid_values_found = True
                continue

            grouped_values.setdefault(group_value, []).append(numeric_value)

        if invalid_values_found:
            raise DatasetAnalysisError(
                f"独立样本 t 检验当前仅支持数值型目标字段：{target_variable}。"
            )

        return grouped_values

    def _build_group_summary_row(self, group_name: str, values: list[float]) -> dict[str, object]:
        """构建单个组别的汇总行。"""
        variance = self._sample_variance(values)
        return {
            "group": group_name,
            "count": len(values),
            "mean": DatasetAnalysisStatsUtils.round_number(mean(values)),
            "variance": DatasetAnalysisStatsUtils.round_number(variance),
            "std_dev": DatasetAnalysisStatsUtils.round_number(variance**0.5),
            "min": DatasetAnalysisStatsUtils.round_number(min(values)),
            "max": DatasetAnalysisStatsUtils.round_number(max(values)),
        }

    def _sample_variance(self, values: list[float]) -> float:
        """计算样本方差。"""
        value_mean = mean(values)
        return sum((value - value_mean) ** 2 for value in values) / (len(values) - 1)

    def _normalize_value(self, value: str | None) -> str | None:
        """把分组字段值整理成去除首尾空格的文本。"""
        if value is None:
            return None

        normalized_value = value.strip()
        return normalized_value or None
