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


class DatasetAnovaAnalysisService:
    """负责生成单因素方差分析结果。"""

    def build_result(
        self,
        prepared_request: DatasetAnalysisPreparedRequest,
        rows: list[dict[str, str | None]],
        raw_row_count: int,
    ) -> DatasetAnalysisResult:
        """基于清洗后的数据生成单因素方差分析结果。"""
        target_variable = prepared_request.variables[0]
        group_variable = prepared_request.group_variable
        if group_variable is None:
            raise DatasetAnalysisError("单因素方差分析必须提供分组字段。")

        grouped_values = self._collect_grouped_numeric_values(rows, target_variable, group_variable)
        if len(grouped_values) < 2:
            raise DatasetAnalysisError("单因素方差分析要求有效样本中至少包含两个组别。")

        group_names = sorted(grouped_values)
        total_count = sum(len(values) for values in grouped_values.values())
        if total_count <= len(group_names):
            raise DatasetAnalysisError("单因素方差分析要求总样本量大于组别数量。")

        all_values = [value for values in grouped_values.values() for value in values]
        grand_mean = mean(all_values)
        ss_between = sum(
            len(grouped_values[group_name]) * (mean(grouped_values[group_name]) - grand_mean) ** 2
            for group_name in group_names
        )
        ss_within = sum(
            sum((value - mean(values)) ** 2 for value in values)
            for values in grouped_values.values()
        )
        df_between = len(group_names) - 1
        df_within = total_count - len(group_names)
        ms_between = ss_between / df_between
        ms_within = ss_within / df_within
        if ms_within == 0:
            raise DatasetAnalysisError("单因素方差分析当前无法处理组内方差为 0 的情况。")

        f_statistic = ms_between / ms_within
        p_value = DatasetAnalysisStatsUtils.f_survival_p_value(
            f_statistic,
            df_between,
            df_within,
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
                title="单因素方差分析",
                analysis_type=prepared_request.analysis_type,
                effective_row_count=cleaned_row_count,
                excluded_row_count=excluded_row_count,
                missing_value_strategy="先应用当前清洗步骤，再按目标字段与分组字段都非缺失的样本执行单因素方差分析。",
                note=(
                    f"目标字段 {target_variable} 按分组字段 {group_variable} 的 "
                    f"{len(group_names)} 个组别进行均值差异比较。"
                ),
            ),
            tables=[
                DatasetAnalysisTable(
                    key="anova_group_summary",
                    title="分组样本汇总",
                    columns=["group", "count", "mean", "variance", "std_dev", "min", "max"],
                    rows=[
                        self._build_group_summary_row(group_name, grouped_values[group_name])
                        for group_name in group_names
                    ],
                ),
                DatasetAnalysisTable(
                    key="anova_summary",
                    title="方差分析表",
                    columns=[
                        "source",
                        "sum_of_squares",
                        "degrees_of_freedom",
                        "mean_square",
                        "f_value",
                        "p_value",
                    ],
                    rows=[
                        {
                            "source": "between_groups",
                            "sum_of_squares": DatasetAnalysisStatsUtils.round_number(ss_between),
                            "degrees_of_freedom": df_between,
                            "mean_square": DatasetAnalysisStatsUtils.round_number(ms_between),
                            "f_value": DatasetAnalysisStatsUtils.round_number(f_statistic),
                            "p_value": DatasetAnalysisStatsUtils.round_number(p_value),
                        },
                        {
                            "source": "within_groups",
                            "sum_of_squares": DatasetAnalysisStatsUtils.round_number(ss_within),
                            "degrees_of_freedom": df_within,
                            "mean_square": DatasetAnalysisStatsUtils.round_number(ms_within),
                            "f_value": None,
                            "p_value": None,
                        },
                        {
                            "source": "total",
                            "sum_of_squares": DatasetAnalysisStatsUtils.round_number(
                                ss_between + ss_within
                            ),
                            "degrees_of_freedom": total_count - 1,
                            "mean_square": None,
                            "f_value": None,
                            "p_value": None,
                        },
                    ],
                ),
            ],
            plots=[
                DatasetAnalysisPlot(
                    key="anova_boxplot",
                    title=f"{target_variable} 按 {group_variable} 分组箱线图",
                    plot_type="grouped_boxplot",
                    spec={
                        "target_variable": target_variable,
                        "group_variable": group_variable,
                        "groups": [
                            {
                                "name": group_name,
                                "values": [
                                    DatasetAnalysisStatsUtils.round_number(value)
                                    for value in grouped_values[group_name]
                                ],
                            }
                            for group_name in group_names
                        ],
                    },
                ),
                DatasetAnalysisPlot(
                    key="anova_mean_bar",
                    title=f"{target_variable} 分组均值对比图",
                    plot_type="bar_chart",
                    spec={
                        "categories": group_names,
                        "values": [
                            DatasetAnalysisStatsUtils.round_number(
                                mean(grouped_values[group_name])
                            )
                            for group_name in group_names
                        ],
                    },
                ),
            ],
            interpretations=[
                (
                    f"字段 {target_variable} 按 {group_variable} 分组后的单因素方差分析 "
                    f"F 统计量为 {DatasetAnalysisStatsUtils.round_number(f_statistic)}，"
                    f"组间自由度为 {df_between}，组内自由度为 {df_within}，"
                    f"p 值为 {DatasetAnalysisStatsUtils.round_number(p_value)}。"
                )
            ],
        )

    def _collect_grouped_numeric_values(
        self,
        rows: list[dict[str, str | None]],
        target_variable: str,
        group_variable: str,
    ) -> dict[str, list[float]]:
        """收集方差分析所需的分组数值。"""
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
                f"单因素方差分析当前仅支持数值型目标字段：{target_variable}。"
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
        if len(values) < 2:
            return 0.0

        value_mean = mean(values)
        return sum((value - value_mean) ** 2 for value in values) / (len(values) - 1)

    def _normalize_value(self, value: str | None) -> str | None:
        """把分组字段值整理成去除首尾空格的文本。"""
        if value is None:
            return None

        normalized_value = value.strip()
        return normalized_value or None
