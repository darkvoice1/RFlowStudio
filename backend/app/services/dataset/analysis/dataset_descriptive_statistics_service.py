from collections import Counter
from statistics import mean, median, stdev

from app.schemas.analysis import (
    DatasetAnalysisPlot,
    DatasetAnalysisPreparedRequest,
    DatasetAnalysisResult,
    DatasetAnalysisSummary,
    DatasetAnalysisTable,
)


class DatasetDescriptiveStatisticsService:
    """负责生成描述统计分析结果。"""

    def build_result(
        self,
        prepared_request: DatasetAnalysisPreparedRequest,
        columns: list[str],
        rows: list[dict[str, str | None]],
        raw_row_count: int,
    ) -> DatasetAnalysisResult:
        """基于清洗后的数据生成描述统计结果。"""
        summary_rows: list[dict[str, object | None]] = []
        extra_tables: list[DatasetAnalysisTable] = []
        plots: list[DatasetAnalysisPlot] = []
        interpretations: list[str] = []

        for variable in prepared_request.variables:
            values = [row.get(variable) for row in rows if variable in columns]
            profile = self._build_variable_profile(variable, values)
            summary_rows.append(profile["summary_row"])  # type: ignore[arg-type]
            extra_tables.extend(profile["tables"])  # type: ignore[arg-type]
            plots.extend(profile["plots"])  # type: ignore[arg-type]
            interpretations.append(profile["interpretation"])  # type: ignore[arg-type]

        summary_table = DatasetAnalysisTable(
            key="descriptive_summary",
            title="描述统计汇总",
            columns=[
                "variable",
                "field_type",
                "valid_count",
                "missing_count",
                "unique_count",
                "mean",
                "median",
                "std_dev",
                "min",
                "max",
            ],
            rows=summary_rows,
        )
        cleaned_row_count = len(rows)
        excluded_row_count = max(raw_row_count - cleaned_row_count, 0)
        note = "各字段默认按非缺失值单独计算统计指标。"
        if excluded_row_count > 0:
            note = (
                f"当前数据清洗步骤已先剔除 {excluded_row_count} 行记录，"
                "其余字段再按非缺失值单独计算统计指标。"
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
                title="描述统计",
                analysis_type=prepared_request.analysis_type,
                effective_row_count=cleaned_row_count,
                excluded_row_count=excluded_row_count,
                missing_value_strategy="先应用当前清洗步骤，再按各字段非缺失值计算描述统计。",
                note=note,
            ),
            tables=[summary_table, *extra_tables],
            plots=plots,
            interpretations=interpretations,
        )

    def _build_variable_profile(
        self,
        variable: str,
        values: list[str | None],
    ) -> dict[str, object]:
        """按字段类型生成描述统计结果。"""
        normalized_values = [self._normalize_value(value) for value in values]
        non_missing_values = [value for value in normalized_values if value is not None]
        missing_count = len(normalized_values) - len(non_missing_values)
        unique_count = len(set(non_missing_values))

        if self._is_numeric_column(non_missing_values):
            numeric_values = [float(value) for value in non_missing_values]
            stats_row = {
                "variable": variable,
                "field_type": "numeric",
                "valid_count": len(numeric_values),
                "missing_count": missing_count,
                "unique_count": unique_count,
                "mean": self._round_number(mean(numeric_values)) if numeric_values else None,
                "median": self._round_number(median(numeric_values)) if numeric_values else None,
                "std_dev": self._round_number(stdev(numeric_values))
                if len(numeric_values) > 1
                else None,
                "min": self._round_number(min(numeric_values)) if numeric_values else None,
                "max": self._round_number(max(numeric_values)) if numeric_values else None,
            }
            plots = [
                DatasetAnalysisPlot(
                    key=f"{variable}_histogram",
                    title=f"{variable} 直方图",
                    plot_type="histogram",
                    spec={
                        "variable": variable,
                        "values": [self._round_number(value) for value in numeric_values],
                    },
                ),
                DatasetAnalysisPlot(
                    key=f"{variable}_boxplot",
                    title=f"{variable} 箱线图",
                    plot_type="boxplot",
                    spec={
                        "variable": variable,
                        "values": [self._round_number(value) for value in numeric_values],
                    },
                ),
            ]
            interpretation = (
                f"字段 {variable} 在清洗后的数据中共有 {len(numeric_values)} 个有效值，"
                f"均值为 {stats_row['mean']}，中位数为 {stats_row['median']}。"
            )
            return {
                "summary_row": stats_row,
                "tables": [],
                "plots": plots,
                "interpretation": interpretation,
            }

        value_counter = Counter(non_missing_values)
        ordered_items = sorted(value_counter.items(), key=lambda item: (-item[1], item[0]))
        total_count = len(non_missing_values)
        frequency_rows = [
            {
                "value": value,
                "count": count,
                "ratio": self._round_number(count / total_count) if total_count else 0,
            }
            for value, count in ordered_items
        ]
        top_value = ordered_items[0][0] if ordered_items else None
        top_count = ordered_items[0][1] if ordered_items else None
        stats_row = {
            "variable": variable,
            "field_type": "categorical",
            "valid_count": total_count,
            "missing_count": missing_count,
            "unique_count": unique_count,
            "mean": None,
            "median": None,
            "std_dev": None,
            "min": None,
            "max": None,
        }
        frequency_table = DatasetAnalysisTable(
            key=f"{variable}_frequency",
            title=f"{variable} 频数分布",
            columns=["value", "count", "ratio"],
            rows=frequency_rows,
        )
        plots = [
            DatasetAnalysisPlot(
                key=f"{variable}_bar_chart",
                title=f"{variable} 条形图",
                plot_type="bar_chart",
                spec={
                    "variable": variable,
                    "categories": [value for value, _ in ordered_items],
                    "counts": [count for _, count in ordered_items],
                },
            )
        ]
        interpretation = (
            f"字段 {variable} 在清洗后的数据中共有 {total_count} 个有效值，"
            f"包含 {unique_count} 个唯一取值，最常见取值为 {top_value}（{top_count} 次）。"
        )
        return {
            "summary_row": stats_row,
            "tables": [frequency_table],
            "plots": plots,
            "interpretation": interpretation,
        }

    def _normalize_value(self, value: str | None) -> str | None:
        """把单元格值整理成去除首尾空格的文本。"""
        if value is None:
            return None

        normalized_value = value.strip()
        return normalized_value or None

    def _is_numeric_column(self, values: list[str]) -> bool:
        """判断当前字段是否适合按数值字段处理。"""
        if not values:
            return False

        try:
            for value in values:
                float(value)
        except ValueError:
            return False

        return True

    def _round_number(self, value: float) -> float | int:
        """把结果整理成更适合展示的数值。"""
        rounded_value = round(value, 4)
        if rounded_value.is_integer():
            return int(rounded_value)

        return rounded_value
