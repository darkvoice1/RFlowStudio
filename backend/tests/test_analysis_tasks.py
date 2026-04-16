import time
from io import BytesIO
from math import erfc, sqrt

import pytest
from fastapi.testclient import TestClient

from app.core.exceptions import DatasetAnalysisError
from app.main import app
from app.schemas.analysis import DatasetAnalysisPreparedRequest, DatasetAnalysisResult
from app.services.dataset.analysis.dataset_analysis_r_execution_service import (
    DatasetAnalysisRExecutionService,
)

client = TestClient(app)


@pytest.fixture(autouse=True)
def patch_r_analysis_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    """测试环境统一模拟 R 执行结果，避免依赖本机真实 R 运行时。"""
    def fake_is_available(self: DatasetAnalysisRExecutionService) -> bool:
        return True

    def fake_build_result(
        self: DatasetAnalysisRExecutionService,
        prepared_request: DatasetAnalysisPreparedRequest,
        columns: list[str],
        rows: list[dict[str, str | None]],
        raw_row_count: int,
    ) -> DatasetAnalysisResult:
        return _build_fake_r_analysis_result(
            prepared_request=prepared_request,
            rows=rows,
            raw_row_count=raw_row_count,
        )

    monkeypatch.setattr(DatasetAnalysisRExecutionService, "is_available", fake_is_available)
    monkeypatch.setattr(DatasetAnalysisRExecutionService, "build_result", fake_build_result)


def _round_value(value: float) -> int | float:
    rounded = round(value, 4)
    if abs(rounded - round(rounded)) < 1e-9:
        return int(round(rounded))
    return rounded


def _parse_numeric_value(raw_value: str | None, column: str) -> float | None:
    if raw_value is None or not raw_value.strip():
        return None

    try:
        return float(raw_value)
    except ValueError as exc:
        raise DatasetAnalysisError(f"相关分析当前仅支持数值型字段：{column}。") from exc


def _build_fake_r_analysis_result(
    prepared_request: DatasetAnalysisPreparedRequest,
    rows: list[dict[str, str | None]],
    raw_row_count: int,
) -> DatasetAnalysisResult:
    if prepared_request.analysis_type == "descriptive_statistics":
        return _build_fake_descriptive_result(prepared_request, rows, raw_row_count)
    if prepared_request.analysis_type == "correlation_analysis":
        return _build_fake_correlation_result(prepared_request, rows, raw_row_count)
    if prepared_request.analysis_type == "chi_square_test":
        return _build_fake_chi_square_result(prepared_request, rows, raw_row_count)
    if prepared_request.analysis_type == "independent_samples_t_test":
        return _build_fake_t_test_result(prepared_request, rows, raw_row_count)
    return _build_fake_anova_result(prepared_request, rows, raw_row_count)


def _build_fake_descriptive_result(
    prepared_request: DatasetAnalysisPreparedRequest,
    rows: list[dict[str, str | None]],
    raw_row_count: int,
) -> DatasetAnalysisResult:
    variable = prepared_request.variables[0]
    values = [
        float(row[variable])
        for row in rows
        if row.get(variable) is not None and str(row[variable]).strip()
    ]
    mean_value = _round_value(sum(values) / len(values))
    effective_row_count = len(rows)
    excluded_row_count = max(raw_row_count - effective_row_count, 0)
    return DatasetAnalysisResult(
        dataset_id=prepared_request.dataset_id,
        dataset_name=prepared_request.dataset_name,
        file_name=prepared_request.file_name,
        analysis_type=prepared_request.analysis_type,
        variables=prepared_request.variables,
        group_variable=prepared_request.group_variable,
        status="completed",
        summary={
            "title": "描述统计",
            "analysis_type": prepared_request.analysis_type,
            "effective_row_count": effective_row_count,
            "excluded_row_count": excluded_row_count,
            "missing_value_strategy": "测试环境模拟 R 执行结果。",
        },
        tables=[
            {
                "key": "descriptive_summary",
                "title": "描述统计汇总",
                "columns": ["variable", "mean"],
                "rows": [{"variable": variable, "mean": mean_value}],
            }
        ],
        plots=[
            {
                "key": f"{variable}_histogram",
                "title": f"{variable} 直方图",
                "plot_type": "histogram",
                "spec": {},
            }
        ],
        interpretations=[f"字段 {variable} 的均值为 {mean_value}。"],
    )


def _build_fake_correlation_result(
    prepared_request: DatasetAnalysisPreparedRequest,
    rows: list[dict[str, str | None]],
    raw_row_count: int,
) -> DatasetAnalysisResult:
    left, right = prepared_request.variables
    x_values: list[float] = []
    y_values: list[float] = []
    for row in rows:
        left_value = _parse_numeric_value(row.get(left), left)
        right_value = _parse_numeric_value(row.get(right), right)
        if left_value is None or right_value is None:
            continue
        x_values.append(left_value)
        y_values.append(right_value)

    pair_count = len(x_values)
    x_mean = sum(x_values) / pair_count
    y_mean = sum(y_values) / pair_count
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values))
    denominator = sqrt(
        sum((x - x_mean) ** 2 for x in x_values)
        * sum((y - y_mean) ** 2 for y in y_values)
    )
    correlation_value = 0.0 if denominator == 0 else numerator / denominator
    rounded_correlation = _round_value(correlation_value)
    effective_row_count = len(rows)
    excluded_row_count = max(raw_row_count - effective_row_count, 0)
    return DatasetAnalysisResult(
        dataset_id=prepared_request.dataset_id,
        dataset_name=prepared_request.dataset_name,
        file_name=prepared_request.file_name,
        analysis_type=prepared_request.analysis_type,
        variables=prepared_request.variables,
        group_variable=prepared_request.group_variable,
        status="completed",
        summary={
            "title": "相关分析",
            "analysis_type": prepared_request.analysis_type,
            "effective_row_count": effective_row_count,
            "excluded_row_count": excluded_row_count,
            "missing_value_strategy": "测试环境模拟 R 执行结果。",
        },
        tables=[
            {
                "key": "correlation_matrix",
                "title": "相关系数矩阵",
                "columns": ["variable", left, right],
                "rows": [
                    {"variable": left, left: 1, right: rounded_correlation},
                    {"variable": right, left: rounded_correlation, right: 1},
                ],
            },
            {
                "key": "correlation_pair_counts",
                "title": "成对样本量矩阵",
                "columns": ["variable", left, right],
                "rows": [
                    {"variable": left, left: None, right: pair_count},
                    {"variable": right, left: pair_count, right: None},
                ],
            },
            {
                "key": "correlation_pairs",
                "title": "字段对相关结果",
                "columns": ["variable_x", "variable_y", "pair_count", "correlation"],
                "rows": [
                    {
                        "variable_x": left,
                        "variable_y": right,
                        "pair_count": pair_count,
                        "correlation": rounded_correlation,
                    }
                ],
            },
        ],
        plots=[
            {
                "key": "correlation_heatmap",
                "title": "相关系数热力图",
                "plot_type": "heatmap",
                "spec": {},
            }
        ],
        interpretations=[f"字段 {left} 与 {right} 的皮尔逊相关系数 {rounded_correlation}。"],
    )


def _build_fake_chi_square_result(
    prepared_request: DatasetAnalysisPreparedRequest,
    rows: list[dict[str, str | None]],
    raw_row_count: int,
) -> DatasetAnalysisResult:
    left, right = prepared_request.variables
    left_levels = sorted({row[left] for row in rows if row.get(left) is not None})
    right_levels = sorted({row[right] for row in rows if row.get(right) is not None})
    observed: dict[str, dict[str, int]] = {
        left_level: {right_level: 0 for right_level in right_levels}
        for left_level in left_levels
    }
    for row in rows:
        observed[row[left]][row[right]] += 1

    row_totals = {left_level: sum(observed[left_level].values()) for left_level in left_levels}
    column_totals = {
        right_level: sum(observed[left_level][right_level] for left_level in left_levels)
        for right_level in right_levels
    }
    total = len(rows)
    chi_square = 0.0
    for left_level in left_levels:
        for right_level in right_levels:
            expected = row_totals[left_level] * column_totals[right_level] / total
            chi_square += ((observed[left_level][right_level] - expected) ** 2) / expected

    rounded_chi_square = _round_value(chi_square)
    p_value = erfc(sqrt(chi_square / 2))
    return DatasetAnalysisResult(
        dataset_id=prepared_request.dataset_id,
        dataset_name=prepared_request.dataset_name,
        file_name=prepared_request.file_name,
        analysis_type=prepared_request.analysis_type,
        variables=prepared_request.variables,
        group_variable=prepared_request.group_variable,
        status="completed",
        summary={
            "title": "卡方检验",
            "analysis_type": prepared_request.analysis_type,
            "effective_row_count": len(rows),
            "excluded_row_count": max(raw_row_count - len(rows), 0),
            "missing_value_strategy": "测试环境模拟 R 执行结果。",
        },
        tables=[
            {
                "key": "chi_square_observed",
                "title": "列联表（观测频数）",
                "columns": ["variable", *right_levels],
                "rows": [
                    {"variable": left_level, **observed[left_level]}
                    for left_level in left_levels
                ],
            },
            {
                "key": "chi_square_expected",
                "title": "列联表（期望频数）",
                "columns": ["variable", *right_levels],
                "rows": [],
            },
            {
                "key": "chi_square_summary",
                "title": "卡方检验汇总",
                "columns": ["chi_square", "degrees_of_freedom", "p_value"],
                "rows": [
                    {
                        "chi_square": rounded_chi_square,
                        "degrees_of_freedom": (len(left_levels) - 1) * (len(right_levels) - 1),
                        "p_value": _round_value(p_value),
                    }
                ],
            },
        ],
        plots=[
            {
                "key": "chi_square_grouped_bar",
                "title": "分组条形图",
                "plot_type": "grouped_bar_chart",
                "spec": {},
            }
        ],
        interpretations=[f"卡方统计量为 {rounded_chi_square}。"],
    )


def _build_fake_t_test_result(
    prepared_request: DatasetAnalysisPreparedRequest,
    rows: list[dict[str, str | None]],
    raw_row_count: int,
) -> DatasetAnalysisResult:
    target = prepared_request.variables[0]
    group = prepared_request.group_variable or ""
    grouped_values: dict[str, list[float]] = {}
    for row in rows:
        grouped_values.setdefault(row[group] or "", []).append(float(row[target] or 0))

    group_names = sorted(grouped_values)
    left_values = grouped_values[group_names[0]]
    right_values = grouped_values[group_names[1]]
    left_mean = sum(left_values) / len(left_values)
    right_mean = sum(right_values) / len(right_values)
    left_variance = sum((value - left_mean) ** 2 for value in left_values) / (
        len(left_values) - 1
    )
    right_variance = sum((value - right_mean) ** 2 for value in right_values) / (
        len(right_values) - 1
    )
    degrees_of_freedom = len(left_values) + len(right_values) - 2
    pooled_variance = (
        ((len(left_values) - 1) * left_variance) + ((len(right_values) - 1) * right_variance)
    ) / degrees_of_freedom
    t_statistic = (left_mean - right_mean) / sqrt(
        pooled_variance * ((1 / len(left_values)) + (1 / len(right_values)))
    )
    rounded_t_statistic = _round_value(t_statistic)
    p_value = 1 - abs(t_statistic) / sqrt((t_statistic**2) + degrees_of_freedom)
    return DatasetAnalysisResult(
        dataset_id=prepared_request.dataset_id,
        dataset_name=prepared_request.dataset_name,
        file_name=prepared_request.file_name,
        analysis_type=prepared_request.analysis_type,
        variables=prepared_request.variables,
        group_variable=prepared_request.group_variable,
        status="completed",
        summary={
            "title": "独立样本 t 检验",
            "analysis_type": prepared_request.analysis_type,
            "effective_row_count": len(rows),
            "excluded_row_count": max(raw_row_count - len(rows), 0),
            "missing_value_strategy": "测试环境模拟 R 执行结果。",
        },
        tables=[
            {
                "key": "t_test_group_summary",
                "title": "分组样本汇总",
                "columns": ["group", "count"],
                "rows": [
                    {"group": name, "count": len(grouped_values[name])}
                    for name in group_names
                ],
            },
            {
                "key": "t_test_result",
                "title": "独立样本 t 检验结果",
                "columns": ["t_statistic", "degrees_of_freedom", "p_value"],
                "rows": [
                    {
                        "t_statistic": rounded_t_statistic,
                        "degrees_of_freedom": degrees_of_freedom,
                        "p_value": _round_value(p_value),
                    }
                ],
            },
        ],
        plots=[
            {
                "key": "t_test_boxplot",
                "title": "分组箱线图",
                "plot_type": "grouped_boxplot",
                "spec": {},
            }
        ],
        interpretations=[f"t 统计量为 {rounded_t_statistic}。"],
    )


def _build_fake_anova_result(
    prepared_request: DatasetAnalysisPreparedRequest,
    rows: list[dict[str, str | None]],
    raw_row_count: int,
) -> DatasetAnalysisResult:
    target = prepared_request.variables[0]
    group = prepared_request.group_variable or ""
    grouped_values: dict[str, list[float]] = {}
    for row in rows:
        grouped_values.setdefault(row[group] or "", []).append(float(row[target] or 0))

    all_values = [value for values in grouped_values.values() for value in values]
    grand_mean = sum(all_values) / len(all_values)
    ss_between = sum(
        len(values) * ((sum(values) / len(values)) - grand_mean) ** 2
        for values in grouped_values.values()
    )
    ss_within = sum(
        sum((value - (sum(values) / len(values))) ** 2 for value in values)
        for values in grouped_values.values()
    )
    df_between = len(grouped_values) - 1
    df_within = len(all_values) - len(grouped_values)
    f_value = (ss_between / df_between) / (ss_within / df_within)
    return DatasetAnalysisResult(
        dataset_id=prepared_request.dataset_id,
        dataset_name=prepared_request.dataset_name,
        file_name=prepared_request.file_name,
        analysis_type=prepared_request.analysis_type,
        variables=prepared_request.variables,
        group_variable=prepared_request.group_variable,
        status="completed",
        summary={
            "title": "单因素方差分析",
            "analysis_type": prepared_request.analysis_type,
            "effective_row_count": len(rows),
            "excluded_row_count": max(raw_row_count - len(rows), 0),
            "missing_value_strategy": "测试环境模拟 R 执行结果。",
        },
        tables=[
            {
                "key": "anova_group_summary",
                "title": "分组样本汇总",
                "columns": ["group", "count"],
                "rows": [
                    {"group": name, "count": len(values)}
                    for name, values in grouped_values.items()
                ],
            },
            {
                "key": "anova_summary",
                "title": "方差分析表",
                "columns": ["f_value", "degrees_of_freedom", "p_value"],
                "rows": [
                    {
                        "f_value": _round_value(f_value),
                        "degrees_of_freedom": df_between,
                        "p_value": 0.005,
                    }
                ],
            },
        ],
        plots=[
            {
                "key": "anova_boxplot",
                "title": "分组箱线图",
                "plot_type": "grouped_boxplot",
                "spec": {},
            }
        ],
        interpretations=[f"F 统计量为 {_round_value(f_value)}。"],
    )


def test_create_dataset_analysis_job_and_poll_until_completed() -> None:
    """验证描述统计任务可以提交并返回真实统计结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score,group\n1,95,A\n2,88,B\n3,90,A\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-jobs",
        json={
            "analysis_type": "descriptive_statistics",
            "variables": ["score"],
            "options": {},
        },
    )
    create_payload = create_response.json()

    assert create_response.status_code == 202
    assert create_payload["task_type"] == "dataset_analysis"
    assert create_payload["dataset_id"] == dataset_id

    final_payload: dict[str, object] | None = None
    for _ in range(20):
        task_response = client.get(f"/api/v1/tasks/{create_payload['id']}")
        final_payload = task_response.json()
        assert task_response.status_code == 200

        if final_payload["status"] in {"completed", "failed"}:
            break

        time.sleep(0.05)

    assert final_payload is not None
    assert final_payload["status"] == "completed"
    assert final_payload["result"] is not None
    assert final_payload["result"]["analysis_type"] == "descriptive_statistics"
    assert final_payload["result"]["variables"] == ["score"]
    assert final_payload["result"]["status"] == "completed"
    assert final_payload["result"]["summary"]["title"] == "描述统计"
    assert final_payload["result"]["summary"]["effective_row_count"] == 3
    assert final_payload["result"]["tables"][0]["key"] == "descriptive_summary"
    assert final_payload["result"]["tables"][0]["rows"][0]["variable"] == "score"
    assert final_payload["result"]["tables"][0]["rows"][0]["mean"] == 91
    assert final_payload["result"]["plots"][0]["plot_type"] == "histogram"
    assert "均值为 91" in final_payload["result"]["interpretations"][0]
    assert "# 数据清洗 + 统计分析 R 代码草稿" in final_payload["result"]["script_draft"]
    assert "# 当前还没有记录任何清洗步骤" in final_payload["result"]["script_draft"]
    assert "analysis_data <- cleaned_data" in final_payload["result"]["script_draft"]
    assert "# 分析方法: 描述统计" in final_payload["result"]["script_draft"]
    assert "descriptive_result <- data.frame(" in final_payload["result"]["script_draft"]

    history_response = client.get(f"/api/v1/datasets/{dataset_id}/analysis-records")
    history_payload = history_response.json()

    assert history_response.status_code == 200
    assert history_payload["dataset_id"] == dataset_id
    assert history_payload["total"] == 1
    assert history_payload["items"][0]["analysis_type"] == "descriptive_statistics"
    assert history_payload["items"][0]["variables"] == ["score"]
    assert history_payload["items"][0]["task_id"] == create_payload["id"]
    assert history_payload["items"][0]["result"]["summary"]["title"] == "描述统计"
    assert (
        "# 数据清洗 + 统计分析 R 代码草稿"
        in history_payload["items"][0]["result"]["script_draft"]
    )
    assert "# 分析方法: 描述统计" in history_payload["items"][0]["result"]["script_draft"]


def test_rerun_dataset_analysis_record_creates_new_analysis_task() -> None:
    """验证可以基于历史分析记录重新创建一次新的统计分析任务。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score,group\n1,95,A\n2,88,B\n3,90,A\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-jobs",
        json={
            "analysis_type": "descriptive_statistics",
            "variables": ["score"],
            "options": {},
        },
    )
    first_task_id = create_response.json()["id"]

    first_final_payload: dict[str, object] | None = None
    for _ in range(20):
        task_response = client.get(f"/api/v1/tasks/{first_task_id}")
        first_final_payload = task_response.json()
        assert task_response.status_code == 200

        if first_final_payload["status"] in {"completed", "failed"}:
            break

        time.sleep(0.05)

    assert first_final_payload is not None
    assert first_final_payload["status"] == "completed"

    history_response = client.get(f"/api/v1/datasets/{dataset_id}/analysis-records")
    history_payload = history_response.json()
    analysis_record_id = history_payload["items"][0]["id"]

    rerun_response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-records/{analysis_record_id}/rerun"
    )
    rerun_payload = rerun_response.json()

    assert rerun_response.status_code == 202
    assert rerun_payload["task_type"] == "dataset_analysis"
    assert rerun_payload["dataset_id"] == dataset_id
    assert rerun_payload["id"] != first_task_id

    rerun_final_payload: dict[str, object] | None = None
    for _ in range(20):
        task_response = client.get(f"/api/v1/tasks/{rerun_payload['id']}")
        rerun_final_payload = task_response.json()
        assert task_response.status_code == 200

        if rerun_final_payload["status"] in {"completed", "failed"}:
            break

        time.sleep(0.05)

    assert rerun_final_payload is not None
    assert rerun_final_payload["status"] == "completed"
    assert rerun_final_payload["result"]["analysis_type"] == "descriptive_statistics"
    assert rerun_final_payload["result"]["tables"][0]["rows"][0]["mean"] == 91

    rerun_history_response = client.get(f"/api/v1/datasets/{dataset_id}/analysis-records")
    rerun_history_payload = rerun_history_response.json()

    assert rerun_history_response.status_code == 200
    assert rerun_history_payload["total"] == 2
    assert rerun_history_payload["items"][0]["task_id"] == rerun_payload["id"]
    assert rerun_history_payload["items"][1]["task_id"] == first_task_id


def test_create_dataset_analysis_job_applies_cleaning_steps_before_statistics() -> None:
    """验证描述统计会基于当前清洗步骤后的数据返回结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score\n1,95\n2,88\n3,60\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    cleaning_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "filter",
            "name": "只保留高分样本",
            "parameters": {
                "column": "score",
                "operator": "gte",
                "value": "90",
            },
        },
    )
    assert cleaning_response.status_code == 201

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-jobs",
        json={
            "analysis_type": "descriptive_statistics",
            "variables": ["score"],
        },
    )
    task_id = create_response.json()["id"]

    final_payload: dict[str, object] | None = None
    for _ in range(20):
        task_response = client.get(f"/api/v1/tasks/{task_id}")
        final_payload = task_response.json()
        assert task_response.status_code == 200

        if final_payload["status"] in {"completed", "failed"}:
            break

        time.sleep(0.05)

    assert final_payload is not None
    assert final_payload["status"] == "completed"
    assert final_payload["result"]["summary"]["effective_row_count"] == 1
    assert final_payload["result"]["summary"]["excluded_row_count"] == 2
    assert final_payload["result"]["tables"][0]["rows"][0]["mean"] == 95
    assert "# 步骤 1: 只保留高分样本" in final_payload["result"]["script_draft"]
    assert (
        'cleaned_data <- cleaned_data[rflow_num(cleaned_data[["score"]]) >= 90, , drop = FALSE]'
        in final_payload["result"]["script_draft"]
    )
    assert "analysis_data <- cleaned_data" in final_payload["result"]["script_draft"]


def test_create_correlation_analysis_job_returns_completed_result() -> None:
    """验证相关分析任务可以返回真实相关结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score,age\n1,95,20\n2,88,19\n3,90,21\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-jobs",
        json={
            "analysis_type": "correlation_analysis",
            "variables": ["score", "age"],
        },
    )
    task_id = create_response.json()["id"]

    final_payload: dict[str, object] | None = None
    for _ in range(20):
        task_response = client.get(f"/api/v1/tasks/{task_id}")
        final_payload = task_response.json()
        assert task_response.status_code == 200

        if final_payload["status"] in {"completed", "failed"}:
            break

        time.sleep(0.05)

    assert final_payload is not None
    assert final_payload["status"] == "completed"
    assert final_payload["result"]["status"] == "completed"
    assert final_payload["result"]["summary"]["title"] == "相关分析"
    assert final_payload["result"]["tables"][0]["key"] == "correlation_matrix"
    assert final_payload["result"]["tables"][0]["rows"][0]["score"] == 1
    assert final_payload["result"]["tables"][0]["rows"][0]["age"] == 0.2774
    assert final_payload["result"]["tables"][2]["rows"][0]["pair_count"] == 3
    assert final_payload["result"]["plots"][0]["plot_type"] == "heatmap"
    assert "皮尔逊相关系数 0.2774" in final_payload["result"]["interpretations"][0]


def test_create_correlation_analysis_job_applies_cleaning_steps() -> None:
    """验证相关分析会基于清洗后的数据返回结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score,age\n1,95,20\n2,88,19\n3,60,30\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    cleaning_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "filter",
            "name": "只保留高分样本",
            "parameters": {
                "column": "score",
                "operator": "gte",
                "value": "88",
            },
        },
    )
    assert cleaning_response.status_code == 201

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-jobs",
        json={
            "analysis_type": "correlation_analysis",
            "variables": ["score", "age"],
        },
    )
    task_id = create_response.json()["id"]

    final_payload: dict[str, object] | None = None
    for _ in range(20):
        task_response = client.get(f"/api/v1/tasks/{task_id}")
        final_payload = task_response.json()
        assert task_response.status_code == 200

        if final_payload["status"] in {"completed", "failed"}:
            break

        time.sleep(0.05)

    assert final_payload is not None
    assert final_payload["status"] == "completed"
    assert final_payload["result"]["summary"]["effective_row_count"] == 2
    assert final_payload["result"]["summary"]["excluded_row_count"] == 1
    assert final_payload["result"]["tables"][2]["rows"][0]["pair_count"] == 2
    assert final_payload["result"]["tables"][2]["rows"][0]["correlation"] == 1


def test_create_correlation_analysis_job_rejects_non_numeric_columns() -> None:
    """验证相关分析会拒绝包含非数值字段的请求。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score,group\n1,95,A\n2,88,B\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-jobs",
        json={
            "analysis_type": "correlation_analysis",
            "variables": ["score", "group"],
        },
    )
    task_id = create_response.json()["id"]

    final_payload: dict[str, object] | None = None
    for _ in range(20):
        task_response = client.get(f"/api/v1/tasks/{task_id}")
        final_payload = task_response.json()
        assert task_response.status_code == 200

        if final_payload["status"] in {"completed", "failed"}:
            break

        time.sleep(0.05)

    assert final_payload is not None
    assert final_payload["status"] == "failed"
    assert final_payload["error_message"] == "相关分析当前仅支持数值型字段：group。"


def test_create_chi_square_analysis_job_returns_completed_result() -> None:
    """验证卡方检验任务可以返回真实检验结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,gender,treatment\n1,M,A\n2,M,A\n3,F,B\n4,F,B\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-jobs",
        json={
            "analysis_type": "chi_square_test",
            "variables": ["gender", "treatment"],
        },
    )
    task_id = create_response.json()["id"]

    final_payload: dict[str, object] | None = None
    for _ in range(20):
        task_response = client.get(f"/api/v1/tasks/{task_id}")
        final_payload = task_response.json()
        assert task_response.status_code == 200

        if final_payload["status"] in {"completed", "failed"}:
            break

        time.sleep(0.05)

    assert final_payload is not None
    assert final_payload["status"] == "completed"
    assert final_payload["result"]["summary"]["title"] == "卡方检验"
    assert final_payload["result"]["tables"][0]["key"] == "chi_square_observed"
    assert final_payload["result"]["tables"][2]["rows"][0]["chi_square"] == 4
    assert final_payload["result"]["tables"][2]["rows"][0]["degrees_of_freedom"] == 1
    assert final_payload["result"]["tables"][2]["rows"][0]["p_value"] < 0.05
    assert final_payload["result"]["plots"][0]["plot_type"] == "grouped_bar_chart"


def test_create_independent_samples_t_test_job_returns_completed_result() -> None:
    """验证独立样本 t 检验任务可以返回真实检验结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score,group\n1,10,A\n2,12,A\n3,14,B\n4,16,B\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-jobs",
        json={
            "analysis_type": "independent_samples_t_test",
            "variables": ["score"],
            "group_variable": "group",
        },
    )
    task_id = create_response.json()["id"]

    final_payload: dict[str, object] | None = None
    for _ in range(20):
        task_response = client.get(f"/api/v1/tasks/{task_id}")
        final_payload = task_response.json()
        assert task_response.status_code == 200

        if final_payload["status"] in {"completed", "failed"}:
            break

        time.sleep(0.05)

    assert final_payload is not None
    assert final_payload["status"] == "completed"
    assert final_payload["result"]["summary"]["title"] == "独立样本 t 检验"
    assert final_payload["result"]["tables"][0]["key"] == "t_test_group_summary"
    assert final_payload["result"]["tables"][1]["rows"][0]["t_statistic"] == -2.8284
    assert final_payload["result"]["tables"][1]["rows"][0]["degrees_of_freedom"] == 2
    assert 0.1 < final_payload["result"]["tables"][1]["rows"][0]["p_value"] < 0.11
    assert final_payload["result"]["plots"][0]["plot_type"] == "grouped_boxplot"


def test_create_one_way_anova_job_returns_completed_result() -> None:
    """验证单因素方差分析任务可以返回真实检验结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(
                    b"id,score,group\n1,10,A\n2,12,A\n3,20,B\n4,22,B\n5,30,C\n6,32,C\n"
                ),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-jobs",
        json={
            "analysis_type": "one_way_anova",
            "variables": ["score"],
            "group_variable": "group",
        },
    )
    task_id = create_response.json()["id"]

    final_payload: dict[str, object] | None = None
    for _ in range(20):
        task_response = client.get(f"/api/v1/tasks/{task_id}")
        final_payload = task_response.json()
        assert task_response.status_code == 200

        if final_payload["status"] in {"completed", "failed"}:
            break

        time.sleep(0.05)

    assert final_payload is not None
    assert final_payload["status"] == "completed"
    assert final_payload["result"]["summary"]["title"] == "单因素方差分析"
    assert final_payload["result"]["tables"][0]["key"] == "anova_group_summary"
    assert final_payload["result"]["tables"][1]["rows"][0]["f_value"] == 100
    assert final_payload["result"]["tables"][1]["rows"][0]["degrees_of_freedom"] == 2
    assert final_payload["result"]["tables"][1]["rows"][0]["p_value"] < 0.01
    assert final_payload["result"]["plots"][0]["plot_type"] == "grouped_boxplot"


def test_create_dataset_analysis_job_rejects_missing_columns() -> None:
    """验证统计分析任务会拒绝不存在的字段名。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score,group\n1,95,A\n2,88,B\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-jobs",
        json={
            "analysis_type": "descriptive_statistics",
            "variables": ["not_exists"],
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "统计分析请求包含不存在的字段：not_exists。"
    }


def test_create_dataset_analysis_job_rejects_missing_group_variable() -> None:
    """验证需要分组字段的分析方法会拒绝缺少分组字段的请求。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score,group\n1,95,A\n2,88,B\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-jobs",
        json={
            "analysis_type": "independent_samples_t_test",
            "variables": ["score"],
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "当前分析方法必须提供分组字段。"
    }


def test_create_dataset_analysis_job_returns_404_for_unknown_dataset() -> None:
    """验证不存在的数据集不会创建统计分析任务。"""
    response = client.post(
        "/api/v1/datasets/not-found/analysis-jobs",
        json={
            "analysis_type": "descriptive_statistics",
            "variables": ["score"],
        },
    )

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的数据集不存在。"
    }


def test_list_dataset_analysis_records_returns_404_for_unknown_dataset() -> None:
    """验证不存在的数据集不会返回分析历史记录列表。"""
    response = client.get("/api/v1/datasets/not-found/analysis-records")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的数据集不存在。"
    }


def test_rerun_dataset_analysis_record_returns_404_for_unknown_record() -> None:
    """验证不存在的历史分析记录不会触发重跑任务。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score\n1,95\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/analysis-records/not-found/rerun"
    )

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的统计分析历史记录不存在。"
    }
