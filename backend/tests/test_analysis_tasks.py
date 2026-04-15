import time
from io import BytesIO

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


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
