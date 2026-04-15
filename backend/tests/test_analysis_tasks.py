import time
from io import BytesIO

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_create_dataset_analysis_job_and_poll_until_completed() -> None:
    """验证统计分析任务骨架可以提交并查询统一结果结构。"""
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
    assert final_payload["result"]["status"] == "skeleton_ready"
    assert final_payload["result"]["summary"]["title"] == "描述统计"
    assert final_payload["result"]["tables"] == []
    assert final_payload["result"]["plots"] == []


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
