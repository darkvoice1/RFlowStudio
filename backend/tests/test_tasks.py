import time
from io import BytesIO

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_create_dataset_profile_job_and_poll_until_completed() -> None:
    """验证字段分析可以通过异步任务提交并查询完成结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score\n1,95\n2,88\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(f"/api/v1/datasets/{dataset_id}/profile-jobs")
    create_payload = create_response.json()

    assert create_response.status_code == 202
    assert create_payload["task_type"] == "dataset_profile"
    assert create_payload["dataset_id"] == dataset_id

    detail_response = client.get(f"/api/v1/datasets/{dataset_id}")
    assert detail_response.status_code == 200
    assert detail_response.json()["status"] in {"processing", "ready"}

    task_id = create_payload["id"]
    final_payload: dict[str, object] | None = None

    # 轮询任务状态，验证异步任务能够从 pending/running 进入 completed。
    for _ in range(20):
        task_response = client.get(f"/api/v1/tasks/{task_id}")
        final_payload = task_response.json()
        assert task_response.status_code == 200

        if final_payload["status"] in {"completed", "failed"}:
            break

        time.sleep(0.05)

    assert final_payload is not None
    assert final_payload["status"] == "completed"
    assert final_payload["result"] is not None
    assert final_payload["result"]["dataset_id"] == dataset_id
    assert final_payload["result"]["row_count"] == 2

    detail_response = client.get(f"/api/v1/datasets/{dataset_id}")
    assert detail_response.status_code == 200
    assert detail_response.json()["status"] == "ready"


def test_get_task_returns_404_for_unknown_task() -> None:
    """验证不存在的任务会返回 404。"""
    response = client.get("/api/v1/tasks/not-found")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的任务不存在。"
    }


def test_list_dataset_tasks_returns_related_jobs_in_reverse_time_order() -> None:
    """验证可以按数据集查看关联任务列表，并默认按最新任务排序。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score\n1,95\n2,88\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    first_job = client.post(f"/api/v1/datasets/{dataset_id}/profile-jobs").json()
    second_job = client.post(f"/api/v1/datasets/{dataset_id}/profile-jobs").json()

    response = client.get(f"/api/v1/datasets/{dataset_id}/tasks")
    payload = response.json()

    assert response.status_code == 200
    assert payload["total"] == 2
    assert payload["items"][0]["id"] == second_job["id"]
    assert payload["items"][1]["id"] == first_job["id"]
    assert payload["items"][0]["dataset_id"] == dataset_id
    assert payload["items"][1]["dataset_id"] == dataset_id


def test_list_dataset_tasks_returns_404_for_unknown_dataset() -> None:
    """验证不存在的数据集不会返回空任务列表，而是明确返回 404。"""
    response = client.get("/api/v1/datasets/not-found/tasks")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的数据集不存在。"
    }


def test_create_dataset_profile_job_returns_404_for_unknown_dataset() -> None:
    """验证不存在的数据集不会创建异步任务。"""
    response = client.post("/api/v1/datasets/not-found/profile-jobs")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的数据集不存在。"
    }
