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


def test_get_task_returns_404_for_unknown_task() -> None:
    """验证不存在的任务会返回 404。"""
    response = client.get("/api/v1/tasks/not-found")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的任务不存在。"
    }
