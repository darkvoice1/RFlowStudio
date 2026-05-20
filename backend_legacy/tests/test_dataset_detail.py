from io import BytesIO

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_get_dataset_detail_returns_uploaded_record() -> None:
    """验证上传成功后可以按 ID 读取数据集详情。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={"file": ("survey.csv", BytesIO(b"id,score\n1,95\n"), "text/csv")},
    )
    dataset_id = upload_response.json()["id"]

    response = client.get(f"/api/v1/datasets/{dataset_id}")
    payload = response.json()

    assert response.status_code == 200
    assert payload["id"] == dataset_id
    assert payload["name"] == "survey"
    assert payload["file_name"] == "survey.csv"
    assert payload["extension"] == ".csv"
    assert payload["stored_path"].startswith("uploads/")
    assert payload["status"] == "draft"


def test_get_dataset_detail_returns_404_for_unknown_id() -> None:
    """验证不存在的数据集会返回 404。"""
    response = client.get("/api/v1/datasets/not-found")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的数据集不存在。"
    }
