from io import BytesIO

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_upload_dataset_saves_supported_file() -> None:
    """验证支持的文件类型可以被成功上传。"""
    response = client.post(
        "/api/v1/datasets/upload",
        files={"file": ("demo.csv", BytesIO(b"id,name\n1,Alice\n"), "text/csv")},
    )

    payload = response.json()

    assert response.status_code == 200
    assert payload["file_name"] == "demo.csv"
    assert payload["stored_path"].startswith("uploads/")
    assert payload["size_bytes"] > 0
    assert payload["status"] == "draft"


def test_upload_dataset_rejects_unsupported_extension() -> None:
    """验证不支持的文件类型会被拒绝。"""
    response = client.post(
        "/api/v1/datasets/upload",
        files={"file": ("demo.txt", BytesIO(b"plain text"), "text/plain")},
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "当前仅支持 csv、xlsx 和 sav 文件。"
    }


def test_upload_dataset_rejects_empty_file() -> None:
    """验证空文件会被拒绝，避免无效数据进入后续流程。"""
    response = client.post(
        "/api/v1/datasets/upload",
        files={"file": ("empty.csv", BytesIO(b""), "text/csv")},
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "上传文件不能为空。"
    }
