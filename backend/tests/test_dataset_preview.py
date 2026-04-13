from io import BytesIO

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_get_dataset_preview_returns_csv_columns_and_rows() -> None:
    """验证 CSV 文件上传后可以返回列名和前几行预览。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "people.csv",
                BytesIO(b"id,name,score\n1,Alice,95\n2,Bob,88\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.get(f"/api/v1/datasets/{dataset_id}/preview")
    payload = response.json()

    assert response.status_code == 200
    assert payload["dataset_id"] == dataset_id
    assert payload["file_name"] == "people.csv"
    assert payload["columns"] == ["id", "name", "score"]
    assert payload["rows"] == [
        {"id": "1", "name": "Alice", "score": "95"},
        {"id": "2", "name": "Bob", "score": "88"},
    ]
    assert payload["preview_row_count"] == 2
    assert payload["limit"] == 20
    assert payload["has_more"] is False
    assert payload["preview_format"] == "csv"


def test_get_dataset_preview_respects_limit_and_reports_more_rows() -> None:
    """验证预览接口会按 limit 截断结果并标记仍有更多数据。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "scores.csv",
                BytesIO(b"id,score\n1,95\n2,88\n3,90\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.get(f"/api/v1/datasets/{dataset_id}/preview?limit=2")
    payload = response.json()

    assert response.status_code == 200
    assert payload["preview_row_count"] == 2
    assert payload["limit"] == 2
    assert payload["has_more"] is True


def test_get_dataset_preview_rejects_invalid_xlsx_file() -> None:
    """验证非法 xlsx 文件会返回清晰的格式异常提示。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "students.xlsx",
                BytesIO(b"fake-xlsx-content"),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.get(f"/api/v1/datasets/{dataset_id}/preview")

    assert response.status_code == 400
    assert response.json() == {
        "detail": "XLSX 文件格式异常，暂时无法预览。"
    }
