from io import BytesIO

from fastapi.testclient import TestClient
from openpyxl import Workbook

from app.main import app

client = TestClient(app)


def _build_xlsx_bytes(rows: list[list[object]]) -> bytes:
    """构造测试用的 xlsx 二进制内容。"""
    workbook = Workbook()
    worksheet = workbook.active

    for row in rows:
        worksheet.append(row)

    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def test_get_dataset_preview_returns_xlsx_columns_and_rows() -> None:
    """验证 xlsx 文件上传后可以返回列名和前几行预览。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "people.xlsx",
                BytesIO(
                    _build_xlsx_bytes(
                        [
                            ["id", "name", "score"],
                            [1, "Alice", 95],
                            [2, "Bob", 88],
                        ]
                    )
                ),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.get(f"/api/v1/datasets/{dataset_id}/preview")
    payload = response.json()

    assert response.status_code == 200
    assert payload["dataset_id"] == dataset_id
    assert payload["file_name"] == "people.xlsx"
    assert payload["columns"] == ["id", "name", "score"]
    assert payload["rows"] == [
        {"id": "1", "name": "Alice", "score": "95"},
        {"id": "2", "name": "Bob", "score": "88"},
    ]
    assert payload["preview_row_count"] == 2
    assert payload["preview_format"] == "xlsx"


def test_get_dataset_profile_returns_xlsx_column_statistics() -> None:
    """验证 xlsx 文件可以返回字段元信息统计结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.xlsx",
                BytesIO(
                    _build_xlsx_bytes(
                        [
                            ["id", "score", "passed", "name"],
                            [1, 95, True, "Alice"],
                            [2, 88, False, "Bob"],
                            [3, None, True, None],
                        ]
                    )
                ),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.get(f"/api/v1/datasets/{dataset_id}/profile")
    payload = response.json()

    assert response.status_code == 200
    assert payload["dataset_id"] == dataset_id
    assert payload["file_name"] == "survey.xlsx"
    assert payload["row_count"] == 3
    assert payload["column_count"] == 4
    assert payload["profile_format"] == "xlsx"
    assert payload["columns"] == [
        {
            "name": "id",
            "inferred_type": "integer",
            "nullable": False,
            "missing_count": 0,
            "unique_count": 3,
            "sample_values": ["1", "2", "3"],
        },
        {
            "name": "score",
            "inferred_type": "integer",
            "nullable": True,
            "missing_count": 1,
            "unique_count": 2,
            "sample_values": ["95", "88"],
        },
        {
            "name": "passed",
            "inferred_type": "boolean",
            "nullable": False,
            "missing_count": 0,
            "unique_count": 2,
            "sample_values": ["True", "False"],
        },
        {
            "name": "name",
            "inferred_type": "string",
            "nullable": True,
            "missing_count": 1,
            "unique_count": 2,
            "sample_values": ["Alice", "Bob"],
        },
    ]
