from io import BytesIO

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_get_dataset_profile_returns_column_statistics() -> None:
    """验证字段分析接口会返回列级元信息统计结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(
                    b"id,score,passed,name\n1,95,true,Alice\n2,88,false,Bob\n3,,true,\n"
                ),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.get(f"/api/v1/datasets/{dataset_id}/profile")
    payload = response.json()

    assert response.status_code == 200
    assert payload["dataset_id"] == dataset_id
    assert payload["file_name"] == "survey.csv"
    assert payload["row_count"] == 3
    assert payload["column_count"] == 4
    assert payload["profile_format"] == "csv"
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
            "sample_values": ["true", "false"],
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


def test_get_dataset_profile_rejects_invalid_xlsx_file() -> None:
    """验证非法 xlsx 文件会返回清晰的字段分析异常提示。"""
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

    response = client.get(f"/api/v1/datasets/{dataset_id}/profile")

    assert response.status_code == 400
    assert response.json() == {
        "detail": "XLSX 文件格式异常，暂时无法分析字段信息。"
    }


def test_get_dataset_profile_applies_enabled_filter_steps() -> None:
    """验证已记录的筛选步骤会同步影响字段分析统计结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(
                    b"id,score,group\n1,95,A\n2,88,B\n3,91,A\n4,,B\n"
                ),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "filter",
            "name": "筛选 A 组样本",
            "parameters": {
                "column": "group",
                "operator": "eq",
                "value": "A",
            },
        },
    )
    response = client.get(f"/api/v1/datasets/{dataset_id}/profile")
    payload = response.json()

    assert create_response.status_code == 201
    assert response.status_code == 200
    assert payload["row_count"] == 2
    assert payload["columns"] == [
        {
            "name": "id",
            "inferred_type": "integer",
            "nullable": False,
            "missing_count": 0,
            "unique_count": 2,
            "sample_values": ["1", "3"],
        },
        {
            "name": "score",
            "inferred_type": "integer",
            "nullable": False,
            "missing_count": 0,
            "unique_count": 2,
            "sample_values": ["95", "91"],
        },
        {
            "name": "group",
            "inferred_type": "string",
            "nullable": False,
            "missing_count": 0,
            "unique_count": 1,
            "sample_values": ["A"],
        },
    ]
