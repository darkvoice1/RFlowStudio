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
    assert payload["offset"] == 0
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
    assert payload["offset"] == 0
    assert payload["limit"] == 2
    assert payload["has_more"] is True


def test_get_dataset_preview_supports_offset_pagination() -> None:
    """验证预览接口支持通过 offset 获取后续分页数据。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "scores.csv",
                BytesIO(b"id,score\n1,95\n2,88\n3,90\n4,91\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.get(f"/api/v1/datasets/{dataset_id}/preview?offset=2&limit=2")
    payload = response.json()

    assert response.status_code == 200
    assert payload["rows"] == [
        {"id": "3", "score": "90"},
        {"id": "4", "score": "91"},
    ]
    assert payload["preview_row_count"] == 2
    assert payload["offset"] == 2
    assert payload["limit"] == 2
    assert payload["has_more"] is False


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


def test_get_dataset_preview_applies_enabled_filter_steps() -> None:
    """验证已记录的筛选步骤会真实作用到数据预览结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "scores.csv",
                BytesIO(b"id,name,score\n1,Alice,95\n2,Bob,88\n3,Carol,91\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "filter",
            "name": "筛选高分样本",
            "parameters": {
                "column": "score",
                "operator": "gte",
                "value": "90",
            },
        },
    )
    response = client.get(f"/api/v1/datasets/{dataset_id}/preview")
    payload = response.json()

    assert create_response.status_code == 201
    assert response.status_code == 200
    assert payload["rows"] == [
        {"id": "1", "name": "Alice", "score": "95"},
        {"id": "3", "name": "Carol", "score": "91"},
    ]
    assert payload["preview_row_count"] == 2
    assert payload["has_more"] is False


def test_get_dataset_preview_applies_missing_value_drop_rows_step() -> None:
    """验证缺失值删除步骤会把含缺失值的整行从预览结果中移除。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "scores.csv",
                BytesIO(b"id,name,score\n1,Alice,95\n2,Bob,\n3,,91\n4,David,85\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "missing_value",
            "name": "删除含缺失值记录",
            "parameters": {
                "method": "drop_rows",
            },
        },
    )
    response = client.get(f"/api/v1/datasets/{dataset_id}/preview")
    payload = response.json()

    assert create_response.status_code == 201
    assert response.status_code == 200
    assert payload["rows"] == [
        {"id": "1", "name": "Alice", "score": "95"},
        {"id": "4", "name": "David", "score": "85"},
    ]
    assert payload["preview_row_count"] == 2
    assert payload["has_more"] is False


def test_get_dataset_preview_applies_sort_step_and_keeps_missing_values_last() -> None:
    """验证排序步骤会真实影响预览顺序，并把空值统一放到最后。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "scores.csv",
                BytesIO(b"id,name,score\n1,Alice,95\n2,Bob,\n3,Carol,88\n4,David,91\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "sort",
            "name": "按分数降序排列",
            "parameters": {
                "column": "score",
                "direction": "desc",
            },
        },
    )
    response = client.get(f"/api/v1/datasets/{dataset_id}/preview")
    payload = response.json()

    assert create_response.status_code == 201
    assert response.status_code == 200
    assert payload["rows"] == [
        {"id": "1", "name": "Alice", "score": "95"},
        {"id": "4", "name": "David", "score": "91"},
        {"id": "3", "name": "Carol", "score": "88"},
        {"id": "2", "name": "Bob", "score": None},
    ]


def test_get_dataset_preview_applies_missing_value_mark_values_step() -> None:
    """验证缺失值标记步骤会把指定业务值转换成真正的空值。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "scores.csv",
                BytesIO(b"id,name,score\n1,Alice,95\n2,Bob,NA\n3,Carol,999\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "missing_value",
            "name": "把特殊值标记为缺失",
            "parameters": {
                "method": "mark_values",
                "column": "score",
                "values": ["NA", "999"],
            },
        },
    )
    response = client.get(f"/api/v1/datasets/{dataset_id}/preview")
    payload = response.json()

    assert create_response.status_code == 201
    assert response.status_code == 200
    assert payload["rows"] == [
        {"id": "1", "name": "Alice", "score": "95"},
        {"id": "2", "name": "Bob", "score": None},
        {"id": "3", "name": "Carol", "score": None},
    ]


def test_get_dataset_preview_applies_recode_step() -> None:
    """验证重编码步骤会把指定列的值按映射关系改写。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,gender,score\n1,1,95\n2,2,88\n3,3,91\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "recode",
            "name": "性别编码转中文",
            "parameters": {
                "column": "gender",
                "mapping": {
                    "1": "男",
                    "2": "女",
                },
            },
        },
    )
    response = client.get(f"/api/v1/datasets/{dataset_id}/preview")
    payload = response.json()

    assert create_response.status_code == 201
    assert response.status_code == 200
    assert payload["rows"] == [
        {"id": "1", "gender": "男", "score": "95"},
        {"id": "2", "gender": "女", "score": "88"},
        {"id": "3", "gender": "3", "score": "91"},
    ]
