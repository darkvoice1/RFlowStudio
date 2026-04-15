from io import BytesIO

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_list_dataset_cleaning_steps_returns_empty_for_new_dataset() -> None:
    """验证新上传的数据集默认还没有任何清洗步骤。"""
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

    response = client.get(f"/api/v1/datasets/{dataset_id}/cleaning-steps")

    assert response.status_code == 200
    assert response.json() == {
        "dataset_id": dataset_id,
        "items": [],
        "total": 0,
    }


def test_create_dataset_cleaning_step_records_reproducible_order() -> None:
    """验证清洗步骤可以按追加顺序记录下来，供后续复现使用。"""
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

    first_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "filter",
            "name": "筛选高分样本",
            "description": "只保留分数大于等于 90 的记录。",
            "enabled": True,
            "parameters": {
                "column": "score",
                "operator": "gte",
                "value": "90",
            },
        },
    )
    second_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "sort",
            "name": "按分数降序排列",
            "description": "先让后续预览结果更接近用户操作顺序。",
            "enabled": True,
            "parameters": {
                "column": "score",
                "direction": "desc",
            },
        },
    )
    list_response = client.get(f"/api/v1/datasets/{dataset_id}/cleaning-steps")

    first_payload = first_response.json()
    second_payload = second_response.json()
    list_payload = list_response.json()

    assert first_response.status_code == 201
    assert first_payload["step_type"] == "filter"
    assert first_payload["name"] == "筛选高分样本"
    assert first_payload["order"] == 1
    assert first_payload["parameters"] == {
        "column": "score",
        "operator": "gte",
        "value": "90",
    }

    assert second_response.status_code == 201
    assert second_payload["step_type"] == "sort"
    assert second_payload["order"] == 2

    assert list_response.status_code == 200
    assert list_payload["dataset_id"] == dataset_id
    assert list_payload["total"] == 2
    assert [item["id"] for item in list_payload["items"]] == [
        first_payload["id"],
        second_payload["id"],
    ]
    assert [item["order"] for item in list_payload["items"]] == [1, 2]


def test_dataset_cleaning_steps_return_404_for_unknown_dataset() -> None:
    """验证不存在的数据集不会返回伪造的清洗步骤结果。"""
    response = client.get("/api/v1/datasets/not-found/cleaning-steps")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的数据集不存在。"
    }


def test_create_dataset_cleaning_step_returns_404_for_unknown_dataset() -> None:
    """验证不存在的数据集不会创建清洗步骤。"""
    response = client.post(
        "/api/v1/datasets/not-found/cleaning-steps",
        json={
            "step_type": "filter",
            "name": "筛选高分样本",
            "parameters": {},
        },
    )

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的数据集不存在。"
    }


def test_create_filter_cleaning_step_rejects_invalid_operator() -> None:
    """验证筛选步骤的非法操作符会被明确拒绝。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score\n1,95\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "filter",
            "name": "非法筛选",
            "parameters": {
                "column": "score",
                "operator": "unknown",
                "value": "90",
            },
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "筛选步骤的操作符不受支持。"
    }


def test_create_missing_value_cleaning_step_records_method_parameters() -> None:
    """验证缺失值处理步骤可以按当前第一版参数结构记录下来。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score\n1,95\n2,\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "missing_value",
            "name": "缺失分数补 0",
            "parameters": {
                "method": "fill_value",
                "column": "score",
                "value": "0",
            },
        },
    )
    payload = response.json()

    assert response.status_code == 201
    assert payload["step_type"] == "missing_value"
    assert payload["parameters"] == {
        "method": "fill_value",
        "column": "score",
        "value": "0",
    }


def test_create_missing_value_cleaning_step_rejects_invalid_method() -> None:
    """验证缺失值处理步骤的非法 method 会被明确拒绝。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score\n1,95\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "missing_value",
            "name": "非法缺失值处理",
            "parameters": {
                "method": "unknown",
            },
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "缺失值处理步骤的 method 不受支持。"
    }
