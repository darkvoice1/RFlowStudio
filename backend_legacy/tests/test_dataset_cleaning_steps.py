from io import BytesIO

from fastapi.testclient import TestClient

from app.db.session import session_scope
from app.main import app
from app.models.dataset import DatasetCleaningStepModel

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


def test_create_dataset_cleaning_step_persists_into_database() -> None:
    """验证新增清洗步骤后，数据库中会留下对应的步骤记录。"""
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

    response = client.post(
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
    payload = response.json()

    with session_scope() as session:
        stored_step = session.get(DatasetCleaningStepModel, payload["id"])

    assert response.status_code == 201
    assert stored_step is not None
    assert stored_step.dataset_id == dataset_id
    assert stored_step.order == 1
    assert stored_step.parameters == {
        "column": "score",
        "operator": "gte",
        "value": "90",
    }


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


def test_create_sort_cleaning_step_rejects_invalid_direction() -> None:
    """验证排序步骤的非法 direction 会被明确拒绝。"""
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

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "sort",
            "name": "非法排序",
            "parameters": {
                "column": "score",
                "direction": "down",
            },
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "排序步骤的 direction 只支持 asc 或 desc。"
    }


def test_create_missing_value_mark_values_step_records_normalized_values() -> None:
    """验证缺失值标记步骤会保存整理后的标记值列表。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score\n1,95\n2,NA\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "missing_value",
            "name": "把特殊值标记成缺失",
            "parameters": {
                "method": "mark_values",
                "column": "score",
                "values": [" NA ", "999"],
            },
        },
    )
    payload = response.json()

    assert response.status_code == 201
    assert payload["parameters"] == {
        "method": "mark_values",
        "column": "score",
        "values": ["NA", "999"],
    }


def test_create_missing_value_mark_values_step_rejects_empty_values() -> None:
    """验证缺失值标记步骤会拒绝空的标记值列表。"""
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
            "name": "非法缺失值标记",
            "parameters": {
                "method": "mark_values",
                "column": "score",
                "values": [],
            },
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "缺失值标记步骤必须提供非空的 values 列表。"
    }


def test_create_recode_cleaning_step_records_normalized_mapping() -> None:
    """验证重编码步骤会保存整理后的映射关系。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,gender\n1,1\n2,2\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "recode",
            "name": "性别编码转中文",
            "parameters": {
                "column": "gender",
                "mapping": {
                    " 1 ": " 男 ",
                    "2": "女",
                },
            },
        },
    )
    payload = response.json()

    assert response.status_code == 201
    assert payload["parameters"] == {
        "column": "gender",
        "mapping": {
            "1": "男",
            "2": "女",
        },
    }


def test_create_recode_cleaning_step_rejects_empty_mapping() -> None:
    """验证重编码步骤会拒绝空映射。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,gender\n1,1\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "recode",
            "name": "非法重编码",
            "parameters": {
                "column": "gender",
                "mapping": {},
            },
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "重编码步骤必须提供非空的 mapping 映射。"
    }


def test_create_derive_variable_cleaning_step_records_concat_parameters() -> None:
    """验证新变量生成步骤会保存整理后的拼接参数。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"first_name,last_name\nA,Li\nB,Wang\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "derive_variable",
            "name": "生成全名",
            "parameters": {
                "method": "concat",
                "new_column": " full_name ",
                "source_columns": [" first_name ", "last_name"],
                "separator": "-",
            },
        },
    )
    payload = response.json()

    assert response.status_code == 201
    assert payload["parameters"] == {
        "method": "concat",
        "new_column": "full_name",
        "source_columns": ["first_name", "last_name"],
        "separator": "-",
    }


def test_create_derive_variable_cleaning_step_rejects_invalid_method() -> None:
    """验证新变量生成步骤会拒绝不支持的 method。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"score,bonus\n95,5\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "derive_variable",
            "name": "非法新变量",
            "parameters": {
                "method": "unknown",
                "new_column": "total",
            },
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "新变量生成步骤的 method 不受支持。"
    }


def test_get_dataset_cleaning_r_script_returns_base_draft_for_new_dataset() -> None:
    """验证没有清洗步骤的数据集也可以导出基础 R 代码草稿。"""
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

    response = client.get(f"/api/v1/datasets/{dataset_id}/cleaning-r-script")
    payload = response.json()

    assert response.status_code == 200
    assert payload["dataset_id"] == dataset_id
    assert payload["file_name"] == "survey.csv"
    assert payload["step_count"] == 0
    assert (
        "# 脚本用途: 根据当前数据集已记录的清洗步骤，生成可复现的 R 清洗脚本。"
        in payload["script"]
    )
    assert "# 包依赖说明" in payload["script"]
    assert "# 数据来源说明" in payload["script"]
    assert "# 参数说明" in payload["script"]
    assert "library(readr)" in payload["script"]
    assert "# 当前还没有记录任何清洗步骤" in payload["script"]


def test_get_dataset_cleaning_r_script_contains_current_step_draft() -> None:
    """验证导出的 R 草稿会按顺序包含当前清洗步骤对应的代码。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,gender,math,english\n1,1,80,90\n2,2,70,85\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "filter",
            "name": "筛选高分样本",
            "parameters": {
                "column": "math",
                "operator": "gte",
                "value": "70",
            },
        },
    )
    client.post(
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
    client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "derive_variable",
            "name": "生成总分",
            "parameters": {
                "method": "binary_operation",
                "new_column": "total",
                "left_column": "math",
                "right_column": "english",
                "operator": "add",
            },
        },
    )

    response = client.get(f"/api/v1/datasets/{dataset_id}/cleaning-r-script")
    payload = response.json()

    assert response.status_code == 200
    assert payload["step_count"] == 3
    assert "# - 当前脚本共包含 3 个清洗步骤。" in payload["script"]
    assert "# 步骤 1: 筛选高分样本" in payload["script"]
    assert (
        "cleaned_data <- cleaned_data[rflow_num(cleaned_data[[\"math\"]]) >= 70, "
        ", drop = FALSE]"
    ) in payload["script"]
    assert "recode_map <- c(\"男\" = \"1\", \"女\" = \"2\")" in payload["script"]
    assert "cleaned_data[[\"total\"]] <- rflow_format_number(result_num)" in payload["script"]


def test_get_dataset_cleaning_r_script_returns_404_for_unknown_dataset() -> None:
    """验证不存在的数据集不能导出伪造的 R 代码草稿。"""
    response = client.get("/api/v1/datasets/not-found/cleaning-r-script")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的数据集不存在。"
    }
