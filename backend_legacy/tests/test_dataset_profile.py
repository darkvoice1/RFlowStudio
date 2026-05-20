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


def test_get_dataset_profile_applies_missing_value_fill_step() -> None:
    """验证缺失值替换步骤会同步影响字段分析统计结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score,group\n1,95,A\n2,,B\n3,91,\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "missing_value",
            "name": "缺失分组补未知",
            "parameters": {
                "method": "fill_value",
                "column": "group",
                "value": "未知",
            },
        },
    )
    response = client.get(f"/api/v1/datasets/{dataset_id}/profile")
    payload = response.json()

    assert create_response.status_code == 201
    assert response.status_code == 200
    assert payload["row_count"] == 3
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
            "sample_values": ["95", "91"],
        },
        {
            "name": "group",
            "inferred_type": "string",
            "nullable": False,
            "missing_count": 0,
            "unique_count": 3,
            "sample_values": ["A", "B", "未知"],
        },
    ]


def test_get_dataset_profile_applies_missing_value_mark_values_step() -> None:
    """验证缺失值标记步骤会同步影响字段分析里的缺失统计。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score,group\n1,95,A\n2,NA,B\n3,999,A\n"),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "missing_value",
            "name": "把特殊分数标记为缺失",
            "parameters": {
                "method": "mark_values",
                "column": "score",
                "values": ["NA", "999"],
            },
        },
    )
    response = client.get(f"/api/v1/datasets/{dataset_id}/profile")
    payload = response.json()

    assert create_response.status_code == 201
    assert response.status_code == 200
    assert payload["row_count"] == 3
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
            "missing_count": 2,
            "unique_count": 1,
            "sample_values": ["95"],
        },
        {
            "name": "group",
            "inferred_type": "string",
            "nullable": False,
            "missing_count": 0,
            "unique_count": 2,
            "sample_values": ["A", "B"],
        },
    ]


def test_get_dataset_profile_applies_recode_step() -> None:
    """验证重编码步骤会同步影响字段分析的字段类型和值分布。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,gender\n1,1\n2,2\n3,1\n"),
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
    response = client.get(f"/api/v1/datasets/{dataset_id}/profile")
    payload = response.json()

    assert create_response.status_code == 201
    assert response.status_code == 200
    assert payload["row_count"] == 3
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
            "name": "gender",
            "inferred_type": "string",
            "nullable": False,
            "missing_count": 0,
            "unique_count": 2,
            "sample_values": ["男", "女"],
        },
    ]


def test_get_dataset_profile_applies_derive_variable_concat_step() -> None:
    """验证字段拼接生成的新变量会同步进入字段分析结果。"""
    upload_response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "people.csv",
                BytesIO(
                    b"first_name,last_name\nLei,Wang\nMing,Li\n,\xe9\x99\x88\n"
                ),
                "text/csv",
            )
        },
    )
    dataset_id = upload_response.json()["id"]

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/cleaning-steps",
        json={
            "step_type": "derive_variable",
            "name": "生成全名",
            "parameters": {
                "method": "concat",
                "new_column": "full_name",
                "source_columns": ["first_name", "last_name"],
                "separator": "",
            },
        },
    )
    response = client.get(f"/api/v1/datasets/{dataset_id}/profile")
    payload = response.json()

    assert create_response.status_code == 201
    assert response.status_code == 200
    assert payload["row_count"] == 3
    assert payload["columns"] == [
        {
            "name": "first_name",
            "inferred_type": "string",
            "nullable": True,
            "missing_count": 1,
            "unique_count": 2,
            "sample_values": ["Lei", "Ming"],
        },
        {
            "name": "last_name",
            "inferred_type": "string",
            "nullable": False,
            "missing_count": 0,
            "unique_count": 3,
            "sample_values": ["Wang", "Li", "陈"],
        },
        {
            "name": "full_name",
            "inferred_type": "string",
            "nullable": False,
            "missing_count": 0,
            "unique_count": 3,
            "sample_values": ["LeiWang", "MingLi", "陈"],
        },
    ]
