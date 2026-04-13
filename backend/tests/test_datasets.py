from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_list_datasets_returns_empty_result_in_scaffold_stage() -> None:
    """验证没有上传记录时列表接口返回空结果。"""
    response = client.get("/api/v1/datasets")

    assert response.status_code == 200
    assert response.json() == {
        "items": [],
        "total": 0,
    }


def test_upload_capabilities_returns_supported_formats() -> None:
    """验证上传能力说明接口返回当前约定的文件格式。"""
    response = client.get("/api/v1/datasets/upload-capabilities")

    assert response.status_code == 200
    assert response.json() == {
        "supported_extensions": [".csv", ".xlsx", ".sav"],
        "max_file_size_mb": 100,
        "upload_strategy": "single_file",
    }
