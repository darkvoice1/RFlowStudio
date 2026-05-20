from io import BytesIO

from fastapi.testclient import TestClient

from app.core.config import settings
from app.main import app

client = TestClient(app)


def test_upload_dataset_persists_metadata_in_database_not_local_json() -> None:
    """验证数据集元信息迁库后不会再落本地 JSON 文件。"""
    response = client.post(
        "/api/v1/datasets/upload",
        files={"file": ("demo.csv", BytesIO(b"id,name\n1,Alice\n"), "text/csv")},
    )

    payload = response.json()

    assert response.status_code == 200
    assert list(settings.dataset_metadata_root.glob("*.json")) == []

    detail_response = client.get(f"/api/v1/datasets/{payload['id']}")
    assert detail_response.status_code == 200
    assert detail_response.json()["id"] == payload["id"]
