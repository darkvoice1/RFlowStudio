from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health_check_returns_expected_payload() -> None:
    """验证健康检查接口能返回预期内容。"""
    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "app_name": "Open R Platform API",
        "version": "0.1.0",
        "environment": "development",
    }
