from fastapi.testclient import TestClient

from app.main import app, create_app


def test_create_app_returns_fastapi_app() -> None:
    """验证新后端入口已经变成可运行的 FastAPI 应用。"""
    created_app = create_app()

    assert created_app.title == "RFlowStudio Backend"


def test_health_endpoint_returns_ok() -> None:
    """验证健康检查接口可访问。"""
    client = TestClient(app)

    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
