from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_sync_plugins_registers_builtin_plugins() -> None:
    """验证同步接口会按单节点粒度注册内置插件。"""
    response = client.post("/api/v1/plugins/sync")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 13
    assert any(item["id"] == "builtin-dataset_input" for item in payload["items"])
    assert any(item["id"] == "builtin-descriptive_statistics" for item in payload["items"])


def test_list_plugins_returns_registered_plugins() -> None:
    """验证插件列表接口会返回单节点插件。"""
    sync_response = client.post("/api/v1/plugins/sync")
    assert sync_response.status_code == 200

    response = client.get("/api/v1/plugins")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 13
    dataset_plugin = next(
        item for item in payload["items"] if item["id"] == "builtin-dataset_input"
    )
    assert dataset_plugin["category"] == "dataset"
    assert dataset_plugin["status"] == "enabled"
    assert dataset_plugin["executor"].endswith("dataset_input/executor.py")


def test_get_plugin_detail_returns_single_node_plugin() -> None:
    """验证插件详情接口返回的是单个功能节点插件。"""
    sync_response = client.post("/api/v1/plugins/sync")
    assert sync_response.status_code == 200

    response = client.get("/api/v1/plugins/builtin-dataset_input")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "builtin-dataset_input"
    assert payload["manifest_path"].endswith(
        "backend/app/plugins/builtin/dataset/nodes/dataset_input/manifest.json"
    )
    assert payload["plugin_path"].endswith(
        "backend/app/plugins/builtin/dataset/nodes/dataset_input"
    )
    assert payload["executor"].endswith("dataset_input/executor.py")


def test_update_builtin_plugin_status_to_disabled() -> None:
    """验证内置单节点插件可以切换为 disabled 状态。"""
    sync_response = client.post("/api/v1/plugins/sync")
    assert sync_response.status_code == 200

    response = client.patch(
        "/api/v1/plugins/builtin-dataset_input/status",
        json={"status": "disabled"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "builtin-dataset_input"
    assert payload["status"] == "disabled"
