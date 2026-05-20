from io import BytesIO

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _upload_dataset() -> str:
    response = client.post(
        "/api/v1/datasets/upload",
        files={
            "file": (
                "survey.csv",
                BytesIO(b"id,score\n1,95\n2,88\n"),
                "text/csv",
            )
        },
    )
    assert response.status_code == 200
    return response.json()["id"]


def _create_workflow(dataset_id: str) -> dict[str, object]:
    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows",
        json={
            "name": "baseline workflow",
            "description": "first workflow draft",
        },
    )
    assert response.status_code == 201
    return response.json()


def test_list_workflow_node_definitions_returns_registered_catalog() -> None:
    """验证节点注册中心能返回当前已注册的节点目录。"""
    response = client.get("/api/v1/workflow-nodes")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 6
    assert any(item["key"] == "dataset_input" for item in payload["items"])
    assert any(item["key"] == "analysis_step" for item in payload["items"])


def test_get_workflow_node_definition_resolves_alias_to_canonical_key() -> None:
    """验证通过别名查询时会返回规范节点定义。"""
    response = client.get("/api/v1/workflow-nodes/analysis_node")

    assert response.status_code == 200
    payload = response.json()
    assert payload["key"] == "analysis_step"
    assert "analysis_node" in payload["aliases"]


def test_list_workflow_node_definitions_includes_enabled_plugin_nodes() -> None:
    """验证已启用插件节点会出现在统一节点目录中。"""
    sync_response = client.post("/api/v1/plugins/sync")
    assert sync_response.status_code == 200

    response = client.get("/api/v1/workflow-nodes")

    assert response.status_code == 200
    payload = response.json()
    dataset_input = next(item for item in payload["items"] if item["key"] == "dataset_input")
    assert dataset_input["source"] == "plugin"
    assert dataset_input["plugin_id"] == "builtin-dataset_input"


def test_disabled_plugin_node_is_hidden_from_workflow_node_catalog() -> None:
    """验证插件节点被禁用后会从工作流节点目录中隐藏。"""
    sync_response = client.post("/api/v1/plugins/sync")
    assert sync_response.status_code == 200

    disable_response = client.patch(
        "/api/v1/plugins/builtin-dataset_input/status",
        json={"status": "disabled"},
    )
    assert disable_response.status_code == 200

    response = client.get("/api/v1/workflow-nodes")

    assert response.status_code == 200
    payload = response.json()
    assert all(item["key"] != "dataset_input" for item in payload["items"])


def test_get_workflow_node_definition_returns_plugin_binding_metadata() -> None:
    """验证节点详情接口能返回插件绑定信息。"""
    sync_response = client.post("/api/v1/plugins/sync")
    assert sync_response.status_code == 200

    response = client.get("/api/v1/workflow-nodes/dataset_input")

    assert response.status_code == 200
    payload = response.json()
    assert payload["key"] == "dataset_input"
    assert payload["source"] == "plugin"
    assert payload["plugin_id"] == "builtin-dataset_input"


def test_create_workflow_node_rejects_unregistered_node_type() -> None:
    """验证未注册的节点类型会被明确拒绝。"""
    dataset_id = _upload_dataset()
    workflow = _create_workflow(dataset_id)

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/nodes",
        json={
            "node_key": "unknown_node",
            "node_type": "unknown_type",
            "name": "Unknown Node",
            "config": {},
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "节点类型 unknown_type 未注册。",
    }
