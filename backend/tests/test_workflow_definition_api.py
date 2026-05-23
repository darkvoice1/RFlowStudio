from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_workflow_definition_api_can_create_and_read_detail() -> None:
    """验证工作流定义接口支持创建和读取详情。"""
    create_response = client.post(
        "/api/v1/workflows",
        json={
            "name": "API Workflow",
            "description": "api create",
        },
    )
    workflow = create_response.json()

    detail_response = client.get(f"/api/v1/workflows/{workflow['id']}")

    assert create_response.status_code == 201
    assert detail_response.status_code == 200
    assert detail_response.json()["workflow"]["name"] == "API Workflow"


def test_workflow_definition_api_can_save_graph() -> None:
    """验证整图保存接口可保存节点和连线。"""
    create_response = client.post(
        "/api/v1/workflows",
        json={
            "name": "Graph Workflow",
            "description": "graph save",
        },
    )
    workflow = create_response.json()

    save_response = client.put(
        f"/api/v1/workflows/{workflow['id']}/graph",
        json={
            "name": "Graph Workflow",
            "description": "graph save updated",
            "nodes": [
                {
                    "id": "node_1",
                    "node_key": "input_1",
                    "node_type": "dataset_input",
                    "name": "Input",
                    "config": {"source": "current_dataset"},
                },
                {
                    "id": "node_2",
                    "node_key": "preview_1",
                    "node_type": "dataset_preview",
                    "name": "Preview",
                    "config": {"limit": 10},
                },
            ],
            "edges": [
                {
                    "id": "edge_1",
                    "edge_key": "input_to_preview",
                    "source_node_id": "node_1",
                    "target_node_id": "node_2",
                    "source_port": "dataset_ref",
                    "target_port": "dataset_ref",
                }
            ],
        },
    )

    assert save_response.status_code == 200
    assert len(save_response.json()["nodes"]) == 2
    assert len(save_response.json()["edges"]) == 1


def test_workflow_definition_api_rejects_duplicate_node_keys() -> None:
    """验证整图保存接口会拦截重复节点 key。"""
    create_response = client.post(
        "/api/v1/workflows",
        json={
            "name": "Duplicate Node Workflow",
            "description": "duplicate key",
        },
    )
    workflow = create_response.json()

    save_response = client.put(
        f"/api/v1/workflows/{workflow['id']}/graph",
        json={
            "name": "Duplicate Node Workflow",
            "nodes": [
                {
                    "node_key": "dup_node",
                    "node_type": "dataset_input",
                    "name": "Input A",
                    "config": {},
                },
                {
                    "node_key": "dup_node",
                    "node_type": "dataset_preview",
                    "name": "Input B",
                    "config": {},
                },
            ],
            "edges": [],
        },
    )

    assert save_response.status_code == 400
    assert save_response.json() == {"detail": "存在重复的节点 key。"}


def test_workflow_definition_api_rejects_missing_edge_nodes() -> None:
    """验证整图保存接口会拦截连线引用缺失节点。"""
    create_response = client.post(
        "/api/v1/workflows",
        json={
            "name": "Missing Edge Node Workflow",
            "description": "missing node",
        },
    )
    workflow = create_response.json()

    save_response = client.put(
        f"/api/v1/workflows/{workflow['id']}/graph",
        json={
            "name": "Missing Edge Node Workflow",
            "nodes": [
                {
                    "id": "node_1",
                    "node_key": "input_1",
                    "node_type": "dataset_input",
                    "name": "Input",
                    "config": {},
                }
            ],
            "edges": [
                {
                    "edge_key": "bad_edge",
                    "source_node_id": "node_1",
                    "target_node_id": "node_2",
                    "source_port": "dataset_ref",
                    "target_port": "dataset_ref",
                    "config": {},
                }
            ],
        },
    )

    assert save_response.status_code == 400
    assert save_response.json() == {"detail": "存在连线引用了未定义的节点。"}
