from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_create_platform_workflow_definition_and_query_detail() -> None:
    """验证平台级工作流定义可以独立创建和读取。"""
    create_response = client.post(
        "/api/v1/workflows",
        json={
            "name": "  Free Workflow  ",
            "description": "  graph first workflow  ",
        },
    )

    assert create_response.status_code == 201
    payload = create_response.json()
    assert payload["name"] == "Free Workflow"
    assert payload["description"] == "graph first workflow"

    detail_response = client.get(f"/api/v1/workflows/{payload['id']}")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["workflow"]["id"] == payload["id"]
    assert detail_payload["nodes"] == []
    assert detail_payload["edges"] == []


def test_platform_workflow_definition_supports_nodes_and_edges() -> None:
    """验证平台级工作流定义可以独立维护节点和边。"""
    workflow = client.post(
        "/api/v1/workflows",
        json={"name": "Flow Graph"},
    ).json()

    source_node = client.post(
        f"/api/v1/workflows/{workflow['id']}/nodes",
        json={
            "node_key": "input_1",
            "node_type": "dataset_input",
            "name": "Input 1",
            "config": {"source": "current_dataset"},
        },
    ).json()
    target_node = client.post(
        f"/api/v1/workflows/{workflow['id']}/nodes",
        json={
            "node_key": "preview_1",
            "node_type": "dataset_preview",
            "name": "Preview 1",
            "config": {"offset": 0, "limit": 20},
        },
    ).json()

    edge_response = client.post(
        f"/api/v1/workflows/{workflow['id']}/edges",
        json={
            "edge_key": "input_to_preview",
            "source_node_id": source_node["id"],
            "target_node_id": target_node["id"],
            "source_port": "dataset_ref",
            "target_port": "dataset_ref",
            "config": {},
        },
    )

    assert edge_response.status_code == 201
    edge_payload = edge_response.json()
    assert edge_payload["source_port"] == "dataset_ref"
    assert edge_payload["target_port"] == "dataset_ref"

    nodes_response = client.get(f"/api/v1/workflows/{workflow['id']}/nodes")
    edges_response = client.get(f"/api/v1/workflows/{workflow['id']}/edges")

    assert nodes_response.status_code == 200
    assert nodes_response.json()["total"] == 2
    assert edges_response.status_code == 200
    assert edges_response.json()["total"] == 1


def test_create_platform_workflow_edge_rejects_unknown_node() -> None:
    """验证平台级工作流边必须连接到真实存在的节点。"""
    workflow = client.post(
        "/api/v1/workflows",
        json={"name": "Invalid Edge Flow"},
    ).json()

    response = client.post(
        f"/api/v1/workflows/{workflow['id']}/edges",
        json={
            "edge_key": "bad_edge",
            "source_node_id": "not-found",
            "target_node_id": "also-not-found",
            "config": {},
        },
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "请求的工作流节点不存在。"}


def test_update_platform_workflow_graph_in_one_request() -> None:
    """验证前端可以一次性保存整张工作流图。"""
    workflow = client.post(
        "/api/v1/workflows",
        json={"name": "Graph Save Flow"},
    ).json()

    response = client.put(
        f"/api/v1/workflows/{workflow['id']}/graph",
        json={
            "name": "  Graph Save Flow v2  ",
            "description": "  save full graph  ",
            "nodes": [
                {
                    "id": "node_input",
                    "node_key": "input_1",
                    "node_type": "dataset_input",
                    "name": " Input ",
                    "config": {"source": "current_dataset"},
                },
                {
                    "id": "node_preview",
                    "node_key": "preview_1",
                    "node_type": "dataset_preview",
                    "name": " Preview ",
                    "config": {"offset": 0, "limit": 10},
                },
            ],
            "edges": [
                {
                    "id": "edge_1",
                    "edge_key": "input_to_preview",
                    "source_node_id": "node_input",
                    "target_node_id": "node_preview",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["workflow"]["name"] == "Graph Save Flow v2"
    assert payload["workflow"]["description"] == "save full graph"
    assert len(payload["nodes"]) == 2
    assert len(payload["edges"]) == 1
    assert payload["edges"][0]["source_port"] == "dataset_ref"
    assert payload["edges"][0]["target_port"] == "dataset_ref"


def test_update_platform_workflow_graph_rejects_cycle() -> None:
    """验证整图保存时会拒绝存在环的流程图。"""
    workflow = client.post(
        "/api/v1/workflows",
        json={"name": "Cycle Flow"},
    ).json()

    response = client.put(
        f"/api/v1/workflows/{workflow['id']}/graph",
        json={
            "name": "Cycle Flow",
            "nodes": [
                {
                    "id": "node_a",
                    "node_key": "input_1",
                    "node_type": "dataset_input",
                    "name": "Input",
                    "config": {"source": "current_dataset"},
                },
                {
                    "id": "node_b",
                    "node_key": "preview_1",
                    "node_type": "dataset_preview",
                    "name": "Preview",
                    "config": {"offset": 0, "limit": 10},
                },
            ],
            "edges": [
                {
                    "edge_key": "a_to_b",
                    "source_node_id": "node_a",
                    "target_node_id": "node_b",
                },
                {
                    "edge_key": "b_to_a",
                    "source_node_id": "node_b",
                    "target_node_id": "node_a",
                    "source_port": "preview_table",
                    "target_port": "dataset_ref",
                },
            ],
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "当前工作流图存在环，无法按依赖顺序执行。"}
