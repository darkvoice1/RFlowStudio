from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_workflow_plan_api_can_return_execution_plan() -> None:
    """验证执行计划接口可以返回最小拓扑计划。"""
    create_response = client.post(
        "/api/v1/workflows",
        json={
            "name": "API Plan Workflow",
            "description": "api plan",
        },
    )
    workflow = create_response.json()

    client.put(
        f"/api/v1/workflows/{workflow['id']}/graph",
        json={
            "name": "API Plan Workflow",
            "description": "api plan",
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

    plan_response = client.get(f"/api/v1/workflows/{workflow['id']}/plan")

    assert plan_response.status_code == 200
    assert plan_response.json()["ordered_node_ids"] == ["node_1", "node_2"]
    assert plan_response.json()["steps"][1]["depends_on_node_ids"] == ["node_1"]


def test_workflow_plan_api_rejects_invalid_graph() -> None:
    """验证无效图在计划接口层会被明确拒绝。"""
    create_response = client.post(
        "/api/v1/workflows",
        json={
            "name": "API Broken Workflow",
            "description": "api broken",
        },
    )
    workflow = create_response.json()

    client.put(
        f"/api/v1/workflows/{workflow['id']}/graph",
        json={
            "name": "API Broken Workflow",
            "description": "api broken",
            "nodes": [
                {
                    "id": "node_1",
                    "node_key": "preview_1",
                    "node_type": "dataset_preview",
                    "name": "Preview",
                    "config": {"limit": 10},
                }
            ],
            "edges": [],
        },
    )

    plan_response = client.get(f"/api/v1/workflows/{workflow['id']}/plan")

    assert plan_response.status_code == 400
    assert "缺少必需输入端口" in plan_response.json()["detail"]
