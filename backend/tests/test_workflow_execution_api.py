from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_workflow_execution_api_can_run_two_node_workflow() -> None:
    """验证执行接口可以跑通最小两节点工作流。"""
    create_response = client.post(
        "/api/v1/workflows",
        json={
            "name": "API Run Workflow",
            "description": "api run",
        },
    )
    workflow = create_response.json()

    client.put(
        f"/api/v1/workflows/{workflow['id']}/graph",
        json={
            "name": "API Run Workflow",
            "description": "api run",
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
                    "config": {"limit": 2},
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

    run_response = client.post(
        f"/api/v1/workflows/{workflow['id']}/run",
        json={
            "node_inputs": {
                "node_1": {
                    "dataset_ref": {
                        "source": "api_dataset",
                    }
                }
            }
        },
    )

    assert run_response.status_code == 200
    assert run_response.json()["status"] == "succeeded"
    assert run_response.json()["succeeded_steps"] == 2
    assert run_response.json()["final_outputs"]["node_2"]["preview_table"]["limit"] == 2
