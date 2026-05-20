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
        json={"name": "execution workflow"},
    )
    assert response.status_code == 201
    return response.json()


def _create_dataset_input_node(
    dataset_id: str,
    workflow_id: str,
    config: dict[str, object] | None = None,
) -> dict[str, object]:
    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow_id}/nodes",
        json={
            "node_key": "load_dataset",
            "node_type": "dataset_input",
            "name": "Load Dataset",
            "config": config or {"source": "current_dataset"},
        },
    )
    assert response.status_code == 201
    return response.json()


def test_execute_dataset_input_node_returns_dataset_ref() -> None:
    """验证 dataset_input 节点可以按统一协议输出数据集引用。"""
    dataset_id = _upload_dataset()
    workflow = _create_workflow(dataset_id)
    node = _create_dataset_input_node(dataset_id, workflow["id"])

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/nodes/{node['id']}/execute",
        json={"input_values": {}, "metadata": {"trigger": "manual_debug"}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["workflow_id"] == workflow["id"]
    assert payload["node_id"] == node["id"]
    assert payload["node_type"] == "dataset_input"
    assert payload["output_values"]["dataset_ref"]["dataset_id"] == dataset_id
    assert payload["output_values"]["dataset_ref"]["file_name"] == "survey.csv"
    assert payload["artifacts"]["dataset_detail"]["id"] == dataset_id
    assert "已加载数据集" in payload["summary"]


def test_execute_dataset_input_node_rejects_non_empty_inputs() -> None:
    """验证起点节点不会错误接受上游输入。"""
    dataset_id = _upload_dataset()
    workflow = _create_workflow(dataset_id)
    node = _create_dataset_input_node(dataset_id, workflow["id"])

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/nodes/{node['id']}/execute",
        json={"input_values": {"dataset_ref": {"dataset_id": "other"}}},
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "数据输入节点作为起点节点，当前不接受上游输入。"
    }


def test_execute_dataset_input_node_rejects_unsupported_source() -> None:
    """验证未接入的数据源配置会被明确拦截。"""
    dataset_id = _upload_dataset()
    workflow = _create_workflow(dataset_id)
    node = _create_dataset_input_node(
        dataset_id,
        workflow["id"],
        config={"source": "external_file"},
    )

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/nodes/{node['id']}/execute",
        json={},
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "数据输入节点当前仅支持 source=current_dataset。"
    }
