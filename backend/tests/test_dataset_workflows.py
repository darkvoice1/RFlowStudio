from io import BytesIO

from fastapi.testclient import TestClient

from app.db.session import session_scope
from app.main import app
from app.models.workflow import (
    DatasetWorkflowEdgeModel,
    DatasetWorkflowModel,
    DatasetWorkflowNodeModel,
    DatasetWorkflowVersionModel,
)

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
            "name": "  baseline workflow  ",
            "description": "  first workflow draft  ",
        },
    )
    assert response.status_code == 201
    return response.json()


def _create_workflow_node(
    dataset_id: str,
    workflow_id: str,
    node_key: str,
) -> dict[str, object]:
    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow_id}/nodes",
        json={
            "node_key": node_key,
            "node_type": "dataset_input",
            "name": node_key.replace("_", " ").title(),
            "config": {"source": node_key},
        },
    )
    assert response.status_code == 201
    return response.json()


def _create_workflow_edge(
    dataset_id: str,
    workflow_id: str,
    source_node_id: str,
    target_node_id: str,
) -> dict[str, object]:
    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow_id}/edges",
        json={
            "edge_key": "load_to_analysis",
            "source_node_id": source_node_id,
            "target_node_id": target_node_id,
            "source_handle": "output",
            "target_handle": "input",
            "config": {"mode": "dataframe"},
        },
    )
    assert response.status_code == 201
    return response.json()


def test_list_dataset_workflows_returns_empty_for_new_dataset() -> None:
    """验证新上传数据集默认还没有任何工作流记录。"""
    dataset_id = _upload_dataset()

    response = client.get(f"/api/v1/datasets/{dataset_id}/workflows")

    assert response.status_code == 200
    assert response.json() == {
        "dataset_id": dataset_id,
        "items": [],
        "total": 0,
    }


def test_create_dataset_workflow_persists_and_can_be_queried() -> None:
    """验证创建工作流后，接口返回和数据库记录都与预期一致。"""
    dataset_id = _upload_dataset()

    payload = _create_workflow(dataset_id)
    detail_response = client.get(
        f"/api/v1/datasets/{dataset_id}/workflows/{payload['id']}"
    )

    with session_scope() as session:
        stored_workflow = session.get(DatasetWorkflowModel, payload["id"])

    assert payload["dataset_id"] == dataset_id
    assert payload["name"] == "baseline workflow"
    assert payload["description"] == "first workflow draft"
    assert payload["status"] == "draft"

    assert stored_workflow is not None
    assert stored_workflow.dataset_id == dataset_id
    assert stored_workflow.name == "baseline workflow"
    assert stored_workflow.description == "first workflow draft"
    assert stored_workflow.status == "draft"

    assert detail_response.status_code == 200
    assert detail_response.json()["workflow"]["id"] == payload["id"]
    assert detail_response.json()["versions"] == []


def test_create_workflow_node_persists_current_editable_state() -> None:
    """验证节点写入工作流当前编辑态，而不是写入某个历史版本。"""
    dataset_id = _upload_dataset()
    workflow = _create_workflow(dataset_id)

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/nodes",
        json={
            "node_key": "  load_dataset  ",
            "node_type": "  dataset_input  ",
            "name": "  Load Dataset  ",
            "description": "  Read current dataset  ",
            "config": {"source": "dataset"},
            "position_x": 120,
            "position_y": 240,
        },
    )
    node_payload = create_response.json()
    list_response = client.get(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/nodes"
    )

    with session_scope() as session:
        stored_node = session.get(DatasetWorkflowNodeModel, node_payload["id"])

    assert create_response.status_code == 201
    assert node_payload["workflow_id"] == workflow["id"]
    assert node_payload["node_key"] == "load_dataset"
    assert node_payload["node_type"] == "dataset_input"
    assert node_payload["name"] == "Load Dataset"
    assert node_payload["description"] == "Read current dataset"
    assert node_payload["config"] == {"source": "dataset"}
    assert node_payload["position_x"] == 120
    assert node_payload["position_y"] == 240

    assert stored_node is not None
    assert stored_node.workflow_id == workflow["id"]
    assert stored_node.node_key == "load_dataset"
    assert stored_node.node_type == "dataset_input"
    assert stored_node.config == {"source": "dataset"}

    assert list_response.status_code == 200
    assert list_response.json()["dataset_id"] == dataset_id
    assert list_response.json()["workflow_id"] == workflow["id"]
    assert list_response.json()["total"] == 1
    assert list_response.json()["items"][0]["id"] == node_payload["id"]


def test_create_workflow_edge_persists_current_editable_state() -> None:
    """验证连线写入工作流当前编辑态，并且能关联同一工作流下的节点。"""
    dataset_id = _upload_dataset()
    workflow = _create_workflow(dataset_id)
    source_node = _create_workflow_node(dataset_id, workflow["id"], "load_dataset")
    target_node = _create_workflow_node(dataset_id, workflow["id"], "analysis_node")

    create_response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/edges",
        json={
            "edge_key": "  load_to_analysis  ",
            "source_node_id": source_node["id"],
            "target_node_id": target_node["id"],
            "source_handle": "  output  ",
            "target_handle": "  input  ",
            "config": {"mode": "dataframe"},
        },
    )
    edge_payload = create_response.json()
    list_response = client.get(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/edges"
    )

    with session_scope() as session:
        stored_edge = session.get(DatasetWorkflowEdgeModel, edge_payload["id"])

    assert create_response.status_code == 201
    assert edge_payload["workflow_id"] == workflow["id"]
    assert edge_payload["edge_key"] == "load_to_analysis"
    assert edge_payload["source_node_id"] == source_node["id"]
    assert edge_payload["target_node_id"] == target_node["id"]
    assert edge_payload["source_handle"] == "output"
    assert edge_payload["target_handle"] == "input"
    assert edge_payload["config"] == {"mode": "dataframe"}

    assert stored_edge is not None
    assert stored_edge.workflow_id == workflow["id"]
    assert stored_edge.edge_key == "load_to_analysis"
    assert stored_edge.source_node_id == source_node["id"]
    assert stored_edge.target_node_id == target_node["id"]

    assert list_response.status_code == 200
    assert list_response.json()["dataset_id"] == dataset_id
    assert list_response.json()["workflow_id"] == workflow["id"]
    assert list_response.json()["total"] == 1
    assert list_response.json()["items"][0]["id"] == edge_payload["id"]


def test_save_workflow_version_creates_immutable_snapshot_from_current_state() -> None:
    """验证保存历史版本时会复制当前节点和连线快照。"""
    dataset_id = _upload_dataset()
    workflow = _create_workflow(dataset_id)
    source_node = _create_workflow_node(dataset_id, workflow["id"], "load_dataset")
    target_node = _create_workflow_node(dataset_id, workflow["id"], "analysis_node")
    edge = _create_workflow_edge(
        dataset_id,
        workflow["id"],
        source_node["id"],
        target_node["id"],
    )

    first_response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/versions",
        json={"description": "first stable version"},
    )
    first_version = first_response.json()
    _create_workflow_node(dataset_id, workflow["id"], "report_node")
    second_response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/versions",
        json={"description": "second stable version"},
    )
    second_version = second_response.json()
    list_response = client.get(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/versions"
    )

    with session_scope() as session:
        stored_first = session.get(DatasetWorkflowVersionModel, first_version["id"])
        stored_second = session.get(DatasetWorkflowVersionModel, second_version["id"])

    assert first_response.status_code == 201
    assert first_version["version_number"] == 1
    assert first_version["description"] == "first stable version"
    assert len(first_version["nodes_snapshot"]) == 2
    assert len(first_version["edges_snapshot"]) == 1
    assert first_version["nodes_snapshot"][0]["workflow_id"] == workflow["id"]
    assert first_version["edges_snapshot"][0]["id"] == edge["id"]

    assert second_response.status_code == 201
    assert second_version["version_number"] == 2
    assert len(second_version["nodes_snapshot"]) == 3

    assert stored_first is not None
    assert len(stored_first.nodes_snapshot) == 2
    assert len(stored_first.edges_snapshot) == 1
    assert stored_second is not None
    assert len(stored_second.nodes_snapshot) == 3

    assert list_response.status_code == 200
    assert list_response.json()["total"] == 2
    assert [item["version_number"] for item in list_response.json()["items"]] == [2, 1]


def test_create_workflow_edge_rejects_node_from_other_workflow() -> None:
    """验证连线两端节点必须属于同一个工作流当前编辑态。"""
    dataset_id = _upload_dataset()
    first_workflow = _create_workflow(dataset_id)
    second_workflow = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows",
        json={"name": "second workflow"},
    ).json()
    source_node = _create_workflow_node(dataset_id, first_workflow["id"], "load_dataset")
    target_node = _create_workflow_node(dataset_id, second_workflow["id"], "analysis_node")

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{first_workflow['id']}/edges",
        json={
            "edge_key": "invalid_cross_workflow_edge",
            "source_node_id": source_node["id"],
            "target_node_id": target_node["id"],
            "config": {},
        },
    )

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的工作流节点不存在。",
    }


def test_dataset_workflow_endpoints_return_404_for_unknown_dataset() -> None:
    """验证不存在的数据集不会返回伪造的工作流结果。"""
    response = client.get("/api/v1/datasets/not-found/workflows")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的数据集不存在。",
    }


def test_create_workflow_version_returns_404_for_unknown_workflow() -> None:
    """验证不存在的工作流不会被保存为历史版本。"""
    dataset_id = _upload_dataset()

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/not-found/versions",
        json={"description": "invalid"},
    )

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的工作流不存在。",
    }


def test_create_workflow_node_returns_404_for_unknown_workflow() -> None:
    """验证不存在的工作流不会被创建节点。"""
    dataset_id = _upload_dataset()

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/not-found/nodes",
        json={
            "node_key": "load_dataset",
            "node_type": "dataset_input",
            "name": "Load Dataset",
            "config": {},
        },
    )

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的工作流不存在。",
    }


def test_create_workflow_edge_returns_404_for_unknown_workflow() -> None:
    """验证不存在的工作流不会被创建连线。"""
    dataset_id = _upload_dataset()

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/not-found/edges",
        json={
            "edge_key": "load_to_analysis",
            "source_node_id": "source-node",
            "target_node_id": "target-node",
            "config": {},
        },
    )

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的工作流不存在。",
    }
