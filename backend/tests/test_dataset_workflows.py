from io import BytesIO

from fastapi.testclient import TestClient

from app.db.session import session_scope
from app.main import app
from app.models.workflow import (
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


def _create_workflow_version(dataset_id: str, workflow_id: str) -> dict[str, object]:
    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow_id}/versions",
        json={
            "description": "  initial published snapshot  ",
            "status": "published",
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


def test_create_workflow_versions_returns_incrementing_versions_in_reverse_order() -> None:
    """验证同一工作流下的版本号会递增，列表默认按最新版本倒序返回。"""
    dataset_id = _upload_dataset()
    workflow = _create_workflow(dataset_id)

    first_version = _create_workflow_version(dataset_id, workflow["id"])
    second_response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/versions",
        json={"description": "second draft"},
    )
    second_version = second_response.json()
    list_response = client.get(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/versions"
    )
    detail_response = client.get(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}"
    )

    with session_scope() as session:
        stored_first = session.get(DatasetWorkflowVersionModel, first_version["id"])
        stored_second = session.get(DatasetWorkflowVersionModel, second_version["id"])

    assert second_response.status_code == 201

    assert first_version["version_number"] == 1
    assert first_version["status"] == "published"
    assert first_version["description"] == "initial published snapshot"

    assert second_version["version_number"] == 2
    assert second_version["status"] == "draft"
    assert second_version["description"] == "second draft"

    assert stored_first is not None
    assert stored_first.workflow_id == workflow["id"]
    assert stored_first.version_number == 1
    assert stored_first.status == "published"

    assert stored_second is not None
    assert stored_second.workflow_id == workflow["id"]
    assert stored_second.version_number == 2
    assert stored_second.status == "draft"

    assert list_response.status_code == 200
    assert list_response.json()["total"] == 2
    assert [item["version_number"] for item in list_response.json()["items"]] == [2, 1]

    assert detail_response.status_code == 200
    assert [item["version_number"] for item in detail_response.json()["versions"]] == [2, 1]


def test_create_workflow_node_persists_and_is_scoped_to_version() -> None:
    """验证节点能写入指定版本，并且不会混到其他版本节点列表里。"""
    dataset_id = _upload_dataset()
    workflow = _create_workflow(dataset_id)
    first_version = _create_workflow_version(dataset_id, workflow["id"])
    second_version = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/versions",
        json={"description": "second version"},
    ).json()

    create_response = client.post(
        (
            f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}"
            f"/versions/{first_version['id']}/nodes"
        ),
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
    first_list_response = client.get(
        (
            f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}"
            f"/versions/{first_version['id']}/nodes"
        )
    )
    second_list_response = client.get(
        (
            f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}"
            f"/versions/{second_version['id']}/nodes"
        )
    )

    with session_scope() as session:
        stored_node = session.get(DatasetWorkflowNodeModel, node_payload["id"])

    assert create_response.status_code == 201
    assert node_payload["workflow_version_id"] == first_version["id"]
    assert node_payload["node_key"] == "load_dataset"
    assert node_payload["node_type"] == "dataset_input"
    assert node_payload["name"] == "Load Dataset"
    assert node_payload["description"] == "Read current dataset"
    assert node_payload["config"] == {"source": "dataset"}
    assert node_payload["position_x"] == 120
    assert node_payload["position_y"] == 240

    assert stored_node is not None
    assert stored_node.workflow_version_id == first_version["id"]
    assert stored_node.node_key == "load_dataset"
    assert stored_node.node_type == "dataset_input"
    assert stored_node.config == {"source": "dataset"}

    assert first_list_response.status_code == 200
    assert first_list_response.json()["total"] == 1
    assert first_list_response.json()["items"][0]["id"] == node_payload["id"]

    assert second_list_response.status_code == 200
    assert second_list_response.json() == {
        "dataset_id": dataset_id,
        "workflow_id": workflow["id"],
        "workflow_version_id": second_version["id"],
        "items": [],
        "total": 0,
    }


def test_dataset_workflow_endpoints_return_404_for_unknown_dataset() -> None:
    """验证不存在的数据集不会返回伪造的工作流结果。"""
    response = client.get("/api/v1/datasets/not-found/workflows")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的数据集不存在。",
    }


def test_create_workflow_version_returns_404_for_unknown_workflow() -> None:
    """验证不存在的工作流不会被创建出版本记录。"""
    dataset_id = _upload_dataset()

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/not-found/versions",
        json={"description": "invalid"},
    )

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的工作流不存在。",
    }


def test_create_workflow_node_returns_404_for_unknown_version() -> None:
    """验证不存在的工作流版本不会被创建节点。"""
    dataset_id = _upload_dataset()
    workflow = _create_workflow(dataset_id)

    response = client.post(
        f"/api/v1/datasets/{dataset_id}/workflows/{workflow['id']}/versions/not-found/nodes",
        json={
            "node_key": "load_dataset",
            "node_type": "dataset_input",
            "name": "Load Dataset",
            "config": {},
        },
    )

    assert response.status_code == 404
    assert response.json() == {
        "detail": "请求的工作流版本不存在。",
    }


