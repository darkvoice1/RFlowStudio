from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.core.exceptions import ResourceNotFoundError
from app.schemas.workflow_definition import (
    WorkflowDefinitionCreateRequest,
    WorkflowDefinitionEdgeRecord,
    WorkflowDefinitionNodeRecord,
)
from app.services.workflow import (
    WorkflowDefinitionService,
    WorkflowDefinitionWriter,
)


def test_workflow_definition_service_can_create_and_list_workflows() -> None:
    """验证工作流定义服务能够创建并列出工作流。"""
    service = WorkflowDefinitionService()

    created = service.create_workflow(
        WorkflowDefinitionCreateRequest(
            name="  Dataset Flow  ",
            description="  first draft  ",
        )
    )
    listed = service.list_workflows()

    assert created.name == "Dataset Flow"
    assert created.description == "first draft"
    assert listed.total == 1
    assert listed.items[0].id == created.id


def test_workflow_definition_service_can_load_detail_with_nodes_and_edges() -> None:
    """验证工作流详情查询会聚合节点和连线。"""
    service = WorkflowDefinitionService()
    writer = WorkflowDefinitionWriter()
    workflow = service.create_workflow(
        WorkflowDefinitionCreateRequest(
            name="Graph Flow",
            description="detail check",
        )
    )
    now = datetime.now(UTC)
    source_node_id = uuid4().hex
    target_node_id = uuid4().hex

    writer.save_workflow_node(
        WorkflowDefinitionNodeRecord(
            id=source_node_id,
            workflow_id=workflow.id,
            node_key="input_1",
            node_type="dataset_input",
            name="Input",
            description=None,
            config={"source": "current_dataset"},
            position_x=120,
            position_y=200,
            created_at=now,
            updated_at=now,
        )
    )
    writer.save_workflow_node(
        WorkflowDefinitionNodeRecord(
            id=target_node_id,
            workflow_id=workflow.id,
            node_key="preview_1",
            node_type="dataset_preview",
            name="Preview",
            description=None,
            config={"limit": 20},
            position_x=420,
            position_y=200,
            created_at=now,
            updated_at=now,
        )
    )
    writer.save_workflow_edge(
        WorkflowDefinitionEdgeRecord(
            id=uuid4().hex,
            workflow_id=workflow.id,
            edge_key="input_to_preview",
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            source_port="dataset_ref",
            target_port="dataset_ref",
            config={},
            created_at=now,
            updated_at=now,
        )
    )

    detail = service.get_workflow_detail(workflow.id)

    assert detail.workflow.id == workflow.id
    assert len(detail.nodes) == 2
    assert len(detail.edges) == 1
    assert detail.nodes[0].node_key == "input_1"
    assert detail.edges[0].edge_key == "input_to_preview"


def test_workflow_definition_service_rejects_missing_workflow_detail() -> None:
    """验证查询不存在的工作流详情时会抛出资源不存在异常。"""
    service = WorkflowDefinitionService()

    with pytest.raises(ResourceNotFoundError):
        service.get_workflow_detail("missing-workflow-id")
