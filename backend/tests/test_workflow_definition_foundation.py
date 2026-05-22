from pydantic import ValidationError
from sqlalchemy import inspect

from app.db.session import get_engine
from app.schemas.workflow_definition import (
    WorkflowDefinitionGraphUpdateRequest,
    WorkflowDefinitionNodePayload,
)


def test_initialize_database_creates_workflow_definition_tables() -> None:
    """验证新后端已经注册工作流定义层的三张核心表。"""
    inspector = inspect(get_engine())
    table_names = set(inspector.get_table_names())

    assert "workflow_definitions" in table_names
    assert "workflow_definition_nodes" in table_names
    assert "workflow_definition_edges" in table_names


def test_workflow_graph_update_schema_accepts_graph_payload() -> None:
    """验证工作流保存请求可以完整表达图结构。"""
    payload = WorkflowDefinitionGraphUpdateRequest(
        name="dataset flow",
        description="minimal graph",
        nodes=[
            {
                "id": "node_1",
                "node_key": "input_1",
                "node_type": "dataset_input",
                "name": "Dataset Input",
                "config": {"source": "current_dataset"},
                "position_x": 120,
                "position_y": 240,
            },
            {
                "id": "node_2",
                "node_key": "preview_1",
                "node_type": "dataset_preview",
                "name": "Dataset Preview",
                "config": {"limit": 20},
                "position_x": 420,
                "position_y": 240,
            },
        ],
        edges=[
            {
                "id": "edge_1",
                "edge_key": "input_to_preview",
                "source_node_id": "node_1",
                "target_node_id": "node_2",
                "source_port": "dataset_ref",
                "target_port": "dataset_ref",
            }
        ],
    )

    assert payload.name == "dataset flow"
    assert len(payload.nodes) == 2
    assert payload.nodes[0].position_x == 120
    assert payload.edges[0].target_port == "dataset_ref"


def test_workflow_node_payload_rejects_blank_required_strings() -> None:
    """验证节点基础标识字段不会接受纯空白字符串。"""
    try:
        WorkflowDefinitionNodePayload(
            node_key="   ",
            node_type="dataset_input",
            name="Dataset Input",
        )
    except ValidationError:
        return

    raise AssertionError("WorkflowDefinitionNodePayload 应拒绝纯空白 node_key。")
