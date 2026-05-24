import json
from pathlib import Path

import pytest

from app.core.exceptions import ValidationError
from app.schemas.workflow_definition import (
    WorkflowDefinitionCreateRequest,
    WorkflowDefinitionEdgeRecord,
    WorkflowDefinitionNodeRecord,
)
from app.services.platform import PluginLoaderService, PluginRegistryService
from app.services.workflow import (
    WorkflowDefinitionService,
    WorkflowDefinitionWriter,
    WorkflowExecutionService,
    WorkflowNodeRegistryService,
)


def test_workflow_execution_service_can_build_two_node_plan() -> None:
    """验证执行计划服务可以生成两节点 DAG 的拓扑顺序。"""
    definition_service = WorkflowDefinitionService()
    writer = WorkflowDefinitionWriter()
    execution_service = WorkflowExecutionService()

    workflow = definition_service.create_workflow(
        WorkflowDefinitionCreateRequest(name="Plan Flow", description="plan test")
    )
    writer.replace_workflow_graph(
        workflow.id,
        nodes=[
            WorkflowDefinitionNodeRecord(
                id="node_input",
                workflow_id=workflow.id,
                node_key="input_1",
                node_type="dataset_input",
                name="Input",
                description=None,
                config={"source": "current_dataset"},
                position_x=0,
                position_y=0,
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            ),
            WorkflowDefinitionNodeRecord(
                id="node_preview",
                workflow_id=workflow.id,
                node_key="preview_1",
                node_type="dataset_preview",
                name="Preview",
                description=None,
                config={"limit": 10},
                position_x=320,
                position_y=0,
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            ),
        ],
        edges=[
            WorkflowDefinitionEdgeRecord(
                id="edge_1",
                workflow_id=workflow.id,
                edge_key="input_to_preview",
                source_node_id="node_input",
                target_node_id="node_preview",
                source_port="dataset_ref",
                target_port="dataset_ref",
                config={},
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            )
        ],
    )

    plan = execution_service.build_workflow_plan(workflow.id)

    assert plan.workflow_id == workflow.id
    assert plan.start_node_ids == ["node_input"]
    assert plan.ordered_node_ids == ["node_input", "node_preview"]
    assert plan.steps[1].depends_on_node_ids == ["node_input"]
    assert plan.steps[1].incoming_bindings[0].edge_key == "input_to_preview"


def test_workflow_execution_service_rejects_missing_required_input() -> None:
    """验证缺少必需输入端口的工作流无法生成执行计划。"""
    definition_service = WorkflowDefinitionService()
    writer = WorkflowDefinitionWriter()
    execution_service = WorkflowExecutionService()

    workflow = definition_service.create_workflow(
        WorkflowDefinitionCreateRequest(name="Broken Plan", description="missing input")
    )
    writer.replace_workflow_graph(
        workflow.id,
        nodes=[
            WorkflowDefinitionNodeRecord(
                id="node_preview",
                workflow_id=workflow.id,
                node_key="preview_1",
                node_type="dataset_preview",
                name="Preview",
                description=None,
                config={"limit": 10},
                position_x=0,
                position_y=0,
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            )
        ],
        edges=[],
    )

    with pytest.raises(ValidationError, match="缺少必需输入端口"):
        execution_service.build_workflow_plan(workflow.id)


def test_workflow_execution_service_rejects_port_type_mismatch(tmp_path: Path) -> None:
    """验证端口数据类型不兼容时会拒绝生成执行计划。"""
    node_registry_service = _build_registry_with_plugin_node(
        tmp_path,
        plugin_key="table_consumer",
        input_data_type="table",
        output_data_type="table",
    )
    definition_service = WorkflowDefinitionService()
    writer = WorkflowDefinitionWriter()
    execution_service = WorkflowExecutionService(
        node_registry_service=node_registry_service
    )

    workflow = definition_service.create_workflow(
        WorkflowDefinitionCreateRequest(name="Port Mismatch", description="bad port")
    )
    writer.replace_workflow_graph(
        workflow.id,
        nodes=[
            WorkflowDefinitionNodeRecord(
                id="node_input",
                workflow_id=workflow.id,
                node_key="input_1",
                node_type="dataset_input",
                name="Input",
                description=None,
                config={"source": "current_dataset"},
                position_x=0,
                position_y=0,
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            ),
            WorkflowDefinitionNodeRecord(
                id="node_table",
                workflow_id=workflow.id,
                node_key="table_1",
                node_type="table_consumer",
                name="Table Consumer",
                description=None,
                config={},
                position_x=320,
                position_y=0,
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            ),
        ],
        edges=[
            WorkflowDefinitionEdgeRecord(
                id="edge_1",
                workflow_id=workflow.id,
                edge_key="input_to_table",
                source_node_id="node_input",
                target_node_id="node_table",
                source_port="dataset_ref",
                target_port="table_input",
                config={},
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            )
        ],
    )

    with pytest.raises(ValidationError, match="端口类型不匹配"):
        execution_service.build_workflow_plan(workflow.id)


def test_workflow_execution_service_rejects_cycle(tmp_path: Path) -> None:
    """验证存在环的工作流图无法生成执行计划。"""
    node_registry_service = _build_registry_with_plugin_node(
        tmp_path,
        plugin_key="dataset_passthrough",
        input_data_type="dataset",
        output_data_type="dataset",
    )
    definition_service = WorkflowDefinitionService()
    writer = WorkflowDefinitionWriter()
    execution_service = WorkflowExecutionService(
        node_registry_service=node_registry_service
    )

    workflow = definition_service.create_workflow(
        WorkflowDefinitionCreateRequest(name="Cycle Plan", description="cycle")
    )
    writer.replace_workflow_graph(
        workflow.id,
        nodes=[
            WorkflowDefinitionNodeRecord(
                id="node_a",
                workflow_id=workflow.id,
                node_key="pass_a",
                node_type="dataset_passthrough",
                name="Pass A",
                description=None,
                config={},
                position_x=0,
                position_y=0,
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            ),
            WorkflowDefinitionNodeRecord(
                id="node_b",
                workflow_id=workflow.id,
                node_key="pass_b",
                node_type="dataset_passthrough",
                name="Pass B",
                description=None,
                config={},
                position_x=320,
                position_y=0,
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            ),
        ],
        edges=[
            WorkflowDefinitionEdgeRecord(
                id="edge_ab",
                workflow_id=workflow.id,
                edge_key="a_to_b",
                source_node_id="node_a",
                target_node_id="node_b",
                source_port="dataset_out",
                target_port="dataset_in",
                config={},
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            ),
            WorkflowDefinitionEdgeRecord(
                id="edge_ba",
                workflow_id=workflow.id,
                edge_key="b_to_a",
                source_node_id="node_b",
                target_node_id="node_a",
                source_port="dataset_out",
                target_port="dataset_in",
                config={},
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            ),
        ],
    )

    with pytest.raises(ValidationError, match="存在环"):
        execution_service.build_workflow_plan(workflow.id)


def _build_registry_with_plugin_node(
    tmp_path: Path,
    *,
    plugin_key: str,
    input_data_type: str,
    output_data_type: str,
) -> WorkflowNodeRegistryService:
    """构造带临时插件节点的节点注册中心。"""
    installed_root = tmp_path / "nodes" / "installed"
    node_dir = installed_root / plugin_key
    node_dir.mkdir(parents=True)
    (node_dir / "manifest.json").write_text(
        json.dumps(
            {
                "key": plugin_key,
                "name": plugin_key,
                "version": "1.0.0",
                "category": "transform",
                "executor": "executor.py",
                "executor_kind": "script",
                "input_schema": [
                    {
                        "key": "dataset_in" if input_data_type == "dataset" else "table_input",
                        "name": "Input",
                        "data_type": input_data_type,
                        "required": True,
                    }
                ],
                "output_schema": [
                    {
                        "key": "dataset_out" if output_data_type == "dataset" else "table_output",
                        "name": "Output",
                        "data_type": output_data_type,
                        "required": True,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    loader = PluginLoaderService(
        installed_root=installed_root,
        disabled_root=tmp_path / "nodes" / "disabled",
    )
    registry = PluginRegistryService(loader=loader)
    return WorkflowNodeRegistryService(plugin_registry_service=registry)
