import json
from pathlib import Path

from app.schemas.workflow_definition import (
    WorkflowDefinitionCreateRequest,
    WorkflowDefinitionEdgeRecord,
    WorkflowDefinitionNodeRecord,
)
from app.schemas.workflow_execution_run import WorkflowExecutionRunRequest
from app.services.platform import PluginLoaderService, PluginRegistryService
from app.services.workflow import (
    WorkflowDefinitionService,
    WorkflowDefinitionWriter,
    WorkflowExecutionService,
    WorkflowNodeRegistryService,
)
from app.services.workflow.workflow_execution import (
    WorkflowEngineService,
    WorkflowNodeExecutorService,
)
from app.services.workflow.workflow_plan import WorkflowPlanService


def test_workflow_execution_service_can_run_two_node_dag() -> None:
    """验证执行层可以按计划跑通最小两节点 DAG。"""
    definition_service = WorkflowDefinitionService()
    writer = WorkflowDefinitionWriter()
    execution_service = WorkflowExecutionService()

    workflow = definition_service.create_workflow(
        WorkflowDefinitionCreateRequest(name="Run Flow", description="run test")
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
                config={"offset": 1, "limit": 2},
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

    run = execution_service.execute_workflow(
        workflow.id,
        WorkflowExecutionRunRequest(
            node_inputs={
                "node_input": {
                    "dataset_ref": {
                        "source": "uploaded_csv",
                    }
                }
            }
        ),
    )

    assert run.status == "succeeded"
    assert run.succeeded_steps == 2
    assert run.steps[0].outputs[0].key == "dataset_ref"
    assert run.steps[0].outputs[0].payload_kind == "reference"
    assert run.steps[0].outputs[0].reference["source"] == "uploaded_csv"
    assert run.steps[1].inputs[0].key == "dataset_ref"
    assert run.steps[1].inputs[0].reference["source"] == "uploaded_csv"
    assert run.steps[1].outputs[0].key == "preview_table"
    assert run.steps[1].outputs[0].payload_kind == "value"
    assert run.steps[1].outputs[0].value["rows"][0]["row_index"] == 1
    assert run.final_outputs["node_preview"][0].value["limit"] == 2


def test_workflow_execution_service_reports_failed_plugin_node(tmp_path: Path) -> None:
    """验证执行层会读取插件清单中的执行器声明并返回明确错误。"""
    node_registry_service, plugin_registry_service = _build_registry_with_plugin_node(
        tmp_path
    )
    execution_service = WorkflowExecutionService(
        plan_service=WorkflowPlanService(node_registry_service=node_registry_service),
        node_registry_service=node_registry_service,
        engine=WorkflowEngineService(
            node_executor_service=WorkflowNodeExecutorService(
                plugin_registry_service=plugin_registry_service
            )
        ),
    )
    definition_service = WorkflowDefinitionService()
    writer = WorkflowDefinitionWriter()

    workflow = definition_service.create_workflow(
        WorkflowDefinitionCreateRequest(name="Plugin Run", description="plugin test")
    )
    writer.replace_workflow_graph(
        workflow.id,
        nodes=[
            WorkflowDefinitionNodeRecord(
                id="node_plugin",
                workflow_id=workflow.id,
                node_key="plugin_1",
                node_type="plugin_dataset_source",
                name="Plugin Source",
                description=None,
                config={},
                position_x=0,
                position_y=0,
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
            )
        ],
        edges=[],
    )

    run = execution_service.execute_workflow(workflow.id)

    assert run.status == "failed"
    assert run.failure is not None
    assert run.failure.node_id == "node_plugin"
    assert "执行器文件不存在" in run.failure.message
    assert "missing_executor.py" in run.failure.message


def _build_registry_with_plugin_node(
    tmp_path: Path,
 ) -> tuple[WorkflowNodeRegistryService, PluginRegistryService]:
    """构造带临时插件节点的节点注册中心。"""
    installed_root = tmp_path / "nodes" / "installed"
    node_dir = installed_root / "plugin_dataset_source"
    node_dir.mkdir(parents=True)
    (node_dir / "manifest.json").write_text(
        json.dumps(
            {
                "key": "plugin_dataset_source",
                "name": "Plugin Source",
                "version": "1.0.0",
                "category": "input",
                "executor": "missing_executor.py",
                "executor_kind": "script",
                "output_schema": [
                    {
                        "key": "dataset_ref",
                        "name": "数据引用",
                        "data_type": "dataset",
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
    plugin_registry_service = PluginRegistryService(loader=loader)
    return (
        WorkflowNodeRegistryService(plugin_registry_service=plugin_registry_service),
        plugin_registry_service,
    )
