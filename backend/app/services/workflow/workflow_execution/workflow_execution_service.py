"""工作流执行层总协调服务。"""

from __future__ import annotations

from app.core.exceptions import ResourceNotFoundError, ValidationError
from app.schemas.workflow_execution_run import (
    WorkflowExecutionRunRequest,
    WorkflowExecutionRunResponse,
)
from app.schemas.workflow_node import WorkflowNodeDefinitionResponse
from app.services.workflow.node_registry_service import WorkflowNodeRegistryService
from app.services.workflow.workflow_definition.workflow_definition_reader import (
    WorkflowDefinitionReader,
)
from app.services.workflow.workflow_execution.workflow_engine_service import (
    WorkflowEngineService,
)
from app.services.workflow.workflow_plan import WorkflowPlanService


class WorkflowExecutionService:
    """协调工作流计划层与执行引擎，产出真实运行结果。"""

    def __init__(
        self,
        reader: WorkflowDefinitionReader | None = None,
        plan_service: WorkflowPlanService | None = None,
        node_registry_service: WorkflowNodeRegistryService | None = None,
        engine: WorkflowEngineService | None = None,
    ) -> None:
        self.reader = reader or WorkflowDefinitionReader()
        self.node_registry_service = node_registry_service or WorkflowNodeRegistryService()
        self.plan_service = plan_service or WorkflowPlanService(
            reader=self.reader,
            node_registry_service=self.node_registry_service,
        )
        self.engine = engine or WorkflowEngineService()

    def execute_workflow(
        self,
        workflow_id: str,
        payload: WorkflowExecutionRunRequest | None = None,
    ) -> WorkflowExecutionRunResponse:
        """执行指定工作流图，并返回最小运行结果。"""
        workflow = self.reader.get_workflow(workflow_id)
        if workflow is None:
            raise ResourceNotFoundError("请求的工作流不存在。")

        plan = self.plan_service.build_workflow_plan(workflow_id)
        node_map = {
            node.id: node
            for node in self.reader.list_workflow_nodes(workflow_id)
        }
        definition_map = self._build_definition_map(node_map.values())

        return self.engine.execute_plan(
            workflow=workflow,
            plan=plan,
            node_map=node_map,
            definition_map=definition_map,
            payload=payload or WorkflowExecutionRunRequest(),
        )

    def _build_definition_map(
        self,
        nodes,
    ) -> dict[str, WorkflowNodeDefinitionResponse]:
        """读取当前工作流中每个节点的目录定义。"""
        definition_map: dict[str, WorkflowNodeDefinitionResponse] = {}
        for node in nodes:
            try:
                definition_map[node.id] = self.node_registry_service.get_node_definition(
                    node.node_type
                )
            except ResourceNotFoundError as exc:
                raise ValidationError(
                    f"节点类型 {node.node_type} 未注册，无法执行当前工作流。"
                ) from exc
        return definition_map
