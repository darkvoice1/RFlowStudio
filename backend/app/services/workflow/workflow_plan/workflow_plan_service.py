"""工作流计划层应用服务。"""

from app.core.exceptions import ResourceNotFoundError, ValidationError
from app.schemas.workflow_definition import WorkflowDefinitionNodeRecord
from app.schemas.workflow_node import WorkflowNodeDefinitionResponse
from app.schemas.workflow_plan import WorkflowExecutionPlanResponse
from app.services.workflow.node_registry_service import WorkflowNodeRegistryService
from app.services.workflow.workflow_definition.workflow_definition_reader import (
    WorkflowDefinitionReader,
)
from app.services.workflow.workflow_plan.workflow_plan_builder import (
    WorkflowPlanBuilder,
)
from app.services.workflow.workflow_plan.workflow_plan_planner import (
    WorkflowPlanPlanner,
)
from app.services.workflow.workflow_plan.workflow_plan_validator import (
    WorkflowPlanValidator,
)


class WorkflowPlanService:
    """协调工作流计划层的最小业务动作。"""

    def __init__(
        self,
        reader: WorkflowDefinitionReader | None = None,
        node_registry_service: WorkflowNodeRegistryService | None = None,
        validator: WorkflowPlanValidator | None = None,
        planner: WorkflowPlanPlanner | None = None,
        builder: WorkflowPlanBuilder | None = None,
    ) -> None:
        self.reader = reader or WorkflowDefinitionReader()
        self.node_registry_service = node_registry_service or WorkflowNodeRegistryService()
        self.validator = validator or WorkflowPlanValidator()
        self.planner = planner or WorkflowPlanPlanner()
        self.builder = builder or WorkflowPlanBuilder()

    def build_workflow_plan(self, workflow_id: str) -> WorkflowExecutionPlanResponse:
        """读取指定工作流并生成稳定执行计划。"""
        workflow = self.reader.get_workflow(workflow_id)
        if workflow is None:
            raise ResourceNotFoundError("请求的工作流不存在。")

        nodes = self.reader.list_workflow_nodes(workflow_id)
        edges = self.reader.list_workflow_edges(workflow_id)
        if not nodes:
            raise ValidationError("当前工作流图没有可执行节点。")

        node_map = {node.id: node for node in nodes}
        node_index_map = {node.id: index for index, node in enumerate(nodes)}
        definition_map = self._build_definition_map(nodes)
        normalized_edges = self.validator.normalize_edges(node_map, definition_map, edges)
        self.validator.validate_required_inputs(nodes, definition_map, normalized_edges)

        ordered_node_ids = self.planner.build_topological_order(
            nodes,
            normalized_edges,
            node_index_map,
        )
        start_node_ids = self.planner.build_start_node_ids(
            nodes,
            normalized_edges,
            node_index_map,
        )
        return self.builder.build_plan_response(
            workflow_id=workflow.id,
            workflow_name=workflow.name,
            ordered_node_ids=ordered_node_ids,
            start_node_ids=start_node_ids,
            node_map=node_map,
            normalized_edges=normalized_edges,
        )

    def _build_definition_map(
        self,
        nodes: list[WorkflowDefinitionNodeRecord],
    ) -> dict[str, WorkflowNodeDefinitionResponse]:
        """为每个节点读取对应的节点定义。"""
        definition_map: dict[str, WorkflowNodeDefinitionResponse] = {}
        for node in nodes:
            try:
                definition_map[node.id] = self.node_registry_service.get_node_definition(
                    node.node_type
                )
            except ResourceNotFoundError as exc:
                self.validator.validate_registered_node_definition(node, exc)
        return definition_map
