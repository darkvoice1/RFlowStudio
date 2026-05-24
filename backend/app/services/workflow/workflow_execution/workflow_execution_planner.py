"""工作流执行层计划器。"""

from app.core.exceptions import ValidationError
from app.schemas.workflow_definition import WorkflowDefinitionNodeRecord
from app.services.workflow.workflow_execution.workflow_execution_builder import (
    WorkflowPlanEdgeContext,
)


class WorkflowExecutionPlanner:
    """负责生成拓扑顺序和起始节点集合。"""

    def build_topological_order(
        self,
        nodes: list[WorkflowDefinitionNodeRecord],
        normalized_edges: list[WorkflowPlanEdgeContext],
        node_index_map: dict[str, int],
    ) -> list[str]:
        """按稳定顺序生成拓扑执行队列。"""
        incoming_count = {node.id: 0 for node in nodes}
        outgoing_map: dict[str, list[WorkflowPlanEdgeContext]] = {node.id: [] for node in nodes}

        for edge in normalized_edges:
            incoming_count[edge.target_node_id] += 1
            outgoing_map[edge.source_node_id].append(edge)

        ready_node_ids = sorted(
            [node.id for node in nodes if incoming_count[node.id] == 0],
            key=node_index_map.__getitem__,
        )
        ordered_node_ids: list[str] = []

        while ready_node_ids:
            node_id = ready_node_ids.pop(0)
            ordered_node_ids.append(node_id)

            for edge in outgoing_map[node_id]:
                incoming_count[edge.target_node_id] -= 1
                if incoming_count[edge.target_node_id] == 0:
                    ready_node_ids.append(edge.target_node_id)
                    ready_node_ids.sort(key=node_index_map.__getitem__)

        if len(ordered_node_ids) != len(nodes):
            raise ValidationError("当前工作流图存在环，无法生成执行计划。")
        return ordered_node_ids

    def build_start_node_ids(
        self,
        nodes: list[WorkflowDefinitionNodeRecord],
        normalized_edges: list[WorkflowPlanEdgeContext],
        node_index_map: dict[str, int],
    ) -> list[str]:
        """返回全部起始节点 id。"""
        incoming_targets = {edge.target_node_id for edge in normalized_edges}
        start_node_ids = [node.id for node in nodes if node.id not in incoming_targets]
        return sorted(start_node_ids, key=node_index_map.__getitem__)
