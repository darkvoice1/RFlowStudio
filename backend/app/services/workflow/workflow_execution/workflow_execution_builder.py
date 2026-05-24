"""工作流执行计划构造器。"""

from __future__ import annotations

from dataclasses import dataclass

from app.schemas.workflow_definition import WorkflowDefinitionNodeRecord
from app.schemas.workflow_execution_plan import (
    WorkflowExecutionPlanEdgeBinding,
    WorkflowExecutionPlanResponse,
    WorkflowExecutionPlanStep,
)


@dataclass(frozen=True)
class WorkflowPlanEdgeContext:
    """保存执行计划阶段使用的规范化连线信息。"""

    edge_id: str
    edge_key: str
    source_node_id: str
    target_node_id: str
    source_port: str
    target_port: str


class WorkflowExecutionBuilder:
    """负责把拓扑顺序和连线信息组装成执行计划对象。"""

    def build_plan_response(
        self,
        *,
        workflow_id: str,
        workflow_name: str,
        ordered_node_ids: list[str],
        start_node_ids: list[str],
        node_map: dict[str, WorkflowDefinitionNodeRecord],
        normalized_edges: list[WorkflowPlanEdgeContext],
    ) -> WorkflowExecutionPlanResponse:
        """构造整张工作流图的执行计划响应。"""
        steps = self._build_plan_steps(
            ordered_node_ids=ordered_node_ids,
            node_map=node_map,
            normalized_edges=normalized_edges,
        )
        return WorkflowExecutionPlanResponse(
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            start_node_ids=start_node_ids,
            ordered_node_ids=ordered_node_ids,
            steps=steps,
            total=len(steps),
        )

    def _build_plan_steps(
        self,
        *,
        ordered_node_ids: list[str],
        node_map: dict[str, WorkflowDefinitionNodeRecord],
        normalized_edges: list[WorkflowPlanEdgeContext],
    ) -> list[WorkflowExecutionPlanStep]:
        """把拓扑顺序和端口绑定组装成执行步骤。"""
        edge_index_map = {edge.edge_id: index for index, edge in enumerate(normalized_edges)}
        steps: list[WorkflowExecutionPlanStep] = []
        for sequence, node_id in enumerate(ordered_node_ids, start=1):
            node = node_map[node_id]
            incoming_edges = sorted(
                [edge for edge in normalized_edges if edge.target_node_id == node_id],
                key=lambda edge: edge_index_map[edge.edge_id],
            )
            outgoing_edges = sorted(
                [edge for edge in normalized_edges if edge.source_node_id == node_id],
                key=lambda edge: edge_index_map[edge.edge_id],
            )

            steps.append(
                WorkflowExecutionPlanStep(
                    node_id=node.id,
                    node_key=node.node_key,
                    node_type=node.node_type,
                    node_name=node.name,
                    sequence=sequence,
                    depends_on_node_ids=[edge.source_node_id for edge in incoming_edges],
                    incoming_bindings=[
                        self._build_edge_binding(edge, node_map)
                        for edge in incoming_edges
                    ],
                    outgoing_bindings=[
                        self._build_edge_binding(edge, node_map)
                        for edge in outgoing_edges
                    ],
                )
            )
        return steps

    def _build_edge_binding(
        self,
        edge: WorkflowPlanEdgeContext,
        node_map: dict[str, WorkflowDefinitionNodeRecord],
    ) -> WorkflowExecutionPlanEdgeBinding:
        """把规范化连线转换成响应绑定对象。"""
        source_node = node_map[edge.source_node_id]
        target_node = node_map[edge.target_node_id]
        return WorkflowExecutionPlanEdgeBinding(
            edge_id=edge.edge_id,
            edge_key=edge.edge_key,
            source_node_id=edge.source_node_id,
            source_node_key=source_node.node_key,
            target_node_id=edge.target_node_id,
            target_node_key=target_node.node_key,
            source_port=edge.source_port,
            target_port=edge.target_port,
        )
