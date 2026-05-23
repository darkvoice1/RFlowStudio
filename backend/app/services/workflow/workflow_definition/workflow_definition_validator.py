"""工作流定义层校验器。"""

from app.core.exceptions import ValidationError
from app.schemas.workflow_definition import (
    WorkflowDefinitionEdgePayload,
    WorkflowDefinitionGraphUpdateRequest,
    WorkflowDefinitionNodePayload,
    WorkflowDefinitionRecord,
)


class WorkflowDefinitionValidator:
    """负责工作流定义层的基础业务校验。"""

    def validate_workflow_name_uniqueness(
        self,
        existing_workflow: WorkflowDefinitionRecord | None,
        current_workflow_id: str | None = None,
    ) -> None:
        """校验工作流名称是否与其他工作流冲突。"""
        if existing_workflow is None:
            return
        if current_workflow_id is not None and existing_workflow.id == current_workflow_id:
            return
        raise ValidationError("已存在同名工作流，请使用其他名称。")

    def validate_graph_payload(self, payload: WorkflowDefinitionGraphUpdateRequest) -> None:
        """执行整图保存前的最小基础校验。"""
        self._validate_duplicate_node_keys(payload.nodes)
        self._validate_duplicate_edge_keys(payload.edges)

    def validate_edge_node_references(
        self,
        node_ids: set[str],
        edge_node_ids: list[tuple[str, str]],
    ) -> None:
        """校验连线引用的节点是否都存在。"""
        for source_node_id, target_node_id in edge_node_ids:
            if source_node_id not in node_ids or target_node_id not in node_ids:
                raise ValidationError("存在连线引用了未定义的节点。")

    def _validate_duplicate_node_keys(self, nodes: list[WorkflowDefinitionNodePayload]) -> None:
        """校验节点 key 不重复。"""
        seen_keys: set[str] = set()
        for node in nodes:
            if node.node_key in seen_keys:
                raise ValidationError("存在重复的节点 key。")
            seen_keys.add(node.node_key)

    def _validate_duplicate_edge_keys(self, edges: list[WorkflowDefinitionEdgePayload]) -> None:
        """校验连线 key 不重复。"""
        seen_keys: set[str] = set()
        for edge in edges:
            if edge.edge_key in seen_keys:
                raise ValidationError("存在重复的连线 key。")
            seen_keys.add(edge.edge_key)
