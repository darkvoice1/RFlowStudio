"""工作流定义层记录构造器。"""

from datetime import UTC, datetime
from uuid import uuid4

from app.schemas.workflow_definition import (
    WorkflowDefinitionEdgeRecord,
    WorkflowDefinitionGraphUpdateRequest,
    WorkflowDefinitionNodeRecord,
)


class WorkflowDefinitionBuilder:
    """负责把图保存请求构造成记录对象。"""

    def build_graph_records(
        self,
        workflow_id: str,
        payload: WorkflowDefinitionGraphUpdateRequest,
    ) -> tuple[list[WorkflowDefinitionNodeRecord], list[WorkflowDefinitionEdgeRecord]]:
        """把整图保存请求转换成节点和连线记录。"""
        now = datetime.now(UTC)
        nodes = [
            WorkflowDefinitionNodeRecord(
                id=node.id or uuid4().hex,
                workflow_id=workflow_id,
                node_key=node.node_key,
                node_type=node.node_type,
                name=node.name,
                description=node.description,
                config=dict(node.config),
                position_x=node.position_x,
                position_y=node.position_y,
                created_at=now,
                updated_at=now,
            )
            for node in payload.nodes
        ]
        edges = [
            WorkflowDefinitionEdgeRecord(
                id=edge.id or uuid4().hex,
                workflow_id=workflow_id,
                edge_key=edge.edge_key,
                source_node_id=edge.source_node_id,
                target_node_id=edge.target_node_id,
                source_port=edge.source_port,
                target_port=edge.target_port,
                config=dict(edge.config),
                created_at=now,
                updated_at=now,
            )
            for edge in payload.edges
        ]
        return nodes, edges
