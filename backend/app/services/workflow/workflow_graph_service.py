from __future__ import annotations

from collections import deque
from uuid import uuid4

from app.core.exceptions import WorkflowDefinitionValidationError
from app.schemas.workflow_definition import (
    WorkflowDefinitionEdgePayload,
    WorkflowDefinitionNodePayload,
)
from app.schemas.workflow_node import WorkflowNodeDefinitionResponse
from app.services.workflow.node_registry_service import workflow_node_registry_service


class WorkflowGraphService:
    """Validate and normalize free-form workflow graphs."""

    def __init__(self) -> None:
        self.node_registry_service = workflow_node_registry_service

    def normalize_graph(
        self,
        nodes: list[WorkflowDefinitionNodePayload],
        edges: list[WorkflowDefinitionEdgePayload],
    ) -> tuple[list[WorkflowDefinitionNodePayload], list[WorkflowDefinitionEdgePayload]]:
        normalized_nodes = self.normalize_nodes(nodes)
        node_map = {node.id: node for node in normalized_nodes if node.id is not None}
        topology_edges = self._normalize_topology_edges(edges, node_map=node_map)
        self._ensure_acyclic(normalized_nodes, topology_edges)
        normalized_edges = self.normalize_edges(edges, node_map=node_map)
        return normalized_nodes, normalized_edges

    def normalize_nodes(
        self,
        nodes: list[WorkflowDefinitionNodePayload],
    ) -> list[WorkflowDefinitionNodePayload]:
        normalized_nodes: list[WorkflowDefinitionNodePayload] = []
        node_ids: set[str] = set()
        node_keys: set[str] = set()

        for node in nodes:
            normalized_id = (
                self._normalize_optional_identifier(node.id, "节点 id") or uuid4().hex
            )
            normalized_key = self._normalize_required_text(node.node_key, "节点 node_key")
            normalized_name = self._normalize_required_text(node.name, "节点名称")
            definition = self.node_registry_service.validate_node_type(node.node_type)

            self._ensure_unique(node_ids, normalized_id, f"节点 id {normalized_id} 重复。")
            self._ensure_unique(
                node_keys,
                normalized_key,
                f"节点 node_key {normalized_key} 重复。",
            )

            normalized_nodes.append(
                node.model_copy(
                    update={
                        "id": normalized_id,
                        "node_key": normalized_key,
                        "node_type": definition.key,
                        "name": normalized_name,
                        "description": self._normalize_optional_text(node.description),
                        "config": dict(node.config),
                    }
                )
            )

        return normalized_nodes

    def normalize_edge(
        self,
        edge: WorkflowDefinitionEdgePayload,
        *,
        node_map: dict[str, WorkflowDefinitionNodePayload],
    ) -> WorkflowDefinitionEdgePayload:
        normalized_id = self._normalize_optional_identifier(edge.id, "边 id")
        normalized_key = self._normalize_required_text(edge.edge_key, "边 edge_key")
        source_node_id = self._normalize_required_text(edge.source_node_id, "source_node_id")
        target_node_id = self._normalize_required_text(edge.target_node_id, "target_node_id")

        source_node = node_map.get(source_node_id)
        target_node = node_map.get(target_node_id)
        if source_node is None or target_node is None:
            raise WorkflowDefinitionValidationError("边连接的节点不存在于当前工作流图中。")

        source_definition = self.node_registry_service.get_node_definition(source_node.node_type)
        target_definition = self.node_registry_service.get_node_definition(target_node.node_type)

        source_port = self._normalize_port(
            edge.source_port,
            definition=source_definition,
            direction="output",
            node_name=source_node.name,
        )
        target_port = self._normalize_port(
            edge.target_port,
            definition=target_definition,
            direction="input",
            node_name=target_node.name,
        )

        return edge.model_copy(
            update={
                "id": normalized_id,
                "edge_key": normalized_key,
                "source_node_id": source_node_id,
                "target_node_id": target_node_id,
                "source_port": source_port,
                "target_port": target_port,
                "config": dict(edge.config),
            }
        )

    def normalize_edges(
        self,
        edges: list[WorkflowDefinitionEdgePayload],
        *,
        node_map: dict[str, WorkflowDefinitionNodePayload],
    ) -> list[WorkflowDefinitionEdgePayload]:
        normalized_edges: list[WorkflowDefinitionEdgePayload] = []
        edge_ids: set[str] = set()
        edge_keys: set[str] = set()

        for edge in edges:
            normalized_edge = self.normalize_edge(edge, node_map=node_map)

            if normalized_edge.id is not None:
                self._ensure_unique(
                    edge_ids,
                    normalized_edge.id,
                    f"边 id {normalized_edge.id} 重复。",
                )
            self._ensure_unique(
                edge_keys,
                normalized_edge.edge_key,
                f"边 edge_key {normalized_edge.edge_key} 重复。",
            )
            normalized_edges.append(normalized_edge)

        return normalized_edges

    def _normalize_topology_edges(
        self,
        edges: list[WorkflowDefinitionEdgePayload],
        *,
        node_map: dict[str, WorkflowDefinitionNodePayload],
    ) -> list[WorkflowDefinitionEdgePayload]:
        normalized_edges: list[WorkflowDefinitionEdgePayload] = []
        edge_ids: set[str] = set()
        edge_keys: set[str] = set()

        for edge in edges:
            normalized_id = self._normalize_optional_identifier(edge.id, "边 id")
            normalized_key = self._normalize_required_text(edge.edge_key, "边 edge_key")
            source_node_id = self._normalize_required_text(edge.source_node_id, "source_node_id")
            target_node_id = self._normalize_required_text(edge.target_node_id, "target_node_id")

            if source_node_id not in node_map or target_node_id not in node_map:
                raise WorkflowDefinitionValidationError("边连接的节点不存在于当前工作流图中。")

            if normalized_id is not None:
                self._ensure_unique(edge_ids, normalized_id, f"边 id {normalized_id} 重复。")
            self._ensure_unique(
                edge_keys,
                normalized_key,
                f"边 edge_key {normalized_key} 重复。",
            )
            normalized_edges.append(
                edge.model_copy(
                    update={
                        "id": normalized_id,
                        "edge_key": normalized_key,
                        "source_node_id": source_node_id,
                        "target_node_id": target_node_id,
                        "config": dict(edge.config),
                    }
                )
            )

        return normalized_edges

    def _normalize_port(
        self,
        port_value: str | None,
        *,
        definition: WorkflowNodeDefinitionResponse,
        direction: str,
        node_name: str,
    ) -> str | None:
        schema = (
            definition.output_schema if direction == "output" else definition.input_schema
        )
        normalized_port = self._normalize_optional_text(port_value)

        if not schema:
            if normalized_port is not None:
                raise WorkflowDefinitionValidationError(
                    f"节点 {node_name} 没有可用的 {direction} 端口。"
                )
            return None

        if normalized_port is None:
            if len(schema) == 1:
                return schema[0].key
            raise WorkflowDefinitionValidationError(
                f"节点 {node_name} 存在多个 {direction} 端口，必须明确指定连接端口。"
            )

        for port in schema:
            if port.key == normalized_port:
                return normalized_port

        raise WorkflowDefinitionValidationError(
            f"节点 {node_name} 不存在 {direction} 端口 {normalized_port}。"
        )

    def _ensure_acyclic(
        self,
        nodes: list[WorkflowDefinitionNodePayload],
        edges: list[WorkflowDefinitionEdgePayload],
    ) -> None:
        node_ids = [node.id for node in nodes if node.id is not None]
        indegree = {node_id: 0 for node_id in node_ids}
        adjacency = {node_id: [] for node_id in node_ids}

        for edge in edges:
            source_node_id = edge.source_node_id
            target_node_id = edge.target_node_id
            adjacency[source_node_id].append(target_node_id)
            indegree[target_node_id] += 1

        queue = deque(
            node_id for node_id, degree in indegree.items() if degree == 0
        )
        visited_count = 0
        while queue:
            node_id = queue.popleft()
            visited_count += 1
            for target_node_id in adjacency[node_id]:
                indegree[target_node_id] -= 1
                if indegree[target_node_id] == 0:
                    queue.append(target_node_id)

        if visited_count != len(node_ids):
            raise WorkflowDefinitionValidationError("当前工作流图存在环，无法按依赖顺序执行。")

    def _normalize_required_text(self, value: str, field_name: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise WorkflowDefinitionValidationError(f"{field_name} 不能为空。")
        return normalized

    def _normalize_optional_identifier(
        self,
        value: str | None,
        field_name: str,
    ) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise WorkflowDefinitionValidationError(f"{field_name} 不能为空字符串。")
        return normalized

    def _normalize_optional_text(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    def _ensure_unique(self, existing: set[str], value: str, error_message: str) -> None:
        if value in existing:
            raise WorkflowDefinitionValidationError(error_message)
        existing.add(value)
