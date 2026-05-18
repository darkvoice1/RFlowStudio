from datetime import UTC, datetime
from uuid import uuid4

from app.schemas.workflow_definition import (
    WorkflowDefinitionCreateRequest,
    WorkflowDefinitionDetailResponse,
    WorkflowDefinitionEdgePayload,
    WorkflowDefinitionEdgeListResponse,
    WorkflowDefinitionGraphUpdateRequest,
    WorkflowDefinitionEdgeRecord,
    WorkflowDefinitionEdgeResponse,
    WorkflowDefinitionListResponse,
    WorkflowDefinitionNodeListResponse,
    WorkflowDefinitionNodePayload,
    WorkflowDefinitionNodeRecord,
    WorkflowDefinitionNodeResponse,
    WorkflowDefinitionRecord,
    WorkflowDefinitionResponse,
)
from app.services.workflow.workflow_definition_store import WorkflowDefinitionStore
from app.services.workflow.workflow_graph_service import WorkflowGraphService


class WorkflowDefinitionService:
    """管理平台级工作流定义、节点和连线。"""

    def __init__(self) -> None:
        self.store = WorkflowDefinitionStore()
        self.graph_service = WorkflowGraphService()

    def list_workflows(self) -> WorkflowDefinitionListResponse:
        """返回平台内所有工作流定义。"""
        return self.store.list_workflows()

    def create_workflow(
        self,
        payload: WorkflowDefinitionCreateRequest,
    ) -> WorkflowDefinitionResponse:
        """创建一条平台级工作流定义。"""
        record = self.store.create_workflow(payload.name, payload.description)
        return self._to_workflow_response(record)

    def get_workflow_detail(
        self,
        workflow_id: str,
    ) -> WorkflowDefinitionDetailResponse:
        """返回工作流定义及其节点和连线。"""
        workflow = self.store.get_workflow_record(workflow_id)
        nodes = self.store.list_node_records(workflow_id)
        edges = self.store.list_edge_records(workflow_id)
        return WorkflowDefinitionDetailResponse(
            workflow=self._to_workflow_response(workflow),
            nodes=[self._to_node_response(record) for record in nodes],
            edges=[self._to_edge_response(record) for record in edges],
        )

    def list_workflow_nodes(
        self,
        workflow_id: str,
    ) -> WorkflowDefinitionNodeListResponse:
        """返回工作流节点列表。"""
        self.store.get_workflow_record(workflow_id)
        records = self.store.list_node_records(workflow_id)
        items = [self._to_node_response(record) for record in records]
        return WorkflowDefinitionNodeListResponse(
            workflow_id=workflow_id,
            items=items,
            total=len(items),
        )

    def create_workflow_node(
        self,
        workflow_id: str,
        payload: WorkflowDefinitionNodePayload,
    ) -> WorkflowDefinitionNodeResponse:
        """为工作流创建一个节点。"""
        workflow = self.store.get_workflow_record(workflow_id)
        normalized_node = self.graph_service.normalize_nodes([payload])[0]
        now = datetime.now(UTC)
        record = WorkflowDefinitionNodeRecord(
            id=normalized_node.id or uuid4().hex,
            workflow_id=workflow.id,
            node_key=normalized_node.node_key,
            node_type=normalized_node.node_type,
            name=normalized_node.name,
            description=normalized_node.description,
            config=dict(normalized_node.config),
            position_x=normalized_node.position_x,
            position_y=normalized_node.position_y,
            created_at=now,
            updated_at=now,
        )
        self.store.save_node_record(record)
        self._touch_workflow(workflow)
        return self._to_node_response(record)

    def list_workflow_edges(
        self,
        workflow_id: str,
    ) -> WorkflowDefinitionEdgeListResponse:
        """返回工作流边列表。"""
        self.store.get_workflow_record(workflow_id)
        records = self.store.list_edge_records(workflow_id)
        items = [self._to_edge_response(record) for record in records]
        return WorkflowDefinitionEdgeListResponse(
            workflow_id=workflow_id,
            items=items,
            total=len(items),
        )

    def create_workflow_edge(
        self,
        workflow_id: str,
        payload: WorkflowDefinitionEdgePayload,
    ) -> WorkflowDefinitionEdgeResponse:
        """为工作流创建一条边。"""
        workflow = self.store.get_workflow_record(workflow_id)
        source_node = self.store.get_node_record(workflow_id, payload.source_node_id)
        target_node = self.store.get_node_record(workflow_id, payload.target_node_id)
        normalized_edge = self.graph_service.normalize_edge(
            payload,
            node_map={
                source_node.id: WorkflowDefinitionNodePayload(
                    id=source_node.id,
                    node_key=source_node.node_key,
                    node_type=source_node.node_type,
                    name=source_node.name,
                    description=source_node.description,
                    config=source_node.config,
                    position_x=source_node.position_x,
                    position_y=source_node.position_y,
                ),
                target_node.id: WorkflowDefinitionNodePayload(
                    id=target_node.id,
                    node_key=target_node.node_key,
                    node_type=target_node.node_type,
                    name=target_node.name,
                    description=target_node.description,
                    config=target_node.config,
                    position_x=target_node.position_x,
                    position_y=target_node.position_y,
                ),
            },
        )
        now = datetime.now(UTC)
        record = WorkflowDefinitionEdgeRecord(
            id=normalized_edge.id or uuid4().hex,
            workflow_id=workflow.id,
            edge_key=normalized_edge.edge_key,
            source_node_id=normalized_edge.source_node_id,
            target_node_id=normalized_edge.target_node_id,
            source_port=normalized_edge.source_port,
            target_port=normalized_edge.target_port,
            config=dict(normalized_edge.config),
            created_at=now,
            updated_at=now,
        )
        self.store.save_edge_record(record)
        self._touch_workflow(workflow)
        return self._to_edge_response(record)

    def update_workflow_graph(
        self,
        workflow_id: str,
        payload: WorkflowDefinitionGraphUpdateRequest,
    ) -> WorkflowDefinitionDetailResponse:
        """替换整张工作流图，并返回最新详情。"""
        workflow = self.store.get_workflow_record(workflow_id)
        normalized_nodes, normalized_edges = self.graph_service.normalize_graph(
            payload.nodes,
            payload.edges,
        )
        nodes, edges = self.store.replace_workflow_graph(
            workflow_id=workflow.id,
            nodes=normalized_nodes,
            edges=normalized_edges,
        )
        refreshed_workflow = workflow.model_copy(
            update={
                "name": payload.name.strip(),
                "description": self._normalize_optional_text(payload.description),
                "updated_at": datetime.now(UTC),
            }
        )
        self.store.save_workflow_record(refreshed_workflow)
        return WorkflowDefinitionDetailResponse(
            workflow=self._to_workflow_response(refreshed_workflow),
            nodes=[self._to_node_response(record) for record in nodes],
            edges=[self._to_edge_response(record) for record in edges],
        )

    def _touch_workflow(self, workflow: WorkflowDefinitionRecord) -> None:
        refreshed = workflow.model_copy(update={"updated_at": datetime.now(UTC)})
        self.store.save_workflow_record(refreshed)

    def _to_workflow_response(
        self,
        record: WorkflowDefinitionRecord,
    ) -> WorkflowDefinitionResponse:
        return WorkflowDefinitionResponse(**record.model_dump())

    def _to_node_response(
        self,
        record: WorkflowDefinitionNodeRecord,
    ) -> WorkflowDefinitionNodeResponse:
        return WorkflowDefinitionNodeResponse(**record.model_dump())

    def _to_edge_response(
        self,
        record: WorkflowDefinitionEdgeRecord,
    ) -> WorkflowDefinitionEdgeResponse:
        return WorkflowDefinitionEdgeResponse(**record.model_dump())

    def _normalize_optional_text(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None
