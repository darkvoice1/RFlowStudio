from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import desc, select

from app.core.exceptions import (
    WorkflowDefinitionNotFoundError,
    WorkflowNodeNotFoundError,
)
from app.db.session import session_scope
from app.models.workflow import (
    WorkflowDefinitionEdgeModel,
    WorkflowDefinitionModel,
    WorkflowDefinitionNodeModel,
)
from app.schemas.workflow_definition import (
    WorkflowDefinitionEdgePayload,
    WorkflowDefinitionEdgeRecord,
    WorkflowDefinitionEdgeResponse,
    WorkflowDefinitionListResponse,
    WorkflowDefinitionNodePayload,
    WorkflowDefinitionNodeRecord,
    WorkflowDefinitionNodeResponse,
    WorkflowDefinitionRecord,
    WorkflowDefinitionResponse,
)


class WorkflowDefinitionStore:
    """封装平台级工作流定义的数据库读写。"""

    def list_workflows(self) -> WorkflowDefinitionListResponse:
        with session_scope() as session:
            models = session.scalars(
                select(WorkflowDefinitionModel).order_by(
                    desc(WorkflowDefinitionModel.updated_at)
                )
            ).all()

        items = [self._to_workflow_response(self._to_workflow_record(model)) for model in models]
        return WorkflowDefinitionListResponse(items=items, total=len(items))

    def create_workflow(
        self,
        name: str,
        description: str | None,
    ) -> WorkflowDefinitionRecord:
        now = datetime.now(UTC)
        record = WorkflowDefinitionRecord(
            id=uuid4().hex,
            name=name.strip(),
            description=self._normalize_optional_text(description),
            status="draft",
            created_at=now,
            updated_at=now,
        )
        self.save_workflow_record(record)
        return record

    def get_workflow_record(self, workflow_id: str) -> WorkflowDefinitionRecord:
        with session_scope() as session:
            model = session.get(WorkflowDefinitionModel, workflow_id)
            if model is None:
                raise WorkflowDefinitionNotFoundError("请求的工作流不存在。")
        return self._to_workflow_record(model)

    def save_workflow_record(self, record: WorkflowDefinitionRecord) -> None:
        with session_scope() as session:
            existing_model = session.get(WorkflowDefinitionModel, record.id)
            if existing_model is None:
                session.add(self._to_workflow_model(record))
                return

            existing_model.name = record.name
            existing_model.description = record.description
            existing_model.status = record.status
            existing_model.created_at = record.created_at
            existing_model.updated_at = record.updated_at

    def list_node_records(self, workflow_id: str) -> list[WorkflowDefinitionNodeRecord]:
        with session_scope() as session:
            models = session.scalars(
                select(WorkflowDefinitionNodeModel)
                .where(WorkflowDefinitionNodeModel.workflow_id == workflow_id)
                .order_by(WorkflowDefinitionNodeModel.created_at)
            ).all()
        return [self._to_node_record(model) for model in models]

    def get_node_record(self, workflow_id: str, node_id: str) -> WorkflowDefinitionNodeRecord:
        with session_scope() as session:
            model = session.get(WorkflowDefinitionNodeModel, node_id)
            if model is None or model.workflow_id != workflow_id:
                raise WorkflowNodeNotFoundError("请求的工作流节点不存在。")
        return self._to_node_record(model)

    def save_node_record(self, record: WorkflowDefinitionNodeRecord) -> None:
        with session_scope() as session:
            session.add(self._to_node_model(record))

    def list_edge_records(self, workflow_id: str) -> list[WorkflowDefinitionEdgeRecord]:
        with session_scope() as session:
            models = session.scalars(
                select(WorkflowDefinitionEdgeModel)
                .where(WorkflowDefinitionEdgeModel.workflow_id == workflow_id)
                .order_by(WorkflowDefinitionEdgeModel.created_at)
            ).all()
        return [self._to_edge_record(model) for model in models]

    def save_edge_record(self, record: WorkflowDefinitionEdgeRecord) -> None:
        with session_scope() as session:
            session.add(self._to_edge_model(record))

    def replace_workflow_graph(
        self,
        workflow_id: str,
        nodes: list[WorkflowDefinitionNodePayload],
        edges: list[WorkflowDefinitionEdgePayload],
    ) -> tuple[list[WorkflowDefinitionNodeRecord], list[WorkflowDefinitionEdgeRecord]]:
        node_records = self._materialize_nodes(workflow_id, nodes)
        edge_records = self._materialize_edges(
            workflow_id,
            edges,
            {record.id for record in node_records},
        )

        with session_scope() as session:
            session.query(WorkflowDefinitionEdgeModel).filter(
                WorkflowDefinitionEdgeModel.workflow_id == workflow_id
            ).delete(synchronize_session=False)
            session.query(WorkflowDefinitionNodeModel).filter(
                WorkflowDefinitionNodeModel.workflow_id == workflow_id
            ).delete(synchronize_session=False)
            for record in node_records:
                session.add(self._to_node_model(record))
            for record in edge_records:
                session.add(self._to_edge_model(record))

        return node_records, edge_records

    def _materialize_nodes(
        self,
        workflow_id: str,
        nodes: list[WorkflowDefinitionNodePayload],
    ) -> list[WorkflowDefinitionNodeRecord]:
        now = datetime.now(UTC)
        records: list[WorkflowDefinitionNodeRecord] = []
        for node in nodes:
            records.append(
                WorkflowDefinitionNodeRecord(
                    id=node.id or uuid4().hex,
                    workflow_id=workflow_id,
                    node_key=node.node_key.strip(),
                    node_type=node.node_type.strip(),
                    name=node.name.strip(),
                    description=self._normalize_optional_text(node.description),
                    config=dict(node.config),
                    position_x=node.position_x,
                    position_y=node.position_y,
                    created_at=now,
                    updated_at=now,
                )
            )
        return records

    def _materialize_edges(
        self,
        workflow_id: str,
        edges: list[WorkflowDefinitionEdgePayload],
        node_ids: set[str],
    ) -> list[WorkflowDefinitionEdgeRecord]:
        now = datetime.now(UTC)
        records: list[WorkflowDefinitionEdgeRecord] = []
        for edge in edges:
            if edge.source_node_id not in node_ids or edge.target_node_id not in node_ids:
                raise WorkflowNodeNotFoundError("请求的工作流节点不存在。")
            records.append(
                WorkflowDefinitionEdgeRecord(
                    id=edge.id or uuid4().hex,
                    workflow_id=workflow_id,
                    edge_key=edge.edge_key.strip(),
                    source_node_id=edge.source_node_id.strip(),
                    target_node_id=edge.target_node_id.strip(),
                    source_port=self._normalize_optional_text(edge.source_port),
                    target_port=self._normalize_optional_text(edge.target_port),
                    config=dict(edge.config),
                    created_at=now,
                    updated_at=now,
                )
            )
        return records

    def _to_workflow_record(self, model: WorkflowDefinitionModel) -> WorkflowDefinitionRecord:
        return WorkflowDefinitionRecord(
            id=model.id,
            name=model.name,
            description=model.description,
            status=model.status,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

    def _to_workflow_model(self, record: WorkflowDefinitionRecord) -> WorkflowDefinitionModel:
        return WorkflowDefinitionModel(
            id=record.id,
            name=record.name,
            description=record.description,
            status=record.status,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    def _to_node_record(self, model: WorkflowDefinitionNodeModel) -> WorkflowDefinitionNodeRecord:
        return WorkflowDefinitionNodeRecord(
            id=model.id,
            workflow_id=model.workflow_id,
            node_key=model.node_key,
            node_type=model.node_type,
            name=model.name,
            description=model.description,
            config=model.config,
            position_x=model.position_x,
            position_y=model.position_y,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

    def _to_node_model(self, record: WorkflowDefinitionNodeRecord) -> WorkflowDefinitionNodeModel:
        return WorkflowDefinitionNodeModel(
            id=record.id,
            workflow_id=record.workflow_id,
            node_key=record.node_key,
            node_type=record.node_type,
            name=record.name,
            description=record.description,
            config=record.config,
            position_x=record.position_x,
            position_y=record.position_y,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    def _to_edge_record(self, model: WorkflowDefinitionEdgeModel) -> WorkflowDefinitionEdgeRecord:
        return WorkflowDefinitionEdgeRecord(
            id=model.id,
            workflow_id=model.workflow_id,
            edge_key=model.edge_key,
            source_node_id=model.source_node_id,
            target_node_id=model.target_node_id,
            source_port=model.source_port,
            target_port=model.target_port,
            config=model.config,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

    def _to_edge_model(self, record: WorkflowDefinitionEdgeRecord) -> WorkflowDefinitionEdgeModel:
        return WorkflowDefinitionEdgeModel(
            id=record.id,
            workflow_id=record.workflow_id,
            edge_key=record.edge_key,
            source_node_id=record.source_node_id,
            target_node_id=record.target_node_id,
            source_port=record.source_port,
            target_port=record.target_port,
            config=record.config,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    def _to_workflow_response(
        self,
        record: WorkflowDefinitionRecord,
    ) -> WorkflowDefinitionResponse:
        return WorkflowDefinitionResponse(**record.model_dump())

    def _normalize_optional_text(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None
