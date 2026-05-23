"""工作流定义层写入服务。"""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import delete

from app.db.session import session_scope
from app.models.workflow import (
    WorkflowDefinitionEdgeModel,
    WorkflowDefinitionModel,
    WorkflowDefinitionNodeModel,
)
from app.schemas.workflow_definition import (
    WorkflowDefinitionEdgeRecord,
    WorkflowDefinitionNodeRecord,
    WorkflowDefinitionRecord,
)


class WorkflowDefinitionWriter:
    """只负责工作流定义层的写入。"""

    def create_workflow(self, name: str, description: str | None) -> WorkflowDefinitionRecord:
        """创建一条新的工作流定义记录。"""
        now = datetime.now(UTC)
        record = WorkflowDefinitionRecord(
            id=uuid4().hex,
            name=name.strip(),
            description=self._normalize_optional_text(description),
            status="draft",
            created_at=now,
            updated_at=now,
        )

        with session_scope() as session:
            session.add(self._to_workflow_model(record))

        return record

    def update_workflow(self, record: WorkflowDefinitionRecord) -> None:
        """更新工作流基础信息。"""
        with session_scope() as session:
            model = session.get(WorkflowDefinitionModel, record.id)
            if model is None:
                return

            model.name = record.name
            model.description = record.description
            model.status = record.status
            model.created_at = record.created_at
            model.updated_at = record.updated_at

    def save_workflow_node(self, record: WorkflowDefinitionNodeRecord) -> None:
        """保存工作流节点记录。"""
        with session_scope() as session:
            session.add(self._to_node_model(record))

    def save_workflow_edge(self, record: WorkflowDefinitionEdgeRecord) -> None:
        """保存工作流连线记录。"""
        with session_scope() as session:
            session.add(self._to_edge_model(record))

    def replace_workflow_graph(
        self,
        workflow_id: str,
        nodes: list[WorkflowDefinitionNodeRecord],
        edges: list[WorkflowDefinitionEdgeRecord],
    ) -> None:
        """整批替换某条工作流下的节点和连线。"""
        with session_scope() as session:
            session.execute(
                delete(WorkflowDefinitionEdgeModel).where(
                    WorkflowDefinitionEdgeModel.workflow_id == workflow_id
                )
            )
            session.execute(
                delete(WorkflowDefinitionNodeModel).where(
                    WorkflowDefinitionNodeModel.workflow_id == workflow_id
                )
            )
            session.add_all([self._to_node_model(record) for record in nodes])
            session.add_all([self._to_edge_model(record) for record in edges])

    def _to_workflow_model(self, record: WorkflowDefinitionRecord) -> WorkflowDefinitionModel:
        """把统一记录对象转回 ORM 工作流模型。"""
        return WorkflowDefinitionModel(
            id=record.id,
            name=record.name,
            description=record.description,
            status=record.status,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    def _to_node_model(self, record: WorkflowDefinitionNodeRecord) -> WorkflowDefinitionNodeModel:
        """把统一记录对象转回 ORM 节点模型。"""
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

    def _to_edge_model(self, record: WorkflowDefinitionEdgeRecord) -> WorkflowDefinitionEdgeModel:
        """把统一记录对象转回 ORM 连线模型。"""
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

    def _normalize_optional_text(self, value: str | None) -> str | None:
        """清理可选文本，去掉首尾空白并保留空值。"""
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None
