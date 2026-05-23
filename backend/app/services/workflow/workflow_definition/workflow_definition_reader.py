"""工作流定义层读取服务。"""

from sqlalchemy import desc, select

from app.db.session import session_scope
from app.models.workflow import (
    WorkflowDefinitionEdgeModel,
    WorkflowDefinitionModel,
    WorkflowDefinitionNodeModel,
)
from app.schemas.workflow_definition import (
    WorkflowDefinitionEdgeRecord,
    WorkflowDefinitionListResponse,
    WorkflowDefinitionNodeRecord,
    WorkflowDefinitionRecord,
    WorkflowDefinitionResponse,
)


class WorkflowDefinitionReader:
    """只负责工作流定义层的查询。"""

    def list_workflows(self) -> WorkflowDefinitionListResponse:
        """按更新时间倒序返回工作流列表。"""
        with session_scope() as session:
            models = session.scalars(
                select(WorkflowDefinitionModel).order_by(
                    desc(WorkflowDefinitionModel.updated_at)
                )
            ).all()

        items = [
            WorkflowDefinitionResponse(**self._to_workflow_record(model).model_dump())
            for model in models
        ]
        return WorkflowDefinitionListResponse(items=items, total=len(items))

    def get_workflow(self, workflow_id: str) -> WorkflowDefinitionRecord | None:
        """按主键读取单条工作流定义。"""
        with session_scope() as session:
            model = session.get(WorkflowDefinitionModel, workflow_id)

        if model is None:
            return None
        return self._to_workflow_record(model)

    def get_workflow_by_name(self, name: str) -> WorkflowDefinitionRecord | None:
        """按名称读取单条工作流定义。"""
        with session_scope() as session:
            model = session.scalar(
                select(WorkflowDefinitionModel).where(WorkflowDefinitionModel.name == name)
            )

        if model is None:
            return None
        return self._to_workflow_record(model)

    def list_workflow_nodes(self, workflow_id: str) -> list[WorkflowDefinitionNodeRecord]:
        """读取某条工作流下的全部节点。"""
        with session_scope() as session:
            models = session.scalars(
                select(WorkflowDefinitionNodeModel)
                .where(WorkflowDefinitionNodeModel.workflow_id == workflow_id)
                .order_by(WorkflowDefinitionNodeModel.created_at)
            ).all()

        return [self._to_node_record(model) for model in models]

    def list_workflow_edges(self, workflow_id: str) -> list[WorkflowDefinitionEdgeRecord]:
        """读取某条工作流下的全部连线。"""
        with session_scope() as session:
            models = session.scalars(
                select(WorkflowDefinitionEdgeModel)
                .where(WorkflowDefinitionEdgeModel.workflow_id == workflow_id)
                .order_by(WorkflowDefinitionEdgeModel.created_at)
            ).all()

        return [self._to_edge_record(model) for model in models]

    def _to_workflow_record(self, model: WorkflowDefinitionModel) -> WorkflowDefinitionRecord:
        """把 ORM 工作流模型转成统一记录对象。"""
        return WorkflowDefinitionRecord(
            id=model.id,
            name=model.name,
            description=model.description,
            status=model.status,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

    def _to_node_record(self, model: WorkflowDefinitionNodeModel) -> WorkflowDefinitionNodeRecord:
        """把 ORM 节点模型转成统一记录对象。"""
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

    def _to_edge_record(self, model: WorkflowDefinitionEdgeModel) -> WorkflowDefinitionEdgeRecord:
        """把 ORM 连线模型转成统一记录对象。"""
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
