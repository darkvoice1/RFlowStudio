from sqlalchemy import desc, func, select

from app.core.exceptions import DatasetNotFoundError, DatasetWorkflowNotFoundError
from app.db.session import session_scope
from app.models.dataset import DatasetRecordModel
from app.models.workflow import (
    DatasetWorkflowEdgeModel,
    DatasetWorkflowModel,
    DatasetWorkflowNodeModel,
    DatasetWorkflowVersionModel,
)
from app.schemas.workflow import (
    DatasetWorkflowEdgeRecord,
    DatasetWorkflowNodeRecord,
    DatasetWorkflowRecord,
    DatasetWorkflowVersionRecord,
)


class DatasetWorkflowStore:
    """封装工作流领域相关的数据库读写和记录转换。"""

    def ensure_dataset_exists(self, dataset_id: str) -> None:
        """校验数据集存在。"""
        with session_scope() as session:
            model = session.get(DatasetRecordModel, dataset_id)
            if model is None:
                raise DatasetNotFoundError("请求的数据集不存在。")

    def list_workflow_records(self, dataset_id: str) -> list[DatasetWorkflowRecord]:
        """返回指定数据集下的工作流记录列表。"""
        with session_scope() as session:
            models = session.scalars(
                select(DatasetWorkflowModel)
                .where(DatasetWorkflowModel.dataset_id == dataset_id)
                .order_by(desc(DatasetWorkflowModel.updated_at))
            ).all()

        return [self._to_workflow_record(model) for model in models]

    def get_workflow_record(
        self,
        dataset_id: str,
        workflow_id: str,
    ) -> DatasetWorkflowRecord:
        """读取单个工作流记录。"""
        self.ensure_dataset_exists(dataset_id)

        with session_scope() as session:
            model = session.get(DatasetWorkflowModel, workflow_id)
            if model is None or model.dataset_id != dataset_id:
                raise DatasetWorkflowNotFoundError("请求的工作流不存在。")

        return self._to_workflow_record(model)

    def save_workflow_record(self, record: DatasetWorkflowRecord) -> None:
        """写入或更新工作流记录。"""
        with session_scope() as session:
            existing_model = session.get(DatasetWorkflowModel, record.id)
            if existing_model is None:
                session.add(self._to_workflow_model(record))
                return

            existing_model.name = record.name
            existing_model.description = record.description
            existing_model.status = record.status
            existing_model.created_at = record.created_at
            existing_model.updated_at = record.updated_at

    def list_workflow_version_records(
        self,
        workflow_id: str,
    ) -> list[DatasetWorkflowVersionRecord]:
        """返回指定工作流下的版本记录列表。"""
        with session_scope() as session:
            models = session.scalars(
                select(DatasetWorkflowVersionModel)
                .where(DatasetWorkflowVersionModel.workflow_id == workflow_id)
                .order_by(desc(DatasetWorkflowVersionModel.version_number))
            ).all()

        return [self._to_workflow_version_record(model) for model in models]

    def get_workflow_version_record(
        self,
        workflow_id: str,
        workflow_version_id: str,
    ) -> DatasetWorkflowVersionRecord:
        """读取单个工作流版本记录。"""
        with session_scope() as session:
            model = session.get(DatasetWorkflowVersionModel, workflow_version_id)
            if model is None or model.workflow_id != workflow_id:
                raise DatasetWorkflowNotFoundError("请求的工作流版本不存在。")

        return self._to_workflow_version_record(model)

    def get_next_workflow_version_number(self, workflow_id: str) -> int:
        """返回下一个可用的工作流版本号。"""
        with session_scope() as session:
            max_version = session.scalar(
                select(func.max(DatasetWorkflowVersionModel.version_number)).where(
                    DatasetWorkflowVersionModel.workflow_id == workflow_id
                )
            )

        return (max_version or 0) + 1

    def save_workflow_version_record(
        self,
        record: DatasetWorkflowVersionRecord,
    ) -> None:
        """写入工作流历史版本记录。"""
        with session_scope() as session:
            session.add(self._to_workflow_version_model(record))

    def list_workflow_node_records(
        self,
        workflow_id: str,
    ) -> list[DatasetWorkflowNodeRecord]:
        """返回指定工作流下的节点记录列表。"""
        with session_scope() as session:
            models = session.scalars(
                select(DatasetWorkflowNodeModel)
                .where(DatasetWorkflowNodeModel.workflow_id == workflow_id)
                .order_by(DatasetWorkflowNodeModel.created_at)
            ).all()

        return [self._to_workflow_node_record(model) for model in models]

    def get_workflow_node_record(
        self,
        workflow_id: str,
        node_id: str,
    ) -> DatasetWorkflowNodeRecord:
        """读取单个工作流节点记录。"""
        with session_scope() as session:
            model = session.get(DatasetWorkflowNodeModel, node_id)
            if model is None or model.workflow_id != workflow_id:
                raise DatasetWorkflowNotFoundError("请求的工作流节点不存在。")

        return self._to_workflow_node_record(model)

    def save_workflow_node_record(self, record: DatasetWorkflowNodeRecord) -> None:
        """写入工作流节点记录。"""
        with session_scope() as session:
            session.add(self._to_workflow_node_model(record))

    def list_workflow_edge_records(
        self,
        workflow_id: str,
    ) -> list[DatasetWorkflowEdgeRecord]:
        """返回指定工作流下的连线记录列表。"""
        with session_scope() as session:
            models = session.scalars(
                select(DatasetWorkflowEdgeModel)
                .where(DatasetWorkflowEdgeModel.workflow_id == workflow_id)
                .order_by(DatasetWorkflowEdgeModel.created_at)
            ).all()

        return [self._to_workflow_edge_record(model) for model in models]

    def save_workflow_edge_record(self, record: DatasetWorkflowEdgeRecord) -> None:
        """写入工作流连线记录。"""
        with session_scope() as session:
            session.add(self._to_workflow_edge_model(record))

    def ensure_nodes_belong_to_workflow(
        self,
        workflow_id: str,
        source_node_id: str,
        target_node_id: str,
    ) -> None:
        """校验连线两端节点都属于当前工作流。"""
        with session_scope() as session:
            source_node = session.get(DatasetWorkflowNodeModel, source_node_id.strip())
            target_node = session.get(DatasetWorkflowNodeModel, target_node_id.strip())
            if (
                source_node is None
                or target_node is None
                or source_node.workflow_id != workflow_id
                or target_node.workflow_id != workflow_id
            ):
                raise DatasetWorkflowNotFoundError("请求的工作流节点不存在。")

    def build_workflow_nodes_snapshot(self, workflow_id: str) -> list[dict[str, object]]:
        """生成当前工作流节点快照。"""
        return [
            record.model_dump(mode="json")
            for record in self.list_workflow_node_records(workflow_id)
        ]

    def build_workflow_edges_snapshot(self, workflow_id: str) -> list[dict[str, object]]:
        """生成当前工作流连线快照。"""
        return [
            record.model_dump(mode="json")
            for record in self.list_workflow_edge_records(workflow_id)
        ]

    def _to_workflow_record(
        self,
        model: DatasetWorkflowModel,
    ) -> DatasetWorkflowRecord:
        return DatasetWorkflowRecord(
            id=model.id,
            dataset_id=model.dataset_id,
            name=model.name,
            description=model.description,
            status=model.status,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

    def _to_workflow_model(
        self,
        record: DatasetWorkflowRecord,
    ) -> DatasetWorkflowModel:
        return DatasetWorkflowModel(
            id=record.id,
            dataset_id=record.dataset_id,
            name=record.name,
            description=record.description,
            status=record.status,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    def _to_workflow_version_record(
        self,
        model: DatasetWorkflowVersionModel,
    ) -> DatasetWorkflowVersionRecord:
        return DatasetWorkflowVersionRecord(
            id=model.id,
            workflow_id=model.workflow_id,
            version_number=model.version_number,
            description=model.description,
            nodes_snapshot=model.nodes_snapshot,
            edges_snapshot=model.edges_snapshot,
            created_at=model.created_at,
        )

    def _to_workflow_version_model(
        self,
        record: DatasetWorkflowVersionRecord,
    ) -> DatasetWorkflowVersionModel:
        return DatasetWorkflowVersionModel(
            id=record.id,
            workflow_id=record.workflow_id,
            version_number=record.version_number,
            description=record.description,
            nodes_snapshot=record.nodes_snapshot,
            edges_snapshot=record.edges_snapshot,
            created_at=record.created_at,
        )

    def _to_workflow_node_record(
        self,
        model: DatasetWorkflowNodeModel,
    ) -> DatasetWorkflowNodeRecord:
        return DatasetWorkflowNodeRecord(
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

    def _to_workflow_node_model(
        self,
        record: DatasetWorkflowNodeRecord,
    ) -> DatasetWorkflowNodeModel:
        return DatasetWorkflowNodeModel(
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

    def _to_workflow_edge_record(
        self,
        model: DatasetWorkflowEdgeModel,
    ) -> DatasetWorkflowEdgeRecord:
        return DatasetWorkflowEdgeRecord(
            id=model.id,
            workflow_id=model.workflow_id,
            edge_key=model.edge_key,
            source_node_id=model.source_node_id,
            target_node_id=model.target_node_id,
            source_handle=model.source_handle,
            target_handle=model.target_handle,
            config=model.config,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

    def _to_workflow_edge_model(
        self,
        record: DatasetWorkflowEdgeRecord,
    ) -> DatasetWorkflowEdgeModel:
        return DatasetWorkflowEdgeModel(
            id=record.id,
            workflow_id=record.workflow_id,
            edge_key=record.edge_key,
            source_node_id=record.source_node_id,
            target_node_id=record.target_node_id,
            source_handle=record.source_handle,
            target_handle=record.target_handle,
            config=record.config,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
