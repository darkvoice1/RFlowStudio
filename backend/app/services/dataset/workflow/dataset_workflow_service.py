from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import desc, func, select

from app.core.exceptions import (
    DatasetNotFoundError,
    DatasetWorkflowNotFoundError,
)
from app.db.session import session_scope
from app.models.dataset import DatasetRecordModel
from app.models.workflow import (
    DatasetWorkflowEdgeModel,
    DatasetWorkflowModel,
    DatasetWorkflowNodeModel,
    DatasetWorkflowVersionModel,
)
from app.schemas.workflow import (
    DatasetWorkflowCreateRequest,
    DatasetWorkflowDetailResponse,
    DatasetWorkflowEdgeCreateRequest,
    DatasetWorkflowEdgeListResponse,
    DatasetWorkflowEdgeRecord,
    DatasetWorkflowEdgeResponse,
    DatasetWorkflowListResponse,
    DatasetWorkflowNodeCreateRequest,
    DatasetWorkflowNodeListResponse,
    DatasetWorkflowNodeRecord,
    DatasetWorkflowNodeResponse,
    DatasetWorkflowRecord,
    DatasetWorkflowResponse,
    DatasetWorkflowVersionCreateRequest,
    DatasetWorkflowVersionListResponse,
    DatasetWorkflowVersionRecord,
    DatasetWorkflowVersionResponse,
)


class DatasetWorkflowService:
    """封装数据集工作流、当前编辑态与历史快照管理逻辑。"""

    def list_workflows(self, dataset_id: str) -> DatasetWorkflowListResponse:
        """返回指定数据集下的工作流列表。"""
        self._ensure_dataset_exists(dataset_id)

        with session_scope() as session:
            models = session.scalars(
                select(DatasetWorkflowModel)
                .where(DatasetWorkflowModel.dataset_id == dataset_id)
                .order_by(desc(DatasetWorkflowModel.updated_at))
            ).all()

        items = [
            self._to_workflow_response(self._to_workflow_record(model))
            for model in models
        ]
        return DatasetWorkflowListResponse(
            dataset_id=dataset_id,
            items=items,
            total=len(items),
        )

    def create_workflow(
        self,
        dataset_id: str,
        payload: DatasetWorkflowCreateRequest,
    ) -> DatasetWorkflowResponse:
        """为指定数据集创建一条工作流。"""
        self._ensure_dataset_exists(dataset_id)
        now = datetime.now(UTC)
        record = DatasetWorkflowRecord(
            id=uuid4().hex,
            dataset_id=dataset_id,
            name=payload.name.strip(),
            description=self._normalize_optional_text(payload.description),
            status="draft",
            created_at=now,
            updated_at=now,
        )
        self._save_workflow_record(record)
        return self._to_workflow_response(record)

    def get_workflow_detail(
        self,
        dataset_id: str,
        workflow_id: str,
    ) -> DatasetWorkflowDetailResponse:
        """返回指定工作流详情及其历史版本列表。"""
        workflow = self.get_workflow_record(dataset_id, workflow_id)
        versions = self.list_workflow_versions(dataset_id, workflow_id)
        return DatasetWorkflowDetailResponse(
            workflow=self._to_workflow_response(workflow),
            versions=versions.items,
        )

    def get_workflow_record(
        self,
        dataset_id: str,
        workflow_id: str,
    ) -> DatasetWorkflowRecord:
        """读取单个工作流记录。"""
        self._ensure_dataset_exists(dataset_id)

        with session_scope() as session:
            model = session.get(DatasetWorkflowModel, workflow_id)
            if model is None or model.dataset_id != dataset_id:
                raise DatasetWorkflowNotFoundError("请求的工作流不存在。")

        return self._to_workflow_record(model)

    def list_workflow_versions(
        self,
        dataset_id: str,
        workflow_id: str,
    ) -> DatasetWorkflowVersionListResponse:
        """返回指定工作流下的历史版本列表。"""
        self.get_workflow_record(dataset_id, workflow_id)

        with session_scope() as session:
            models = session.scalars(
                select(DatasetWorkflowVersionModel)
                .where(DatasetWorkflowVersionModel.workflow_id == workflow_id)
                .order_by(desc(DatasetWorkflowVersionModel.version_number))
            ).all()

        items = [
            self._to_workflow_version_response(
                self._to_workflow_version_record(model)
            )
            for model in models
        ]
        return DatasetWorkflowVersionListResponse(
            dataset_id=dataset_id,
            workflow_id=workflow_id,
            items=items,
            total=len(items),
        )

    def create_workflow_version(
        self,
        dataset_id: str,
        workflow_id: str,
        payload: DatasetWorkflowVersionCreateRequest,
    ) -> DatasetWorkflowVersionResponse:
        """把当前可编辑工作流保存为不可变历史快照。"""
        workflow = self.get_workflow_record(dataset_id, workflow_id)

        with session_scope() as session:
            max_version = session.scalar(
                select(func.max(DatasetWorkflowVersionModel.version_number)).where(
                    DatasetWorkflowVersionModel.workflow_id == workflow.id
                )
            )

        version_record = DatasetWorkflowVersionRecord(
            id=uuid4().hex,
            workflow_id=workflow.id,
            version_number=(max_version or 0) + 1,
            description=self._normalize_optional_text(payload.description),
            nodes_snapshot=self._build_workflow_nodes_snapshot(workflow.id),
            edges_snapshot=self._build_workflow_edges_snapshot(workflow.id),
            created_at=datetime.now(UTC),
        )
        self._save_workflow_version_record(version_record)
        self._touch_workflow(workflow)
        return self._to_workflow_version_response(version_record)

    def list_workflow_nodes(
        self,
        dataset_id: str,
        workflow_id: str,
    ) -> DatasetWorkflowNodeListResponse:
        """返回指定工作流当前可编辑节点列表。"""
        self.get_workflow_record(dataset_id, workflow_id)

        with session_scope() as session:
            models = session.scalars(
                select(DatasetWorkflowNodeModel)
                .where(DatasetWorkflowNodeModel.workflow_id == workflow_id)
                .order_by(DatasetWorkflowNodeModel.created_at)
            ).all()

        items = [
            self._to_workflow_node_response(self._to_workflow_node_record(model))
            for model in models
        ]
        return DatasetWorkflowNodeListResponse(
            dataset_id=dataset_id,
            workflow_id=workflow_id,
            items=items,
            total=len(items),
        )

    def create_workflow_node(
        self,
        dataset_id: str,
        workflow_id: str,
        payload: DatasetWorkflowNodeCreateRequest,
    ) -> DatasetWorkflowNodeResponse:
        """为指定工作流当前编辑态创建一个新节点。"""
        workflow = self.get_workflow_record(dataset_id, workflow_id)
        now = datetime.now(UTC)
        node_record = DatasetWorkflowNodeRecord(
            id=uuid4().hex,
            workflow_id=workflow_id,
            node_key=payload.node_key.strip(),
            node_type=payload.node_type.strip(),
            name=payload.name.strip(),
            description=self._normalize_optional_text(payload.description),
            config=dict(payload.config),
            position_x=payload.position_x,
            position_y=payload.position_y,
            created_at=now,
            updated_at=now,
        )
        self._save_workflow_node_record(node_record)
        self._touch_workflow(workflow)
        return self._to_workflow_node_response(node_record)

    def list_workflow_edges(
        self,
        dataset_id: str,
        workflow_id: str,
    ) -> DatasetWorkflowEdgeListResponse:
        """返回指定工作流当前可编辑连线列表。"""
        self.get_workflow_record(dataset_id, workflow_id)

        with session_scope() as session:
            models = session.scalars(
                select(DatasetWorkflowEdgeModel)
                .where(DatasetWorkflowEdgeModel.workflow_id == workflow_id)
                .order_by(DatasetWorkflowEdgeModel.created_at)
            ).all()

        items = [
            self._to_workflow_edge_response(self._to_workflow_edge_record(model))
            for model in models
        ]
        return DatasetWorkflowEdgeListResponse(
            dataset_id=dataset_id,
            workflow_id=workflow_id,
            items=items,
            total=len(items),
        )

    def create_workflow_edge(
        self,
        dataset_id: str,
        workflow_id: str,
        payload: DatasetWorkflowEdgeCreateRequest,
    ) -> DatasetWorkflowEdgeResponse:
        """为指定工作流当前编辑态创建一条节点连线。"""
        workflow = self.get_workflow_record(dataset_id, workflow_id)
        self._ensure_nodes_belong_to_workflow(
            workflow_id=workflow_id,
            source_node_id=payload.source_node_id,
            target_node_id=payload.target_node_id,
        )
        now = datetime.now(UTC)
        edge_record = DatasetWorkflowEdgeRecord(
            id=uuid4().hex,
            workflow_id=workflow_id,
            edge_key=payload.edge_key.strip(),
            source_node_id=payload.source_node_id.strip(),
            target_node_id=payload.target_node_id.strip(),
            source_handle=self._normalize_optional_text(payload.source_handle),
            target_handle=self._normalize_optional_text(payload.target_handle),
            config=dict(payload.config),
            created_at=now,
            updated_at=now,
        )
        self._save_workflow_edge_record(edge_record)
        self._touch_workflow(workflow)
        return self._to_workflow_edge_response(edge_record)

    def get_workflow_version_record(
        self,
        dataset_id: str,
        workflow_id: str,
        workflow_version_id: str,
    ) -> DatasetWorkflowVersionRecord:
        """读取单个工作流历史版本记录。"""
        self.get_workflow_record(dataset_id, workflow_id)

        with session_scope() as session:
            model = session.get(DatasetWorkflowVersionModel, workflow_version_id)
            if model is None or model.workflow_id != workflow_id:
                raise DatasetWorkflowNotFoundError("请求的工作流版本不存在。")

        return self._to_workflow_version_record(model)

    def _touch_workflow(self, workflow: DatasetWorkflowRecord) -> None:
        """刷新工作流更新时间。"""
        refreshed = workflow.model_copy(update={"updated_at": datetime.now(UTC)})
        self._save_workflow_record(refreshed)

    def _ensure_dataset_exists(self, dataset_id: str) -> None:
        """校验数据集存在。"""
        with session_scope() as session:
            model = session.get(DatasetRecordModel, dataset_id)
            if model is None:
                raise DatasetNotFoundError("请求的数据集不存在。")

    def _ensure_nodes_belong_to_workflow(
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

    def _build_workflow_nodes_snapshot(self, workflow_id: str) -> list[dict[str, object]]:
        """生成当前工作流节点快照。"""
        with session_scope() as session:
            models = session.scalars(
                select(DatasetWorkflowNodeModel)
                .where(DatasetWorkflowNodeModel.workflow_id == workflow_id)
                .order_by(DatasetWorkflowNodeModel.created_at)
            ).all()

        return [
            self._to_workflow_node_record(model).model_dump(mode="json")
            for model in models
        ]

    def _build_workflow_edges_snapshot(self, workflow_id: str) -> list[dict[str, object]]:
        """生成当前工作流连线快照。"""
        with session_scope() as session:
            models = session.scalars(
                select(DatasetWorkflowEdgeModel)
                .where(DatasetWorkflowEdgeModel.workflow_id == workflow_id)
                .order_by(DatasetWorkflowEdgeModel.created_at)
            ).all()

        return [
            self._to_workflow_edge_record(model).model_dump(mode="json")
            for model in models
        ]

    def _save_workflow_record(self, record: DatasetWorkflowRecord) -> None:
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

    def _save_workflow_version_record(
        self,
        record: DatasetWorkflowVersionRecord,
    ) -> None:
        """写入工作流历史版本记录。"""
        with session_scope() as session:
            session.add(self._to_workflow_version_model(record))

    def _save_workflow_node_record(
        self,
        record: DatasetWorkflowNodeRecord,
    ) -> None:
        """写入工作流节点记录。"""
        with session_scope() as session:
            session.add(self._to_workflow_node_model(record))

    def _save_workflow_edge_record(
        self,
        record: DatasetWorkflowEdgeRecord,
    ) -> None:
        """写入工作流连线记录。"""
        with session_scope() as session:
            session.add(self._to_workflow_edge_model(record))

    def _to_workflow_record(
        self,
        model: DatasetWorkflowModel,
    ) -> DatasetWorkflowRecord:
        """把工作流模型转换为领域记录。"""
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
        """把工作流记录转换为数据库模型。"""
        return DatasetWorkflowModel(
            id=record.id,
            dataset_id=record.dataset_id,
            name=record.name,
            description=record.description,
            status=record.status,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    def _to_workflow_response(
        self,
        record: DatasetWorkflowRecord,
    ) -> DatasetWorkflowResponse:
        """把工作流记录转换为接口响应结构。"""
        return DatasetWorkflowResponse(**record.model_dump())

    def _to_workflow_version_record(
        self,
        model: DatasetWorkflowVersionModel,
    ) -> DatasetWorkflowVersionRecord:
        """把工作流历史版本模型转换为领域记录。"""
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
        """把工作流历史版本记录转换为数据库模型。"""
        return DatasetWorkflowVersionModel(
            id=record.id,
            workflow_id=record.workflow_id,
            version_number=record.version_number,
            description=record.description,
            nodes_snapshot=record.nodes_snapshot,
            edges_snapshot=record.edges_snapshot,
            created_at=record.created_at,
        )

    def _to_workflow_version_response(
        self,
        record: DatasetWorkflowVersionRecord,
    ) -> DatasetWorkflowVersionResponse:
        """把工作流历史版本记录转换为接口响应结构。"""
        return DatasetWorkflowVersionResponse(**record.model_dump())

    def _to_workflow_node_record(
        self,
        model: DatasetWorkflowNodeModel,
    ) -> DatasetWorkflowNodeRecord:
        """把工作流节点模型转换为领域记录。"""
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
        """把工作流节点记录转换为数据库模型。"""
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

    def _to_workflow_node_response(
        self,
        record: DatasetWorkflowNodeRecord,
    ) -> DatasetWorkflowNodeResponse:
        """把工作流节点记录转换为接口响应结构。"""
        return DatasetWorkflowNodeResponse(**record.model_dump())

    def _to_workflow_edge_record(
        self,
        model: DatasetWorkflowEdgeModel,
    ) -> DatasetWorkflowEdgeRecord:
        """把工作流连线模型转换为领域记录。"""
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
        """把工作流连线记录转换为数据库模型。"""
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

    def _to_workflow_edge_response(
        self,
        record: DatasetWorkflowEdgeRecord,
    ) -> DatasetWorkflowEdgeResponse:
        """把工作流连线记录转换为接口响应结构。"""
        return DatasetWorkflowEdgeResponse(**record.model_dump())

    def _normalize_optional_text(self, value: str | None) -> str | None:
        """统一清洗可选文本。"""
        if value is None:
            return None

        normalized = value.strip()
        return normalized or None
