from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import desc, func, select

from app.core.exceptions import DatasetNotFoundError, DatasetWorkflowNotFoundError
from app.db.session import session_scope
from app.models.dataset import DatasetRecordModel
from app.models.workflow import DatasetWorkflowModel, DatasetWorkflowVersionModel
from app.schemas.workflow import (
    DatasetWorkflowCreateRequest,
    DatasetWorkflowDetailResponse,
    DatasetWorkflowListResponse,
    DatasetWorkflowRecord,
    DatasetWorkflowResponse,
    DatasetWorkflowVersionCreateRequest,
    DatasetWorkflowVersionListResponse,
    DatasetWorkflowVersionRecord,
    DatasetWorkflowVersionResponse,
)


class DatasetWorkflowService:
    """封装数据集工作流与版本管理逻辑。"""

    def list_workflows(self, dataset_id: str) -> DatasetWorkflowListResponse:
        """返回指定数据集下的工作流列表。"""
        self._ensure_dataset_exists(dataset_id)

        with session_scope() as session:
            models = session.scalars(
                select(DatasetWorkflowModel)
                .where(DatasetWorkflowModel.dataset_id == dataset_id)
                .order_by(desc(DatasetWorkflowModel.updated_at))
            ).all()

        items = [self._to_workflow_response(self._to_workflow_record(model)) for model in models]
        return DatasetWorkflowListResponse(dataset_id=dataset_id, items=items, total=len(items))

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
        """返回指定工作流详情及其版本列表。"""
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
        """返回指定工作流下的版本列表。"""
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
        """为指定工作流创建一条新版本。"""
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
            status=payload.status,
            created_at=datetime.now(UTC),
        )
        self._save_workflow_version_record(version_record)
        self._touch_workflow(workflow)
        return self._to_workflow_version_response(version_record)

    def _touch_workflow(self, workflow: DatasetWorkflowRecord) -> None:
        """在创建版本后刷新工作流更新时间。"""
        refreshed = workflow.model_copy(update={"updated_at": datetime.now(UTC)})
        self._save_workflow_record(refreshed)

    def _ensure_dataset_exists(self, dataset_id: str) -> None:
        """校验数据集存在。"""
        with session_scope() as session:
            model = session.get(DatasetRecordModel, dataset_id)
            if model is None:
                raise DatasetNotFoundError("请求的数据集不存在。")

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

    def _save_workflow_version_record(self, record: DatasetWorkflowVersionRecord) -> None:
        """写入工作流版本记录。"""
        with session_scope() as session:
            session.add(self._to_workflow_version_model(record))

    def _to_workflow_record(self, model: DatasetWorkflowModel) -> DatasetWorkflowRecord:
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

    def _to_workflow_model(self, record: DatasetWorkflowRecord) -> DatasetWorkflowModel:
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

    def _to_workflow_response(self, record: DatasetWorkflowRecord) -> DatasetWorkflowResponse:
        """把工作流记录转换为接口响应结构。"""
        return DatasetWorkflowResponse(**record.model_dump())

    def _to_workflow_version_record(
        self,
        model: DatasetWorkflowVersionModel,
    ) -> DatasetWorkflowVersionRecord:
        """把工作流版本模型转换为领域记录。"""
        return DatasetWorkflowVersionRecord(
            id=model.id,
            workflow_id=model.workflow_id,
            version_number=model.version_number,
            description=model.description,
            status=model.status,
            created_at=model.created_at,
        )

    def _to_workflow_version_model(
        self,
        record: DatasetWorkflowVersionRecord,
    ) -> DatasetWorkflowVersionModel:
        """把工作流版本记录转换为数据库模型。"""
        return DatasetWorkflowVersionModel(
            id=record.id,
            workflow_id=record.workflow_id,
            version_number=record.version_number,
            description=record.description,
            status=record.status,
            created_at=record.created_at,
        )

    def _to_workflow_version_response(
        self,
        record: DatasetWorkflowVersionRecord,
    ) -> DatasetWorkflowVersionResponse:
        """把工作流版本记录转换为接口响应结构。"""
        return DatasetWorkflowVersionResponse(**record.model_dump())

    def _normalize_optional_text(self, value: str | None) -> str | None:
        """统一清洗可选文本。"""
        if value is None:
            return None

        normalized = value.strip()
        return normalized or None
