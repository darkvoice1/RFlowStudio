from datetime import UTC, datetime
from uuid import uuid4

from app.schemas.workflow import (
    DatasetWorkflowCreateRequest,
    DatasetWorkflowDetailResponse,
    DatasetWorkflowEdgeCreateRequest,
    DatasetWorkflowEdgeListResponse,
    DatasetWorkflowEdgeRecord,
    DatasetWorkflowEdgeResponse,
    DatasetWorkflowListResponse,
    DatasetWorkflowNodeCreateRequest,
    DatasetWorkflowNodeExecuteResponse,
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
from app.services.dataset.workflow.dataset_workflow_store import DatasetWorkflowStore
from app.services.workflow.node_registry_service import workflow_node_registry_service
from app.services.workflow.workflow_execution_service import WorkflowExecutionService


class DatasetWorkflowService:
    """封装数据集工作流、当前编辑态与历史快照管理逻辑。"""

    def __init__(self) -> None:
        self.node_registry_service = workflow_node_registry_service
        self.execution_service: WorkflowExecutionService | None = None
        self.store = DatasetWorkflowStore()

    def list_workflows(self, dataset_id: str) -> DatasetWorkflowListResponse:
        """返回指定数据集下的工作流列表。"""
        self.store.ensure_dataset_exists(dataset_id)
        records = self.store.list_workflow_records(dataset_id)
        items = [self._to_workflow_response(record) for record in records]
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
        self.store.ensure_dataset_exists(dataset_id)
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
        self.store.save_workflow_record(record)
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
        return self.store.get_workflow_record(dataset_id, workflow_id)

    def list_workflow_versions(
        self,
        dataset_id: str,
        workflow_id: str,
    ) -> DatasetWorkflowVersionListResponse:
        """返回指定工作流下的历史版本列表。"""
        self.get_workflow_record(dataset_id, workflow_id)
        records = self.store.list_workflow_version_records(workflow_id)
        items = [self._to_workflow_version_response(record) for record in records]
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
        version_record = DatasetWorkflowVersionRecord(
            id=uuid4().hex,
            workflow_id=workflow.id,
            version_number=self.store.get_next_workflow_version_number(workflow.id),
            description=self._normalize_optional_text(payload.description),
            nodes_snapshot=self.store.build_workflow_nodes_snapshot(workflow.id),
            edges_snapshot=self.store.build_workflow_edges_snapshot(workflow.id),
            created_at=datetime.now(UTC),
        )
        self.store.save_workflow_version_record(version_record)
        self._touch_workflow(workflow)
        return self._to_workflow_version_response(version_record)

    def list_workflow_nodes(
        self,
        dataset_id: str,
        workflow_id: str,
    ) -> DatasetWorkflowNodeListResponse:
        """返回指定工作流当前可编辑节点列表。"""
        self.get_workflow_record(dataset_id, workflow_id)
        records = self.store.list_workflow_node_records(workflow_id)
        items = [self._to_workflow_node_response(record) for record in records]
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
        node_definition = self.node_registry_service.validate_node_type(
            payload.node_type
        )
        record = DatasetWorkflowNodeRecord(
            id=uuid4().hex,
            workflow_id=workflow_id,
            node_key=payload.node_key.strip(),
            node_type=node_definition.key,
            name=payload.name.strip(),
            description=self._normalize_optional_text(payload.description),
            config=dict(payload.config),
            position_x=payload.position_x,
            position_y=payload.position_y,
            created_at=now,
            updated_at=now,
        )
        self.store.save_workflow_node_record(record)
        self._touch_workflow(workflow)
        return self._to_workflow_node_response(record)

    def list_workflow_edges(
        self,
        dataset_id: str,
        workflow_id: str,
    ) -> DatasetWorkflowEdgeListResponse:
        """返回指定工作流当前可编辑连线列表。"""
        self.get_workflow_record(dataset_id, workflow_id)
        records = self.store.list_workflow_edge_records(workflow_id)
        items = [self._to_workflow_edge_response(record) for record in records]
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
        self.store.ensure_nodes_belong_to_workflow(
            workflow_id=workflow_id,
            source_node_id=payload.source_node_id,
            target_node_id=payload.target_node_id,
        )
        now = datetime.now(UTC)
        record = DatasetWorkflowEdgeRecord(
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
        self.store.save_workflow_edge_record(record)
        self._touch_workflow(workflow)
        return self._to_workflow_edge_response(record)

    def execute_workflow_node(
        self,
        dataset_id: str,
        workflow_id: str,
        node_id: str,
        input_values: dict[str, object],
        metadata: dict[str, object],
    ) -> DatasetWorkflowNodeExecuteResponse:
        """执行当前工作流编辑态中的单个节点。"""
        self.get_workflow_record(dataset_id, workflow_id)
        node_record = self.get_workflow_node_record(dataset_id, workflow_id, node_id)
        if self.execution_service is None:
            raise RuntimeError("Workflow execution service has not been configured.")

        return self.execution_service.execute_node(
            dataset_id=dataset_id,
            workflow_id=workflow_id,
            node_record=node_record,
            input_values=input_values,
            metadata=metadata,
        )

    def get_workflow_version_record(
        self,
        dataset_id: str,
        workflow_id: str,
        workflow_version_id: str,
    ) -> DatasetWorkflowVersionRecord:
        """读取单个工作流历史版本记录。"""
        self.get_workflow_record(dataset_id, workflow_id)
        return self.store.get_workflow_version_record(workflow_id, workflow_version_id)

    def get_workflow_node_record(
        self,
        dataset_id: str,
        workflow_id: str,
        node_id: str,
    ) -> DatasetWorkflowNodeRecord:
        """读取单个工作流节点记录。"""
        self.get_workflow_record(dataset_id, workflow_id)
        return self.store.get_workflow_node_record(workflow_id, node_id)

    def _touch_workflow(self, workflow: DatasetWorkflowRecord) -> None:
        """刷新工作流更新时间。"""
        refreshed = workflow.model_copy(update={"updated_at": datetime.now(UTC)})
        self.store.save_workflow_record(refreshed)

    def _to_workflow_response(
        self,
        record: DatasetWorkflowRecord,
    ) -> DatasetWorkflowResponse:
        """把工作流记录转换为接口响应结构。"""
        return DatasetWorkflowResponse(**record.model_dump())

    def _to_workflow_version_response(
        self,
        record: DatasetWorkflowVersionRecord,
    ) -> DatasetWorkflowVersionResponse:
        """把工作流历史版本记录转换为接口响应结构。"""
        return DatasetWorkflowVersionResponse(**record.model_dump())

    def _to_workflow_node_response(
        self,
        record: DatasetWorkflowNodeRecord,
    ) -> DatasetWorkflowNodeResponse:
        """把工作流节点记录转换为接口响应结构。"""
        return DatasetWorkflowNodeResponse(**record.model_dump())

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
