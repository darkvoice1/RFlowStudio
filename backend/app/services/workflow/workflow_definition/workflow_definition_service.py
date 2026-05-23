"""工作流定义层应用服务。"""

from datetime import UTC, datetime

from app.core.exceptions import ResourceNotFoundError
from app.schemas.workflow_definition import (
    WorkflowDefinitionCreateRequest,
    WorkflowDefinitionDetailResponse,
    WorkflowDefinitionEdgeResponse,
    WorkflowDefinitionGraphUpdateRequest,
    WorkflowDefinitionListResponse,
    WorkflowDefinitionNodeResponse,
    WorkflowDefinitionResponse,
)
from app.services.workflow.workflow_definition.workflow_definition_builder import (
    WorkflowDefinitionBuilder,
)
from app.services.workflow.workflow_definition.workflow_definition_reader import (
    WorkflowDefinitionReader,
)
from app.services.workflow.workflow_definition.workflow_definition_validator import (
    WorkflowDefinitionValidator,
)
from app.services.workflow.workflow_definition.workflow_definition_writer import (
    WorkflowDefinitionWriter,
)


class WorkflowDefinitionService:
    """编排工作流定义层的最小业务动作。"""

    def __init__(
        self,
        reader: WorkflowDefinitionReader | None = None,
        writer: WorkflowDefinitionWriter | None = None,
        validator: WorkflowDefinitionValidator | None = None,
        builder: WorkflowDefinitionBuilder | None = None,
    ) -> None:
        self.reader = reader or WorkflowDefinitionReader()
        self.writer = writer or WorkflowDefinitionWriter()
        self.validator = validator or WorkflowDefinitionValidator()
        self.builder = builder or WorkflowDefinitionBuilder()

    def create_workflow(
        self,
        payload: WorkflowDefinitionCreateRequest,
    ) -> WorkflowDefinitionResponse:
        """创建一条新的工作流定义。"""
        existing_workflow = self.reader.get_workflow_by_name(payload.name.strip())
        self.validator.validate_workflow_name_uniqueness(existing_workflow)

        record = self.writer.create_workflow(payload.name, payload.description)
        return WorkflowDefinitionResponse(**record.model_dump())

    def list_workflows(self) -> WorkflowDefinitionListResponse:
        """返回工作流定义列表。"""
        return self.reader.list_workflows()

    def get_workflow_detail(self, workflow_id: str) -> WorkflowDefinitionDetailResponse:
        """返回工作流定义及其节点和连线。"""
        workflow = self.reader.get_workflow(workflow_id)
        if workflow is None:
            raise ResourceNotFoundError("请求的工作流不存在。")

        nodes = self.reader.list_workflow_nodes(workflow_id)
        edges = self.reader.list_workflow_edges(workflow_id)

        return WorkflowDefinitionDetailResponse(
            workflow=WorkflowDefinitionResponse(**workflow.model_dump()),
            nodes=[WorkflowDefinitionNodeResponse(**node.model_dump()) for node in nodes],
            edges=[WorkflowDefinitionEdgeResponse(**edge.model_dump()) for edge in edges],
        )

    def save_workflow_graph(
        self,
        workflow_id: str,
        payload: WorkflowDefinitionGraphUpdateRequest,
    ) -> WorkflowDefinitionDetailResponse:
        """保存整张工作流图，并返回最新详情。"""
        workflow = self.reader.get_workflow(workflow_id)
        if workflow is None:
            raise ResourceNotFoundError("请求的工作流不存在。")

        existing_workflow = self.reader.get_workflow_by_name(payload.name)
        self.validator.validate_workflow_name_uniqueness(existing_workflow, workflow_id)

        self.validator.validate_graph_payload(payload)
        nodes, edges = self.builder.build_graph_records(workflow_id, payload)
        node_ids = {node.id for node in nodes}
        self.validator.validate_edge_node_references(
            node_ids,
            [
                (edge.source_node_id, edge.target_node_id)
                for edge in edges
            ],
        )

        now = datetime.now(UTC)

        refreshed_workflow = workflow.model_copy(
            update={
                "name": payload.name,
                "description": payload.description,
                "updated_at": now,
            }
        )
        self.writer.replace_workflow_graph(workflow_id, nodes, edges)
        self.writer.update_workflow(refreshed_workflow)
        return self.get_workflow_detail(workflow_id)
