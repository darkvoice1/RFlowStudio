"""工作流定义层应用服务。"""

from app.core.exceptions import ResourceNotFoundError
from app.schemas.workflow_definition import (
    WorkflowDefinitionCreateRequest,
    WorkflowDefinitionDetailResponse,
    WorkflowDefinitionEdgeResponse,
    WorkflowDefinitionListResponse,
    WorkflowDefinitionNodeResponse,
    WorkflowDefinitionResponse,
)
from app.services.workflow.workflow_definition.workflow_definition_reader import (
    WorkflowDefinitionReader,
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
    ) -> None:
        self.reader = reader or WorkflowDefinitionReader()
        self.writer = writer or WorkflowDefinitionWriter()

    def create_workflow(
        self,
        payload: WorkflowDefinitionCreateRequest,
    ) -> WorkflowDefinitionResponse:
        """创建一条新的工作流定义。"""
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
