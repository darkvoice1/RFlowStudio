"""工作流执行计划协议模型。"""

from pydantic import Field

from app.schemas.workflow_definition import WorkflowSchemaModel


class WorkflowExecutionPlanEdgeBinding(WorkflowSchemaModel):
    """描述一条连线在执行计划中的绑定关系。"""

    edge_id: str = Field(min_length=1, max_length=32)
    edge_key: str = Field(min_length=1, max_length=64)
    source_node_id: str = Field(min_length=1, max_length=32)
    source_node_key: str = Field(min_length=1, max_length=64)
    target_node_id: str = Field(min_length=1, max_length=32)
    target_node_key: str = Field(min_length=1, max_length=64)
    source_port: str = Field(min_length=1, max_length=64)
    target_port: str = Field(min_length=1, max_length=64)


class WorkflowExecutionPlanStep(WorkflowSchemaModel):
    """描述单个节点的最小调度步骤。"""

    node_id: str = Field(min_length=1, max_length=32)
    node_key: str = Field(min_length=1, max_length=64)
    node_type: str = Field(min_length=1, max_length=64)
    node_name: str = Field(min_length=1, max_length=255)
    sequence: int = Field(ge=1)
    depends_on_node_ids: list[str] = Field(default_factory=list)
    incoming_bindings: list[WorkflowExecutionPlanEdgeBinding] = Field(default_factory=list)
    outgoing_bindings: list[WorkflowExecutionPlanEdgeBinding] = Field(default_factory=list)


class WorkflowExecutionPlanResponse(WorkflowSchemaModel):
    """描述整张工作流图转换出的执行计划。"""

    workflow_id: str = Field(min_length=1, max_length=32)
    workflow_name: str = Field(min_length=1, max_length=255)
    start_node_ids: list[str] = Field(default_factory=list)
    ordered_node_ids: list[str] = Field(default_factory=list)
    steps: list[WorkflowExecutionPlanStep] = Field(default_factory=list)
    total: int = Field(ge=0)
