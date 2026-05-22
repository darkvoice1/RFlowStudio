"""协议模型包。"""

from app.schemas.workflow_definition import (
    WorkflowDefinitionCreateRequest,
    WorkflowDefinitionDetailResponse,
    WorkflowDefinitionEdgeListResponse,
    WorkflowDefinitionEdgePayload,
    WorkflowDefinitionEdgeRecord,
    WorkflowDefinitionEdgeResponse,
    WorkflowDefinitionGraphUpdateRequest,
    WorkflowDefinitionListResponse,
    WorkflowDefinitionNodeListResponse,
    WorkflowDefinitionNodePayload,
    WorkflowDefinitionNodeRecord,
    WorkflowDefinitionNodeResponse,
    WorkflowDefinitionRecord,
    WorkflowDefinitionResponse,
)

__all__ = [
    "WorkflowDefinitionCreateRequest",
    "WorkflowDefinitionGraphUpdateRequest",
    "WorkflowDefinitionNodePayload",
    "WorkflowDefinitionEdgePayload",
    "WorkflowDefinitionRecord",
    "WorkflowDefinitionNodeRecord",
    "WorkflowDefinitionEdgeRecord",
    "WorkflowDefinitionResponse",
    "WorkflowDefinitionListResponse",
    "WorkflowDefinitionNodeResponse",
    "WorkflowDefinitionNodeListResponse",
    "WorkflowDefinitionEdgeResponse",
    "WorkflowDefinitionEdgeListResponse",
    "WorkflowDefinitionDetailResponse",
]
