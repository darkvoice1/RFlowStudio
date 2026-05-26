"""工作流执行接口。"""

from fastapi import APIRouter, HTTPException, status

from app.core.exceptions import ResourceNotFoundError, ValidationError
from app.schemas.workflow_execution_run import (
    WorkflowExecutionRunRequest,
    WorkflowExecutionRunResponse,
)
from app.services.workflow import WorkflowExecutionService

router = APIRouter(prefix="/workflows", tags=["workflow-runs"])
service = WorkflowExecutionService()


@router.post(
    "/{workflow_id}/run",
    response_model=WorkflowExecutionRunResponse,
    summary="执行一张最小工作流图",
)
def run_workflow(
    workflow_id: str,
    payload: WorkflowExecutionRunRequest,
) -> WorkflowExecutionRunResponse:
    """按图结构执行指定工作流。"""
    try:
        return service.execute_workflow(workflow_id, payload)
    except ResourceNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
