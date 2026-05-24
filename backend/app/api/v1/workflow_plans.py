"""工作流执行计划接口。"""

from fastapi import APIRouter, HTTPException, status

from app.core.exceptions import ResourceNotFoundError, ValidationError
from app.schemas.workflow_execution_plan import WorkflowExecutionPlanResponse
from app.services.workflow import WorkflowExecutionService

router = APIRouter(prefix="/workflows", tags=["workflow-plans"])
service = WorkflowExecutionService()


@router.get(
    "/{workflow_id}/plan",
    response_model=WorkflowExecutionPlanResponse,
    summary="生成并查询工作流执行计划",
)
def get_workflow_plan(workflow_id: str) -> WorkflowExecutionPlanResponse:
    """返回指定工作流图生成出的最小执行计划。"""
    try:
        return service.build_workflow_plan(workflow_id)
    except ResourceNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
