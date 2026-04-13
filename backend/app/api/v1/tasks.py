from fastapi import APIRouter, HTTPException, status

from app.core.exceptions import TaskNotFoundError
from app.schemas.task import TaskResponse
from app.services.task_service import task_service

router = APIRouter(prefix="/tasks")


@router.get("/{task_id}", response_model=TaskResponse, summary="Get task status")
def get_task(task_id: str) -> TaskResponse:
    """按任务 ID 返回异步任务状态。"""
    try:
        return task_service.get_task(task_id)
    except TaskNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
