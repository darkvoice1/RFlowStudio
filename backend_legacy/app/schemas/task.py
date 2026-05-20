from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel


class TaskResponse(BaseModel):
    """定义异步任务状态接口的响应结构。"""

    id: str
    task_type: str
    status: Literal["pending", "running", "completed", "failed"]
    dataset_id: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error_message: str | None
    result: dict[str, Any] | None


class TaskListResponse(BaseModel):
    """定义异步任务列表接口的响应结构。"""

    items: list[TaskResponse]
    total: int
