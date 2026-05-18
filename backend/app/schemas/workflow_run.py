from datetime import datetime

from pydantic import BaseModel


class WorkflowRunResponse(BaseModel):
    id: str
    workflow_id: str
    status: str
    trigger_mode: str
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime
