"""V1 API 包。"""

from fastapi import APIRouter

from .health import router as health_router
from .workflow_definitions import router as workflow_definitions_router

router = APIRouter()
router.include_router(health_router)
router.include_router(workflow_definitions_router)
