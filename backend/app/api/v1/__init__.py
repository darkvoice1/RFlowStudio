"""V1 API 包。"""

from fastapi import APIRouter

from .health import router as health_router
from .workflow_definitions import router as workflow_definitions_router
from .workflow_nodes import router as workflow_nodes_router
from .workflow_plans import router as workflow_plans_router
from .workflow_runs import router as workflow_runs_router

router = APIRouter()
router.include_router(health_router)
router.include_router(workflow_definitions_router)
router.include_router(workflow_nodes_router)
router.include_router(workflow_plans_router)
router.include_router(workflow_runs_router)
