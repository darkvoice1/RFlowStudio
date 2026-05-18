from fastapi import APIRouter

from app.api.v1.datasets import router as datasets_router
from app.api.v1.health import router as health_router
from app.api.v1.plugins import router as plugins_router
from app.api.v1.tasks import router as tasks_router
from app.api.v1.workflow_runs import router as workflow_runs_router
from app.api.v1.workflows import router as workflows_router
from app.api.v1.workflow_nodes import router as workflow_nodes_router

router = APIRouter()
# 按功能聚合 v1 接口模块，方便后续继续追加数据集接口。
router.include_router(datasets_router, tags=["datasets"])
router.include_router(health_router, tags=["health"])
router.include_router(tasks_router, tags=["tasks"])
router.include_router(plugins_router, tags=["plugins"])
router.include_router(workflows_router, prefix="/workflows", tags=["workflows"])
router.include_router(workflow_runs_router, tags=["workflow-runs"])
router.include_router(workflow_nodes_router, tags=["workflow-nodes"])
