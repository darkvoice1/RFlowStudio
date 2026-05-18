from fastapi import APIRouter

from .analysis import router as analysis_router
from .base import router as base_router
from .cleaning import router as cleaning_router
from .workflows import router as workflows_router

router = APIRouter()
router.include_router(base_router, prefix="/datasets")
router.include_router(cleaning_router, prefix="/datasets")
router.include_router(analysis_router, prefix="/datasets")
router.include_router(workflows_router, prefix="/datasets")
