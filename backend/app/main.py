"""新后端应用入口。"""

from fastapi import FastAPI

from app.api.v1.health import router as health_router
from app.core.config import settings


def create_app() -> FastAPI:
    """创建新后端应用实例。"""
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.app_debug,
    )
    app.include_router(health_router, prefix=settings.api_v1_prefix)
    return app


app = create_app()
