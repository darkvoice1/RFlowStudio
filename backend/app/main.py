"""新后端应用入口。"""

from fastapi import FastAPI

from app.api.v1.health import router as health_router


def create_app() -> FastAPI:
    """创建新后端应用实例。"""
    app = FastAPI(title="RFlowStudio Backend", version="0.1.0")
    app.include_router(health_router, prefix="/api/v1")
    return app


app = create_app()
