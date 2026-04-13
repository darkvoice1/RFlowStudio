from fastapi import APIRouter

from app.core.config import settings
from app.schemas.health import HealthCheckResponse

router = APIRouter()


@router.get("/health", response_model=HealthCheckResponse, summary="Health check")
def health_check() -> HealthCheckResponse:
    """返回服务存活状态，供前端和运维探测使用。"""
    return HealthCheckResponse(
        status="ok",
        app_name=settings.app_name,
        version=settings.app_version,
        environment=settings.app_env,
    )
