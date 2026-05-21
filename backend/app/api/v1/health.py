"""健康检查接口。"""

from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
def health_check() -> dict[str, str]:
    """返回最小健康状态。"""
    return {"status": "ok"}
