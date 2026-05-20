from fastapi import APIRouter, HTTPException, status

from app.core.exceptions import WorkflowNodeNotFoundError, WorkflowNodeValidationError
from app.schemas.plugin import (
    PluginDetailResponse,
    PluginListResponse,
    PluginStatusUpdateRequest,
    PluginSyncResponse,
)
from app.services.platform.plugin_install_service import PluginInstallService
from app.services.platform.plugin_market_service import PluginMarketService
from app.services.platform.plugin_registry_service import PluginRegistryService

router = APIRouter(prefix="/plugins")
plugin_registry_service = PluginRegistryService()
plugin_market_service = PluginMarketService()
plugin_install_service = PluginInstallService()


@router.post(
    "/sync",
    response_model=PluginSyncResponse,
    summary="同步本地插件目录",
)
def sync_plugins() -> PluginSyncResponse:
    """扫描本地插件目录并同步插件注册表。"""
    return plugin_market_service.sync_marketplace()


@router.get(
    "",
    response_model=PluginListResponse,
    summary="获取插件列表",
)
def list_plugins() -> PluginListResponse:
    """返回当前已注册插件列表。"""
    return plugin_registry_service.list_plugins()


@router.get(
    "/{plugin_id}",
    response_model=PluginDetailResponse,
    summary="获取插件详情",
)
def get_plugin(plugin_id: str) -> PluginDetailResponse:
    """返回指定插件的详情和节点清单。"""
    try:
        return plugin_registry_service.get_plugin(plugin_id)
    except WorkflowNodeNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.patch(
    "/{plugin_id}/status",
    response_model=PluginDetailResponse,
    summary="更新插件状态",
)
def update_plugin_status(
    plugin_id: str,
    payload: PluginStatusUpdateRequest,
) -> PluginDetailResponse:
    """启用或停用插件。"""
    try:
        if payload.status == "disabled":
            plugin_install_service.disable_plugin(plugin_id)
        elif payload.status == "enabled":
            plugin_install_service.enable_plugin(plugin_id)
        else:
            raise WorkflowNodeValidationError("插件状态仅支持 enabled 或 disabled。")
        return plugin_registry_service.get_plugin(plugin_id)
    except WorkflowNodeNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except (WorkflowNodeValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
