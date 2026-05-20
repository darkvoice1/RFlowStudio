from app.schemas.plugin import PluginListResponse, PluginSyncResponse
from app.services.platform.plugin_registry_service import PluginRegistryService


class PluginMarketService:
    """负责协调插件市场视角下的同步与列表能力。"""

    def __init__(self) -> None:
        """初始化插件注册表服务。"""
        self.registry = PluginRegistryService()

    def list_market_plugins(self) -> PluginListResponse:
        """返回当前市场可见的插件列表。"""
        return self.registry.list_plugins()

    def sync_marketplace(self) -> PluginSyncResponse:
        """触发一次本地插件目录同步。"""
        return self.registry.sync_plugins()
