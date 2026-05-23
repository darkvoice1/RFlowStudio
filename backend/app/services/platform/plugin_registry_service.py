"""插件注册目录服务。"""

from app.schemas.plugin import PluginManifestRecord
from app.services.platform.plugin_loader_service import PluginLoaderService


class PluginRegistryService:
    """负责返回当前已启用的单节点插件。"""

    def __init__(self, loader: PluginLoaderService | None = None) -> None:
        self.loader = loader or PluginLoaderService()

    def list_enabled_plugins(self) -> list[PluginManifestRecord]:
        """返回当前可见的已启用插件节点清单。"""
        return self.loader.list_plugin_manifests()
