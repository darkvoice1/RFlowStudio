"""平台服务包。"""

from app.services.platform.plugin_loader_service import PluginLoaderService
from app.services.platform.plugin_registry_service import PluginRegistryService

__all__ = [
    "PluginLoaderService",
    "PluginRegistryService",
]
