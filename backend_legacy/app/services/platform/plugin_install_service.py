from __future__ import annotations

import shutil
from pathlib import Path

from app.core.exceptions import WorkflowNodeValidationError
from app.services.platform.plugin_loader_service import PluginLoaderService
from app.services.platform.plugin_registry_service import PluginRegistryService


class PluginInstallService:
    """负责单节点插件启停和删除等本地管理操作。"""

    def __init__(self) -> None:
        """初始化单节点插件加载器和注册表服务。"""
        self.loader = PluginLoaderService()
        self.registry = PluginRegistryService()

    def disable_plugin(self, plugin_id: str) -> None:
        """停用插件。当前阶段内置插件只改状态，不搬目录。"""
        manifest = self.loader.load_plugin_manifest(plugin_id)
        if manifest is None:
            raise WorkflowNodeValidationError(f"插件 {plugin_id} 不存在，无法变更状态。")
        # 已安装插件允许物理移入 disabled，内置插件先只改数据库状态。
        if manifest.source == "installed":
            self._move_plugin(plugin_id, target_root=self.loader.disabled_root)
        self.registry.update_plugin_status(plugin_id, "disabled")

    def enable_plugin(self, plugin_id: str) -> None:
        """启用插件。内置插件直接恢复状态，安装插件可搬回启用目录。"""
        manifest = self.loader.load_plugin_manifest(plugin_id)
        if manifest is not None and manifest.source == "builtin":
            self.registry.update_plugin_status(plugin_id, "enabled")
            return

        disabled_dir = self._find_disabled_plugin_dir(plugin_id)
        if disabled_dir is not None and disabled_dir.exists():
            target_root = (
                self.loader.builtin_root
                if plugin_id.startswith("builtin-")
                else self.loader.installed_root
            )
            target_dir = target_root / disabled_dir.name
            if target_dir.exists():
                raise WorkflowNodeValidationError(f"插件目录 {target_dir.name} 已存在，无法启用。")
            shutil.move(str(disabled_dir), str(target_dir))
        self.registry.update_plugin_status(plugin_id, "enabled")
        self.registry.sync_plugins()

    def remove_plugin(self, plugin_id: str) -> None:
        """删除插件。当前不允许删除内置插件。"""
        manifest = self.loader.load_plugin_manifest(plugin_id)
        if manifest is not None and manifest.source == "builtin":
            raise WorkflowNodeValidationError("内置插件不允许删除。")

        manifest_dir = self.loader.get_plugin_directory(plugin_id)
        if manifest_dir is None:
            disabled_guess = self._find_disabled_plugin_dir(plugin_id)
            if disabled_guess is None:
                raise WorkflowNodeValidationError(f"插件 {plugin_id} 不存在，无法删除。")
            manifest_dir = disabled_guess

        shutil.rmtree(manifest_dir, ignore_errors=True)

    def _move_plugin(self, plugin_id: str, *, target_root: Path) -> None:
        """把插件目录移动到目标目录。"""
        manifest_dir = self.loader.get_plugin_directory(plugin_id)
        if manifest_dir is None:
            raise WorkflowNodeValidationError(f"插件 {plugin_id} 不存在，无法变更状态。")
        target_root.mkdir(parents=True, exist_ok=True)
        target_dir = target_root / manifest_dir.name
        if target_dir.exists():
            raise WorkflowNodeValidationError(f"插件目录 {target_dir.name} 已存在，无法移动。")
        shutil.move(str(manifest_dir), str(target_dir))

    def _find_disabled_plugin_dir(self, plugin_id: str) -> Path | None:
        """在 disabled 目录中反查某个插件所在目录。"""
        if not self.loader.disabled_root.exists():
            return None

        for child in self.loader.disabled_root.iterdir():
            if not child.is_dir():
                continue
            for node_dir in child.glob("nodes/*"):
                if not node_dir.is_dir():
                    continue
                manifest_path = node_dir / "manifest.json"
                if not manifest_path.exists():
                    continue
                import json

                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
                if f"disabled-{payload['key']}" == plugin_id:
                    return node_dir
        return None
