from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from app.core.config import PROJECT_ROOT


@dataclass(slots=True)
class PluginManifest:
    """表示单个功能节点插件的元数据。"""

    id: str
    name: str
    version: str
    category: str
    entry_path: str
    executor: str
    source: str
    plugin_path: str
    manifest_path: str


class PluginLoaderService:
    """负责按“单节点插件”模型扫描并读取插件元数据。"""

    def __init__(self) -> None:
        """初始化三个插件目录入口。"""
        self.plugins_root = PROJECT_ROOT / "backend" / "app" / "plugins"
        self.builtin_root = self.plugins_root / "builtin"
        self.installed_root = self.plugins_root / "installed"
        self.disabled_root = self.plugins_root / "disabled"

    def list_plugin_manifests(self) -> list[PluginManifest]:
        """扫描内置和已安装目录，返回全部单节点插件清单。"""
        manifests: list[PluginManifest] = []
        manifests.extend(self._scan_plugin_root(self.builtin_root, source="builtin"))
        manifests.extend(self._scan_plugin_root(self.installed_root, source="installed"))
        return manifests

    def load_plugin_manifest(self, plugin_id: str) -> PluginManifest | None:
        """按插件 id 查找单个插件 manifest。"""
        for manifest in self.list_plugin_manifests():
            if manifest.id == plugin_id:
                return manifest
        return None

    def get_plugin_directory(self, plugin_id: str) -> Path | None:
        """返回插件所在节点目录，供启停或删除时复用。"""
        manifest = self.load_plugin_manifest(plugin_id)
        if manifest is None:
            return None
        return PROJECT_ROOT / manifest.plugin_path

    def _scan_plugin_root(self, root: Path, *, source: str) -> list[PluginManifest]:
        """扫描某个插件根目录下的所有节点插件。"""
        if not root.exists():
            return []

        manifests: list[PluginManifest] = []
        for plugin_dir in sorted(root.iterdir()):
            if not plugin_dir.is_dir():
                continue
            plugin_manifest_path = plugin_dir / "plugin.json"
            if not plugin_manifest_path.exists():
                continue
            plugin_payload = json.loads(plugin_manifest_path.read_text(encoding="utf-8"))
            manifests.extend(
                self._read_node_plugin_manifests(
                    plugin_dir=plugin_dir,
                    plugin_payload=plugin_payload,
                    source=source,
                )
            )
        return manifests

    def _read_node_plugin_manifests(
        self,
        *,
        plugin_dir: Path,
        plugin_payload: dict[str, object],
        source: str,
    ) -> list[PluginManifest]:
        """读取一个插件分类目录下的所有节点插件。"""
        nodes_dir = plugin_dir / "nodes"
        if not nodes_dir.exists():
            return []

        manifests: list[PluginManifest] = []
        for node_dir in sorted(nodes_dir.iterdir()):
            if not node_dir.is_dir():
                continue
            manifest_path = node_dir / "manifest.json"
            if not manifest_path.exists():
                continue
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            plugin_id = f"{source}-{payload['key']}"
            executor = str((node_dir / str(payload["executor"])).relative_to(PROJECT_ROOT)).replace(
                "\\", "/"
            )
            manifests.append(
                PluginManifest(
                    id=plugin_id,
                    name=str(payload["name"]),
                    version=str(plugin_payload["version"]),
                    category=str(payload["category"]),
                    entry_path=executor,
                    executor=executor,
                    source=source,
                    plugin_path=str(node_dir.relative_to(PROJECT_ROOT)).replace("\\", "/"),
                    manifest_path=str(manifest_path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
                )
            )
        return manifests
