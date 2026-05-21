from app.core.config import settings
from app.db.session import get_database_url, get_engine, get_session_factory


def test_settings_exposes_database_url() -> None:
    """验证配置对象能生成当前测试环境下的数据库地址。"""
    database_url = get_database_url()

    assert database_url.startswith("sqlite+pysqlite:///")


def test_database_engine_and_session_factory_can_be_created() -> None:
    """验证数据库引擎和会话工厂可以正常初始化。"""
    engine = get_engine()
    session_factory = get_session_factory()

    assert engine.url.render_as_string(hide_password=False).startswith("sqlite+pysqlite:///")
    assert session_factory is not None


def test_settings_exposes_basic_app_metadata() -> None:
    """验证当前应用基础配置可用。"""
    assert settings.app_name == "RFlowStudio Backend"
    assert settings.api_v1_prefix == "/api/v1"
