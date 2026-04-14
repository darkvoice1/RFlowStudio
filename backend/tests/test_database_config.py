from app.core.config import Settings, settings
from app.db.session import get_database_url


def test_settings_build_database_url_from_database_fields(monkeypatch) -> None:
    """验证数据库连接串会按配置字段正确拼装。"""
    monkeypatch.setenv("DATABASE_DRIVER", "postgresql+psycopg")
    monkeypatch.setenv("DATABASE_HOST", "localhost")
    monkeypatch.setenv("DATABASE_PORT", "5433")
    monkeypatch.setenv("DATABASE_NAME", "open_r_platform")
    monkeypatch.setenv("DATABASE_USER", "tester")
    monkeypatch.setenv("DATABASE_PASSWORD", "secret")

    test_settings = Settings(_env_file=None)

    assert (
        test_settings.database_url
        == "postgresql+psycopg://tester:secret@localhost:5433/open_r_platform"
    )


def test_settings_build_database_url_escapes_special_characters(monkeypatch) -> None:
    """验证用户名和密码中的特殊字符会被安全转义。"""
    monkeypatch.setenv("DATABASE_USER", "test user")
    monkeypatch.setenv("DATABASE_PASSWORD", "p@ss word")

    test_settings = Settings(_env_file=None)

    assert test_settings.database_url == (
        "postgresql+psycopg://test+user:p%40ss+word@127.0.0.1:5432/rflowstudio"
    )


def test_settings_build_sqlite_database_url(monkeypatch, tmp_path) -> None:
    """验证 SQLite 驱动会按文件库格式拼装连接串。"""
    monkeypatch.setenv("DATABASE_DRIVER", "sqlite+pysqlite")
    monkeypatch.setenv("DATABASE_NAME", (tmp_path / "test.db").as_posix())

    test_settings = Settings(_env_file=None)

    assert test_settings.database_url == (
        f"sqlite+pysqlite:///{(tmp_path / 'test.db').as_posix()}"
    )


def test_get_database_url_returns_global_settings_value() -> None:
    """验证数据库连接模块会复用全局配置对象。"""
    assert get_database_url() == settings.database_url
