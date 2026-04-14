from collections.abc import Generator

from app.core.config import settings


def get_database_url() -> str:
    """返回当前项目使用的数据库连接地址。"""
    return settings.database_url


def get_db_session() -> Generator[None, None, None]:
    """预留数据库会话依赖入口，后续接入 ORM 会话时继续扩展。"""
    # 当前小步骤先把依赖入口留出来，避免后续接入数据表时再改路由签名。
    yield None
