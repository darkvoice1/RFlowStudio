from collections.abc import Generator
from contextlib import contextmanager
from threading import Lock

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import settings

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None
_current_database_url: str | None = None
_schema_initialized = False
_engine_lock = Lock()


def get_database_url() -> str:
    """返回当前项目使用的数据库连接地址。"""
    return settings.database_url


def get_engine() -> Engine:
    """返回当前配置对应的数据库引擎。"""
    global _engine, _session_factory, _current_database_url, _schema_initialized

    database_url = get_database_url()
    with _engine_lock:
        if _engine is not None and _current_database_url == database_url:
            return _engine

        if _engine is not None:
            _engine.dispose()

        connect_args: dict[str, object] = {}
        if database_url.startswith("sqlite"):
            # 测试环境会走 SQLite 文件库，需要关闭线程检查以兼容 TestClient。
            connect_args["check_same_thread"] = False

        _engine = create_engine(
            database_url,
            future=True,
            pool_pre_ping=True,
            connect_args=connect_args,
        )
        _session_factory = sessionmaker(
            bind=_engine,
            autoflush=False,
            expire_on_commit=False,
            class_=Session,
        )
        _current_database_url = database_url
        _schema_initialized = False
        return _engine


def initialize_database() -> None:
    """初始化当前数据库所需的数据表。"""
    global _schema_initialized

    engine = get_engine()
    with _engine_lock:
        if _schema_initialized:
            return

        # 导入模型模块以注册所有表，再统一创建缺失的表结构。
        from app.db.base import Base
        from app.models.dataset import (  # noqa: F401
            DatasetAnalysisRecordModel,
            DatasetCleaningStepModel,
            DatasetRecordModel,
        )

        Base.metadata.create_all(bind=engine)
        _schema_initialized = True


def get_session_factory() -> sessionmaker[Session]:
    """返回数据库会话工厂。"""
    global _session_factory

    initialize_database()
    if _session_factory is None:
        raise RuntimeError("数据库会话工厂尚未初始化。")

    return _session_factory


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """提供带提交和回滚保护的数据库会话上下文。"""
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db_session() -> Generator[Session, None, None]:
    """提供 FastAPI 可复用的数据库会话依赖。"""
    with session_scope() as session:
        yield session


def dispose_database_engine() -> None:
    """释放当前数据库引擎，供测试切换配置时重建连接。"""
    global _engine, _session_factory, _current_database_url, _schema_initialized

    with _engine_lock:
        if _engine is not None:
            _engine.dispose()

        _engine = None
        _session_factory = None
        _current_database_url = None
        _schema_initialized = False
