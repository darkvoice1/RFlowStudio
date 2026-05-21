"""数据库模型基类。"""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """新后端统一 ORM 基类。"""
