from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class WorkflowRunModel(Base):
    """Store workflow run records."""

    __tablename__ = "workflow_runs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("workflow_definitions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    trigger_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
