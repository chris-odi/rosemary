from datetime import datetime

from sqlalchemy import Column, BIGINT, DateTime, JSON, Enum, String, Integer, UUID as UUID_DB
from uuid import UUID
from rosemary.core.task_Rosemary.constants import StatusTask
from rosemary.core.task_Rosemary.db.db import Base


class RosemaryTaskModel(Base):
    __tablename__ = "Rosemary_tasks"

    id: int = Column(BIGINT, primary_key=True, autoincrement=True, index=True)
    data: dict = Column(JSON, default=None)
    name: str = Column(String, nullable=False)
    status = Column(Enum(StatusTask), default=StatusTask.NEW, nullable=False)
    error: str = Column(String, default=None)
    retryd: int = Column(Integer, default=0, nullable=False)
    worker: UUID = Column(UUID_DB, default=None)
    max_retry: int = Column(Integer, default=1, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
