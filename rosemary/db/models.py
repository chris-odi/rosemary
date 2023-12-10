from datetime import datetime

from sqlalchemy import Column, BIGINT, DateTime, JSON, Enum, String, Integer, UUID as UUID_DB
from uuid import UUID

from rosemary.constants import StatusTaskRosemary, TypeTaskRosemary, StatusWorkerRosemary
from rosemary.db.db import Base


class RosemaryTaskModel(Base):
    __tablename__ = "rosemary_tasks"

    id: int = Column(BIGINT, primary_key=True, autoincrement=True, index=True)
    data: dict = Column(JSON, default=None)
    name: str = Column(String, nullable=False)
    type_task = Column(Enum(TypeTaskRosemary), default=TypeTaskRosemary.NOT_SETUP, nullable=False)
    status = Column(Enum(StatusTaskRosemary), default=StatusTaskRosemary.NEW, nullable=False)
    error: str = Column(String, default=None)
    retryd: int = Column(Integer, default=0, nullable=False)
    worker: UUID = Column(UUID_DB, default=None)
    max_retry: int = Column(Integer, default=1, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RosemaryWorkerModel(Base):
    __tablename__ = 'rosemary_worker'

    id: int = Column(BIGINT, primary_key=True, autoincrement=True, index=True)
    uuid: UUID = Column(UUID_DB, unique=True, nullable=False)
    status = Column(Enum(StatusWorkerRosemary), default=StatusWorkerRosemary.CHECKING, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# class RosemaryCronModel(Base):
#     __tablename__ = 'rosemary_cron'
#
#     id: int = Column(BIGINT, primary_key=True, autoincrement=True, index=True)
#     status = ...
#     worker: UUID = Column(UUID_DB, default=None)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
