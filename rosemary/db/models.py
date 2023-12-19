from sqlalchemy import (
    Column, BIGINT, DateTime, JSON, Enum, String, Integer, UUID as UUID_DB, ForeignKey, func
)
from uuid import UUID

from sqlalchemy.ext.declarative import declarative_base

from rosemary.tasks.constants import StatusTaskRosemary, TypeTaskRosemary
from rosemary.worker.constants import StatusWorkerRosemary


Base = declarative_base()


class RosemaryTaskModel(Base):
    __tablename__ = "rosemary_tasks"

    id = Column(BIGINT, primary_key=True, autoincrement=True, index=True)
    name: str = Column(String, nullable=False)
    data: dict = Column(JSON, default=None)
    type_task = Column(Enum(TypeTaskRosemary), default=TypeTaskRosemary.NOT_SETUP, nullable=False)
    status = Column(Enum(StatusTaskRosemary), default=StatusTaskRosemary.NEW, nullable=False)
    retry: int = Column(Integer, default=0, nullable=False)
    max_retry: int = Column(Integer, default=1, nullable=False)
    error: str = Column(String, default=None)
    task_return: str = Column(String, default=None)
    worker = Column(Integer, ForeignKey('rosemary_worker.id'), nullable=True, default=None)
    timeout: int = Column(Integer, default=30, nullable=False)
    delay = Column(DateTime, default=func.now(), nullable=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class RosemaryWorkerModel(Base):
    __tablename__ = 'rosemary_worker'

    id = Column(BIGINT, primary_key=True, autoincrement=True, index=True)
    uuid: UUID = Column(UUID_DB, unique=True, nullable=False)
    status = Column(Enum(StatusWorkerRosemary), default=StatusWorkerRosemary.CHECKING, nullable=False)
    ping_time = Column(DateTime, default=func.now())
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


# class RosemaryCronModel(Base):
#     __tablename__ = 'rosemary_cron'
#
#     id: int = Column(BIGINT, primary_key=True, autoincrement=True, index=True)
#     status = ...
#     worker: UUID = Column(UUID_DB, default=None)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
