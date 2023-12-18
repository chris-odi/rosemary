from datetime import datetime

from sqlalchemy import Column, BIGINT, DateTime, JSON, Enum, String, Integer, UUID as UUID_DB
from uuid import UUID

from sqlalchemy.ext.declarative import declarative_base

from rosemary.constants import StatusTaskRosemary, TypeTaskRosemary, StatusWorkerRosemary


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
    worker: UUID = Column(UUID_DB, default=None)
    timeout: int = Column(Integer, default=30, nullable=False)
    # delay = Column(DateTime, default=datetime.utcnow(), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RosemaryWorkerModel(Base):
    __tablename__ = 'rosemary_worker'

    id: int = Column(BIGINT, primary_key=True, autoincrement=True, index=True)
    uuid: UUID = Column(UUID_DB, unique=True, nullable=False)
    status = Column(Enum(StatusWorkerRosemary), default=StatusWorkerRosemary.CHECKING, nullable=False)
    ping_time = Column(DateTime, default=datetime.utcnow)
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
