import datetime
from abc import ABC

from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession

from rosemary.constants import TypeTaskRosemary
from rosemary.db.models import RosemaryTaskModel
from rosemary.tasks.task_interface import InterfaceRosemaryTask


class InterfaceManualTask(InterfaceRosemaryTask, ABC):
    max_retry = 3
    type_task = TypeTaskRosemary.MANUAL
    timeout = 30

    async def _create_task(self, data: dict, session: AsyncSession, delay: datetime.datetime):
        new_task = RosemaryTaskModel(
            data=data,
            name=self.__class__.__name__,
            type_task=self.type_task,
            max_retry=self.max_retry,
            timeout=self.timeout,
            delay=func.now() if delay is None else delay
        )
        session.add(new_task)
        await session.commit()
        return new_task.id
