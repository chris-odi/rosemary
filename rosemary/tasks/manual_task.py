from abc import ABC

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from rosemary.constants import TypeTaskRosemary
from rosemary.db.models import RosemaryTaskModel
from rosemary.tasks.task_interface import InterfaceRosemaryTask


class InterfaceManualTask(InterfaceRosemaryTask, ABC):
    max_retry = 3
    type_task = TypeTaskRosemary.MANUAL
    timeout = 30

    async def _create_task(self, data: dict, session: AsyncSession, *args, **kwargs):
        new_task = RosemaryTaskModel(
            data=data,
            name=self.__class__.__name__,
            type_task=self.type_task,
            max_retry=self.max_retry,
            timeout=self.timeout
        )
        session.add(new_task)
        await session.commit()
        return new_task.id

    async def create(
            self, *, data: dict | BaseModel | None = None,
            session: AsyncSession | None = None,
            check_exist_repeatable: bool = True
    ):
        data = self._prepare_data_to_db(data)

        if session is None:
            async with self.get_session() as session:
                return await self._create_task(data, session, check_exist_repeatable)
        else:
            return await self._create_task(data, session, check_exist_repeatable)

