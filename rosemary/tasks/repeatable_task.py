from datetime import datetime
import json
from abc import ABC

from sqlalchemy import select, and_, String, cast, Sequence, func
from sqlalchemy.ext.asyncio import AsyncSession

from rosemary.constants import TypeTaskRosemary, StatusTaskRosemary
from rosemary.db.models import RosemaryTaskModel
from rosemary.tasks.task_interface import InterfaceRosemaryTask


class InterfaceRepeatableTask(InterfaceRosemaryTask, ABC):
    max_retry = 0
    type_task = TypeTaskRosemary.REPEATABLE

    async def _create_task(
            self,
            data: dict,
            session: AsyncSession,
            delay: datetime
    ):
        try:
            query = select(RosemaryTaskModel).where(and_(
                RosemaryTaskModel.name == self.get_name(),
                cast(RosemaryTaskModel.data, String) == json.dumps(data)
            )).where(
                RosemaryTaskModel.status.in_([
                    StatusTaskRosemary.IN_PROGRESS.value,
                    StatusTaskRosemary.FAILED.value,
                    StatusTaskRosemary.NEW.value,
                ])
            ).with_for_update()

            result = await session.execute(query)
            task_db: Sequence[RosemaryTaskModel] = result.scalars().all()

            if task_db:
                return task_db[-1].id
        except :
            return None

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
