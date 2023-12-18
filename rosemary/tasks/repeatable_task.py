import asyncio
import inspect
import json
from abc import abstractmethod, ABC

from pydantic import BaseModel
from pydantic.v1 import BaseModel as BaseModel_V1
from sqlalchemy import select, and_, String, cast
from sqlalchemy.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.ext.asyncio import AsyncSession

from rosemary.constants import TypeTaskRosemary, StatusTaskRosemary
from rosemary.db.models import RosemaryTaskModel
from rosemary.tasks.task_interface import InterfaceRosemaryTask


class InterfaceRepeatableTask(InterfaceRosemaryTask, ABC):
    max_retry = 0
    type_task = TypeTaskRosemary.REPEATABLE
    timeout = 30

    async def _create_task(self, data: dict, session: AsyncSession, check_exist_repeatable: bool = True):
        if check_exist_repeatable:
            for _ in range(self.timeout + 10):
                await asyncio.sleep(1)
                query = select(RosemaryTaskModel).where(and_(
                    RosemaryTaskModel.name == self.get_name(),
                    cast(RosemaryTaskModel.data, String) == json.dumps(data)
                )).where(
                    RosemaryTaskModel.status.in_([
                        StatusTaskRosemary.IN_PROGRESS.value,
                        StatusTaskRosemary.FAILED.value,
                        StatusTaskRosemary.NEW.value,
                    ])
                )
                result = await session.execute(query)
                try:
                    task_db: RosemaryTaskModel = result.scalars().one()
                except NoResultFound:
                    continue
                except MultipleResultsFound:
                    # TODO !!!
                    return
                if task_db:
                    return task_db.id
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

    def prepare_data_for_run(self, data: dict):
        return data

    def _prepare_data_to_db(self, data) -> dict | None:
        if data is None:
            return data
        if isinstance(data, dict):
            return data
        if isinstance(data, BaseModel):
            return data.model_dump()
        if isinstance(data, BaseModel_V1):
            return data.dict()
        raise TypeError(f'Incorrect type of data: "{type(data)}"')

    # def create_sync(
    #         self,
    #         *,
    #         data: dict | BaseModel | None = None,
    #         session: AsyncSession | None = None,
    #         check_exist_repeatable: bool = True,
    # ):
    #     return asyncio.run(self.create(
    #         data=data, session=session, check_exist_repeatable=check_exist_repeatable
    #     ))

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

    # async def create(
    #         self,
    #         *,
    #         data: dict | BaseModel | None = None,
    #         session: AsyncSession | None = None,
    #         check_exist_repeatable: bool = True
    # ):
    #
    #     data = self._prepare_data_to_db(data)
    #
    #     if session is None:
    #         async with self.get_session() as session:
    #             asyncio.create_task(self._create_task(data, session, check_exist_repeatable))
    #     else:
    #         asyncio.create_task(self._create_task(data, session, check_exist_repeatable))

    async def prepare_and_run(self, data: dict | None, session: AsyncSession | None):
        prepared_data = self.prepare_data_for_run(data)
        params = inspect.signature(self.run)
        kwargs = {'data': prepared_data}
        for param in params.parameters.keys():
            if param == 'session':
                data['session'] = session
        return await self.run(**kwargs)

    @abstractmethod
    def get_session(self) -> AsyncSession:
        ...

    @abstractmethod
    async def run(self, data):
        ...
