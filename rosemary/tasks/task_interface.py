import asyncio
import inspect
from abc import abstractmethod, ABC

from pydantic import BaseModel
from pydantic.v1 import BaseModel as BaseModel_V1
from sqlalchemy.ext.asyncio import AsyncSession

from rosemary.constants import TypeTaskRosemary


class InterfaceRosemaryTask(ABC):
    max_retry = 3
    delay_retry = 5  # in sec

    type_task = TypeTaskRosemary.NOT_SETUP
    timeout = 30

    def __init__(self):
        if isinstance(self.type_task, TypeTaskRosemary):
            self._type_task = self.type_task.value
            return

        if isinstance(self.type_task, str):
            if self.type_task in TypeTaskRosemary.values():
                self._type_task = self.type_task
                return
            raise ValueError(
                f'Incorrect value of type task. Expected one of {TypeTaskRosemary.values()}. Got"{self.type_task}"'
            )
        raise TypeError(f'Incorrect type of type task. Expected str or TypeTask. Got "{type(self.type_task)}"')

    @classmethod
    def _is_event_loop_running(cls):
        try:
            loop = asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

    def cron_setup(self, *, hour: int | str ='*', minutes: int | str = '*', days: str = '*'):
        ...

    def get_name(self) -> str:
        return self.__class__.__name__

    def get_type(self) -> str:
        return self._type_task

    @abstractmethod
    async def _create_task(self, data: dict, session: AsyncSession, check_exist_repeatable: bool = True):
        ...

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
        params = inspect.signature(self.run)
        kwargs = {}
        for param in params.parameters.keys():
            if param == 'data':
                prepared_data = self.prepare_data_for_run(data)
                kwargs['data'] = prepared_data
            if param == 'session':
                kwargs['session'] = session
        return await self.run(**kwargs)

    @abstractmethod
    def get_session(self) -> AsyncSession:
        ...

    @abstractmethod
    async def run(self, data):
        ...
