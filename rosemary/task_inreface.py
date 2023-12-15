from abc import abstractmethod, ABC

from sqlalchemy.ext.asyncio import AsyncSession

from rosemary.constants import TypeTaskRosemary
from rosemary.db.models import RosemaryTaskModel


class RosemaryTask(ABC):

    def __init__(self):
        self.max_retry = 3
        self.type_task: str | TypeTaskRosemary = TypeTaskRosemary.NOT_SETUP
        self.timeout = 30

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

    def cron_setup(self, *, hour: int | str ='*', minutes: int | str = '*', days: str = '*'):
        ...

    def get_name(self) -> str:
        return self.__class__.__name__

    def get_type(self) -> str:
        return self._type_task

    def prepare_data(self, data: dict):
        return data

    async def create(self, data: dict | None = None):
        async with self.get_session() as session:
            new_task = RosemaryTaskModel(
                data=data,
                name=self.__class__.__name__,
                type_task=self.type_task,
                max_retry=self.max_retry,
                timeout=self.timeout
            )
            session.add(new_task)
            res = await session.commit()
            return res

    @abstractmethod
    def get_session(self) -> AsyncSession:
        ...

    @abstractmethod
    async def run(self, data):
        ...
