from abc import abstractmethod, ABC
from rosemary.constants import TypeTaskRosemary


class RosemaryTask(ABC):

    def __init__(self, type_task: str | TypeTaskRosemary):
        if isinstance(type_task, TypeTaskRosemary):
            self._type_task = type_task.value
            return

        if isinstance(type_task, str):
            if type_task in TypeTaskRosemary.values():
                self._type_task = type_task
                return
            raise ValueError(f'Incorrect value of type task. Expected one of {TypeTaskRosemary.values()}. Got"{type_task}"')
        raise TypeError(f'Incorrect type of type task. Expected str or TypeTask. Got "{type(type_task)}"')

    def cron_setup(self, *, hour: int | str ='*', minutes: int | str = '*', days: str = '*'):
        ...

    def get_name(self) -> str:
        return self.__class__.__name__

    def get_type(self) -> str:
        return self._type_task

    def prepare_data(self, data: dict):
        return data

    def create(self, data: dict | None = None):
        ...

    @abstractmethod
    async def run(self, data):
        ...
