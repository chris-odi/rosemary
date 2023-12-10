from logging import Logger

from rosemary.constants import TypeTaskRosemary
from rosemary.task_inreface import RosemaryTask


class RosemaryBuilder:

    def __init__(self, *, logger):
        self._registered_tasks: dict[str, RosemaryTask] = {}
        self._repeatable_tasks: list[RosemaryTask] = []

        self.logger: Logger = logger
        self.shutdown_requested: bool = False

    def _register_task(self, task: RosemaryTask):
        self.logger.info(f'Registered {task.get_type()} task "{task.get_name()}"')
        self._registered_tasks[task.get_name()] = task

        if task.get_type() == TypeTaskRosemary.REPEATABLE.value:
            self._repeatable_tasks.append(task)

    def get_task_by_name(self, task_name: str):
        return self._registered_tasks[task_name]
