from logging import Logger

from rosemary.constants import TypeTaskRosemary
from rosemary.task_inreface import RosemaryTask


class RosemaryBuilder:

    def __init__(self, *, logger, max_tasks_per_worker: int = 50, workers: int = 1):
        self._registered_tasks: dict[str, RosemaryTask] = {}
        self._repeatable_tasks: list[RosemaryTask] = []
        self._max_tasks_per_worker = max_tasks_per_worker
        self.logger: Logger = logger
        self.shutdown_requested: bool = False
        self._workers = 1


    def _register_task(self, task: RosemaryTask):
        self.logger.info(f'Registered {task.get_type()} task "{task.get_name()}"')
        self._registered_tasks[task.get_name()] = task

        if task.get_type() == TypeTaskRosemary.REPEATABLE.value:
            self._repeatable_tasks.append(task)

    def get_task_by_name(self, task_name: str):
        return self._registered_tasks[task_name]

    def get_max_tasks_per_worker(self):
        return self._max_tasks_per_worker

    def get_count_workers(self):
        return self._workers
