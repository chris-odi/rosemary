import asyncio
import signal
import threading
import time
from abc import ABC
from logging import Logger
from threading import Thread
from typing import Type, Iterable

from sqlalchemy.ext.asyncio import AsyncSession

from rosemary import RosemaryInterval
from rosemary.db.alembic import Alembic
from rosemary.db.db import DBConnector
from rosemary.core.logger import get_logger
from rosemary.settings import ALEMBIC_REVISION, PAUSE_FOR_CYCLE_ROSEMARY
from rosemary.tasks.constants import TypeTaskRosemary
from rosemary.worker.main_worker import RosemaryMainWorker
from rosemary.worker.worker import RosemaryWorker
from rosemary.tasks.manual_task import InterfaceManualTask
from rosemary.tasks.repeatable_task import InterfaceRepeatableTask
from rosemary.tasks.task_interface import InterfaceRosemaryTask


class Rosemary:

    @property
    def RepeatableTask(self) -> Type[InterfaceRepeatableTask]:
        session = self.db_connector.get_session

        class RepeatableTask(InterfaceRepeatableTask, ABC):
            @classmethod
            def get_session(cls) -> AsyncSession:
                return session()

            @classmethod
            async def run(self, data):
                ...

        return RepeatableTask

    @property
    def ManualTask(self) -> Type[InterfaceManualTask]:
        session = self.db_connector.get_session

        class ManualTask(InterfaceManualTask, ABC):
            @classmethod
            def get_session(cls) -> AsyncSession:
                return session()

        return ManualTask

    def __init__(
            self,
            *,
            db_host: str,
            db_port: int | str = 5432,
            db_user: str,
            db_password: str,
            db_name_db: str,
            db_schema: str = 'public',
            logger: Logger | None = None,
            max_tasks_per_worker: int = 50,
            workers: int = 1,
            delete_old_tasks: RosemaryInterval | None = None
    ):
        self._max_task_semaphore: int = max_tasks_per_worker
        self.logger: Logger = logger or get_logger('Main')
        self._count_workers: int = workers

        self._workers = []
        self._registered_tasks = {}
        self._repeatable_tasks = []

        self._delete_old_tasks: RosemaryInterval | None = delete_old_tasks

        self.__db_host = db_host
        self.__db_port = db_port
        self.__db_user = db_user
        self.__db_password = db_password
        self.__db_name = db_name_db
        self.__db_schema = db_schema

        self.db_connector = DBConnector(
            host=self.__db_host,
            db=self.__db_name,
            user=self.__db_user,
            password=self.__db_password,
            port=self.__db_port,
            schema=self.__db_schema,
        )
        self.__register_signal()
        self.__shutdown_requested = False
        self.__shutdown_event = threading.Event()

    def __handle_signal(self, *args, **kwargs):
        self.logger.error(f"Rosemary is shutdowning warm...")
        self.__shutdown_requested = True
        self.__shutdown_event.set()

    def __register_signal(self):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        signal.signal(signal.SIGINT, self.__handle_signal)

    def register_task(self, task: Type[InterfaceRosemaryTask]):
        ex_task = task()
        self.logger.info(f'Registered {ex_task.get_type()} task "{ex_task.get_name()}"')
        self._registered_tasks[ex_task.get_name()] = task

        if ex_task.get_type() == TypeTaskRosemary.REPEATABLE.value:
            self._repeatable_tasks.append(task)

    def _run_migration(self):
        self.logger.info('Migration is running...')
        alembic = Alembic(
            host=self.__db_host,
            db=self.__db_name,
            user=self.__db_user,
            password=self.__db_password,
            port=self.__db_port,
            schema=self.__db_schema,
        )
        alembic.upgrade(ALEMBIC_REVISION)
        self.logger.info('Migration finished!')

    @classmethod
    async def _create_tasks(cls, tasks: Iterable[Type[InterfaceRepeatableTask]]):
        await asyncio.gather(*tasks)

    def _create_worker(self, shutdown_event) -> tuple[RosemaryWorker, Thread]:
        worker = RosemaryWorker(
            db_user=self.__db_user,
            db_password=self.__db_password,
            db_name_db=self.__db_name,
            db_host=self.__db_host,
            db_schema=self.__db_schema,
            db_port=self.__db_port,
            max_task_semaphore=self._max_task_semaphore,
            tasks=self._registered_tasks,
            shutdown_event=shutdown_event,
        )
        worker.logger.info(f'Worker {worker.uuid} created!')
        thread = threading.Thread(target=worker.run)
        return worker, thread

    def _create_main_worker(self, shutdown_event):
        main_worker = RosemaryMainWorker(
            db_user=self.__db_user,
            db_password=self.__db_password,
            db_name_db=self.__db_name,
            db_host=self.__db_host,
            db_schema=self.__db_schema,
            db_port=self.__db_port,
            shutdown_event=shutdown_event,
            delete_old_tasks=self._delete_old_tasks,
        )
        main_worker.logger.info(f'Worker created!')
        thread = threading.Thread(target=main_worker.run)
        return thread

    def run(self, repeat_tasks: Iterable[Type[InterfaceRepeatableTask]] | None = None):
        workers_threads = {}
        self._run_migration()
        if repeat_tasks:
            self.logger.info('Registration repeatable tasks')
            asyncio.run(self._create_tasks(repeat_tasks))
            self.logger.info('Registration repeatable tasks completed')

        main_worker = self._create_main_worker(self.__shutdown_event)
        main_worker.start()

        while not self.__shutdown_requested:
            if len(workers_threads) < self._count_workers:
                for _ in range(self._count_workers):
                    worker, thread = self._create_worker(shutdown_event=self.__shutdown_event)
                    workers_threads[thread] = worker
                    thread.start()
            for_delete = []
            for th, worker in workers_threads.items():
                if not th.is_alive():
                    self.logger.error(f'Worker {worker.uuid} is deleted!')
                    for_delete.append(th)
            for th in for_delete:
                workers_threads.pop(th)

            if not main_worker.is_alive():
                main_worker = self._create_main_worker(self.__shutdown_event)
                main_worker.start()

            time.sleep(PAUSE_FOR_CYCLE_ROSEMARY)
        self.logger.error('Rosemary is shutdown!')
