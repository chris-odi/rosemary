import asyncio
import logging
import signal
import threading
from abc import ABC
from logging import Logger
from typing import Type

from sqlalchemy import select, update, and_, Result
from sqlalchemy.ext.asyncio import AsyncSession

from rosemary.db.alembic import alembic_upgrade_head
from rosemary.constants import StatusTaskRosemary, TypeTaskRosemary
from rosemary.custom_semaphore import CustomSemaphore
from rosemary.db.db import DBConnector
from rosemary.db.models import RosemaryTaskModel
from rosemary.rosemary_worker import RosemaryWorker
from rosemary.task_inreface import RosemaryTask


class Rosemary:

    @property
    def Task(self):
        session = self.db_connector.get_session

        class Task(RosemaryTask, ABC):
            def get_session(self) -> AsyncSession:
                return session()

        return Task

    def __init__(
            self,
            *,
            db_host: str,
            db_port: int | str = 5432,
            db_user: str,
            db_password: str,
            db_name_db: str,
            logger: Logger | None = None,
            max_tasks_per_worker: int = 50,
            workers: int = 1,
    ):
        # self.Rosemary_builder: RosemaryBuilder = rosemary_builder
        self._max_task_semaphore: int = max_tasks_per_worker
        self.logger: Logger = logger or logging.getLogger()
        self.__shutdown_requested: bool = False
        self._count_workers: int = workers

        self._workers = []
        self._registered_tasks = {}
        self._repeatable_tasks = []

        self.__db_host = db_host
        self.__db_port = db_port
        self.__db_user = db_user
        self.__db_password = db_password
        self.__db_name_db = db_name_db

        self.db_connector = DBConnector(db_host, db_name_db, db_user, db_password, db_port)

    def register_task(self, task: Type[RosemaryTask]):
        ex_task = task()
        self.logger.info(f'Registered {ex_task.get_type()} task "{ex_task.get_name()}"')
        self._registered_tasks[ex_task.get_name()] = task

        if ex_task.get_type() == TypeTaskRosemary.REPEATABLE.value:
            self._repeatable_tasks.append(task)

    def get_task_by_name(self, task_name: str):
        return self._registered_tasks[task_name]

    def __handle_signal(self, *args):
        self.logger.error("Rosemary runner is shotdowning warm...")
        self.__shutdown_requested = True

    def __register_signal(self):
        signal.signal(signal.SIGTERM, self.__handle_signal)
        signal.signal(signal.SIGINT, self.__handle_signal)

    async def _get_new_tasks(
            self, session: AsyncSession, limit: int, worker_name: str
    ) -> list[int]:
        select_query = select(RosemaryTaskModel.id).where(
            RosemaryTaskModel.status.in_([StatusTaskRosemary.NEW.value, StatusTaskRosemary.FAILED.value])
        ).limit(limit).with_for_update(skip_locked=True)

        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()

        update_status = update(RosemaryTaskModel).where(
            RosemaryTaskModel.id.in_(ids_to_update)
        ).values(
            status=StatusTaskRosemary.IN_PROGRESS.value,
            worker=worker_name,
        )
        await session.execute(update_status)

        query = select(RosemaryTaskModel.id).where(and_(
            RosemaryTaskModel.status == StatusTaskRosemary.IN_PROGRESS.value,
            RosemaryTaskModel.worker == worker_name
        ))
        tasks_ids_row: Result = await session.execute(query)
        ids_tasks = [task_id_row[0] for task_id_row in tasks_ids_row]
        return ids_tasks

    def _run_migration(self):
        alembic_upgrade_head(
            self.__db_host, self.__db_name_db, self.__db_user, self.__db_password, self.__db_port
        )

    def _run_looping(self, worker: RosemaryWorker):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.looping(worker))
        loop.close()

    async def looping(self, worker: RosemaryWorker):
        async with self.db_connector.get_session() as session:
            await worker.register_in_db(session)
            self.logger.info(f'Start looping by worker {worker.uuid}')
            semaphore = CustomSemaphore(self._max_task_semaphore)
            # self.logger.info(self._max_task_semaphore)
            self.logger.info(semaphore.tasks_remaining())
            while True:
                await worker.ping(session)
                if self.__shutdown_requested:
                    self.logger.info(f'Rosemary worker {worker.uuid} is shotdowned warm!')
                    return
                if semaphore.tasks_remaining() > 0:
                    ids_tasks = await self._get_new_tasks(session, semaphore.tasks_remaining(), worker.uuid)
                    for id_task in ids_tasks:
                        await semaphore.acquire()
                        asyncio.create_task(self._run_task(id_task, semaphore))
                else:
                    # Skip one run cycle
                    self.logger.info('skip')
                    await asyncio.sleep(10)

    def run(self):
        workers_threads = []
        self._run_migration()
        for _ in range(self._count_workers):
            worker = RosemaryWorker()
            self._workers.append(worker)
            self.logger.info(f'Worker {worker.uuid} registered!')
            thread = threading.Thread(target=self._run_looping, args=(worker,))
            workers_threads.append(thread)
        for thread in workers_threads:
            thread.start()
        for thread in workers_threads:
            thread.join()
        self.logger.error('Rosemary is stopped because all workers is stopped!')

    async def _run_task(self, id_task: int, pool: CustomSemaphore):
        try:
            async with self.db_connector.get_session() as session:
                async with session.begin():
                    try:
                        query = select(RosemaryTaskModel).where(
                            RosemaryTaskModel.id == id_task
                        ).with_for_update()
                        result = await session.execute(query)
                        task_db: RosemaryTaskModel = result.scalars().one()
                    except Exception as e:
                        self.logger.error(f'Error while getting task from DB {e}', exc_info=e)
                        return
                    try:
                        task: RosemaryTask = self.get_task_by_name(task_db.name)
                        task = task()
                        await asyncio.wait_for(
                            task.run(task.prepare_data(task_db.data)), timeout=task_db.timeout
                        )
                    except Exception as e:
                        if isinstance(e, asyncio.TimeoutError):
                            error = f'TimeoutError: The task has timed out {task_db.timeout}'
                        else:
                            error = f'{e.__class__.__name__}: {repr(e)}'
                        task_db.retry += 1
                        task_db.error = error
                        if task_db.max_retry <= task_db.retry:
                            task_db.status = StatusTaskRosemary.FATAL.value
                        else:
                            task_db.status = StatusTaskRosemary.FAILED.value
                        await session.commit()

        except Exception as e:
            self.logger.error(f'Error while creating session for DB {e}', exc_info=e)
        finally:
            await pool.release()
