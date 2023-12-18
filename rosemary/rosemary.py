import asyncio
import signal
import threading
import traceback
from abc import ABC
from logging import Logger
from typing import Type, Sequence, Iterable

from sqlalchemy import select, update, case, or_, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from rosemary.constants import TypeTaskRosemary, StatusTaskRosemary
from rosemary.custom_semaphore import CustomSemaphore
from rosemary.db.alembic import alembic_upgrade_head
from rosemary.db.db import DBConnector
from rosemary.db.models import RosemaryTaskModel
from rosemary.logger import get_logger
from rosemary.rosemary_worker import RosemaryWorker
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
    ):
        self._max_task_semaphore: int = max_tasks_per_worker
        self.logger: Logger = logger or get_logger('Main')
        self.__shutdown_requested: bool = False
        self._count_workers: int = workers

        self._workers = []
        self._registered_tasks = {}
        self._repeatable_tasks = []

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

    def register_task(self, task: Type[InterfaceRosemaryTask]):
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
        signal.signal(signal.SIGKILL, self.__handle_signal)

    async def _get_new_tasks(
            self, session: AsyncSession, limit: int, worker_name: str
    ) -> Sequence[int]:
        select_query = select(RosemaryTaskModel.id).where(
            and_(or_(
                RosemaryTaskModel.status.in_([
                    StatusTaskRosemary.NEW.value,
                    StatusTaskRosemary.FAILED.value
                ]),

                and_(
                    RosemaryTaskModel.status == StatusTaskRosemary.IN_PROGRESS.value,
                    RosemaryTaskModel.updated_at < (
                            func.now() - func.make_interval(0, 0, 0, 0, 0, 0, (RosemaryTaskModel.timeout + 10)))
                )
            ),
                RosemaryTaskModel.delay <= func.now()
            )
        ).limit(limit).with_for_update(skip_locked=True)

        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()
        update_status = update(RosemaryTaskModel).where(
            RosemaryTaskModel.id.in_(ids_to_update)
        ).values(
            status=StatusTaskRosemary.IN_PROGRESS.value,
            worker=worker_name,
            retry=case(
                (RosemaryTaskModel.status.in_(
                    [StatusTaskRosemary.FAILED.value, StatusTaskRosemary.IN_PROGRESS.value]
                ), RosemaryTaskModel.retry + 1),  # condition and result as a tuple
                else_=RosemaryTaskModel.retry
            )
        )
        await session.execute(update_status)
        await session.commit()
        return ids_to_update

    def _run_migration(self):
        alembic_upgrade_head(
            host=self.__db_host,
            db=self.__db_name,
            user=self.__db_user,
            password=self.__db_password,
            port=self.__db_port,
            schema=self.__db_schema,
        )

    def _run_looping(self, worker: RosemaryWorker):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._looping(worker))
        loop.close()

    async def _looping(self, worker: RosemaryWorker):
        worker.db_connector = DBConnector(
            self.__db_host, self.__db_name, self.__db_user, self.__db_password, self.__db_port, self.__db_schema
        )
        async with worker.db_connector.get_session() as session:
            await worker.register_in_db(session)
            worker.logger.info(f'Start looping by worker {worker.uuid}')
            semaphore = CustomSemaphore(self._max_task_semaphore)
            while True:
                await worker.ping(session)
                if self.__shutdown_requested:
                    worker.logger.info(f'Rosemary worker {worker.uuid} is shotdowned warm!')
                    return
                if semaphore.tasks_remaining() > 0:
                    ids_tasks = await self._get_new_tasks(session, semaphore.tasks_remaining(), worker.uuid)
                    for id_task in ids_tasks:
                        await semaphore.acquire()
                        asyncio.create_task(self._run_task(id_task, semaphore, worker))
                else:
                    await asyncio.sleep(2)
                await asyncio.sleep(1)

    @classmethod
    async def _create_tasks(cls, tasks: Iterable[Type[InterfaceRepeatableTask]]):
        await asyncio.gather(*tasks)

    def run(self, repeat_tasks: Iterable[Type[InterfaceRepeatableTask]] | None = None):
        workers_threads = []
        self._run_migration()
        for _ in range(self._count_workers):
            worker = RosemaryWorker()
            self._workers.append(worker)
            worker.logger.info(f'Worker {worker.uuid} registered!')
            thread = threading.Thread(target=self._run_looping, args=(worker,))
            workers_threads.append(thread)
        if repeat_tasks:
            self.logger.info('Registration repeatable tasks')
            asyncio.run(self._create_tasks(repeat_tasks))
            self.logger.info('Registration repeatable tasks completed')
        for thread in workers_threads:
            thread.start()
        for thread in workers_threads:
            thread.join()
        self.logger.error('Rosemary is stopped because all workers is stopped!')

    async def _run_task(self, id_task: int, pool: CustomSemaphore, worker: RosemaryWorker):
        error: str | None = None
        task: InterfaceRosemaryTask | None = None

        try:
            async with worker.db_connector.get_session() as session:
                try:
                    query = select(RosemaryTaskModel).where(
                        RosemaryTaskModel.id == id_task
                    )
                    result = await session.execute(query)
                    task_db: RosemaryTaskModel = result.scalars().one()
                except Exception as e:
                    worker.logger.exception(f'Error while getting task {id_task} from DB {e}', exc_info=e)
                    return
                try:
                    task = self.get_task_by_name(task_db.name)()
                except KeyError:
                    error = 'Task not registered in rosemary!'
                except Exception as e:
                    error = f'{e.__class__.__name__}: {repr(e)}. Traceback: {traceback.print_tb(e.__traceback__)}'
                else:
                    try:
                        worker.logger.info(f'Start task "{task_db.name}" id: "{task_db.id}" with data {task_db.data}')
                        result_task = await asyncio.wait_for(
                            task.prepare_and_run(task_db.data, session), timeout=task_db.timeout
                        )
                        worker.logger.info(f'Finished task "{task_db.name}" id: "{task_db.id}" '
                                           f'with data {task_db.data} with result: {result_task}')
                    except Exception as e:
                        if isinstance(e, asyncio.TimeoutError):
                            error = f'TimeoutError: The task has timed out {task_db.timeout}'
                        else:
                            error = f'{e.__class__.__name__}: {repr(e)}. Traceback: {traceback.print_tb(e.__traceback__)}'
                        worker.logger.info(f'Error task "{task_db.name}" id: "{task_db.id}" '
                                           f'with data {task_db.data} with error: {error}')
                if error:
                    task_db.error = error
                    if task_db.retry >= task_db.max_retry:
                        will_not_repeat = True
                        task_db.status = StatusTaskRosemary.FATAL.value
                    else:
                        will_not_repeat = False
                        task_db.status = StatusTaskRosemary.FAILED.value
                        task_db.delay = func.now() + task.delay_retry
                else:
                    task_db.status = StatusTaskRosemary.FINISHED.value
                    task_db.task_return = str(result_task)
                    will_not_repeat = True
                await session.commit()
            if will_not_repeat and task and task.get_type() == TypeTaskRosemary.REPEATABLE.value:
                await task.create(data=task_db.data, session=session, delay=task.delay_retry + func.now())
        except Exception as e:
            worker.logger.exception(f'Error while creating session for DB {e}. Task: {id_task}', exc_info=e)
        finally:
            await pool.release()
