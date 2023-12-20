import threading
import traceback
import uuid
from logging import Logger

import asyncio
from typing import Type

from sqlalchemy import select, Sequence, and_, func, update, case, or_
from sqlalchemy.ext.asyncio import AsyncSession

from rosemary import RosemaryInterval
from rosemary.tasks.constants import StatusTaskRosemary, TypeTaskRosemary
from rosemary.worker.constants import StatusWorkerRosemary
from rosemary.core.custom_semaphore import CustomSemaphore
from rosemary.db.db import DBConnector
from rosemary.db.models import RosemaryWorkerModel, RosemaryTaskModel
from rosemary.core.logger import get_logger
from rosemary.tasks.task_interface import InterfaceRosemaryTask
from rosemary.worker.exception import WorkerAliveException
from rosemary.worker.worker_interface import RosemaryWorkerInterface


class RosemaryWorker(RosemaryWorkerInterface):
    def __init__(
            self,
            db_host: str,
            db_port: int | str,
            db_user: str,
            db_password: str,
            db_name_db: str,
            db_schema: str,
            tasks: dict[str, Type[InterfaceRosemaryTask]],
            shutdown_event: threading.Event,
            logger: Logger | None = None,
            max_task_semaphore: int = 30,
    ):
        self.uuid = uuid.uuid4()
        self.worker_db: RosemaryWorkerModel | None = None
        self.db_connector = None
        self.logger: Logger | None = get_logger(str(self.uuid)) or logger

        self.db_connector = DBConnector(
            db_host, db_name_db, db_user, db_password, db_port, db_schema
        )
        self._max_task_semaphore = max_task_semaphore
        self._registered_tasks = tasks
        self.__shutdown_event = shutdown_event

    def get_task_by_name(self, task_name: str) -> Type[InterfaceRosemaryTask]:
        return self._registered_tasks[task_name]

    async def _get_new_tasks(
            self, session: AsyncSession, limit: int
    ) -> Sequence[int]:
        select_query = select(RosemaryTaskModel.id).where(
            and_(
                RosemaryTaskModel.status.in_([
                    StatusTaskRosemary.NEW.value,
                    StatusTaskRosemary.FAILED.value
                ]),
                RosemaryTaskModel.delay <= func.now(),
                or_(
                    RosemaryTaskModel.worker == None,
                    RosemaryTaskModel.worker == self.worker_db.id,
                )
            )
        ).limit(limit).with_for_update(skip_locked=True).order_by(RosemaryTaskModel.id)

        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()
        update_status = update(RosemaryTaskModel).where(
            RosemaryTaskModel.id.in_(ids_to_update)
        ).values(
            status=StatusTaskRosemary.IN_PROGRESS.value,
            worker=self.worker_db.id,
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

    async def register_in_db(self, session: AsyncSession):
        self.worker_db = RosemaryWorkerModel(uuid=self.uuid)
        session.add(self.worker_db)
        await session.commit()

    async def __ping(self, session: AsyncSession):
        query = update(RosemaryWorkerModel).where(
            and_(RosemaryWorkerModel.id == self.worker_db.id, RosemaryWorkerModel.status.in_(
                [StatusWorkerRosemary.WORKING.value, StatusWorkerRosemary.CHECKING.value]
            ))
        ).values(
            status=StatusWorkerRosemary.WORKING.value,
            ping_time=func.now()
        )
        res = await session.execute(query)
        await session.commit()
        if res.rowcount != 1:
            raise WorkerAliveException()

    async def __suicide(self, session: AsyncSession):
        self.worker_db.status = StatusWorkerRosemary.KILLED.value
        await session.commit()

    async def _looping(self):
        async with self.db_connector.get_session() as session:
            await self.register_in_db(session)
            self.logger.info(f'Start looping by worker {self.uuid}')
            semaphore = CustomSemaphore(self._max_task_semaphore)
            while not self.__shutdown_event.is_set():
                await self.__ping(session)
                if semaphore.tasks_remaining() > 0:
                    ids_tasks = await self._get_new_tasks(session, semaphore.tasks_remaining())
                    for id_task in ids_tasks:
                        await semaphore.acquire()
                        asyncio.create_task(self._run_task(id_task, semaphore))
                else:
                    await asyncio.sleep(2)
                await asyncio.sleep(1)

            self.logger.info(f'Rosemary worker {self.uuid} is shutdowning warm...')
            while semaphore.tasks_remaining() != self._max_task_semaphore:
                await asyncio.sleep(1)
            await self.__suicide(session)
            self.logger.info(f'Rosemary worker {self.uuid} is shutdown warm!')

    async def _run_task(self, id_task: int, pool: CustomSemaphore):
        error: str | None = None
        task: InterfaceRosemaryTask | None = None

        try:
            async with self.db_connector.get_session() as session:
                try:
                    query = select(RosemaryTaskModel).where(
                        RosemaryTaskModel.id == id_task
                    )
                    result = await session.execute(query)
                    task_db: RosemaryTaskModel = result.scalars().one()
                except Exception as e:
                    self.logger.exception(f'Task [{id_task}] -> can\'t get  from DB {e}', exc_info=e)
                    return
                try:
                    task = self.get_task_by_name(task_db.name)()
                except KeyError:
                    error = 'Task not registered in rosemary!'
                except Exception as e:
                    error = f'{e.__class__.__name__}: {repr(e)}. Traceback: {traceback.print_tb(e.__traceback__)}'
                else:
                    try:
                        self.logger.info(f'>Task [{id_task}] -> "{task_db.name}" >>Started. Data: {task_db.data}')
                        result_task = await asyncio.wait_for(
                            task.prepare_and_run(task_db.data, session), timeout=task_db.timeout
                        )
                        self.logger.info(f'<Task [{id_task}] -> "{task_db.name}" <<Finished. Data: {task_db.data}. '
                                         f'Result: {result_task}')
                    except Exception as e:
                        if isinstance(e, asyncio.TimeoutError):
                            error = f'TimeoutError: The task has timed out {task_db.timeout}'
                        else:
                            error = (f'{e.__class__.__name__}: {repr(e)}. '
                                     f'Traceback: {traceback.print_tb(e.__traceback__)}')
                        self.logger.info(
                            f'<Task [{id_task}] -> "{task_db.name}". <<Exception. Data: {task_db.data}. Error: {error}'
                        )
                if error:
                    task_db.error = error
                    if task_db.retry >= task_db.max_retry:
                        will_not_repeat = True
                        task_db.status = StatusTaskRosemary.FATAL.value
                    else:
                        will_not_repeat = False
                        task_db.status = StatusTaskRosemary.FAILED.value
                        task_db.delay = task.delay_retry.get_datetime_plus_interval()
                else:
                    task_db.status = StatusTaskRosemary.FINISHED.value
                    task_db.task_return = str(result_task)
                    will_not_repeat = True
                await session.commit()
            if (will_not_repeat
                    and task and task.get_type() == TypeTaskRosemary.REPEATABLE.value
                    and self.get_task_by_name(task_db.name)):
                await task.create(
                    data=task_db.data, session=session, delay=task.delay_repeat.get_datetime_plus_interval()
                )
        except Exception as e:
            self.logger.exception(f'>>> Error while creating session for DB {e}. Task: {id_task}', exc_info=e)
        finally:
            await pool.release()
