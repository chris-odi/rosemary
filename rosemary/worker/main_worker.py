import uuid
from logging import Logger

import asyncio
import threading
from sqlalchemy import update, select, func, and_, or_, delete, exists
from sqlalchemy.ext.asyncio import AsyncSession

from rosemary import RosemaryInterval
from rosemary.core.logger import get_logger
from rosemary.db.db import DBConnector
from rosemary.db.models import RosemaryTaskModel, RosemaryWorkerModel
from rosemary.settings import TIME_FOR_WAITING_PING, TIME_FACTOR_TO_FIX_STATUS, PAUSE_FOR_CYCLE_MAIN_WORKER, \
    TIME_FOR_DELETE_OLD_WORKER
from rosemary.tasks.constants import StatusTaskRosemary
from rosemary.worker.constants import StatusWorkerRosemary, ESCAPE_ERROR
from rosemary.worker.worker_interface import RosemaryWorkerInterface


class RosemaryMainWorker(RosemaryWorkerInterface):
    def __init__(
            self,
            db_host: str,
            db_port: int | str,
            db_user: str,
            db_password: str,
            db_name_db: str,
            db_schema: str,
            shutdown_event: threading.Event,
            logger: Logger | None = None,
            delete_old_tasks: RosemaryInterval | None = None,
    ):
        self._delete_old_tasks = delete_old_tasks
        self.db_connector = None
        self.logger: Logger | None = get_logger('MAIN') or logger

        self.db_connector = DBConnector(
            db_host, db_name_db, db_user, db_password, db_port, db_schema
        )
        self.__shutdown_event = shutdown_event

    async def __check_stuck_tasks_by_other_workers(self, session: AsyncSession):
        select_query = select(RosemaryTaskModel.id).join(RosemaryWorkerModel).where(
            and_(
                RosemaryTaskModel.status.in_([
                    StatusTaskRosemary.FAILED.value,
                    StatusTaskRosemary.NEW.value,
                ]),
                RosemaryWorkerModel.status == StatusWorkerRosemary.KILLED.value,
                RosemaryTaskModel.retry >= RosemaryTaskModel.max_retry
            )
        ).with_for_update(skip_locked=True)
        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()

        update_status = update(RosemaryTaskModel).where(
            RosemaryTaskModel.id.in_(ids_to_update)
        ).values(
            status=StatusTaskRosemary.FATAL.value,
            worker=None
        )
        await session.execute(update_status)
        await session.commit()

        select_query = select(RosemaryTaskModel.id).join(RosemaryWorkerModel).where(
            and_(
                RosemaryTaskModel.status.in_([
                    StatusTaskRosemary.FAILED.value,
                    StatusTaskRosemary.NEW.value,
                ]),
                RosemaryWorkerModel.status == StatusWorkerRosemary.KILLED.value,
                RosemaryTaskModel.retry < RosemaryTaskModel.max_retry
            )
        ).with_for_update(skip_locked=True)

        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()
        update_status = update(RosemaryTaskModel).where(
            RosemaryTaskModel.id.in_(ids_to_update)
        ).values(
            status=StatusTaskRosemary.FAILED.value,
            worker=None
        )
        await session.execute(update_status)
        await session.commit()

        # IN PROGRESS
        select_query = select(RosemaryTaskModel.id).join(RosemaryWorkerModel).where(
            and_(
                RosemaryTaskModel.status == StatusTaskRosemary.IN_PROGRESS.value,
                RosemaryWorkerModel.status == StatusWorkerRosemary.KILLED.value,
                RosemaryTaskModel.retry < RosemaryTaskModel.max_retry
            )
        ).with_for_update(skip_locked=True)
        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()
        update_status = update(RosemaryTaskModel).where(
            RosemaryTaskModel.id.in_(ids_to_update)
        ).values(
            status=StatusTaskRosemary.FAILED.value,
            worker=None,
            error=ESCAPE_ERROR
        )
        await session.execute(update_status)
        await session.commit()

        select_query = select(RosemaryTaskModel.id).join(RosemaryWorkerModel).where(
            and_(
                RosemaryTaskModel.status == StatusTaskRosemary.IN_PROGRESS.value,
                RosemaryWorkerModel.status == StatusWorkerRosemary.KILLED.value,
                RosemaryTaskModel.retry >= RosemaryTaskModel.max_retry
            )
        ).with_for_update(skip_locked=True)
        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()

        update_status = update(RosemaryTaskModel).where(
            RosemaryTaskModel.id.in_(ids_to_update)
        ).values(
            status=StatusTaskRosemary.FATAL.value,
            worker=None,
            error=ESCAPE_ERROR
        )
        await session.execute(update_status)
        await session.commit()

    async def __check_deaths_workers(self, session: AsyncSession):
        select_query = select(RosemaryWorkerModel.id).where(
            and_(
                RosemaryWorkerModel.status == StatusWorkerRosemary.WORKING.value,
                RosemaryWorkerModel.updated_at < (
                        func.now() - func.make_interval(0, 0, 0, 0, 0, TIME_FOR_WAITING_PING, 0)
                ),
            )
        ).with_for_update(skip_locked=True)
        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()
        if ids_to_update:
            self.logger.error(f'Workers {ids_to_update} is checking')
            update_status = update(RosemaryWorkerModel).where(
                RosemaryWorkerModel.id.in_(ids_to_update)
            ).values(
                status=StatusWorkerRosemary.CHECKING.value
            )
            await session.execute(update_status)
        await session.commit()
        select_query = select(RosemaryWorkerModel.id).where(
            and_(
                RosemaryWorkerModel.status == StatusWorkerRosemary.CHECKING.value,
                RosemaryWorkerModel.updated_at < (
                        func.now() - func.make_interval(0, 0, 0, 0, 0, TIME_FOR_WAITING_PING, 0)
                ),
            )
        ).with_for_update(skip_locked=True)
        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()
        if ids_to_update:
            self.logger.error(f'Workers {ids_to_update} is killed!!!')
            update_status = update(RosemaryWorkerModel).where(
                RosemaryWorkerModel.id.in_(ids_to_update)
            ).values(
                status=StatusWorkerRosemary.KILLED.value
            )
            await session.execute(update_status)

    async def __check_stuck_tasks(self, session: AsyncSession):
        select_query = select(RosemaryTaskModel.id).join(RosemaryWorkerModel).where(
            and_(
                RosemaryTaskModel.status == StatusTaskRosemary.IN_PROGRESS.value,
                or_(
                    RosemaryTaskModel.updated_at < (
                            func.now() - func.make_interval(0, 0, 0, 0, 0, 0, (RosemaryTaskModel.timeout * TIME_FACTOR_TO_FIX_STATUS))
                    ),
                    RosemaryWorkerModel.status == StatusWorkerRosemary.KILLED.value,
                ),
                RosemaryTaskModel.retry >= RosemaryTaskModel.max_retry
            )
        ).with_for_update(skip_locked=True)
        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()
        update_status = update(RosemaryTaskModel).where(
            RosemaryTaskModel.id.in_(ids_to_update)
        ).values(
            status=StatusTaskRosemary.FATAL.value,
            error=ESCAPE_ERROR,
        )
        await session.execute(update_status)
        await session.commit()

        select_query = select(RosemaryTaskModel.id).join(RosemaryWorkerModel).where(
            and_(
                RosemaryTaskModel.status == StatusTaskRosemary.IN_PROGRESS.value,
                or_(
                    RosemaryTaskModel.updated_at < (
                            func.now() - func.make_interval(0, 0, 0, 0, 0, 0, (RosemaryTaskModel.timeout * TIME_FACTOR_TO_FIX_STATUS))
                    ),
                    RosemaryWorkerModel.status == StatusWorkerRosemary.KILLED.value,
                ),
                RosemaryTaskModel.retry < RosemaryTaskModel.max_retry
            )
        ).with_for_update(skip_locked=True)
        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()
        update_status = update(RosemaryTaskModel).where(
            RosemaryTaskModel.id.in_(ids_to_update)
        ).values(
            status=StatusTaskRosemary.FAILED.value,
            error=ESCAPE_ERROR,
        )
        await session.execute(update_status)
        await session.commit()

    async def __delete_old_tasks(self, session: AsyncSession):
        if self._delete_old_tasks:
            query = delete(RosemaryTaskModel).where(
                and_(
                    RosemaryTaskModel.status.in_([
                        StatusTaskRosemary.FATAL.value,
                        StatusTaskRosemary.FINISHED.value,
                    ]),
                    RosemaryTaskModel.updated_at < func.now() - self._delete_old_tasks.get_interval()
                )
            )
            await session.execute(query)
            await session.commit()

    async def __delete_old_worker(self, session: AsyncSession):
        subquery = ~exists().where(RosemaryTaskModel.worker == RosemaryWorkerModel.id)

        query = delete(RosemaryWorkerModel).where(
            and_(
                RosemaryWorkerModel.status == StatusWorkerRosemary.KILLED.value,
                RosemaryWorkerModel.ping_time < func.now() - func.make_interval(
                    0, 0, 0, 0, TIME_FOR_DELETE_OLD_WORKER, 0, 0
                ),
                subquery
            )
        )
        await session.execute(query)
        await session.commit()

    async def _looping(self):
        async with self.db_connector.get_session() as session:
            self.logger.info(f'Start looping by main worker')
            while not self.__shutdown_event.is_set():
                await self.__check_stuck_tasks_by_other_workers(session)
                await self.__check_stuck_tasks(session)
                await self.__check_deaths_workers(session)
                await self.__delete_old_worker(session)
                await self.__delete_old_tasks(session)

                await asyncio.sleep(PAUSE_FOR_CYCLE_MAIN_WORKER)
            self.logger.info(f'Rosemary main worker is shutdown warm!')
