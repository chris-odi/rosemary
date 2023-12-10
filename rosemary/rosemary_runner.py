import asyncio
import signal
from logging import Logger

from sqlalchemy import select, update, and_, Result
from sqlalchemy.ext.asyncio import AsyncSession

from rosemary.constants import StatusTaskRosemary
from rosemary.custom_semaphore import CustomSemaphore
from rosemary.db.db import async_session
from rosemary.db.models import RosemaryTaskModel
from rosemary.rosemary_builder import RosemaryBuilder
from rosemary.task_inreface import RosemaryTask


class RosemaryRunner:

    def __init__(self, rosemary_builder: RosemaryBuilder):
        self.Rosemary_builder: RosemaryBuilder = rosemary_builder
        self._max_task_semaphore: int = 50
        self.logger: Logger = rosemary_builder.logger
        self.__shutdown_requested: bool = False

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
        ).limit(limit)
        res = await session.execute(select_query)
        ids_to_update = res.scalars().all()

        update_status = update(RosemaryTaskModel).where(
            RosemaryTaskModel.status.in_(
            )
        ).values(
            status=StatusTaskRosemary.IN_PROGRESS.value,
            worker=worker_name,
        ).limit(limit)
        await session.execute(update_status)

        query = select(RosemaryTaskModel.id).where(and_(
            RosemaryTaskModel.status == StatusTaskRosemary.IN_PROGRESS.value,
            RosemaryTaskModel.worker == worker_name
        ))
        tasks_ids_row: Result = await session.execute(query)
        ids_tasks = [task_id_row[0] for task_id_row in tasks_ids_row]
        return ids_tasks

    async def _run_migration(self, conn: AsyncSession):
        ...

    async def _run_looping(self, session):
        semaphore = CustomSemaphore(self._max_task_semaphore)
        while True:
            if self.__shutdown_requested:
                self.logger.info('Rosemary runner is shotdowned warm!')
                return
            if semaphore.tasks_remaining() > 0:
                ids_tasks = await self._get_new_tasks(session, semaphore.tasks_remaining(), session)
                for id_task in ids_tasks:
                    await semaphore.acquire()
                    asyncio.create_task(self._run_task(id_task, semaphore, session))
            else:
                # Skip one run cycle
                await asyncio.sleep(0.1)

    async def run(self):
        async with async_session() as session:
            await self._run_migration(session)
            await self._run_looping(session)

    async def _run_task(self, id_task: int, pool: CustomSemaphore, session: AsyncSession):
        try:
            async with session.begin():
                query = select([RosemaryTaskModel]).where(
                    RosemaryTaskModel.id == id_task
                ).with_for_update()
                record = await session.execute(query)
                task_db = record.fetchone()

                task: RosemaryTask = self.Rosemary_builder.get_task_by_name(task_db.name)
                await asyncio.wait_for(task.run(task.prepare_data(task_db.data)), timeout=task_db.timeout)
        except asyncio.TimeoutError:
            # Обработка таймаута
            pass
        except Exception as e:
            ...
        finally:
            pool.release()
