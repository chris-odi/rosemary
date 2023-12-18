import asyncio
import random
from datetime import datetime

from pydantic import BaseModel
from sqlalchemy import select

from example.rosemary_config import rosemary
from rosemary.constants import TypeTaskRosemary
from rosemary.db.models import RosemaryTaskModel
import logging

logger = logging.getLogger('Task')
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)


class SleepTask(rosemary.ManualTask):
    type_task = TypeTaskRosemary.MANUAL

    async def run(self, data):
        sleep = random.randint(1, 10)
        await asyncio.sleep(sleep)
        logger.error(f'Test {datetime.utcnow()}. Slept: {sleep}')
        return f"I slept {sleep} sec"


class WorkWithDBTask(rosemary.ManualTask):
    async def run(self, session):
        query = select(RosemaryTaskModel).where(
            RosemaryTaskModel.name == self.get_name()
        ).order_by(RosemaryTaskModel.id)
        result = await session.execute(query)
        task_db: RosemaryTaskModel = result.scalars().all()
        result = task_db[-1].id
        logger.info(f'Task ID GET: {result}')
        return result


class RepeatableTaskModel(BaseModel):
    time_sleep: int


class ErrorTask(rosemary.ManualTask):

    async def run(self, data):
        1/0


class RepeatableTask(rosemary.RepeatableTask):
    type_task = TypeTaskRosemary.REPEATABLE
    timeout = 10

    def prepare_data_for_run(self, data: dict):
        return RepeatableTaskModel(**data)

    async def run(self, data: RepeatableTaskModel):
        await asyncio.sleep(data.time_sleep)
        logger.info(f"I repeated at {datetime.utcnow()} with params {data}")
