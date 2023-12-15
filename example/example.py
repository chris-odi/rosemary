import asyncio
import logging
import random
from datetime import datetime
from rosemary.rosemary import Rosemary

rosemary = Rosemary(
    db_host='0.0.0.0',
    db_password='postgres',
    db_port=5432,
    db_user='postgres',
    db_name_db='postgres',
    max_tasks_per_worker=100,
    workers=3,
)

logger = logging.getLogger('Task')


class SleepTask(rosemary.RosemaryTask):
    async def run(self, data):
        sleep = random.randint(1, 50)
        await asyncio.sleep(sleep)
        logger.error(f'Test {datetime.utcnow()}. Slept: {sleep}')


class CheckLastIdTask(rosemary.RosemaryTask):
    async def run(self, data):
        ...


rosemary.register_task(SleepTask)
rosemary.register_task(CheckLastIdTask)

rosemary.run()
