import asyncio
import random
from datetime import datetime
from rosemary.rosemary import Rosemary
import logging

# Создание логгера
logger = logging.getLogger('Rosemary Task')
logger.setLevel(logging.DEBUG)  # Установка уровня логирования для логгера

# Создание обработчика, который выводит логи в консоль
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Установка уровня логирования для обработчика

# Форматирование вывода логов
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Добавление обработчика к логгеру
logger.addHandler(console_handler)


rosemary = Rosemary(
    db_host='0.0.0.0',
    db_password='postgres',
    db_port=5432,
    db_user='postgres',
    db_name_db='postgres',
    max_tasks_per_worker=100,
    workers=1,
    logger=logger
)


class SleepTask(rosemary.Task):
    async def run(self, data):
        sleep = random.randint(1, 50)
        await asyncio.sleep(sleep)
        logger.error(f'Test {datetime.utcnow()}. Slept: {sleep}')


class CheckLastIdTask(rosemary.Task):
    async def run(self, data):
        async with self.get_session() as session:
            ...


rosemary.register_task(SleepTask)
rosemary.register_task(CheckLastIdTask)


async def main():
    await SleepTask().create(None)

asyncio.run(main())
rosemary.run()
