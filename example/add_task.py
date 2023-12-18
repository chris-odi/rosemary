import random

import asyncio
from pydantic import BaseModel

from example.tasks import SleepTask, RepeatableTask, RepeatableTaskModel, ErrorTask, WorkWithDBTask


class A(BaseModel):
    x: int


async def main():
    for _ in range(100):
        a = A(x=random.randint(1, 100))
        task = await SleepTask().create(data=a)
        print(task)
        task = await ErrorTask().create(data={random.randint(1, 100): random.randint(1, 100)})
        print(task)

    task = await WorkWithDBTask().create()
    print(task)
    task = await RepeatableTask().create(data=RepeatableTaskModel(time_sleep=7))
    print(task)

asyncio.run(main())
