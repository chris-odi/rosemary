import random

import asyncio
from pydantic import BaseModel

from example.tasks import SleepTask, RepeatableTask, RepeatableTaskModel


class A(BaseModel):
    x: int


async def main():
    a = A(x=random.randint(1, 100))
    for _ in range(1000):
        task = await SleepTask().create(data=a)
        print(task)
    task = await RepeatableTask().create(data=RepeatableTaskModel(time_sleep=7))
    print(task)

asyncio.run(main())
