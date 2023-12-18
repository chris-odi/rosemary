import asyncio


class CustomSemaphore(asyncio.Semaphore):
    def __init__(self, value):
        super().__init__(value)
        self._initial_value = value
        self._current_value = value

    def tasks_remaining(self):
        return self._current_value

    async def acquire(self):
        await super().acquire()
        self._current_value -= 1

    async def release(self):
        super().release()
        self._current_value += 1
