import asyncio


class CustomSemaphore(asyncio.Semaphore):
    def __init__(self, value):
        super().__init__(value)
        self._initial_value = value

    def tasks_remaining(self):
        return self._initial_value - self._value
