import asyncio
from abc import abstractmethod, ABC


class RosemaryWorkerInterface(ABC):
    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._looping())
        loop.close()

    @abstractmethod
    async def _looping(self):
        ...
