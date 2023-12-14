import uuid


class RosemaryWorker:
    def __init__(self):
        self.uuid = uuid.uuid4()

    async def register_in_db(self):
        ...

    async def ping(self):
        ...
