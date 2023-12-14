import datetime
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from rosemary.db.models import RosemaryWorkerModel


class RosemaryWorker:
    def __init__(self):
        self.uuid = uuid.uuid4()
        self.worker = None

    async def register_in_db(self, session: AsyncSession):
        self.worker: RosemaryWorkerModel = RosemaryWorkerModel(uuid=self.uuid)
        await session.commit()

    async def ping(self, session: AsyncSession):
        self.worker.ping = datetime.datetime.utcnow()
        await session.commit()
