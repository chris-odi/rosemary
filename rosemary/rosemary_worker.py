import datetime
import uuid
from logging import Logger

from sqlalchemy.ext.asyncio import AsyncSession

from rosemary.constants import StatusWorkerRosemary
from rosemary.db.models import RosemaryWorkerModel
from rosemary.logger import get_logger


class RosemaryWorker:
    def __init__(self):
        self.uuid = uuid.uuid4()
        self.worker: RosemaryWorkerModel | None = None
        self.db_connector = None
        self.logger: Logger | None = get_logger(str(self.uuid))

    async def register_in_db(self, session: AsyncSession):
        self.worker = RosemaryWorkerModel(uuid=self.uuid)
        session.add(self.worker)
        await session.commit()

    async def ping(self, session: AsyncSession):
        self.worker.ping_time = datetime.datetime.utcnow()
        self.worker.status = StatusWorkerRosemary.WORKING.value
        await session.commit()
