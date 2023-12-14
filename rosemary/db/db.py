from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DBConnector:
    def __init__(self, host: str, db: str, user: str, password: str | None = None, port: int | str = 5432):
        connect_string = self.build_asyncpg_connect_string(host, db, user, password, port)
        self.engine = create_async_engine(connect_string)
        self.AsyncSession = async_sessionmaker(self.engine, expire_on_commit=False)

    def build_asyncpg_connect_string(self,
            host: str, db: str, user: str, password: str | None = None, port: int | str = 5432
    ):
        return f'postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}'

    # def init_db_engine(self, host, db, user, password, port):
    #     connect_string = self.build_asyncpg_connect_string(host, db, user, password, port)
    #     self.engine = create_async_engine(connect_string)
    #     self.AsyncSession = async_sessionmaker(self.engine, expire_on_commit=False)

    async def get_session(self):
        assert self.engine is not None, "Database engine is not initialized."
        async with self.AsyncSession() as session:
            yield session
