import contextlib

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


class DBConnector:
    def __init__(self, host: str, db: str, user: str, password: str, port: int | str = 5432):
        connect_string = self._build_asyncpg_connect_string(host, db, user, password, port)
        self.engine = create_async_engine(connect_string)
        self.AsyncSession = async_sessionmaker(self.engine, expire_on_commit=False)

    def _build_asyncpg_connect_string(self,
            host: str, db: str, user: str, password: str, port: int | str = 5432
    ):
        return f'postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}'

    @contextlib.asynccontextmanager
    async def get_session(self):
        assert self.engine is not None, "Database engine is not initialized."
        async with self.AsyncSession() as session:
            yield session
