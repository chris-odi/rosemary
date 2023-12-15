import contextlib

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession


class DBConnector:
    def __init__(self, host: str, db: str, user: str, password: str, port: int | str = 5432):
        self.__connect_string = self._build_asyncpg_connect_string(host, db, user, password, port)
        self.engine = create_async_engine(
            self.__connect_string
        )

    def _build_asyncpg_connect_string(self,
            host: str, db: str, user: str, password: str, port: int | str = 5432
    ):
        return f'postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}'

    @contextlib.asynccontextmanager
    async def get_session(self):
        async with AsyncSession(self.engine, expire_on_commit=False) as session:
            yield session
