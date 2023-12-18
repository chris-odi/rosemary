import contextlib

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import MetaData



class DBConnector:
    def __init__(
            self, host: str, db: str, user: str, password: str, port: int | str = 5432, schema: str = 'public'
    ):
        self.__connect_string = self._build_asyncpg_connect_string(host, db, user, password, port)
        self.engine = create_async_engine(
            self.__connect_string,
            connect_args={"server_settings": {"search_path": schema}}
        )

    def _build_asyncpg_connect_string(self,
            host: str, db: str, user: str, password: str, port: int | str = 5432
    ):
        return f'postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}'

    @contextlib.asynccontextmanager
    async def get_session(self):
        async with AsyncSession(self.engine, expire_on_commit=False) as session:
            yield session
