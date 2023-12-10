from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import declarative_base


def get_session(SQLALCHEMY_DATABASE_URL: str):
    engine = create_async_engine(SQLALCHEMY_DATABASE_URL)
    async_session = async_sessionmaker(engine)
    return async_session


# SQLALCHEMY_DATABASE_URL = (
#     f'postgresql+asyncpg://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}'
#     f'@{settings.POSTGRES_HOST}:5432/{settings.POSTGRES_DB}')


Base = declarative_base()
