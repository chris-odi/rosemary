from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    POSTGRES_USER: str = 'postgres'
    POSTGRES_PASSWORD: str = 'postgres'
    POSTGRES_HOST: str = 'postgres'
    POSTGRES_PORT: int | str = '5432'
    POSTGRES_DB: str = 'postgres'


settings = Settings()
