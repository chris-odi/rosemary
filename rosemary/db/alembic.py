from alembic.config import Config
from alembic import command


def alembic_upgrade_head(host: str, db: str, user: str, password: str | None = None, port: int | str = 5432):
    connect_string = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    alembic_cfg = Config()
    alembic_cfg.set_main_option("sqlalchemy.url", connect_string)
    alembic_cfg.set_main_option("script_location", "alembic")
    command.upgrade(alembic_cfg, "head")
