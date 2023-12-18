from pathlib import Path

from alembic.config import Config
from alembic import command


def alembic_upgrade_head(
        host: str, db: str, user: str, password: str | None = None, port: int | str = 5432, schema: str = 'public'
):
    connect_string = f'postgresql://{user}:{password}@{host}:{port}/{db}?options=-csearch_path={schema}'
    root_dir = Path(__file__).parent.parent.parent
    alembic_dir = root_dir / 'rosemary/alembic'
    alembic_cfg = Config()
    alembic_cfg.set_main_option("sqlalchemy.url", connect_string)
    alembic_cfg.set_main_option("script_location", str(alembic_dir))
    command.upgrade(alembic_cfg, "head")
