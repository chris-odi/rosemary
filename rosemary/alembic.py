from alembic.config import Config
from alembic import command


def alembic_upgrade_head(connect_string: str):
    alembic_cfg = Config()
    alembic_cfg.set_main_option("sqlalchemy.url", connect_string)
    alembic_cfg.set_main_option("script_location", "alembic")
    command.upgrade(alembic_cfg, "head")
