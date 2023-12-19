from pathlib import Path

from alembic.config import Config
from alembic import command
import logging


class Alembic:
    def __init__(self, host: str, db: str, user: str, password: str | None, port: int | str, schema: str):
        logging.basicConfig()
        logging.getLogger('alembic').setLevel(logging.INFO)
        __connect_string = f'postgresql://{user}:{password}@{host}:{port}/{db}?options=-csearch_path={schema}'
        __root_dir = Path(__file__).parent.parent.parent
        __alembic_dir = __root_dir / 'rosemary/alembic'
        self.__alembic_cfg = Config()
        self.__alembic_cfg.set_main_option("sqlalchemy.url", __connect_string)
        self.__alembic_cfg.set_main_option("script_location", str(__alembic_dir))


    def upgrade(self, revision):
        command.upgrade(self.__alembic_cfg, revision=revision)
