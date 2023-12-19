from pathlib import Path

from alembic.config import Config
from alembic import command
import logging


class Alembic:
    def __init__(self, host: str, db: str, user: str, password: str | None, port: int | str, schema: str):
        alembic_logger = logging.getLogger('alembic')
        handler = logging.StreamHandler()  # Вы можете выбрать другой handler при необходимости
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        alembic_logger.addHandler(handler)
        alembic_logger.setLevel(logging.DEBUG)  # Или другой уровень логирования по вашему выбору

        __connect_string = f'postgresql://{user}:{password}@{host}:{port}/{db}?options=-csearch_path={schema}'
        __root_dir = Path(__file__).parent.parent.parent
        __alembic_dir = __root_dir / 'rosemary/alembic'
        alembic_logger.info(__alembic_dir)

        self.__alembic_cfg = Config()
        self.__alembic_cfg.set_main_option("sqlalchemy.url", __connect_string)
        self.__alembic_cfg.set_main_option("script_location", str(__alembic_dir))


    def upgrade(self, revision):
        command.upgrade(self.__alembic_cfg, revision=revision)
