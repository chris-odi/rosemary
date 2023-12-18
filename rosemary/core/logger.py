import logging


def get_logger(name_worker: str):
    logger = logging.getLogger(f'Rosemary Task -> {name_worker}')
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    return logger
