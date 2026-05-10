import logging
import sys
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler

_LOGGER_INITIALIZED = False
LOG_DIR = "logs"
LOG_FILE = "application.log"
LOG_FORMAT = (
    "%(asctime)s | "
    "%(levelname)-5s | "
    "%(name)-20s | "
    "%(message)s"
)

def setup_logger():
    global _LOGGER_INITIALIZED

    if _LOGGER_INITIALIZED:
        return

    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(LOG_FORMAT)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # avoid duplicate handlers
    if root_logger.handlers:
        root_logger.handlers.clear()

    # console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # rotating file
    file_handler = TimedRotatingFileHandler(
        filename=Path(LOG_DIR) / LOG_FILE,
        when="midnight",
        interval=1,
        backupCount=7,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    _LOGGER_INITIALIZED = True


def get_logger(name:str) -> logging.Logger:
    setup_logger()
    return logging.getLogger(name)
