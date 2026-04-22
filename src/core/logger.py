import logging
import sys
import os

_LOGGER_INITIALIZED = False


def setup_logger(log_file="logs/app.log"):
    global _LOGGER_INITIALIZED

    if _LOGGER_INITIALIZED:
        return

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-5s | %(name)-15s | %(message)s"
    )

    ch = logging.StreamHandler(sys.stdout)
    fh = logging.FileHandler(log_file)

    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    root_logger.addHandler(ch)
    root_logger.addHandler(fh)

    _LOGGER_INITIALIZED = True


def get_logger(name: str):
    return logging.getLogger(name)