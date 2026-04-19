import os
import logging
from datetime import datetime

LOG_DIR = "logs"
LOG_FILE_PREFIX = "log"
LOG_FORMAT = "%(asctime)s | %(name)-32s | %(levelname)-8s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
FILE_LEVEL = "DEBUG"
CONSOLE_LEVEL = "INFO"

os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(name:str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    
    log_file = os.path.join(LOG_DIR, f"{LOG_FILE_PREFIX}_{datetime.now():%y%m%d}.log")
    fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_file)
    fh.setLevel(getattr(logging, FILE_LEVEL))
    fh.setFormatter(fmt)

    fh = logging.StreamHandler()
    fh.setLevel(getattr(logging, CONSOLE_LEVEL))
    fh.setFormatter(fmt)

    logger.addFilter(fh)
    logger.addFilter(ch)

    return logger