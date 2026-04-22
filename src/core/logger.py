import os
import logging
from datetime import datetime

LOG_DIR         = "logs"
LOG_FILE_PREFIX = "log"
LOG_FORMAT      = "%(asctime)s | %(name)-32s | %(levelname)-8s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
FILE_LEVEL      = "DEBUG"
CONSOLE_LEVEL   = "INFO"

try:
    os.makedirs(LOG_DIR, exist_ok=True)
except Exception as e:
    print(f"[WARN] Could not create log directory '{LOG_DIR}': {e}")


def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger with file and console handlers.

    Parameters
    ----------
    name : logger name (typically __name__)

    Returns
    -------
    logging.Logger
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    try:
        # File handler — DEBUG and above
        log_file = os.path.join(LOG_DIR, f"{LOG_FILE_PREFIX}_{datetime.now():%Y%m%d}.log")
        fh = logging.FileHandler(log_file)
        fh.setLevel(getattr(logging, FILE_LEVEL))
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception as e:
        print(f"[WARN] Could not attach file handler for logger '{name}': {e}")

    # Console handler — INFO and above
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, CONSOLE_LEVEL))
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger