# =============================================================================
# common/logger.py
# Structured logging for the price tracking pipeline.
# In Databricks, output surfaces in the cluster driver logs and job run UI.
# Usage:
#     from common.logger import get_logger
#     log = get_logger(__name__)
#     log.info("Processing %s", retailer_name)
# =============================================================================

import logging
import sys


def get_logger(name: str = "price_tracking") -> logging.Logger:
    """
    Return a logger configured for Databricks.
    Calling this multiple times with the same name returns the same logger.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        # Already configured — avoid adding duplicate handlers
        return logger

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Prevent log records from propagating to the root logger
    logger.propagate = False

    return logger
