"""Logging setup."""
from __future__ import annotations
import logging
import sys
import warnings
from pathlib import Path
from datetime import datetime

# qlib index_data.py 在空切片上求均值时触发，属正常边界情况，无需暴露给用户
warnings.filterwarnings("ignore", message="Mean of empty slice", category=RuntimeWarning, module="qlib")


def setup_logger(
    name: str = "quant_ex",
    level: int = logging.INFO,
    log_dir: str = "./logs",
    console: bool = True,
) -> logging.Logger:
    """Return a logger with file + optional console handlers."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if console:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    fh = logging.FileHandler(f"{log_dir}/quant_ex_{today}.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger
