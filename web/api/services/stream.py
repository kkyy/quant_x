"""Log capture -> SSE event helpers."""
from __future__ import annotations

import logging
import asyncio
from typing import Optional


class SSELogHandler(logging.Handler):
    """Captures log records and emits them as SSE events via TaskManager."""

    def __init__(self, task_manager, task_id: str, loop: Optional[asyncio.AbstractEventLoop] = None):
        super().__init__()
        self.task_manager = task_manager
        self.task_id = task_id
        self._loop = loop

    def emit(self, record: logging.LogRecord):
        msg = self.format(record)
        data = {"level": record.levelname.lower(), "message": msg}
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(
                self.task_manager.emit, self.task_id, "log", data
            )
        else:
            self.task_manager.emit(self.task_id, "log", data)
