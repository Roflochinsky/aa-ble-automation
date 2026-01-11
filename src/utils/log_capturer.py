# src/utils/log_capturer.py
import io
import logging
from datetime import datetime

class MemoryLogHandler(logging.Handler):
    """Хендлер для накопления логов в памяти (для команды /logs)."""
    def __init__(self):
        super().__init__()
        self.log_buffer = io.StringIO()
        self.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    def emit(self, record):
        msg = self.format(record)
        self.log_buffer.write(msg + '\n')

    def get_logs(self):
        return self.log_buffer.getvalue()

    def clear(self):
        self.log_buffer.truncate(0)
        self.log_buffer.seek(0)

# Глобальный экземпляр для доступа из разных модулей
memory_handler = MemoryLogHandler()

