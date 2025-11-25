import os
import time
from datetime import datetime, timedelta
from loguru import logger


def clean_old_logs(logs_dir: str, keep_days: int = 21) -> int:
    """Delete files in logs_dir older than keep_days. Returns deleted count."""
    if not os.path.isdir(logs_dir):
        return 0
    now = time.time()
    threshold = now - keep_days * 86400
    deleted = 0
    for name in os.listdir(logs_dir):
        path = os.path.join(logs_dir, name)
        try:
            if not os.path.isfile(path):
                continue
            mtime = os.path.getmtime(path)
            if mtime < threshold:
                os.remove(path)
                deleted += 1
        except Exception as e:
            logger.warning(f"Не удалось удалить {path}: {e}")
    logger.info(f"Очистка логов: удалено {deleted} файлов старше {keep_days} дней из {logs_dir}")
    return deleted

