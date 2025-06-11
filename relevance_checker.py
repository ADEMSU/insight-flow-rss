import asyncio
import logging
import logging.handlers
from loguru import logger
import sys
import traceback
from datetime import datetime
from typing import Dict, List, Tuple

from sqlalchemy import func

from db_manager import DBManager, PostModel
from lm_studio_client import LMStudioClient

# ---------------------------------------------------------------------------
#  LOGGING
# ---------------------------------------------------------------------------
logger = logging.getLogger("insightflow.relevance_checker")
logger.setLevel(logging.INFO)

# stderr (human‑readable)
handler_stream = logging.StreamHandler(sys.stderr)
handler_stream.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", "%Y-%m-%d %H:%M:%S")
)
logger.addHandler(handler_stream)

# file (rotating ― 10 MB per file)
handler_file = logging.handlers.RotatingFileHandler(
    "/app/logs/relevance_checker.log", maxBytes=10 * 1024 * 1024, backupCount=5
)
handler_file.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")
)
logger.addHandler(handler_file)

# ---------------------------------------------------------------------------
#  MAIN CLASS
# ---------------------------------------------------------------------------
class RelevanceChecker:
    """Проверка релевантности непроверенных постов с помощью LM Studio."""

    #: размер батча для одного параллельного прохода
    batch_size: int = 10
    #: максимальное число одновременных HTTP‑запросов к LM Studio
    max_concurrent: int = 3

    def __init__(self) -> None:
        # DB -----------------------------------------------------------------
        try:
            self.db: DBManager = DBManager()
            logger.info("DBManager инициализирован")
        except Exception as exc:
            logger.exception("Не удалось создать DBManager: %s", exc)
            raise

        # LM Studio -----------------------------------------------------------
        try:
            self.lm: LMStudioClient = LMStudioClient()
            logger.info("LM Studio клиент инициализирован (%s)", self.lm.base_url)
        except Exception as exc:
            logger.exception("Не удалось инициализировать LM Studio: %s", exc)
            raise

        # семафор ограничивает параллелизм внутри батча
        self._semaphore = asyncio.Semaphore(self.max_concurrent)

    # ---------------------------------------------------------------------
    #  PUBLIC API
    # ---------------------------------------------------------------------
    async def process_unchecked_posts(self, limit: int = 50) -> int:
        """Главная точка входа — проверяет *до* ``limit`` непроверенных постов.

        Возвращает количество постов, для которых обновлена колонка
        ``relevance``.
        """
        if not await self._ensure_lm_alive():
            return 0

        posts = self.db.get_unchecked_posts(limit)
        if not posts:
            logger.info("Непроверенных постов нет")
            return 0

        logger.info("Начало проверки релевантности: %s постов", len(posts))
        processed = 0

        # батч‑обработка -----------------------------------------------------
        for start in range(0, len(posts), self.batch_size):
            batch = posts[start : start + self.batch_size]
            batch_num = start // self.batch_size + 1
            logger.info("Батч %s ⇒ %s постов", batch_num, len(batch))

            results = await self._check_batch(batch)
            if results:
                processed += self.db.update_posts_relevance_batch(results)
                relevant_in_batch = sum(1 for r, _ in results.values() if r)
                logger.info(
                    "Батч %s завершён: релевантно %s из %s", batch_num, relevant_in_batch, len(results)
                )

            if start + self.batch_size < len(posts):
                await asyncio.sleep(1)  # мягкая пауза между батчами

        logger.info("Проверка завершена, обновлено %s постов", processed)
        self._log_global_stats()
        return processed

    # ---------------------------------------------------------------------
    #  INTERNAL HELPERS
    # ---------------------------------------------------------------------
    async def _ensure_lm_alive(self) -> bool:
        """Пингуем LM Studio, чтобы не тратить время на пустые запросы."""
        ok = await self.lm.test_connection()
        if not ok:
            logger.error("LM Studio API недоступен — остановка работы")
        return ok

    async def _check_batch(self, posts: List[PostModel]) -> Dict[str, Tuple[bool, float]]:
        """Запускает параллельную проверку батча и собирает результаты."""

        async def _check_single(post: PostModel) -> Tuple[str, Tuple[bool, float]]:
            async with self._semaphore:
                post_id = post.post_id  # строковый GUID
                title = post.title or ""
                content = post.content or ""

                # слишком короткий текст → нерелевантно без запроса к модели
                if len(title) + len(content) < 50:
                    return post_id, (False, 0.0)

                try:
                    relevance, score = await self.lm.check_relevance(post_id, title, content)
                    return post_id, (relevance, score)
                except Exception as exc:
                    logger.warning("Ошибка LM Studio для %s: %s", post_id, exc)
                    logger.debug("%s", traceback.format_exc())
                    return post_id, (False, 0.0)

        coros = [_check_single(p) for p in posts]
        pairs = await asyncio.gather(*coros)
        return {pid: val for pid, val in pairs}

    def _log_global_stats(self) -> None:
        """Печатает агрегированную статистику по таблице ``posts``."""
        with self.db.session_scope() as s:
            total = s.query(func.count(PostModel.id)).scalar() or 0
            relevant = (
                s.query(func.count(PostModel.id))
                .filter(PostModel.relevance.is_(True))
                .scalar()
                or 0
            )
        pct = relevant / total * 100 if total else 0.0
        logger.info("Всего постов: %s, релевантных: %s (%.1f%%)", total, relevant, pct)


# ---------------------------------------------------------------------------
#  ENTRY POINT FOR CLI
# ---------------------------------------------------------------------------
async def _cli(limit: int) -> int:
    checker = RelevanceChecker()
    return await checker.process_unchecked_posts(limit)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser("Проверка релевантности постов")
    parser.add_argument("--limit", type=int, default=50, help="Максимум постов за проход")
    args = parser.parse_args()

    logger.info("Запуск relevance_checker (limit=%s)", args.limit)
    processed = asyncio.run(_cli(args.limit))
    logger.info("Готово. Обработано %s постов", processed)
