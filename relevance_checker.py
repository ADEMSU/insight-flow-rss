import asyncio
from loguru import logger
from db_manager import DBManager
from lm_studio_client import LMStudioClient


class RelevanceChecker:
    def __init__(self):
        self.db_manager = DBManager()
        self.lm_client = LMStudioClient()

    async def process_unchecked_posts(self):
        logger.info("🔍 Поиск постов с relevance = NULL...")
        posts = self.db_manager.get_unchecked_posts(limit=None)

        if not posts:
            logger.info("✅ Нет постов для проверки релевантности")
            return 0

        logger.info(f"🔍 Найдено {len(posts)} непроверенных постов")
        results = {}

        for i, post in enumerate(posts, 1):
            if not post.title and not post.content:
                logger.warning(f"{post.post_id} — пустой контент, пропускаем")
                continue

            if len((post.title or '') + (post.content or '')) < 50:
                logger.warning(f"{post.post_id} — слишком короткий контент для анализа")
                continue

            try:
                relevant, score = await self.lm_client.check_relevance(
                    post_id=post.post_id,
                    title=post.title,
                    content=post.content,
                )
                results[post.post_id] = (relevant, score)
                logger.info(f"[{i}/{len(posts)}] {post.post_id}: rel={relevant}, score={score:.2f}")
            except Exception as e:
                logger.error(f"Ошибка при проверке {post.post_id}: {e}")

        updated = self.db_manager.update_posts_relevance_batch(results)
        logger.success(f"✅ Обновлено {updated} постов (relevance + score)")
        return updated


if __name__ == "__main__":
    asyncio.run(RelevanceChecker().process_unchecked_posts())
