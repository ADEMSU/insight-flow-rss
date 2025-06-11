# scheduler.py
"""Central scheduler for InsightFlow RSS pipeline."""

import asyncio
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from insightflow_service import InsightFlow
from relevance_checker import RelevanceChecker
from content_classifier import classify_relevant_posts_task
from data_manager import DataManager  # для fetch_posts
from db_manager import DBManager
from datetime import timedelta


# Часовой пояс (МСК)
TZ_MOSCOW = ZoneInfo("Europe/Moscow")

# Планировщик (глобальный)
scheduler: AsyncIOScheduler | None = None


def _mnow() -> datetime:
    """Return timezone-aware now in MSK."""
    return datetime.now(TZ_MOSCOW)


# ─────────────────────────────────────────────
#  Pipeline tasks
# ─────────────────────────────────────────────

async def hourly_pipeline():
    logger.info("[⏰] Hourly pipeline started")

    # 1. Получаем посты из RSS
    manager = DataManager()
    all_posts = await manager.fetch_posts()
    logger.info("Fetched {} posts from RSS", len(all_posts))

    # 2. Фильтруем посты за последние 24 часа
    now = datetime.now(TZ_MOSCOW)
    day_ago = now - timedelta(days=1)
    recent_posts = [
        p for p in all_posts if p.published_on and p.published_on >= day_ago
    ]
    logger.info("Отобрано {} постов за последние сутки", len(recent_posts))

    # 3. Убираем те, которые уже есть в базе (по post_id или url)
    db = DBManager()
    existing_urls = set(db.get_all_posts_urls())
    new_posts = [p for p in recent_posts if p.url not in existing_urls]
    logger.info("Найдено {} новых постов (не в БД)", len(new_posts))

    # 4. Сохраняем только новые посты в БД
    db.save_posts_bulk(new_posts)

    # 5. Проверяем релевантность только новых
    checker = RelevanceChecker()
    await checker.process_unchecked_posts()

    # 6. Классифицируем релевантные
    await classify_relevant_posts_task()

    logger.success("Hourly pipeline finished")


async def daily_digest():
    """Daily digest at 09:00 MSK via InsightFlow"""
    logger.info("[📊] Daily digest started via InsightFlow")
    service = InsightFlow()
    await service.run_daily_job()


# ─────────────────────────────────────────────
#  Scheduler setup
# ─────────────────────────────────────────────

def _configure_scheduler() -> AsyncIOScheduler:
    global scheduler
    if scheduler is not None:
        return scheduler

    scheduler = AsyncIOScheduler(timezone=TZ_MOSCOW)

    # Добавляем задачи по расписанию
    scheduler.add_job(hourly_pipeline, CronTrigger(minute=0))
    scheduler.add_job(daily_digest, CronTrigger(hour=9, minute=0))

    scheduler.start()
    logger.info("Scheduler started: hourly + daily digest at 09:00 MSK")
    return scheduler


async def _run_on_startup():
    """Run hourly pipeline if RUN_ON_STARTUP=true."""
    if os.getenv("RUN_ON_STARTUP", "false").lower() == "true":
        logger.info("RUN_ON_STARTUP=true → executing hourly_pipeline once at boot")
        await hourly_pipeline()


async def _main_async():
    """Async entry point"""
    _configure_scheduler()
    await _run_on_startup()

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.warning("Scheduler shut down")


def main():
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
