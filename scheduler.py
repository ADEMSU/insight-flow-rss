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
    await InsightFlow().run_hourly_job()

async def daily_digest():
    logger.info("[📊] Daily digest started via InsightFlow")
    await InsightFlow().run_daily_job()


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
    scheduler.add_job(daily_digest, CronTrigger(hour=9, minute=30))

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
    logger.info("[👋] Scheduler main() инициализирован")

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
