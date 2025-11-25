import os
import sys
import traceback
from datetime import datetime
from datetime import timedelta
import asyncio
import json

from loguru import logger
from stats_collector import StatsCollector
from dotenv import load_dotenv

# Импортируем созданные и обновленные модули
from data_manager import DataManager, get_msk_date_range
from text_preprocessing import TextPreprocessor
from lm_studio_client import LMStudioClient
from telegram_sender import TelegramSender
from token_estimator import TokenEstimator
from db_manager import DBManager
from post import Post

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logger.remove()
logger.add(sys.stderr, level="INFO")
logs_dir = os.path.join(os.getcwd(), "logs")
os.makedirs(logs_dir, exist_ok=True)
logger.add(
    os.path.join(logs_dir, "insightflow_{time}.log"),
    rotation="10 MB",
    retention="21 days",
    encoding="utf-8",
    enqueue=True,
    backtrace=False,
    diagnose=False,
    level="INFO",
)


class InsightFlow:
    def __init__(self):
        self.data_manager = DataManager()
        self.text_preprocessor = TextPreprocessor(
            similarity_threshold=0.65,
            min_content_length=100,
            max_tokens=50000
        )
        self.lm_client = LMStudioClient()
        self.telegram_sender = TelegramSender()
        self.token_estimator = TokenEstimator()

        try:
            self.db_manager = DBManager()
            self.db_manager.create_tables()
            logger.info("Подключение к базе данных успешно инициализировано")
        except Exception as e:
            logger.error(f"Ошибка при инициализации подключения к базе данных: {e}")
            self.db_manager = None

        logger.info("DataManager инициализирован (включая RSS и Медиалогию)")

    async def run_daily_job(self):
        try:
            from zoneinfo import ZoneInfo
            now = datetime.now(ZoneInfo("Europe/Moscow"))
            yesterday_09 = (now - timedelta(days=1)).replace(hour=9, minute=1, second=0, microsecond=0)
            today_09 = now.replace(hour=9, minute=0, second=0, microsecond=0)

            logger.info(f"📆 Ежедневный дайджест: {yesterday_09} → {today_09}")

            if not await self.lm_client.test_connection():
                logger.error("LM Studio недоступен, отменяем анализ")
                await self.telegram_sender.send_message("⚠️ LM Studio недоступен. Ежедневный анализ отменен.")
                return

            # 1. Получаем релевантные посты из БД
            db_posts = self.db_manager.get_relevant_posts(
                since=yesterday_09,
                until=today_09,
                limit=1000,
            )

            initial = [
                {
                    "post_id": p.post_id,
                    "title": p.title,
                    "content": p.content,
                    "url": p.url,
                    "score": p.relevance_score or 0.0,
                }
                for p in db_posts
                if (p.relevance_score or 0.0) >= 0.7 and p.content
            ]

            logger.info(f"🔎 Найдено {len(initial)} постов с score ≥ 0.7")

            # 2. Удаляем дубликаты
            unique = self.text_preprocessor.remove_duplicates(initial)
            logger.info(f"🧹 После удаления дубликатов: {len(unique)}")

            # 3. Повторная строгая проверка
            rechecked = await self.lm_client.recheck_relevance_strict(unique)
            logger.info(f"✅ Повторно релевантных: {len(rechecked)}")

            # 4. Отбор до 7 лучших
            top_posts = await self.lm_client.select_top_posts(rechecked, top_n=7)

            if not top_posts:
                logger.warning("Нет постов для Telegram")
                await self.telegram_sender.send_message("📊 За сутки не найдено релевантных публикаций.")
                return

            # 5. Суммаризация
            summaries = await self.lm_client.analyze_and_summarize(top_posts)

            if summaries:
                # Финальный дедуп сюжетов перед отправкой в Telegram
                try:
                    deduped = self.text_preprocessor.dedupe_summaries(
                        summaries, title_threshold=0.85, content_threshold=0.70
                    )
                    logger.info(f"Финальный дедуп: {len(summaries)} → {len(deduped)}")
                except Exception as e:
                    logger.warning(f"Ошибка финального дедупа, отправляем как есть: {e}")
                    deduped = summaries

                mapping = self.db_manager.create_post_mapping_from_db(db_posts)
                await self.telegram_sender.send_analysis(deduped, mapping)
                self.db_manager.update_post_summaries(summaries)
                # Обновить и сохранить метрики (день/неделя/месяц) в месячный JSON
                try:
                    from zoneinfo import ZoneInfo
                    day = datetime.now(ZoneInfo("Europe/Moscow")).date()
                    sc = StatsCollector()
                    sc.reset()
                    sc.scan_logs_for_date(logs_dir, day)
                    sc.flush_monthly(logs_dir, day)
                except Exception as se:
                    logger.warning(f"Не удалось сформировать сводку метрик: {se}")
                logger.info("✅ Результаты анализа отправлены и сохранены")

            else:
                logger.warning("Анализ не удался")
                await self.telegram_sender.send_message("⚠️ Не удалось выполнить анализ публикаций.")

        except Exception as e:
            logger.error(f"❌ Ошибка в run_daily_job: {e}")
            logger.error(traceback.format_exc())
            await self.telegram_sender.send_message(f"❌ Ошибка в ежедневном анализе:\n{str(e)[:200]}")

    async def run_hourly_job(self):
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("Europe/Moscow"))
        date_from = now - timedelta(hours=24)
        date_to = now - timedelta(minutes=1)

        logger.info("[⏰] Hourly job window: {} → {}", date_from, date_to)

        # Переводим в UTC для API
        utc_from = date_from.astimezone(ZoneInfo("UTC"))
        utc_to = date_to.astimezone(ZoneInfo("UTC"))

        posts = await self.data_manager.fetch_posts(
            date_from=utc_from,
            date_to=utc_to,
        )

        logger.info(f"Загружено {len(posts)} постов за окно {date_from} – {date_to}")

        existing_urls = set(self.db_manager.get_all_posts_urls())
        new_posts = [p for p in posts if p.url not in existing_urls]
        logger.info(f"Найдено {len(new_posts)} новых постов")

        inserted = self.db_manager.save_posts(new_posts)
        logger.info(f"Сохранено в БД: {inserted} постов")

        if inserted == 0:
            logger.info("Новых постов нет, но запускаем обработку старых с relevance=NULL и без категории")


        # Запускаем проверку релевантности для всех relevance=NULL
        from relevance_checker import RelevanceChecker
        checker = RelevanceChecker()
        await checker.process_unchecked_posts()

        # Запускаем классификацию релевантных постов с score >= 0.7
        from content_classifier import classify_relevant_posts_task
        await classify_relevant_posts_task()

        logger.success("✅ Hourly job завершён")


# Основной запуск
async def run_insight_flow():
    try:
        service = InsightFlow()
        await service.run_daily_job()
    except Exception as e:
        logger.error(f"Ошибка при запуске InsightFlow: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(run_insight_flow())
