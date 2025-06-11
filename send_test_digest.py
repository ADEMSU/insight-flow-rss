# send_test_digest.py

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from db_manager import DBManager
from telegram_sender import TelegramSender
from text_preprocessing import TextPreprocessor
from lm_studio_client import LMStudioClient
from post import Post  # обязательно нужен для типизации и передачи

# Часовой пояс для фильтрации по Москве
TZ_MOSCOW = ZoneInfo("Europe/Moscow")


async def run():
    # Устанавливаем диапазон последних суток
    now = datetime.now(TZ_MOSCOW)
    start = now - timedelta(days=1)

    db = DBManager()
    posts = db.get_posts_by_date_range(
        date_from=start,
        date_to=now,
        only_relevant=True,
        only_classified=True,
    )

    if not posts:
        print("❗ Нет релевантных и классифицированных постов за последние сутки.")
        return

    print(f"✅ Найдено {len(posts)} постов за последние сутки")

    # Предобработка
    preprocessor = TextPreprocessor()
    posts_limited = posts[:20]  # ограничим вручную
    
    filtered = preprocessor.process_posts(posts[:20])

    if not filtered:
        print("⚠️ После фильтрации не осталось подходящих постов.")
        return

    # Суммаризация
    client = LMStudioClient()
    summary = await client.analyze_and_summarize([
        {
            "post_id": p.post_id,
            "title": p.title,
            "content": p.content,
            "url": p.url,
            "category": p.category,
            "subcategory": p.subcategory,
            "published_on": p.published_on.isoformat() if p.published_on else None,
        }
        for p in filtered
    ])

    if not summary:
        print("⚠️ LM Studio не вернул результата.")
        return

    # Создаем маппинг для URL
    post_mapping = {p.post_id: p.url for p in filtered}

    # Отправка в Telegram
    sender = TelegramSender()
    success = await sender.send_analysis(summary, post_mapping)

    if success:
        print("✅ Отправка в Telegram выполнена успешно")
    else:
        print("❌ Ошибка при отправке в Telegram")


if __name__ == "__main__":
    asyncio.run(run())
