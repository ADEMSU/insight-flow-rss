import schedule
import time
import asyncio
from loguru import logger
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Импорт необходимых модулей
from rss_manager import RSSManager
from relevance_checker import check_relevance_task
from content_classifier import classify_relevant_posts_task
from insightflow_service import run_insight_flow


# Функция для сбора RSS каждый час
async def fetch_rss_hourly():
   """Собирает посты из RSS-источников за последний час"""
   logger.info("Запуск почасового сбора RSS")
   try:
       rss_manager = RSSManager()
       # Получаем данные за последний час
       now = datetime.now()
       hour_ago = now - timedelta(hours=1)
       posts = await rss_manager.get_posts(hour_ago, now)
       logger.info(f"Получено {len(posts)} постов из RSS")
   except Exception as e:
       logger.error(f"Ошибка при сборе RSS: {e}")


# Функция для проверки релевантности новых постов
async def check_relevance():
   """Проверяет релевантность новых постов из базы данных"""
   logger.info("Запуск проверки релевантности")
   try:
       # Проверяем до 50 постов за раз
       limit = int(os.getenv("RELEVANCE_CHECK_LIMIT", "50"))
       processed = await check_relevance_task(limit)
       logger.info(f"Проверена релевантность {processed} постов")
   except Exception as e:
       logger.error(f"Ошибка при проверке релевантности: {e}")


# Функция для классификации релевантных постов
async def classify_posts():
   """Классифицирует релевантные посты"""
   logger.info("Запуск классификации релевантных постов")
   try:
       # Классифицируем до 10 постов за раз
       limit = int(os.getenv("CLASSIFICATION_LIMIT", "10"))
       classified = await classify_relevant_posts_task(limit)
       logger.info(f"Классифицировано {classified} постов")
   except Exception as e:
       logger.error(f"Ошибка при классификации: {e}")


# Функция для ежедневного анализа и отправки в Telegram
async def daily_analysis():
   """Выполняет ежедневный анализ и отправку результатов"""
   logger.info("Запуск ежедневного анализа")
   try:
       await run_insight_flow()
       logger.info("Ежедневный анализ успешно выполнен")
   except Exception as e:
       logger.error(f"Ошибка при ежедневном анализе: {e}")


# Функция для проверки работоспособности RSS-источников
async def check_rss_health():
   """Проверяет работоспособность RSS-источников"""
   try:
       rss_manager = RSSManager()
       report = await rss_manager.generate_rss_health_report()
       logger.info("Выполнена проверка работоспособности RSS-источников")
   except Exception as e:
       logger.error(f"Ошибка при проверке работоспособности RSS: {e}")


# Вспомогательная функция для запуска асинхронных задач
def run_async(coro):
   """Запускает асинхронную корутину в синхронном контексте"""
   try:
       loop = asyncio.new_event_loop()
       asyncio.set_event_loop(loop)
       return loop.run_until_complete(coro)
   finally:
       loop.close()


def setup_schedule():
   """Настраивает расписание выполнения задач"""
   
   # 1. Сбор RSS каждый час
   schedule.every(1).hour.do(lambda: run_async(fetch_rss_hourly()))
   logger.info("Запланирован сбор RSS каждый час")
   
   # 2. Проверка релевантности после сбора RSS (через 5 минут после часа)
   schedule.every(1).hour.at(":05").do(lambda: run_async(check_relevance()))
   logger.info("Запланирована проверка релевантности каждый час в :05")
   
   # 3. Классификация релевантных постов каждые 15 минут
   schedule.every(15).minutes.do(lambda: run_async(classify_posts()))
   logger.info("Запланирована классификация каждые 15 минут")
   
   # 4. Ежедневный анализ и отправка в Telegram в 8:00 по МСК
   schedule.every().day.at("08:00").do(lambda: run_async(daily_analysis()))
   logger.info("Запланирован ежедневный анализ в 08:00")
   
   # 5. Проверка работоспособности RSS каждые 12 часов
   schedule.every(12).hours.do(lambda: run_async(check_rss_health()))
   logger.info("Запланирована проверка работоспособности RSS каждые 12 часов")
   
   # Опциональные задачи на основе переменных окружения
   if os.getenv("RUN_ON_STARTUP", "true").lower() == "true":
       logger.info("Запуск начальных задач при старте...")
       # Сразу запускаем сбор RSS
       run_async(fetch_rss_hourly())
       # Через 30 секунд проверяем релевантность
       time.sleep(30)
       run_async(check_relevance())
       # Через минуту запускаем классификацию
       time.sleep(30)
       run_async(classify_posts())


def run_scheduler():
   """Основная функция планировщика"""
   logger.info("=== Запуск планировщика InsightFlow ===")
   logger.info(f"Текущее время: {datetime.now()}")
   logger.info(f"Часовой пояс: {time.tzname}")
   
   # Настраиваем расписание
   setup_schedule()
   
   # Выводим информацию о запланированных задачах
   logger.info("Запланированные задачи:")
   for job in schedule.jobs:
       logger.info(f"  - {job}")
   
   # Основной цикл планировщика
   logger.info("Планировщик запущен. Ожидание выполнения задач...")
   
   while True:
       try:
           # Выполняем запланированные задачи
           schedule.run_pending()
           
           # Выводим следующую задачу каждые 10 минут
           current_minute = datetime.now().minute
           if current_minute % 10 == 0:
               next_job = schedule.next_run()
               if next_job:
                   logger.debug(f"Следующая задача запланирована на: {next_job}")
           
           # Пауза на 30 секунд
           time.sleep(30)
           
       except KeyboardInterrupt:
           logger.info("Получен сигнал остановки. Завершение работы...")
           break
       except Exception as e:
           logger.error(f"Ошибка в основном цикле планировщика: {e}")
           # Продолжаем работу после ошибки
           time.sleep(60)


def main():
   """Точка входа"""
   try:
       # Проверяем наличие необходимых переменных окружения
       required_vars = ["LM_STUDIO_URL", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
       missing_vars = [var for var in required_vars if not os.getenv(var)]
       
       if missing_vars:
           logger.warning(f"Отсутствуют переменные окружения: {', '.join(missing_vars)}")
           logger.warning("Некоторые функции могут работать некорректно")
       
       # Запускаем планировщик
       run_scheduler()
       
   except Exception as e:
       logger.error(f"Критическая ошибка при запуске планировщика: {e}")
       raise


if __name__ == "__main__":
   main()