import asyncio
import sys
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv
from typing import List, Dict, Tuple
import traceback

# Загрузка переменных окружения
load_dotenv()

# Импорт необходимых модулей
from db_manager import DBManager, Text  # Добавляем импорт модели Text
from lm_studio_client import LMStudioClient
from post import Post

# Настройка логирования
logger.remove()
logger.add(sys.stderr, level='INFO')
logger.add('/app/logs/relevance_checker_{time}.log', rotation='10 MB', level='INFO')


class RelevanceChecker:
   """
   Модуль для проверки релевантности новых постов
   с использованием LM Studio
   """
   
   def __init__(self):
       """
       Инициализирует модуль проверки релевантности
       """
       try:
           # Инициализация подключения к базе данных
           self.db_manager = DBManager()
           logger.info("Подключение к базе данных успешно инициализировано")
       except Exception as e:
           logger.error(f"Ошибка при инициализации подключения к базе данных: {e}")
           self.db_manager = None
           raise
       
       try:
           # Инициализация клиента LM Studio
           self.lm_client = LMStudioClient()
           logger.info("LM Studio клиент успешно инициализирован")
       except Exception as e:
           logger.error(f"Ошибка при инициализации LM Studio клиента: {e}")
           self.lm_client = None
           raise
       
       # Параметры для батчевой обработки
       self.batch_size = 10  # Количество постов для одновременной проверки
       self.max_concurrent = 3  # Максимальное количество параллельных запросов
   
   async def check_relevance_batch(self, posts: List) -> Dict[str, Tuple[bool, float]]:
       """
       Проверяет релевантность батча постов
       
       Args:
           posts: Список объектов постов из базы данных
           
       Returns:
           Dict: Словарь post_id -> (relevance, score)
       """
       results = {}
       
       # Создаем семафор для ограничения параллельных запросов
       semaphore = asyncio.Semaphore(self.max_concurrent)
       
       async def check_single_post(post):
           """Проверяет релевантность одного поста"""
           async with semaphore:
               try:
                   # Извлекаем данные из объекта поста
                   post_id = post.post_id
                   title = post.title or ""
                   content = post.content or ""
                   
                   # Если контент слишком короткий, сразу помечаем как нерелевантный
                   if len(title) + len(content) < 50:
                       logger.info(f"Пост {post_id} слишком короткий, помечен как нерелевантный")
                       return post_id, (False, 0.0)
                   
                   # Проверяем релевантность через LM Studio
                   relevance, score = await self.lm_client.check_relevance(
                       post_id, title, content
                   )
                   
                   return post_id, (relevance, score)
                   
               except Exception as e:
                   logger.error(f"Ошибка при проверке релевантности поста {post.post_id}: {e}")
                   logger.error(traceback.format_exc())
                   # В случае ошибки возвращаем False
                   return post.post_id, (False, 0.0)
       
       # Запускаем проверку всех постов параллельно
       tasks = [check_single_post(post) for post in posts]
       check_results = await asyncio.gather(*tasks)
       
       # Формируем результаты
       for post_id, (relevance, score) in check_results:
           results[post_id] = (relevance, score)
       
       return results
   
   async def process_unchecked_posts(self, limit: int = 50) -> int:
       """
       Обрабатывает непроверенные посты из базы данных
       
       Args:
           limit: Максимальное количество постов для обработки
           
       Returns:
           int: Количество обработанных постов
       """
       if not self.db_manager:
           logger.error("База данных недоступна")
           return 0
       
       if not self.lm_client:
           logger.error("LM Studio клиент недоступен")
           return 0
       
       try:
           # Проверяем соединение с LM Studio
           if not await self.lm_client.test_connection():
               logger.error("LM Studio API недоступен")
               return 0
           
           # Получаем непроверенные посты
           unchecked_posts = self.db_manager.get_unchecked_posts(limit)
           
           if not unchecked_posts:
               logger.info("Нет непроверенных постов для обработки")
               return 0
           
           logger.info(f"Начинаем проверку релевантности для {len(unchecked_posts)} постов")
           
           processed_count = 0
           
           # Обрабатываем посты батчами
           for i in range(0, len(unchecked_posts), self.batch_size):
               batch = unchecked_posts[i:i + self.batch_size]
               logger.info(f"Обработка батча {i//self.batch_size + 1}, размер: {len(batch)}")
               
               # Проверяем релевантность батча
               results = await self.check_relevance_batch(batch)
               
               # Обновляем результаты в базе данных
               if results:
                   update_count = self.db_manager.update_posts_relevance_batch(results)
                   processed_count += update_count
                   
                   # Логируем статистику по батчу
                   relevant_count = sum(1 for r, _ in results.values() if r)
                   logger.info(f"Батч обработан: {relevant_count} из {len(results)} постов релевантны")
               
               # Небольшая пауза между батчами
               if i + self.batch_size < len(unchecked_posts):
                   await asyncio.sleep(1)
           
           logger.info(f"Завершена проверка релевантности. Обработано {processed_count} постов")
           
           # Выводим общую статистику
           self._log_statistics()
           
           return processed_count
           
       except Exception as e:
           logger.error(f"Критическая ошибка при проверке релевантности: {e}")
           logger.error(traceback.format_exc())
           return 0
   
   def _log_statistics(self):
       """
       Выводит статистику по релевантности постов
       """
       try:
           if not self.db_manager:
               return
           
           # Получаем статистику из базы данных
           from sqlalchemy import func
           session = self.db_manager.Session()
           
           # Общее количество постов
           total_posts = session.query(func.count(Text.id)).scalar()
           
           # Количество проверенных постов
           checked_posts = session.query(func.count(Text.id)).filter(
               Text.relevance.isnot(None)
           ).scalar()
           
           # Количество релевантных постов
           relevant_posts = session.query(func.count(Text.id)).filter(
               Text.relevance == True
           ).scalar()
           
           # Средний score релевантных постов
           avg_score = session.query(func.avg(Text.relevance_score)).filter(
               Text.relevance == True
           ).scalar() or 0
           
           session.close()
           
           # Выводим статистику
           logger.info("=== Статистика по релевантности ===")
           logger.info(f"Всего постов в БД: {total_posts}")
           if total_posts > 0:
               logger.info(f"Проверено на релевантность: {checked_posts} ({checked_posts/total_posts*100:.1f}%)")
               if checked_posts > 0:
                   logger.info(f"Релевантных постов: {relevant_posts} ({relevant_posts/checked_posts*100:.1f}% от проверенных)")
               logger.info(f"Средний score релевантных: {avg_score:.3f}")
           logger.info("===================================")
           
       except Exception as e:
           logger.error(f"Ошибка при получении статистики: {e}")
           logger.error(traceback.format_exc())


async def check_relevance_task(limit: int = 50) -> int:
   """
   Задача для запуска из планировщика
   
   Args:
       limit: Максимальное количество постов для проверки
       
   Returns:
       int: Количество обработанных постов
   """
   try:
       checker = RelevanceChecker()
       return await checker.process_unchecked_posts(limit)
   except Exception as e:
       logger.error(f"Ошибка при выполнении задачи проверки релевантности: {e}")
       return 0


# Для запуска как отдельного скрипта
if __name__ == "__main__":
   import argparse
   
   parser = argparse.ArgumentParser(description='Проверка релевантности постов')
   parser.add_argument('--limit', type=int, default=50, help='Максимальное количество постов для проверки')
   args = parser.parse_args()
   
   logger.info(f"Запуск проверки релевантности с лимитом {args.limit}")
   processed = asyncio.run(check_relevance_task(args.limit))
   logger.info(f"Обработано {processed} постов")