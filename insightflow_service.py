import os
import sys
import traceback
from datetime import datetime
import asyncio

from loguru import logger
from dotenv import load_dotenv

# Импортируем созданные и обновленные модули
from data_manager import DataManager, get_msk_date_range
from text_preprocessing import TextPreprocessor
from lm_studio_client import LMStudioClient
from telegram_sender import TelegramSender
from token_estimator import TokenEstimator
from db_manager import DBManager

# Импортируем существующие модули
from post import Post

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("/app/logs/insightflow_{time}.log", rotation="10 MB", level="INFO")


class InsightFlow:
   def __init__(self):
       """
       Инициализирует основной сервис InsightFlow для работы с LM Studio
       """
       # Инициализация компонентов
       self.data_manager = DataManager()
       
       # Используем более строгий порог сходства для фильтрации
       self.text_preprocessor = TextPreprocessor(
           similarity_threshold=0.65,
           min_content_length=100,
           max_tokens=50000  # Увеличиваем лимит для локальных моделей
       )
       
       # Инициализация LM Studio клиента вместо OpenRouter
       self.lm_client = LMStudioClient()
       self.telegram_sender = TelegramSender()
       self.token_estimator = TokenEstimator()

       # Инициализация подключения к БД
       try:
           self.db_manager = DBManager()
           self.db_manager.create_tables()
           logger.info("Подключение к базе данных успешно инициализировано")
       except Exception as e:
           logger.error(f"Ошибка при инициализации подключения к базе данных: {e}")
           self.db_manager = None
       
       # RSS менеджер
       self.rss_manager = None
       if hasattr(self.data_manager, 'rss_manager'):
           self.rss_manager = self.data_manager.rss_manager
           logger.info("RSS-менеджер инициализирован")

   async def run_daily_job(self):
       """
       Ежедневная задача анализа релевантных и классифицированных данных.
       Обрабатывает только посты с relevance=true и заполненной категорией.
       """
       try:
           # Получаем диапазон дат
           date_from, date_to = get_msk_date_range()
           
           logger.info(f"Начало ежедневного анализа за период с {date_from} по {date_to} (МСК)")
           
           # Проверяем доступность LM Studio
           if not await self.lm_client.test_connection():
               logger.error("LM Studio недоступен, отменяем ежедневный анализ")
               await self.telegram_sender.send_message(
                   "⚠️ Внимание: LM Studio недоступен. Ежедневный анализ отменен."
               )
               return
           
           # Получаем только релевантные и классифицированные посты из БД
           posts = []
           if self.db_manager:
               try:
                   # Получаем посты с relevance=true и заполненной категорией
                   db_posts = self.db_manager.get_posts_by_date_range(
                       date_from, date_to,
                       limit=None,
                       only_relevant=True,      # Только релевантные
                       only_classified=True     # Только с категорией
                   )
                   
                   if db_posts:
                       logger.info(f"Найдено {len(db_posts)} релевантных классифицированных публикаций для анализа")
                       
                       # Преобразуем записи БД в объекты Post
                       for db_post in db_posts:
                           post = Post(
                               post_id=db_post.post_id,
                               content=db_post.content,
                               blog_host=db_post.blog_host,
                               blog_host_type=db_post.blog_host_type,
                               published_on=db_post.published_on,
                               simhash=db_post.simhash,
                               url=db_post.url,
                               title=db_post.title
                           )
                           # Добавляем информацию о категории для анализа
                           post.category = db_post.category
                           post.subcategory = db_post.subcategory
                           posts.append(post)
                       
                       # Создаем post_mapping из базы данных
                       db_post_mapping = self.db_manager.create_post_mapping_from_db(db_posts)
                       logger.info(f"Создан post_mapping из базы данных с {len(db_post_mapping)} элементами")
                   else:
                       logger.warning("Не найдено релевантных классифицированных публикаций за указанный период")
                       await self.telegram_sender.send_message(
                           f"📊 За период с {date_from.strftime('%d.%m.%Y')} по {date_to.strftime('%d.%m.%Y')} "
                           f"не найдено релевантных публикаций для анализа."
                       )
                       return
               except Exception as e:
                   logger.error(f"Ошибка при получении постов из базы данных: {e}")
                   logger.error(traceback.format_exc())
                   return
           else:
               logger.error("База данных недоступна")
               return
           
           # Сохраняем исходные посты в файл для логирования
           raw_file_path = await self.data_manager.save_posts_to_file(
               posts, date_from, "_relevant_classified"
           )
           if raw_file_path:
               logger.info(f"Релевантные классифицированные публикации сохранены в {raw_file_path}")
           
           # Применяем предобработку и фильтрацию текста
           filtered_posts = self.text_preprocessor.process_posts(posts)

           if not filtered_posts:
               logger.warning("После фильтрации не осталось публикаций для анализа")
               await self.telegram_sender.send_message(
                   "📊 После фильтрации дубликатов не осталось уникальных публикаций для анализа."
               )
               return

           logger.info(f"После фильтрации осталось {len(filtered_posts)} уникальных публикаций")

           # Сохраняем отфильтрованные посты
           filtered_file_path = await self.data_manager.save_posts_to_file(
               filtered_posts, date_from, "_filtered_final"
           )
           if filtered_file_path:
               logger.info(f"Отфильтрованные публикации сохранены в {filtered_file_path}")
           
           # Подготавливаем данные для анализа в LM Studio
           posts_data = []
           for post in filtered_posts:
               post_data = {
                   'post_id': post.post_id,
                   'title': post.title,
                   'content': post.content,
                   'blog_host': post.blog_host,
                   'url': post.url,
                   'category': getattr(post, 'category', 'Не указана'),
                   'subcategory': getattr(post, 'subcategory', 'Не указана')
               }
               posts_data.append(post_data)
           
           # Анализируем с помощью LM Studio
           logger.info(f"Отправляем {len(posts_data)} постов на анализ в LM Studio")
           analysis = await self.lm_client.analyze_and_summarize(posts_data, max_stories=10)
           
           if analysis:
               # Отправляем результаты в Telegram с информацией о источниках
               await self.telegram_sender.send_analysis(analysis, db_post_mapping)
               logger.info("Анализ успешно отправлен в Telegram")
               
               # Сохраняем результаты анализа в файл для истории
               analysis_file_path = self.data_manager.data_dir / "analysis" / f"analysis_{date_from.strftime('%Y-%m-%d')}.txt"
               os.makedirs(os.path.dirname(analysis_file_path), exist_ok=True)
               
               with open(analysis_file_path, 'w', encoding='utf-8') as f:
                   f.write(f"Анализ за {date_from.strftime('%d.%m.%Y')} - {date_to.strftime('%d.%m.%Y')}\n")
                   f.write(f"Обработано {len(filtered_posts)} уникальных релевантных публикаций\n")
                   f.write("="*50 + "\n\n")
                   f.write(analysis)
               
               logger.info(f"Результаты анализа сохранены в {analysis_file_path}")
           else:
               logger.warning("Анализ публикаций не удался")
               await self.telegram_sender.send_message(
                   "⚠️ Не удалось выполнить анализ публикаций. Проверьте работу LM Studio."
               )
       
       except Exception as e:
           logger.error(f"Критическая ошибка в ежедневной задаче анализа: {e}")
           logger.error(traceback.format_exc())
           
           # Отправляем уведомление об ошибке
           await self.telegram_sender.send_message(
               f"❌ Критическая ошибка при выполнении ежедневного анализа:\n{str(e)[:200]}"
           )

   async def run_manual_job(self, date_from, date_to):
       """
       Ручная задача для анализа данных за указанный период
       
       Args:
           date_from: начальная дата
           date_to: конечная дата
       """
       try:
           logger.info(f"Запуск ручного анализа данных за период с {date_from} по {date_to}")
           
           # Проверяем доступность LM Studio
           if not await self.lm_client.test_connection():
               logger.error("LM Studio недоступен")
               return None
           
           # Проверяем наличие данных в БД за указанный период
           if self.db_manager:
               db_posts = self.db_manager.get_posts_by_date_range(
                   date_from, date_to,
                   only_relevant=True,
                   only_classified=True
               )
               
               if db_posts:
                   logger.info(f"Найдено {len(db_posts)} релевантных классифицированных постов за указанный период")
                   
                   # Конвертируем записи БД в объекты Post
                   posts = []
                   for db_post in db_posts:
                       post = Post(
                           post_id=db_post.post_id,
                           content=db_post.content,
                           blog_host=db_post.blog_host,
                           blog_host_type=db_post.blog_host_type,
                           published_on=db_post.published_on,
                           simhash=db_post.simhash,
                           url=db_post.url,
                           title=db_post.title
                       )
                       post.category = db_post.category
                       post.subcategory = db_post.subcategory
                       posts.append(post)
                   
                   # Создаем mapping для источников
                   post_mapping = self.db_manager.create_post_mapping_from_db(db_posts)
               else:
                   logger.warning("Нет релевантных классифицированных данных за указанный период")
                   return None
           else:
               logger.error("База данных недоступна")
               return None
           
           # Применяем предобработку текста
           filtered_posts = self.text_preprocessor.process_posts(posts)
           
           if not filtered_posts:
               logger.warning("После фильтрации не осталось публикаций для анализа")
               return None
           
           # Подготавливаем данные для анализа
           posts_data = []
           for post in filtered_posts:
               post_data = {
                   'post_id': post.post_id,
                   'title': post.title,
                   'content': post.content,
                   'blog_host': post.blog_host,
                   'url': post.url,
                   'category': getattr(post, 'category', 'Не указана'),
                   'subcategory': getattr(post, 'subcategory', 'Не указана')
               }
               posts_data.append(post_data)
           
           # Анализируем с помощью LM Studio
           analysis = await self.lm_client.analyze_and_summarize(posts_data)
           
           if analysis:
               # Отправляем результаты в Telegram
               await self.telegram_sender.send_analysis(analysis, post_mapping)
               logger.info("Результаты ручного анализа успешно отправлены")
               
               # Сохраняем результаты в файл
               timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
               analysis_file_path = self.data_manager.data_dir / "analysis" / f"manual_analysis_{timestamp}.txt"
               os.makedirs(os.path.dirname(analysis_file_path), exist_ok=True)
               
               with open(analysis_file_path, 'w', encoding='utf-8') as f:
                   f.write(f"Ручной анализ за период {date_from} - {date_to}\n")
                   f.write(f"Обработано {len(filtered_posts)} публикаций\n")
                   f.write("="*50 + "\n\n")
                   f.write(analysis)
               
               logger.info(f"Результаты ручного анализа сохранены в {analysis_file_path}")
               return analysis_file_path
           else:
               logger.warning("Ручной анализ публикаций не удался")
               return None
               
       except Exception as e:
           logger.error(f"Критическая ошибка при ручном анализе: {e}")
           logger.error(traceback.format_exc())
           return None


# Эти функции должны быть вне класса InsightFlow
async def run_insight_flow():
   """
   Основная функция запуска сервиса для планировщика
   """
   try:
       service = InsightFlow()
       await service.run_daily_job()
   except Exception as e:
       logger.error(f"Ошибка при запуске InsightFlow: {e}")
       logger.error(traceback.format_exc())


if __name__ == "__main__":
   asyncio.run(run_insight_flow())