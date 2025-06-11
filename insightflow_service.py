import os
import sys
import traceback
from datetime import datetime
import asyncio
import json

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

       self.rss_manager = getattr(self.data_manager, 'rss_manager', None)
       if self.rss_manager:
           logger.info("RSS-менеджер инициализирован")

   async def run_daily_job(self):
       try:
           date_from, date_to = get_msk_date_range()
           logger.info(f"Начало ежедневного анализа за период с {date_from} по {date_to} (МСК)")

           if not await self.lm_client.test_connection():
               logger.error("LM Studio недоступен, отменяем ежедневный анализ")
               await self.telegram_sender.send_message("⚠️ Внимание: LM Studio недоступен. Ежедневный анализ отменен.")
               return

           posts = []
           if self.db_manager:
               try:
                   db_posts = self.db_manager.get_posts_by_date_range(
                       date_from, date_to, only_relevant=True, only_classified=True
                   )
                   if db_posts:
                       logger.info(f"Найдено {len(db_posts)} релевантных классифицированных публикаций для анализа")
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
                           logger.debug(f"Пост из БД: post_id='{db_post.post_id}', url='{db_post.url[:50]}...'")
                       db_post_mapping = self.db_manager.create_post_mapping_from_db(db_posts)
                    
                    # Логируем первые несколько элементов mapping для отладки
                       if db_post_mapping:
                           sample_items = list(db_post_mapping.items())[:3]
                           for post_id, url in sample_items:
                               logger.debug(f"Sample mapping: post_id='{post_id}' -> url='{url}'")
                    

                       logger.info(f"Создан post_mapping из базы данных с {len(db_post_mapping)} элементами")
                   else:
                       logger.warning("Не найдено релевантных публикаций за указанный период")
                       await self.telegram_sender.send_message(
                           f"📊 За период с {date_from.strftime('%d.%m.%Y')} по {date_to.strftime('%d.%m.%Y')} не найдено релевантных публикаций."
                       )
                       return
               except Exception as e:
                   logger.error(f"Ошибка при получении постов из БД: {e}")
                   logger.error(traceback.format_exc())
                   return
           else:
               logger.error("База данных недоступна")
               return

           raw_file_path = await self.data_manager.save_posts_to_file(posts, date_from, "_relevant_classified")
           if raw_file_path:
               logger.info(f"Релевантные публикации сохранены в {raw_file_path}")

           filtered_posts = self.text_preprocessor.process_posts(posts)
           if not filtered_posts:
               logger.warning("После фильтрации не осталось публикаций")
               await self.telegram_sender.send_message("📊 После фильтрации не осталось уникальных публикаций.")
               return

           filtered_file_path = await self.data_manager.save_posts_to_file(filtered_posts, date_from, "_filtered_final")
           if filtered_file_path:
               logger.info(f"Фильтрованные публикации сохранены в {filtered_file_path}")

           posts_data = []
           for post in filtered_posts:
               post_data = {
                   'post_id': post.post_id,
                   'title': post.title,
                   'content': post.content,
                   'url': post.url,
                   'category': getattr(post, 'category', 'Не указана'),
                   'subcategory': getattr(post, 'subcategory', 'Не указана'),
                   'published_on': post.published_on
               }
               posts_data.append(post_data)
           
           logger.warning(f"Подготовлено {len(posts_data)} постов для анализа")
           for i, post_data in enumerate(posts_data[:3]):
                logger.warning(f"Post data {i}: {json.dumps(post_data, ensure_ascii=False)[:200]}")

            # Анализируем с помощью LM Studio
           logger.info(f"Отправляем {len(posts_data)} постов на анализ в LM Studio")

           summaries = await self.lm_client.analyze_and_summarize(posts_data, max_stories=2)

           if summaries:
               await self.telegram_sender.send_analysis(summaries, db_post_mapping)
               logger.info("Анализ успешно отправлен в Telegram")

               self.db_manager.update_post_summaries(summaries)
               logger.info("Результаты суммаризации сохранены в БД")

               analysis_file_path = self.data_manager.data_dir / "analysis" / f"analysis_{date_from.strftime('%Y-%m-%d')}.txt"
               os.makedirs(os.path.dirname(analysis_file_path), exist_ok=True)
               with open(analysis_file_path, 'w', encoding='utf-8') as f:
                   for idx, story in enumerate(summaries, 1):
                       f.write(f"Сюжет {idx}: {story['title']}\n")
                       f.write(f"Содержание: {story['summary']}\n")
                       f.write(f"POST_ID: {story['post_id']}\n\n")
               logger.info(f"Результаты анализа сохранены в {analysis_file_path}")
           else:
               logger.warning("Анализ не удался")
               await self.telegram_sender.send_message("⚠️ Не удалось выполнить анализ публикаций. Проверьте LM Studio.")

       except Exception as e:
           logger.error(f"Критическая ошибка в run_daily_job: {e}")
           logger.error(traceback.format_exc())
           await self.telegram_sender.send_message(f"❌ Ошибка в ежедневном анализе:\n{str(e)[:200]}")


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
