import os
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Float, Text, ForeignKey, JSON, ARRAY, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSONB
import datetime
from loguru import logger
from dotenv import load_dotenv
import traceback
from zoneinfo import ZoneInfo

# Загрузка переменных окружения
load_dotenv()

Base = declarative_base()

# Определение моделей
class RSSSource(Base):
   __tablename__ = 'rss_sources'
   
   id = Column(Integer, primary_key=True)
   name = Column(String(255), nullable=False)
   url = Column(String(512), nullable=False, unique=True)
   category_id = Column(Integer)
   priority = Column(Integer, default=0)
   status = Column(String(20), default='active')
   last_fetched_at = Column(DateTime)
   error_count = Column(Integer, default=0)
   last_error = Column(Text)
   created_at = Column(DateTime, default=datetime.datetime.utcnow)
   updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

# Обновленная модель Text с полями для релевантности
class Text(Base):
   __tablename__ = 'texts'
   
   id = Column(Integer, primary_key=True)
   post_id = Column(String(255), nullable=False)
   source_id = Column(Integer, ForeignKey('rss_sources.id'))
   blog_host = Column(String(255))
   blog_host_type = Column(Integer)
   title = Column(Text)
   content = Column(Text)
   url = Column(String(1024))
   published_on = Column(DateTime)
   fetched_at = Column(DateTime, default=datetime.datetime.utcnow)
   simhash = Column(String(255))
   categories = Column(ARRAY(Integer))
   is_processed = Column(Boolean, default=False)
   processing_status = Column(String(20), default='pending')
   
   # Поля для релевантности
   relevance = Column(Boolean, default=None)  # None = не проверено, True/False = результат проверки
   relevance_score = Column(Float, default=0.0)  # Опциональный score от LM Studio (0.0 - 1.0)
   relevance_checked_at = Column(DateTime)  # Время проверки релевантности
   
   # Поля для классификации
   category = Column(String(100))
   subcategory = Column(String(100))
   classification_confidence = Column(Float, default=0.0)
   classified_at = Column(DateTime)
   
   # Отношения
   source = relationship("RSSSource")

class DBManager:
   def __init__(self):
       """
       Инициализирует менеджер базы данных с настройками из переменных окружения
       """
       host = os.getenv("POSTGRES_HOST", "localhost")
       port = os.getenv("POSTGRES_PORT", "5432")
       db = os.getenv("POSTGRES_DB", "insightflow")
       user = os.getenv("POSTGRES_USER", "insightflow")
       password = os.getenv("POSTGRES_PASSWORD", "insightflow_password")
       
       # Строка подключения к PostgreSQL
       connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
       
       try:
           self.engine = create_engine(connection_string)
           self.Session = sessionmaker(bind=self.engine)
           logger.info(f"Успешное подключение к базе данных PostgreSQL на {host}:{port}/{db}")
       except Exception as e:
           logger.error(f"Ошибка при подключении к базе данных: {e}")
           raise
   
   def create_tables(self):
       """
       Создает таблицы в базе данных, если они не существуют
       """
       try:
           Base.metadata.create_all(self.engine)
           logger.info("Таблицы успешно созданы/проверены в базе данных")
           
           # Создаем партицию для текущего месяца, если нужно
           self.create_partition_if_not_exists()
       except Exception as e:
           logger.error(f"Ошибка при создании таблиц: {e}")
           raise
   
   def create_partition_if_not_exists(self):
       """
       Создает партицию для текущего месяца, если она не существует
       """
       session = None
       try:
           session = self.Session()
           
           # Получаем текущую дату с timezone
           current_date = datetime.datetime.now(ZoneInfo('UTC'))
           year = current_date.year
           month = current_date.month
           
           # Формируем название партиции
           partition_name = f"texts_y{year}m{month:02d}"
           
           # Проверяем существование партиции
           check_query = text(f"""
           SELECT EXISTS (
               SELECT FROM information_schema.tables 
               WHERE table_schema = 'public' 
               AND table_name = :partition_name
           )
           """)
           
           result = session.execute(check_query, {"partition_name": partition_name}).scalar()
           
           if not result:
               # Формируем дату начала и конца месяца
               start_date = datetime.datetime(year, month, 1)
               
               # Для последнего месяца года
               if month == 12:
                   end_date = datetime.datetime(year + 1, 1, 1)
               else:
                   end_date = datetime.datetime(year, month + 1, 1)
               
               # Создаем новую партицию
               create_query = text(f"""
               CREATE TABLE {partition_name} PARTITION OF texts
                   FOR VALUES FROM (:start_date) TO (:end_date)
               """)
               
               session.execute(create_query, {
                   "start_date": start_date.strftime("%Y-%m-%d"),
                   "end_date": end_date.strftime("%Y-%m-%d")
               })
               session.commit()
               
               logger.info(f"Создана новая партиция {partition_name} для таблицы texts")
           else:
               logger.info(f"Партиция {partition_name} уже существует")
           
       except Exception as e:
           logger.error(f"Ошибка при создании партиции: {e}")
           if session:
               session.rollback()
       finally:
           if session:
               session.close()
   
   def add_rss_sources(self, sources):
       """
       Добавляет источники RSS в базу данных
       
       Args:
           sources (list): Список словарей с информацией об источниках
       """
       session = None
       try:
           session = self.Session()
           
           for source_data in sources:
               # Проверяем, существует ли уже этот источник
               existing_source = session.query(RSSSource).filter_by(url=source_data['url']).first()
               
               if not existing_source:
                   # Создаем новый источник
                   new_source = RSSSource(
                       name=source_data.get('name', ''),
                       url=source_data.get('url', ''),
                       category_id=source_data.get('category_id'),
                       priority=source_data.get('priority', 0),
                       status='active'
                   )
                   session.add(new_source)
                   logger.info(f"Добавлен новый RSS-источник: {source_data.get('name')} - {source_data.get('url')}")
               else:
                   logger.info(f"RSS-источник уже существует: {source_data.get('url')}")
           
           session.commit()
           logger.info(f"Добавлено {len(sources)} RSS-источников в базу данных")
       except Exception as e:
           if session:
               session.rollback()
           logger.error(f"Ошибка при добавлении RSS-источников: {e}")
           raise
       finally:
           if session:
               session.close()
   
   def save_posts(self, posts):
       """
       Сохраняет посты в базу данных
       
       Args:
           posts (list): Список объектов Post
           
       Returns:
           int: Количество новых сохраненных постов
       """
       session = None
       try:
           # Проверяем наличие партиции текущего месяца
           self.create_partition_if_not_exists()
           
           session = self.Session()
           new_posts_count = 0
           
           for post in posts:
               # Проверяем, существует ли уже этот пост
               existing_post = session.query(Text).filter_by(post_id=post.post_id).first()
               
               if not existing_post:
                   # Находим источник по blog_host
                   source = session.query(RSSSource).filter_by(name=post.blog_host).first()
                   source_id = source.id if source else None
                   
                   # Создаем новый пост
                   new_post = Text(
                       post_id=post.post_id,
                       source_id=source_id,
                       blog_host=post.blog_host,
                       blog_host_type=post.blog_host_type.value if hasattr(post.blog_host_type, 'value') else post.blog_host_type,
                       title=post.title,
                       content=post.content,
                       url=post.url,
                       published_on=post.published_on,
                       simhash=post.simhash,
                       is_processed=False,
                       processing_status='pending',
                       relevance=None  # Новые посты еще не проверены на релевантность
                   )
                   session.add(new_post)
                   new_posts_count += 1
                   logger.info(f"Добавлен новый пост: {post.post_id} - {post.title}")
               else:
                   logger.debug(f"Пост уже существует: {post.post_id}")
           
           session.commit()
           logger.info(f"Добавлено {new_posts_count} новых постов в базу данных")
           return new_posts_count
       except Exception as e:
           if session:
               session.rollback()
           logger.error(f"Ошибка при сохранении постов: {e}")
           raise
       finally:
           if session:
               session.close()
   
   def get_unchecked_posts(self, limit=50):
       """
       Получает посты, которые еще не проверены на релевантность
       
       Args:
           limit (int): Максимальное количество постов для получения
           
       Returns:
           list: Список непроверенных постов
       """
       session = None
       try:
           session = self.Session()
           posts = session.query(Text).filter(
               Text.relevance.is_(None)
           ).order_by(Text.published_on.desc()).limit(limit).all()
           
           logger.info(f"Получено {len(posts)} непроверенных на релевантность постов")
           return posts
       except Exception as e:
           logger.error(f"Ошибка при получении непроверенных постов: {e}")
           return []
       finally:
           if session:
               session.close()
   
   def update_post_relevance(self, post_id, relevance, relevance_score=None):
       """
       Обновляет релевантность поста
       
       Args:
           post_id (str): Идентификатор поста
           relevance (bool): Релевантность (True/False)
           relevance_score (float, optional): Численная оценка релевантности
           
       Returns:
           bool: True если обновление выполнено успешно
       """
       session = None
       try:
           session = self.Session()
           
           post = session.query(Text).filter_by(post_id=post_id).first()
           
           if post:
               post.relevance = relevance
               post.relevance_score = relevance_score if relevance_score is not None else 0.0
               post.relevance_checked_at = datetime.datetime.utcnow()
               
               session.commit()
               logger.info(f"Обновлена релевантность поста {post_id}: {relevance} (score: {relevance_score})")
               return True
           else:
               logger.warning(f"Пост {post_id} не найден при обновлении релевантности")
               return False
               
       except Exception as e:
           if session:
               session.rollback()
           logger.error(f"Ошибка при обновлении релевантности поста {post_id}: {e}")
           return False
       finally:
           if session:
               session.close()
   
   def get_relevant_unclassified_posts(self, limit=10):
       """
       Получает релевантные посты без классификации
       
       Args:
           limit (int): Максимальное количество постов
           
       Returns:
           list: Список релевантных неклассифицированных постов
       """
       session = None
       try:
           session = self.Session()
           posts = session.query(Text).filter(
               Text.relevance == True,
               Text.category.is_(None)
           ).order_by(Text.published_on.desc()).limit(limit).all()
           
           logger.info(f"Получено {len(posts)} релевантных неклассифицированных постов")
           return posts
       except Exception as e:
           logger.error(f"Ошибка при получении релевантных неклассифицированных постов: {e}")
           return []
       finally:
           if session:
               session.close()
   
   def get_posts_by_date_range(self, date_from, date_to, limit=None, category=None, 
                              subcategory=None, only_classified=False, only_relevant=False):
       """
       Получает посты из базы данных за указанный период с возможностью фильтрации
       
       Args:
           date_from (datetime): Начальная дата
           date_to (datetime): Конечная дата
           limit (int, optional): Максимальное количество постов
           category (str, optional): Фильтр по категории
           subcategory (str, optional): Фильтр по подкатегории
           only_classified (bool, optional): Возвращать только классифицированные посты
           only_relevant (bool, optional): Возвращать только релевантные посты
           
       Returns:
           list: Список постов
       """
       session = None
       try:
           session = self.Session()
           query = session.query(Text).filter(
               Text.published_on >= date_from,
               Text.published_on <= date_to
           )
           
           # Фильтр по релевантности
           if only_relevant:
               query = query.filter(Text.relevance == True)
           
           # Добавляем фильтры по категории и подкатегории, если они указаны
           if category:
               query = query.filter(Text.category == category)
               
           if subcategory:
               query = query.filter(Text.subcategory == subcategory)
               
           # Добавляем фильтр только по классифицированным постам
           if only_classified:
               query = query.filter(Text.category.isnot(None))
               
           # Сортировка по дате публикации, от новых к старым
           query = query.order_by(Text.published_on.desc())
           
           if limit:
               query = query.limit(limit)
               
           posts = query.all()
           
           filters_desc = []
           if only_relevant:
               filters_desc.append("только релевантные")
           if only_classified:
               filters_desc.append("только классифицированные")
           if category or subcategory:
               filters_desc.append(f"категория {category}/{subcategory}")
               
           filter_str = f" ({', '.join(filters_desc)})" if filters_desc else ""
           
           logger.info(f"Получено {len(posts)} постов из базы данных за период с {date_from} по {date_to}{filter_str}")
           return posts
       except Exception as e:
           logger.error(f"Ошибка при получении постов из базы данных: {e}")
           return []
       finally:
           if session:
               session.close()
   
   def update_post_classification(self, post_id, category, subcategory, confidence):
       """
       Обновляет категорию и подкатегорию поста
       
       Args:
           post_id (str): Идентификатор поста
           category (str): Основная категория
           subcategory (str): Подкатегория
           confidence (float): Уверенность классификации (0-1)
           
       Returns:
           bool: True, если обновление выполнено успешно, иначе False
       """
       session = None
       try:
           session = self.Session()
           
           # Находим пост по post_id
           post = session.query(Text).filter_by(post_id=post_id).first()
           
           if post:
               # Обновляем поля категории и подкатегории
               post.category = category
               post.subcategory = subcategory
               post.classification_confidence = confidence
               post.classified_at = datetime.datetime.utcnow()
               
               session.commit()
               logger.info(f"Обновлена классификация поста {post_id}: {category}/{subcategory} ({confidence:.2f})")
               return True
           else:
               logger.warning(f"Пост {post_id} не найден при обновлении классификации")
               return False
               
       except Exception as e:
           if session:
               session.rollback()
           logger.error(f"Ошибка при обновлении классификации поста {post_id}: {e}")
           logger.error(traceback.format_exc())
           return False
       finally:
           if session:
               session.close()

   def update_posts_classification(self, classifications):
       """
       Массовое обновление классификации постов
       
       Args:
           classifications (dict): Словарь соответствия post_id -> (категория, подкатегория, достоверность)
           
       Returns:
           int: Количество успешно обновленных постов
       """
       updated_count = 0
       session = None
       
       try:
           session = self.Session()
           
           for post_id, (category, subcategory, confidence) in classifications.items():
               try:
                   # Находим пост по post_id
                   post = session.query(Text).filter_by(post_id=post_id).first()
                   
                   if post:
                       # Обновляем поля категории и подкатегории
                       post.category = category
                       post.subcategory = subcategory
                       post.classification_confidence = confidence
                       post.classified_at = datetime.datetime.utcnow()
                       updated_count += 1
                   else:
                       logger.warning(f"Пост {post_id} не найден при обновлении классификации")
               except Exception as e:
                   logger.error(f"Ошибка при обновлении классификации поста {post_id}: {e}")
                   # Продолжаем обработку других постов
           
           session.commit()
           logger.info(f"Обновлена классификация {updated_count} постов из {len(classifications)}")
           return updated_count
               
       except Exception as e:
           if session:
               session.rollback()
           logger.error(f"Ошибка при массовом обновлении классификации постов: {e}")
           logger.error(traceback.format_exc())
           return 0
       finally:
           if session:
               session.close()
   
   def update_posts_relevance_batch(self, relevance_results):
       """
       Массовое обновление релевантности постов
       
       Args:
           relevance_results (dict): Словарь post_id -> (relevance, score)
           
       Returns:
           int: Количество обновленных постов
       """
       updated_count = 0
       session = None
       
       try:
           session = self.Session()
           
           for post_id, (relevance, score) in relevance_results.items():
               try:
                   post = session.query(Text).filter_by(post_id=post_id).first()
                   
                   if post:
                       post.relevance = relevance
                       post.relevance_score = score if score is not None else 0.0
                       post.relevance_checked_at = datetime.datetime.utcnow()
                       updated_count += 1
                   else:
                       logger.warning(f"Пост {post_id} не найден при обновлении релевантности")
               except Exception as e:
                   logger.error(f"Ошибка при обновлении релевантности поста {post_id}: {e}")
           
           session.commit()
           logger.info(f"Обновлена релевантность {updated_count} постов из {len(relevance_results)}")
           return updated_count
               
       except Exception as e:
           if session:
               session.rollback()
           logger.error(f"Ошибка при массовом обновлении релевантности: {e}")
           logger.error(traceback.format_exc())
           return 0
       finally:
           if session:
               session.close()

   def get_categories_statistics(self, date_from=None, date_to=None, only_relevant=False):
       """
       Получает статистику по категориям и подкатегориям
       
       Args:
           date_from (datetime, optional): Начальная дата для фильтрации
           date_to (datetime, optional): Конечная дата для фильтрации
           only_relevant (bool, optional): Учитывать только релевантные посты
           
       Returns:
           dict: Статистика по категориям и подкатегориям
       """
       session = None
       try:
           from sqlalchemy import func
           session = self.Session()
           
           # Базовый запрос
           query = session.query(
               Text.category, 
               Text.subcategory, 
               func.count(Text.id).label('count')
           ).group_by(Text.category, Text.subcategory)
           
           # Фильтр по релевантности
           if only_relevant:
               query = query.filter(Text.relevance == True)
           
           # Добавляем фильтры по дате, если они указаны
           if date_from:
               query = query.filter(Text.published_on >= date_from)
           if date_to:
               query = query.filter(Text.published_on <= date_to)
           
           # Выполняем запрос
           results = query.all()
           
           # Форматируем результаты
           statistics = {}
           for category, subcategory, count in results:
               if category not in statistics:
                   statistics[category] = {'total': 0, 'subcategories': {}}
               
               statistics[category]['total'] += count
               
               if subcategory:
                   if subcategory not in statistics[category]['subcategories']:
                       statistics[category]['subcategories'][subcategory] = 0
                   
                   statistics[category]['subcategories'][subcategory] += count
           
           return statistics
       except Exception as e:
           logger.error(f"Ошибка при получении статистики по категориям: {e}")
           return {}
       finally:
           if session:
               session.close()    
   
   def create_post_mapping_from_db(self, posts):
       """
       Создает mapping post_id -> метаданные для постов из базы данных
       
       Args:
           posts: список объектов Post или записей из базы данных
           
       Returns:
           dict: словарь для связи post_id с URL и другими метаданными
       """
       mapping = {}
       
       for post in posts:
           # Определяем, какого типа объект
           if hasattr(post, 'dict') and callable(getattr(post, 'dict')):
               # Это объект Post
               post_id = post.post_id
               url = post.url
               blog_host = post.blog_host
               title = post.title
           else:
               # Это запись из БД
               post_id = post.post_id
               url = post.url
               blog_host = post.blog_host
               title = post.title
           
           # Добавляем в mapping, используя post_id как ключ
           mapping[post_id] = {
               'post_id': post_id,
               'url': url,
               'blog_host': blog_host,
               'title': title
           }
       
       logger.info(f"Создан post_mapping для {len(mapping)} постов")
       return mapping