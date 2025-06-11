import os
import json
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# Импортируем модуль для работы с RSS
from rss_manager import RSSManager
from post import Post

# Загрузка переменных окружения
load_dotenv()

def get_msk_date_range():
    """
    Получает диапазон дат с 8:00 до 8:00 в московском часовом поясе
    (всегда возвращает период с 8:00 вчера до 8:00 сегодня)
    
    Returns:
        tuple: (date_from, date_to) в UTC
    """
    # Получаем текущее время в московском часовом поясе
    msk_now = datetime.now(ZoneInfo('Europe/Moscow'))
    
    # Устанавливаем конечную дату как сегодня в 8:00
    date_to = msk_now.replace(hour=8, minute=0, second=0, microsecond=0)
    
    # Если сейчас раньше 8:00, значит нам нужен период с позавчера 8:00 до вчера 8:00
    if msk_now.hour < 8:
        date_to = date_to - timedelta(days=1)
    
    # Начальная дата - это конечная минус 24 часа
    date_from = date_to - timedelta(days=1)
    
    # Преобразуем в UTC для совместимости с API
    date_from = date_from.astimezone(ZoneInfo('UTC'))
    date_to = date_to.astimezone(ZoneInfo('UTC'))
    
    return date_from, date_to

class DataManager:
    def __init__(self):
        """
        Инициализирует менеджер данных для работы с RSS-фидами и файлами
        """
        # Директории для данных
        self.data_dir = Path(os.getenv("DATA_DIRECTORY", "data"))
        self.data_dir.mkdir(exist_ok=True)
        
        self.mentions_dir = self.data_dir / "mentions"
        self.mentions_dir.mkdir(exist_ok=True)

        # Инициализация RSS менеджера
        self.rss_manager = None
        try:
            self.rss_manager = RSSManager()
            logger.info("RSS-менеджер успешно инициализирован")
        except Exception as e:
            logger.error(f"Ошибка при инициализации RSS-менеджера: {e}")
            logger.error(traceback.format_exc())
    
    async def fetch_posts(self, date_from=None, date_to=None):
        """
        Получает посты из RSS за указанный период
        
        Args:
            date_from (datetime, optional): Начальная дата
            date_to (datetime, optional): Конечная дата
            
        Returns:
            list: Список объектов Post
        """
        # Инициализируем список постов
        posts = []
        
        # Если даты не указаны, используем стандартный диапазон
        if date_from is None or date_to is None:
            date_from, date_to = get_msk_date_range()
            logger.info(f"Используем стандартный диапазон дат: {date_from} - {date_to}")
        
        # Получаем данные из RSS, если RSS-менеджер инициализирован
        if self.rss_manager:
            try:
                rss_posts = await self.rss_manager.get_posts(date_from, date_to)
                if rss_posts:
                    posts.extend(rss_posts)
                    logger.info(f"Получено {len(rss_posts)} постов из RSS-источников")
                else:
                    logger.warning("Не получено постов из RSS-источников")
            except Exception as e:
                logger.error(f"Ошибка при получении постов из RSS: {e}")
                logger.error(traceback.format_exc())
        else:
            logger.warning("RSS-менеджер не инициализирован")
        
        # Здесь можно добавить получение данных из других источников
        # ...
        
        logger.info(f"Всего получено {len(posts)} постов")
        return posts
     

    async def save_posts_to_file(self, posts, date, suffix=""):
        """
        Сохраняет посты в файл для последующей обработки
        
        Args:
            posts (list): Список объектов Post
            date (datetime): Дата для имени файла
            suffix (str, optional): Суффикс для имени файла
            
        Returns:
            Path: Путь к созданному файлу или None в случае ошибки
        """
        if not posts:
            logger.warning("Нет постов для сохранения")
            return None
            
        date_str = date.strftime("%Y-%m-%d")
        file_name = f"mentions_{date_str}{suffix}.jsonl"
        file_path = self.mentions_dir / file_name
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                for post in posts:
                    try:
                        # Преобразуем объект Post в словарь и сохраняем как JSON-строку
                        post_dict = post.dict()
                        # Преобразуем datetime объекты в строки
                        if post_dict.get('published_on'):
                            post_dict['published_on'] = post_dict['published_on'].isoformat()
                        # Сохраняем как отдельную строку в формате JSON
                        f.write(json.dumps(post_dict, ensure_ascii=False) + '\n')
                    except Exception as e:
                        logger.error(f"Ошибка при сохранении поста {post.post_id}: {e}")
                        continue
            
            logger.info(f"Сохранено {len(posts)} постов в {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Ошибка сохранения постов в файл: {e}")
            logger.error(traceback.format_exc())
            return None
    
    async def load_posts_from_file(self, file_path):
        """
        Загружает посты из JSONL файла
        
        Args:
            file_path (Path or str): Путь к файлу
            
        Returns:
            list: Список объектов Post
        """
        posts = []
        file_path = Path(file_path)
        
        if not file_path.exists():
            logger.error(f"Файл не найден: {file_path}")
            return posts
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        if not line.strip():
                            continue
                        
                        # Загружаем JSON из строки
                        post_data = json.loads(line)
                        
                        # Преобразуем строку даты обратно в datetime
                        if post_data.get('published_on'):
                            post_data['published_on'] = datetime.fromisoformat(post_data['published_on'])
                        
                        # Создаем объект Post
                        post = Post(**post_data)
                        posts.append(post)
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Ошибка парсинга JSON в строке {line_num}: {e}")
                    except Exception as e:
                        logger.error(f"Ошибка создания поста из строки {line_num}: {e}")
            
            logger.info(f"Загружено {len(posts)} постов из {file_path}")
        except Exception as e:
            logger.error(f"Ошибка загрузки постов из файла: {e}")
            logger.error(traceback.format_exc())
        
        return posts
    
    async def prepare_post_texts(self, posts, max_tokens=None):
        """
        Подготавливает тексты постов для анализа с учетом токенов,
        и сохраняет связь между текстами и их источниками используя post_id как ключ
        
        Args:
            posts (list): Список объектов Post
            max_tokens (int, optional): Максимальное количество токенов. 
                                       Если None, возвращает все тексты без ограничений
            
        Returns:
            tuple: (список подготовленных текстов, словарь соответствия post_id и метаданных постов)
        """
        if not posts:
            logger.warning("Нет постов для подготовки текстов")
            return [], {}
            
        post_texts = []
        post_mapping = {}  # Словарь для связи post_id с метаданными постов
        current_tokens = 0

        for i, post in enumerate(posts):
            try:
                # Подготовка текста поста с уникальным идентификатором
                post_text = f"[POST_ID:{post.post_id}] [{post.blog_host}] {post.title or ''}\n{post.content or ''}"
                
                # Простая оценка токенов (приблизительная)
                estimated_tokens = len(post_text.split())
                
                # Проверка токенов, только если max_tokens задан
                if max_tokens is not None and current_tokens + estimated_tokens > max_tokens:
                    logger.info(f"Достигнут лимит токенов ({max_tokens}), обработано {len(post_texts)} постов")
                    break
                
                # Ограничиваем размер каждого поста для лучшей обработки
                if len(post_text) > 10000:  # Увеличиваем ограничение, чтобы сохранить больше контекста
                    logger.warning(f"Пост {post.post_id} слишком большой ({len(post_text)} символов), усекаем до 10000")
                    post_text = post_text[:9997] + "..."
                
                post_texts.append(post_text)
                
                # Сохраняем связь post_id с метаданными
                post_mapping[post.post_id] = {
                    'index': i,  # Сохраняем индекс для обратной совместимости
                    'post_id': post.post_id,
                    'url': post.url,
                    'blog_host': post.blog_host,
                    'title': post.title
                }
                
                # Обновляем счетчик токенов только если max_tokens задан
                if max_tokens is not None:
                    current_tokens += estimated_tokens
                    logger.debug(f"Добавлен пост: {post.post_id} ({estimated_tokens} токенов, всего: {current_tokens})")
                else:
                    logger.debug(f"Добавлен пост: {post.post_id} (без ограничения токенов)")
                    
            except Exception as e:
                logger.error(f"Ошибка при подготовке текста поста {post.post_id}: {e}")
                continue
        
        logger.info(f"Подготовлено {len(post_texts)} постов для анализа с метаданными для источников")
        return post_texts, post_mapping
    
    def get_analysis_history(self, days=7):
        """
        Получает историю анализов за последние N дней
        
        Args:
            days (int): Количество дней
            
        Returns:
            list: Список файлов с анализами
        """
        analysis_dir = self.data_dir / "analysis"
        if not analysis_dir.exists():
            return []
        
        analysis_files = []
        cutoff_date = datetime.now() - timedelta(days=days)
        
        try:
            for file_path in analysis_dir.glob("analysis_*.txt"):
                if file_path.stat().st_mtime > cutoff_date.timestamp():
                    analysis_files.append(file_path)
            
            analysis_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            logger.info(f"Найдено {len(analysis_files)} файлов анализа за последние {days} дней")
            
        except Exception as e:
            logger.error(f"Ошибка при получении истории анализов: {e}")
        
        return analysis_files