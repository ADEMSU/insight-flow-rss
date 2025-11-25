import asyncio
import feedparser
import aiohttp
import datetime
import re
import hashlib
import traceback
import time
from loguru import logger
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import random  
import time    
import json    

# Импортируем существующие модули
from post import Post
from db_manager import DBManager

# Загрузка переменных окружения
load_dotenv()

class RSSManager:
    """
    Класс для получения и обработки данных из RSS-фидов.
    Заменяет функциональность MlgManager для работы с RSS вместо Медиалогии.
    """
    
    def __init__(self):
        """
        Инициализирует менеджер RSS фидов
        """
        logger.info("Инициализация RSSManager")
        # Путь к файлу с конфигурацией RSS-источников
        self.config_file = os.getenv("RSS_CONFIG_FILE", "rss_sources.json")
        
        # Инициализируем подключение к БД
        try:
            self.db_manager = DBManager()
            self.db_manager.create_tables()
        except Exception as e:
            logger.error(f"Ошибка при подключении к базе данных: {e}")
            self.db_manager = None
        
        # Загружаем список RSS каналов
        self.rss_sources = self._load_rss_sources()
        logger.info(f"Инициализировано {len(self.rss_sources)} RSS-источников")
        
        # Максимальное количество одновременных запросов
        self.max_concurrent_requests = int(os.getenv("MAX_CONCURRENT_REQUESTS", "10"))
        
        # Таймаут для HTTP запросов (в секундах)
        self.request_timeout = int(os.getenv("REQUEST_TIMEOUT", "30"))

        # Инициализируем логирование RSS
        self.setup_rss_logging()  # Раскомментировать или добавить эту строку
        
        # Заголовки для HTTP запросов
        self.headers = {
            "User-Agent": "InsightFlow RSS Reader/1.0"
        }
    
    def _convert_priority(self, priority_value):
        """
        Конвертирует строковые значения приоритетов в числовые
        
        Args:
            priority_value: Значение приоритета (строка или число)
            
        Returns:
            int: Числовое значение приоритета
        """
        # Маппинг строковых приоритетов в числа
        priority_map = {
            'high': 1,
            'medium': 5,
            'low': 10,
            'default': 5
        }
        
        # Если уже число, возвращаем как есть
        if isinstance(priority_value, int):
            return priority_value
        
        # Если строка, конвертируем
        if isinstance(priority_value, str):
            return priority_map.get(priority_value.lower(), 5)
        
        # По умолчанию средний приоритет
        return 5
    
    def _load_rss_sources(self) -> List[Dict[str, str]]:
        """
        Загружает список RSS-источников из файла конфигурации или переменной окружения
        
        Returns:
            List[Dict[str, str]]: список источников
        """
        # Сначала пробуем загрузить из файла конфигурации
        sources_from_file = self._load_rss_sources_from_file()
        
        # Если из файла не удалось загрузить или нет источников, 
        # пробуем из переменной окружения
        if not sources_from_file:
            sources_from_env = self._parse_rss_sources_from_env(os.getenv("RSS_SOURCES", ""))
            if sources_from_env:
                return sources_from_env
            else:
                logger.warning("Не найдены RSS-источники ни в файле, ни в переменной окружения")
                return []
        
        return sources_from_file
    
    def _load_rss_sources_from_file(self) -> List[Dict[str, str]]:
        """
        Загружает список RSS-источников из файла конфигурации и сохраняет их в базе данных
        
        Returns:
            List[Dict[str, str]]: список источников из файла или пустой список при ошибке
        """
        sources = []
        
        if not os.path.exists(self.config_file):
            logger.warning(f"Файл конфигурации RSS не найден: {self.config_file}")
            return sources
        
        try:
            # Определяем формат файла по расширению
            file_ext = os.path.splitext(self.config_file)[1].lower()
            
        except Exception as e:
            logger.error(f"Ошибка при определении формата файла {self.config_file}: {e}")
            logger.error(traceback.format_exc())
            return sources
            
        config = None
        
        if file_ext == '.json':
            # Загружаем JSON
            import json
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
        elif file_ext in ['.yaml', '.yml']:
            # Загружаем YAML
            try:
                import yaml
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
            except ImportError:
                logger.error("Для работы с YAML необходимо установить PyYAML: pip install pyyaml")
                return sources
        else:
            logger.error(f"Неподдерживаемый формат файла: {file_ext}. Поддерживаются только .json, .yaml, .yml")
            return sources
            
        if not config:
            logger.warning(f"Не удалось загрузить конфигурацию из файла: {self.config_file}")
            return sources
        
        if 'sources' not in config:
            logger.warning(f"В файле конфигурации отсутствует ключ 'sources': {self.config_file}")
            return sources
        
        for source in config['sources']:
            if 'name' not in source or 'url' not in source:
                logger.warning(f"Некорректный формат источника: {source}")
                continue
            
            name = source['name']
            url = source['url']
            
            if not url.startswith(("http://", "https://")):
                logger.warning(f"Неверный URL в источнике {name}: {url}")
                continue
                
            # Копируем все дополнительные поля из конфигурации
            source_copy = {k: v for k, v in source.items()}
            
            # Конвертируем приоритет из строки в число
            if 'priority' in source_copy:
                source_copy['priority'] = self._convert_priority(source_copy['priority'])
            else:
                source_copy['priority'] = 5  # Средний приоритет по умолчанию
            
            # Добавляем category_id на основе category, если есть
            if 'category' in source and 'category_id' not in source:
                # В будущем тут можно реализовать маппинг категорий на ID
                pass
                
            sources.append(source_copy)
            
            # Логируем с дополнительной информацией, если есть
            additional_info = ""
            if 'category' in source:
                additional_info += f", категория: {source['category']}"
            additional_info += f", приоритет: {source_copy['priority']}"
            
            logger.info(f"Загружен RSS-источник из файла: {name} ({url}{additional_info})")
        
        logger.info(f"Загружено {len(sources)} RSS-источников из файла {self.config_file}. Источники: {[s['name'] for s in sources]}")
        
        # Сохраняем источники в базу данных, если она доступна
        if self.db_manager:
            try:
                # Преобразуем категорию в category_id если нужно
                for source in sources:
                    if 'category' in source and 'category_id' not in source:
                        source['category_id'] = None
                
                self.db_manager.add_rss_sources(sources)
                logger.info(f"Успешно сохранено {len(sources)} RSS-источников в базу данных")
            except Exception as e:
                logger.error(f"Ошибка при сохранении RSS-источников в базу данных: {e}")
                logger.debug(traceback.format_exc())
        
        return sources
    
    def _parse_rss_sources_from_env(self, sources_str: str) -> List[Dict[str, str]]:
        """
        Парсит строку с источниками RSS из переменной окружения
        
        Args:
            sources_str: строка с источниками в формате "название1:url1,название2:url2"
            
        Returns:
            List[Dict[str, str]]: список источников
        """
        sources = []
        
        if not sources_str:
            logger.warning("Строка с RSS-источниками пуста")
            return sources
        
        try:
            # Разделяем строку на отдельные источники
            source_pairs = sources_str.split(",")
            
            for pair in source_pairs:
                if ":" not in pair:
                    logger.warning(f"Неверный формат источника: {pair}, ожидается 'название:url'")
                    continue
                
                name, url = pair.split(":", 1)
                name = name.strip()
                url = url.strip()
                
                if not url.startswith(("http://", "https://")):
                    logger.warning(f"Неверный URL в источнике {name}: {url}")
                    continue
                
                sources.append({"name": name, "url": url})
                logger.info(f"Добавлен RSS-источник из переменной окружения: {name} ({url})")
        
        except Exception as e:
            logger.error(f"Ошибка при разборе строки с RSS-источниками: {e}")
            logger.error(traceback.format_exc())
        
        return sources
    
    async def fetch_rss(self, source: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Асинхронно получает данные из RSS-фида с улучшенной обработкой ошибок
        """
        name = source["name"]
        url = source["url"]
        
        logger.info(f"Начало получения RSS из {name}: {url}")
        
        start_time = time.time()
        
        try:
            # Используем контекстный менеджер для тайм-аута соединения
            timeout = aiohttp.ClientTimeout(total=self.request_timeout)
            
            async with aiohttp.ClientSession(headers=self.headers, timeout=timeout) as session:
                try:
                    async with session.get(url) as response:
                        # Проверяем HTTP-статус
                        if response.status != 200:
                            logger.error(f"Ошибка HTTP при получении RSS из {name}: статус {response.status}, "
                                        f"причина: {response.reason}")
                            return []
                        
                        # Получаем содержимое ответа
                        content = await response.text()
                        content_size = len(content)
                        
                        # Метрики для логирования
                        elapsed_time = time.time() - start_time
                        logger.info(f"Получен ответ от {name} за {elapsed_time:.2f} сек, "
                                f"размер: {content_size/1024:.1f} КБ")
                        
                        # Используем feedparser для разбора RSS
                        feed = feedparser.parse(content)
                        
                        # Проверка на ошибки парсинга
                        if hasattr(feed, 'bozo_exception'):
                            logger.warning(f"Предупреждение при разборе RSS из {name}: "
                                        f"{type(feed.bozo_exception).__name__}: {feed.bozo_exception}")
                        
                        if not feed.entries:
                            logger.warning(f"Нет записей в RSS из {name}")
                            return []
                        
                        # Базовая проверка структуры фида
                        if not hasattr(feed, 'feed') or not hasattr(feed, 'entries'):
                            logger.warning(f"Неверная структура RSS из {name}, отсутствуют обязательные поля")
                            return []
                        
                        # Логируем успешное получение с метаданными
                        logger.info(f"Успешно получено {len(feed.entries)} записей из RSS-источника {name}. "
                                f"Название фида: {feed.feed.get('title', 'Н/Д')}, "
                                f"описание: {feed.feed.get('description', 'Н/Д')[:50]}...")
                        
                        # Добавляем информацию об источнике к каждой записи
                        for entry in feed.entries:
                            entry['source_name'] = name
                            entry['source_url'] = url
                            entry['fetch_timestamp'] = datetime.now(ZoneInfo('UTC')).isoformat()
                        
                        return feed.entries
                        
                except aiohttp.ClientResponseError as e:
                    logger.error(f"Ошибка ответа HTTP при получении RSS из {name}: {e.status} {e.message}")
                except aiohttp.ClientConnectorError as e:
                    logger.error(f"Ошибка соединения при получении RSS из {name}: {e}")
                except aiohttp.ClientPayloadError as e:
                    logger.error(f"Ошибка загрузки данных при получении RSS из {name}: {e}")
                except aiohttp.ClientError as e:
                    logger.error(f"Общая ошибка HTTP-клиента при получении RSS из {name}: {e}")
                
        except asyncio.TimeoutError:
            logger.error(f"Тайм-аут при получении RSS из {name}: превышен лимит {self.request_timeout} сек")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при получении RSS из {name}: {type(e).__name__}: {e}")
            logger.debug(traceback.format_exc())
        
        # Если мы дошли до этой точки, значит произошла ошибка
        logger.error(f"Не удалось получить данные из {name} ({url})")
        return []
    
    async def fetch_rss_with_retry(self, source: Dict[str, str], max_retries=3) -> List[Dict[str, Any]]:
        """
        Асинхронно получает данные из RSS-фида с поддержкой повторных попыток
        
        Args:
            source: словарь с информацией об источнике {"name": name, "url": url}
            max_retries: максимальное количество повторных попыток
            
        Returns:
            List[Dict[str, Any]]: список записей из RSS-фида
        """
        name = source["name"]
        url = source["url"]
        
        # Получаем индивидуальный таймаут для источника, если он задан
        timeout = source.get("timeout", self.request_timeout)
        
        # Определяем базовую задержку между попытками
        base_delay = 2
        
        for attempt in range(max_retries + 1):
            if attempt > 0:
                # Экспоненциальная задержка с небольшим случайным компонентом
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                logger.info(f"Повторная попытка {attempt}/{max_retries} для {name} через {delay:.2f} сек")
                await asyncio.sleep(delay)
            
            try:
                logger.info(f"Попытка {attempt + 1}/{max_retries + 1} получения RSS из {name}: {url}")
                
                entries = await self.fetch_rss(source)
                
                if entries:
                    if attempt > 0:
                        logger.info(f"Успешно получены данные из {name} после {attempt + 1} попыток")
                    return entries
            except Exception as e:
                logger.error(f"Ошибка при попытке {attempt + 1} для {name}: {e}")
        
        logger.error(f"Исчерпаны все {max_retries + 1} попытки для {name}")
        return []                           


    async def fetch_all_rss(self) -> List[Dict[str, Any]]:
        """
        Асинхронно получает данные из всех RSS-источников с оптимизацией
        
        Returns:
            List[Dict[str, Any]]: объединенный список записей из всех RSS-фидов
        """
        if not self.rss_sources:
            logger.warning("Нет настроенных RSS-источников")
            return []
        
        # Создаем словарь для хранения статистики источников
        if not hasattr(self, 'source_stats'):
            self.source_stats = {}
        
        # Группируем источники по приоритету
        priority_groups = {}
        for source in self.rss_sources:
            # Конвертируем приоритет в число, если он строковый
            priority = self._convert_priority(source.get('priority', 5))
            source['priority'] = priority  # Обновляем значение в источнике
            
            if priority not in priority_groups:
                priority_groups[priority] = []
            priority_groups[priority].append(source)
        
        # Сортируем приоритеты (от высшего к низшему)
        sorted_priorities = sorted(priority_groups.keys())
        
        all_entries = []
        
        # Обрабатываем источники группами по приоритету
        for priority in sorted_priorities:
            sources = priority_groups[priority]
            logger.info(f"Обработка {len(sources)} источников с приоритетом {priority}")
            
            # Создаем задачи для асинхронного выполнения
            tasks = []
            for source in sources:
                name = source["name"]
                
                # Определяем, нужны ли повторные попытки на основе предыдущей статистики
                max_retries = 1  # По умолчанию одна попытка
                if name in self.source_stats:
                    # Если были проблемы в прошлый раз, увеличиваем число попыток
                    if self.source_stats[name].get('last_status', 'OK') != 'OK':
                        max_retries = 3
                
                # Создаем задачу
                task = self.fetch_rss_with_retry(source, max_retries=max_retries)
                tasks.append((name, task))
            
            # Выполняем задачи с ограничением на количество одновременных запросов
            for i in range(0, len(tasks), self.max_concurrent_requests):
                batch = tasks[i:i + self.max_concurrent_requests]
                batch_names = [name for name, _ in batch]
                batch_tasks = [task for _, task in batch]
                
                logger.info(f"Обработка пакета из {len(batch)} источников: {', '.join(batch_names)}")
                
                # Запускаем все задачи в пакете одновременно
                batch_start_time = time.time()
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                batch_time = time.time() - batch_start_time
                
                logger.info(f"Пакет обработан за {batch_time:.2f} сек")
                
                # Обрабатываем результаты
                for idx, (name, result) in enumerate(zip(batch_names, batch_results)):
                    source_url = [s["url"] for s in self.rss_sources if s["name"] == name][0]
                    
                    # Обновляем статистику источника
                    if name not in self.source_stats:
                        self.source_stats[name] = {'success_count': 0, 'error_count': 0}
                    
                    if isinstance(result, Exception):
                        # Произошла ошибка
                        logger.error(f"Ошибка при получении RSS из {name}: {result}")
                        self.source_stats[name]['error_count'] = self.source_stats[name].get('error_count', 0) + 1
                        self.source_stats[name]['last_status'] = 'ERROR'
                        self.source_stats[name]['last_error'] = str(result)
                        
                        # Логируем статистику
                        # self.log_rss_stats(name, source_url, 'ERROR', batch_time, 0, str(result))
                    else:
                        # Успешное получение
                        entries_count = len(result)
                        all_entries.extend(result)
                        
                        self.source_stats[name]['success_count'] = self.source_stats[name].get('success_count', 0) + 1
                        self.source_stats[name]['last_status'] = 'OK'
                        self.source_stats[name]['last_entries_count'] = entries_count
                        
                        # Логируем статистику
                        self.log_rss_stats(name, source_url, 'OK', batch_time, entries_count)
                        
                        logger.info(f"Источник {name}: получено {entries_count} записей")
        
        # Выводим сводную статистику
        success_sources = sum(1 for stats in self.source_stats.values() if stats.get('last_status') == 'OK')
        error_sources = sum(1 for stats in self.source_stats.values() if stats.get('last_status') == 'ERROR')
        
        logger.info(f"Итоги загрузки RSS: {success_sources} успешных источников, "
                f"{error_sources} источников с ошибками, "
                f"всего получено {len(all_entries)} записей")
        
        # Сохраняем отчет о состоянии
        # self.save_rss_status_report()
        
        return all_entries
    
    def _generate_simhash(self, text: str) -> str:
        """
        Генерирует simhash для текста
        
        Args:
            text: текст для генерации хэша
            
        Returns:
            str: значение simhash
        """
        if not text:
            return ""
        
        # Для простоты используем SHA-256, в продакшн-решении 
        # можно заменить на полноценный simhash алгоритм
        return hashlib.sha256(text.encode('utf-8')).hexdigest()
    
    def _parse_rss_date(self, date_str: str) -> Optional[datetime]:
        """
        Парсит дату из строки в формате RSS
        
        Args:
            date_str: строка с датой
            
        Returns:
            Optional[datetime]: объект datetime или None, если не удалось распарсить
        """
        if not date_str:
            return None
        
        try:
            # feedparser обычно преобразует даты в объекты time.struct_time
            if isinstance(date_str, tuple) and len(date_str) == 9:
                return datetime.fromtimestamp(time.mktime(date_str), tz=ZoneInfo('UTC'))
            
            # Иначе пробуем распарсить строку
            parsed_date = feedparser._parse_date(date_str)
            if parsed_date:
                return datetime.fromtimestamp(time.mktime(parsed_date), tz=ZoneInfo('UTC'))
        except Exception as e:
            logger.warning(f"Не удалось распарсить дату: {date_str}, ошибка: {e}")
        
        return None
    
    def _extract_content(self, entry: Dict[str, Any]) -> tuple[str, str]:
        """
        Извлекает контент из RSS-записи, сохраняя как HTML, так и чистый текст
        
        Args:
            entry: запись из RSS-фида
            
        Returns:
            tuple[str, str]: (полный HTML контент, очищенный текст)
        """
        html_parts = []
        text_parts = []
        
        # 1. Извлекаем контент из поля 'content' (обычно самое полное)
        if hasattr(entry, 'content') and entry.content:
            for content_item in entry.content:
                if isinstance(content_item, dict) and 'value' in content_item:
                    html_parts.append(content_item['value'])
                elif hasattr(content_item, 'value'):
                    html_parts.append(content_item.value)
        
        # 2. Если content пустой, пробуем summary_detail (часто содержит полный HTML)
        if not html_parts and hasattr(entry, 'summary_detail') and entry.summary_detail:
            if isinstance(entry.summary_detail, dict) and 'value' in entry.summary_detail:
                html_parts.append(entry.summary_detail['value'])
        
        # 3. Затем проверяем summary (может быть краткое описание)
        if hasattr(entry, 'summary') and entry.summary:
            if entry.summary not in html_parts:  # Избегаем дублирования
                html_parts.append(entry.summary)
        
        # 4. И наконец description (иногда дублирует summary)
        if hasattr(entry, 'description') and entry.description:
            if entry.description not in html_parts:  # Избегаем дублирования
                html_parts.append(entry.description)
        
        # Объединяем все HTML части
        full_html_content = "\n\n".join(html_parts)
        
        # Создаем текстовую версию
        from bs4 import BeautifulSoup
        
        for html_part in html_parts:
            try:
                # Используем BeautifulSoup для правильного извлечения текста
                soup = BeautifulSoup(html_part, 'html.parser')
                
                # Удаляем скрипты и стили
                for script in soup(["script", "style"]):
                    script.decompose()
                
                # Извлекаем текст с сохранением структуры абзацев
                text = soup.get_text(separator='\n', strip=True)
                
                # Очищаем лишние пробелы, но сохраняем переносы строк
                text = '\n'.join(line.strip() for line in text.split('\n') if line.strip())
                
                if text:
                    text_parts.append(text)
                    
            except Exception as e:
                logger.warning(f"Ошибка при парсинге HTML с BeautifulSoup: {e}")
                # Fallback на простое удаление тегов
                clean_text = re.sub(r'<[^>]+>', ' ', html_part)
                clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                if clean_text:
                    text_parts.append(clean_text)
        
        # Объединяем текстовые части
        full_text_content = "\n\n".join(text_parts)
        
        # Логируем размер извлеченного контента
        logger.debug(f"Извлечено {len(full_html_content)} символов HTML и {len(full_text_content)} символов текста")
        logger.debug(f"RSS fields used: content={bool(entry.get('content'))}, summary={bool(entry.get('summary'))}, description={bool(entry.get('description'))}")
        
        return full_html_content, full_text_content


    def convert_entries_to_posts(self, entries: List[Dict[str, Any]]) -> List[Post]:
        posts = []
        for idx, entry in enumerate(entries):
            try:
                title = entry.get('title', '')
                link = entry.get('link', '')
                source_name = entry.get('source_name', 'RSS')
                html_content, text_content = self._extract_content(entry)
                content = text_content
                
                if not content or len(content) < 100:
                    logger.warning(f"Короткий контент для '{title}' из {source_name}: {len(content)} символов")
                
                published_date = None
                if 'published_parsed' in entry and entry.published_parsed:
                    published_date = datetime.fromtimestamp(time.mktime(entry.published_parsed), tz=ZoneInfo('UTC'))
                elif 'updated_parsed' in entry and entry.updated_parsed:
                    published_date = datetime.fromtimestamp(time.mktime(entry.updated_parsed), tz=ZoneInfo('UTC'))
                elif 'published' in entry and entry.published:
                    published_date = self._parse_rss_date(entry.published)
                elif 'updated' in entry and entry.updated:
                    published_date = self._parse_rss_date(entry.updated)
                
                if not published_date:
                    published_date = datetime.now(ZoneInfo('UTC'))
                    logger.warning(f"Не удалось определить дату для '{title}', используется текущее время")
                
                if link:
                    post_id = f"rss_{hashlib.md5(link.encode()).hexdigest()}"
                else:
                    unique_str = f"{source_name}_{title}_{published_date.isoformat()}"
                    post_id = f"rss_{hashlib.md5(unique_str.encode()).hexdigest()}"
                
                simhash = self._generate_simhash(content)
                
                post = Post(
                    post_id=post_id,
                    content=content,
                    blog_host=source_name,
                    blog_host_type=5,
                    published_on=published_date,
                    simhash=simhash,
                    url=link,
                    title=title
                )
                post.html_content = html_content  # Сохраняем HTML-контент
                posts.append(post)
                logger.info(f"Создан пост: {title[:50]}... ({len(content)} символов) из {source_name}")
            except Exception as e:
                logger.error(f"Ошибка при преобразовании RSS-записи: {e}")
        logger.info(f"Преобразовано {len(posts)} RSS-записей")
        if self.db_manager and posts:
            try:
                new_posts_count = self.db_manager.save_posts(posts)
                logger.info(f"Сохранено {new_posts_count} новых постов в БД")
            except Exception as e:
                logger.error(f"Ошибка при сохранении постов: {e}")
        return posts
    
    def _filter_entries_by_date(self, entries: List[Dict[str, Any]], date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """
        Фильтрует записи по дате публикации
        
        Args:
            entries: список записей из RSS-фидов
            date_from: начальная дата диапазона
            date_to: конечная дата диапазона
            
        Returns:
            List[Dict[str, Any]]: отфильтрованный список записей
        """
        filtered_entries = []
        
        # Убедимся, что date_from и date_to имеют timezone
        if date_from.tzinfo is None:
            date_from = date_from.replace(tzinfo=ZoneInfo('UTC'))
        if date_to.tzinfo is None:
            date_to = date_to.replace(tzinfo=ZoneInfo('UTC'))
        
        for entry in entries:
            # Получаем дату публикации
            published_date = None
            
            if 'published_parsed' in entry and entry.published_parsed:
                published_date = datetime.fromtimestamp(time.mktime(entry.published_parsed), tz=ZoneInfo('UTC'))
            elif 'updated_parsed' in entry and entry.updated_parsed:
                published_date = datetime.fromtimestamp(time.mktime(entry.updated_parsed), tz=ZoneInfo('UTC'))
            elif 'published' in entry and entry.published:
                published_date = self._parse_rss_date(entry.published)
            elif 'updated' in entry and entry.updated:
                published_date = self._parse_rss_date(entry.updated)
            
            # Если дата не определена, пропускаем запись
            if not published_date:
                logger.warning(f"Не удалось определить дату публикации для записи: {entry.get('title', 'Без заголовка')}")
                continue
            
            # Убедимся, что published_date имеет timezone
            if published_date.tzinfo is None:
                published_date = published_date.replace(tzinfo=ZoneInfo('UTC'))
            
            # Проверяем, входит ли дата в заданный диапазон
            if date_from <= published_date <= date_to:
                filtered_entries.append(entry)
            else:
                logger.debug(f"Запись вне диапазона дат: {published_date} (диапазон: {date_from} - {date_to})")
        
        logger.info(f"Отфильтровано {len(filtered_entries)} записей из {len(entries)} по дате")
        return filtered_entries
    
    
    async def get_posts(self, date_from: datetime, date_to: datetime) -> List[Post]:
        """
        Получает посты из RSS-источников за указанный период, сначала проверяя базу данных,
        а затем при необходимости получая новые записи
        
        Args:
            date_from: начальная дата диапазона
            date_to: конечная дата диапазона
            
        Returns:
            List[Post]: список объектов Post
        """
        logger.info("Вызов метода get_posts")
        all_posts = []
        
        # Убедимся, что даты имеют timezone
        if date_from.tzinfo is None:
            date_from = date_from.replace(tzinfo=ZoneInfo('UTC'))
        if date_to.tzinfo is None:
            date_to = date_to.replace(tzinfo=ZoneInfo('UTC'))
        
        # Сначала проверяем, есть ли посты в базе данных за указанный период
        if self.db_manager:
            try:
                # Получаем посты из базы данных
                db_posts = self.db_manager.get_posts_by_date_range(date_from, date_to)
                
                if db_posts:
                    logger.info(f"Получено {len(db_posts)} постов из базы данных за период с {date_from} по {date_to}")
                    
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
                        all_posts.append(post)
                    
                    # Если нашли достаточно постов в БД, возвращаем их
                    if len(all_posts) >= 10:  # Можно настроить минимальное количество
                        return all_posts
            except Exception as e:
                logger.error(f"Ошибка при получении постов из базы данных: {e}")
                logger.error(traceback.format_exc())
        
        # Если в базе недостаточно постов или произошла ошибка, получаем новые из RSS
        # Получаем все записи из RSS
        all_entries = await self.fetch_all_rss()
        
        if not all_entries:
            logger.warning("Не получено записей из RSS-источников")
            return all_posts  # Возвращаем то, что уже есть (возможно, из БД)
        
        # Фильтруем записи по дате
        filtered_entries = self._filter_entries_by_date(all_entries, date_from, date_to)
        
        if not filtered_entries:
            logger.warning(f"Нет новых записей в RSS в диапазоне дат: {date_from} - {date_to}")
            return all_posts  # Возвращаем то, что уже есть (возможно, из БД)
        
        # Преобразуем записи в объекты Post и сохраняем в БД
        new_posts = self.convert_entries_to_posts(filtered_entries)
        if self.db_manager and new_posts:
            try:
                saved = self.db_manager.save_posts(new_posts)
                logger.info(
                    f"Дополнительно сохранено {saved} постов в БД из get_posts()"
                )
            except Exception as e:
                logger.error(f"Ошибка при дополнительном сохранении постов: {e}")
                
        # Объединяем с постами из БД, избегая дубликатов
        existing_post_ids = {post.post_id for post in all_posts}
        for post in new_posts:
            if post.post_id not in existing_post_ids:
                all_posts.append(post)
                existing_post_ids.add(post.post_id)
        
        logger.info(f"Получено {len(all_posts)} постов из RSS и базы данных за период с {date_from} по {date_to}")
        return all_posts
    
    def setup_rss_logging(self):
        """
        Настраивает специализированное логирование для RSS-компонента
        """
        # Создаем отдельный лог-файл для RSS

        logs_dir = os.path.join(os.getcwd(), "logs")
        os.makedirs(logs_dir, exist_ok=True)
        log_path = os.path.join(logs_dir, "rss_manager_{time}.log")
        
        # Добавляем логгер с дополнительной информацией
        logger.add(
            log_path,
            rotation="10 MB",
            retention="21 days",
            encoding="utf-8",
            enqueue=True,
            backtrace=False,
            diagnose=False,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
            level="INFO",
        )
        
        # Создаем файл со статистикой RSS-запросов

        self.stats_file = os.path.join(logs_dir, "rss_stats.csv")
        
        # Если файл не существует, создаем его с заголовками
        if not os.path.exists(self.stats_file):
            os.makedirs(os.path.dirname(self.stats_file), exist_ok=True)
            with open(self.stats_file, 'w', encoding='utf-8') as f:
                f.write("timestamp,source,url,status,time_taken_ms,entries_count,error\n")
        
        logger.info("Настроено расширенное логирование для RSS-менеджера")

    def log_rss_stats(self, source: str, url: str, status: str, time_taken: float, 
                    entries_count: int = 0, error: str = ""):
        """
        Записывает статистику RSS-запроса в CSV-файл
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Экранируем запятые в строковых полях
        source_esc = source.replace(',', ';')
        url_esc = url.replace(',', ';')
        error_esc = error.replace(',', ';').replace('\n', ' ').replace('\r', '')
        
        # Формируем строку для записи
        row = f"{timestamp},{source_esc},{url_esc},{status},{time_taken*1000:.2f},{entries_count},{error_esc}\n"
        
        # Записываем в файл статистики
        try:
            with open(self.stats_file, 'a', encoding='utf-8') as f:
                f.write(row)
        except Exception as e:
            logger.error(f"Ошибка при записи статистики RSS: {e}")

    async def generate_rss_health_report(self) -> str:
        """
        Генерирует отчет о состоянии RSS-источников
        
        Returns:
            str: текстовый отчет о состоянии источников
        """
        report_lines = ["# Отчет о состоянии RSS-источников", ""]
        
        # Временная карта для хранения результатов проверки
        health_results = {}
        
        # Проверяем каждый источник
        for source in self.rss_sources:
            name = source["name"]
            url = source["url"]
            
            # Выполняем быструю проверку подключения
            start_time = time.time()
            
            try:
                timeout = aiohttp.ClientTimeout(total=5)  # Короткий таймаут для проверки
                
                async with aiohttp.ClientSession(headers=self.headers, timeout=timeout) as session:
                    async with session.get(url) as response:
                        status_code = response.status
                        response_time = time.time() - start_time
                        
                        if status_code == 200:
                            try:
                                content = await response.text()
                                feed = feedparser.parse(content)
                                entries_count = len(feed.entries)
                                
                                # Проверяем наличие ошибок парсинга
                                parsing_error = ""
                                if hasattr(feed, 'bozo_exception'):
                                    parsing_error = f"Ошибка парсинга: {feed.bozo_exception}"
                                
                                health_results[name] = {
                                    "status": "OK" if entries_count > 0 and not parsing_error else "WARN",
                                    "url": url,
                                    "response_time": response_time,
                                    "status_code": status_code,
                                    "entries_count": entries_count,
                                    "parsing_error": parsing_error
                                }
                            except Exception as e:
                                health_results[name] = {
                                    "status": "ERROR",
                                    "url": url,
                                    "response_time": response_time,
                                    "status_code": status_code,
                                    "error": f"Ошибка обработки: {str(e)}"
                                }
                        else:
                            health_results[name] = {
                                "status": "ERROR",
                                "url": url,
                                "response_time": response_time,
                                "status_code": status_code,
                                "error": f"Неверный HTTP-статус: {status_code}"
                            }
            except Exception as e:
                response_time = time.time() - start_time
                health_results[name] = {
                    "status": "ERROR",
                    "url": url,
                    "response_time": response_time,
                    "error": f"Ошибка подключения: {str(e)}"
                }
        
        # Формируем отчет по категориям статусов
        status_categories = {
            "OK": [],
            "WARN": [],
            "ERROR": []
        }
        
        for name, result in health_results.items():
            status_categories[result["status"]].append((name, result))
        
        # Добавляем сводную информацию
        total_sources = len(health_results)
        ok_count = len(status_categories["OK"])
        warn_count = len(status_categories["WARN"])
        error_count = len(status_categories["ERROR"])
        
        report_lines.append(f"## Сводка")
        report_lines.append(f"- Всего источников: {total_sources}")
        report_lines.append(f"- Работают нормально: {ok_count} ({ok_count/total_sources*100:.1f}%)")
        report_lines.append(f"- С предупреждениями: {warn_count} ({warn_count/total_sources*100:.1f}%)")
        report_lines.append(f"- С ошибками: {error_count} ({error_count/total_sources*100:.1f}%)")
        report_lines.append("")
        
        # Добавляем детали по каждому источнику
        for status, sources in status_categories.items():
            if sources:
                report_lines.append(f"## Источники со статусом: {status}")
                for name, result in sources:
                    report_lines.append(f"### {name}")
                    report_lines.append(f"- URL: {result['url']}")
                    report_lines.append(f"- Время ответа: {result['response_time']*1000:.2f} мс")
                    
                    if "status_code" in result:
                        report_lines.append(f"- HTTP-статус: {result['status_code']}")
                    
                    if "entries_count" in result:
                        report_lines.append(f"- Количество записей: {result['entries_count']}")
                    
                    if "parsing_error" in result and result["parsing_error"]:
                        report_lines.append(f"- Ошибка парсинга: {result['parsing_error']}")
                    
                    if "error" in result:
                        report_lines.append(f"- Ошибка: {result['error']}")
                    
                    report_lines.append("")
        
        # Формируем итоговый отчет
        report = "\n".join(report_lines)
        
        # Сохраняем отчет в файл
        report_path = "/app/logs/rss_health_report.md"
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report)
        
        logger.info(f"Сформирован отчет о состоянии RSS-источников: {report_path}")
        
        return report

    async def monitor_rss_sources(self, interval_hours=12, telegram_sender=None):
        """
        Запускает периодический мониторинг RSS-источников
        
        Args:
            interval_hours: интервал проверки в часах
            telegram_sender: объект для отправки уведомлений в Telegram
        """
        while True:
            logger.info(f"Запуск мониторинга RSS-источников")
            
            try:
                # Генерируем отчет о состоянии
                report = await self.generate_rss_health_report()
                
                # Анализируем результаты
                ok_pattern = r"- Работают нормально: (\d+) \([\d\.]+%\)"
                error_pattern = r"- С ошибками: (\d+) \([\d\.]+%\)"
                
                ok_match = re.search(ok_pattern, report)
                error_match = re.search(error_pattern, report)
                
                ok_count = int(ok_match.group(1)) if ok_match else 0
                error_count = int(error_match.group(1)) if error_match else 0
                
                # Если есть источники с ошибками, отправляем уведомление
                if error_count > 0 and telegram_sender:
                    # Создаем короткое сообщение для Telegram
                    message = f"⚠️ *Внимание: Проблемы с RSS-источниками*\n\n"
                    message += f"• Всего источников: {len(self.rss_sources)}\n"
                    message += f"• Работают нормально: {ok_count}\n"
                    message += f"• С ошибками: {error_count}\n\n"
                    
                    # Добавляем список проблемных источников
                    error_sources = re.findall(r"### (.+?)\n- URL: (.+?)\n.*?- Ошибка: (.+?)\n", report, re.DOTALL)
                    if error_sources:
                        message += "*Проблемные источники:*\n"
                        for name, url, error in error_sources[:5]:  # Ограничиваем до 5 источников
                            short_error = error[:50] + "..." if len(error) > 50 else error
                            message += f"• {name}: {short_error}\n"
                        
                        if len(error_sources) > 5:
                            message += f"... и еще {len(error_sources) - 5} источников с ошибками\n"
                    
                    message += f"\nПолный отчет доступен в файле логов: `/app/logs/rss_health_report.md`"
                    
                    # Отправляем уведомление
                    await telegram_sender.send_message(message)
                    logger.info(f"Отправлено уведомление о проблемах с RSS-источниками")
                
            except Exception as e:
                logger.error(f"Ошибка при мониторинге RSS-источников: {e}")
                logger.error(traceback.format_exc())
            
            # Ждем до следующей проверки
            logger.info(f"Следующая проверка RSS-источников через {interval_hours} часов")
            await asyncio.sleep(interval_hours * 3600)

    def save_rss_status_report(self):
        """
        Сохраняет отчет о состоянии RSS-источников в JSON-файл
        """
        if not hasattr(self, 'source_stats'):
            logger.warning("Нет данных о состоянии RSS-источников")
            return
        
        # Добавляем дополнительную информацию
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "total_sources": len(self.rss_sources),
            "sources": {}
        }
        
        # Собираем информацию по каждому источнику
        for source in self.rss_sources:
            name = source["name"]
            url = source["url"]
            
            source_data = {
                "url": url,
                "category": source.get("category", ""),
                "priority": source.get("priority", 5)
            }
            
            # Добавляем статистику, если она есть
            if name in self.source_stats:
                source_data.update(self.source_stats[name])
            
            report_data["sources"][name] = source_data
        
        # Сохраняем отчет в файл
        report_path = "/app/logs/rss_status_report.json"
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Сохранен отчет о состоянии RSS-источников: {report_path}")
        return report_path


async def fetch_all_rss() -> list[Post]:
    """
    Универсальный вход из планировщика: RSS → Post
    Берёт посты за последние 2 дня.
    """
    manager = RSSManager()
    now = datetime.now(tz=ZoneInfo("Europe/Moscow"))
    date_from = now - timedelta(days=2)
    return await manager.get_posts(date_from=date_from, date_to=now)


