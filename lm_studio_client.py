import os
import json
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional, Tuple
from loguru import logger
from dotenv import load_dotenv
import traceback

# Загрузка переменных окружения
load_dotenv()

class LMStudioClient:
   """
   Клиент для работы с LM Studio API.
   LM Studio предоставляет OpenAI-совместимый API для локальных языковых моделей.
   """
   
   def __init__(self):
       """
       Инициализирует клиент для работы с LM Studio
       """
       # URL для LM Studio API (по умолчанию работает на localhost:1234)
       self.base_url = os.getenv("LM_STUDIO_URL", "http://localhost:1234/v1")
       
       # Модели для разных задач (можно настроить в .env)
       self.relevance_model = os.getenv("LM_STUDIO_RELEVANCE_MODEL", "local-model")
       self.classification_model = os.getenv("LM_STUDIO_CLASSIFICATION_MODEL", "local-model")
       self.analysis_model = os.getenv("LM_STUDIO_ANALYSIS_MODEL", "local-model")
       
       # Параметры запросов
       self.timeout = int(os.getenv("LM_STUDIO_TIMEOUT", "120"))  # Таймаут в секундах
       self.max_retries = int(os.getenv("LM_STUDIO_MAX_RETRIES", "3"))
       
       # Температура для разных задач
       self.relevance_temperature = float(os.getenv("LM_STUDIO_RELEVANCE_TEMP", "0.1"))
       self.classification_temperature = float(os.getenv("LM_STUDIO_CLASSIFICATION_TEMP", "0.1"))
       self.analysis_temperature = float(os.getenv("LM_STUDIO_ANALYSIS_TEMP", "0.3"))
       
       logger.info(f"Инициализирован LM Studio клиент: {self.base_url}")
   
   async def _make_request(self, endpoint: str, payload: Dict[str, Any], retry_count: int = 0) -> Optional[Dict[str, Any]]:
       """
       Выполняет HTTP запрос к LM Studio API
       
       Args:
           endpoint: Конечная точка API (например, "/chat/completions")
           payload: Тело запроса
           retry_count: Текущее количество попыток
           
       Returns:
           Dict с ответом или None в случае ошибки
       """
       url = f"{self.base_url}{endpoint}"
       
       try:
           timeout = aiohttp.ClientTimeout(total=self.timeout)
           async with aiohttp.ClientSession(timeout=timeout) as session:
               async with session.post(url, json=payload) as response:
                   if response.status == 200:
                       return await response.json()
                   else:
                       error_text = await response.text()
                       logger.error(f"Ошибка LM Studio API: {response.status} - {error_text}")
                       
                       # Повторная попытка при временных ошибках
                       if response.status in [500, 502, 503, 504] and retry_count < self.max_retries:
                           wait_time = (retry_count + 1) * 2
                           logger.info(f"Повторная попытка через {wait_time} секунд...")
                           await asyncio.sleep(wait_time)
                           return await self._make_request(endpoint, payload, retry_count + 1)
                       
                       return None
                       
       except asyncio.TimeoutError:
           logger.error(f"Таймаут при запросе к LM Studio: {self.timeout}с")
           if retry_count < self.max_retries:
               wait_time = (retry_count + 1) * 2
               logger.info(f"Повторная попытка после таймаута через {wait_time} секунд...")
               await asyncio.sleep(wait_time)
               return await self._make_request(endpoint, payload, retry_count + 1)
           return None
           
       except Exception as e:
           logger.error(f"Ошибка при запросе к LM Studio: {e}")
           logger.error(traceback.format_exc())
           return None
   
   async def check_relevance(self, post_id: str, title: str, content: str) -> Tuple[bool, float]:
       """
       Проверяет релевантность поста для бизнеса в области репутационного консалтинга
       
       Args:
           post_id: Идентификатор поста
           title: Заголовок поста
           content: Содержание поста
           
       Returns:
           Tuple[bool, float]: (релевантность True/False, оценка от 0 до 1)
       """
       prompt = f"""Определи, релевантна ли эта публикация для бизнеса в области репутационного консалтинга.

Релевантными считаются темы:

1. COMPLIANCE И ПРОВЕРКИ:
- KYC (Know Your Customer) - идентификация и проверка клиентов
- AML (Anti-Money Laundering) - противодействие отмыванию денег
- Compliance и комплаенс - соответствие регуляторным требованиям
- Due Diligence - комплексные проверки контрагентов

2. САНКЦИОННЫЕ СПИСКИ И ПРОВЕРКИ:
- Санкционные списки и проверка по ним
- OFAC (Office of Foreign Assets Control)
- PEP (Politically Exposed Persons) - политически значимые лица
- World-Check, LexisNexis и другие системы проверки

3. БАНКОВСКИЕ БЛОКИРОВКИ:
- Блокировка счетов (в пределах 3 слов от "счет")
- Закрытие счетов (в пределах 3 слов от "счет")
- Проверка благонадежности (в пределах 5 слов)
- Частный капитал, private wealth management

4. РЕПУТАЦИОННЫЕ РИСКИ:
- Репутационные риски, кризисы, ущерб (в пределах 5 слов)
- Онлайн-репутация, цифровая репутация
- Репутационные скандалы и атаки

5. УПРАВЛЕНИЕ ПОИСКОВОЙ ВЫДАЧЕЙ:
- Негатив в поисковой выдаче или результатах поиска
- Ложная информация или компромат в поиске
- Негативные или фейковые отзывы, влияющие на репутацию бизнеса

6. PR-КРИЗИСЫ И АТАКИ:
- Черный PR против компаний или брендов
- Информационные атаки на бизнес
- PR-кризисы компаний

7. УСЛУГИ УПРАВЛЕНИЯ РЕПУТАЦИЕЙ:
- Управление репутацией как услуга
- SERM (Search Engine Reputation Management)
- Специалисты по цифровому профилю

НЕ РЕЛЕВАНТНЫ (исключения):
- Спорт (футбол, хоккей, теннис и т.д.) - КРОМЕ коррупционных скандалов
- Шоу-бизнес (артисты, певцы, актеры) - КРОМЕ связи с бизнес-репутацией
- Бытовые происшествия без связи с бизнесом
- Общие политические новости без связи с санкциями или бизнесом
- Технологические новости без связи с compliance или безопасностью

Публикация:
Заголовок: {title}
Содержание: {content[:1000]}...

Внимательно проанализируй текст на наличие ключевых терминов и контекста.
Учитывай близость слов (например, "блокировка" должна быть рядом со "счет").

Ответь ТОЛЬКО в формате JSON:
{{
 "relevant": true/false,
 "score": 0.0-1.0,
 "reason": "краткое обоснование",
 "matched_topics": ["список совпавших тем"]
}}"""

       payload = {
           "model": self.relevance_model,
           "messages": [
               {"role": "user", "content": prompt}
           ],
           "temperature": self.relevance_temperature,
           "max_tokens": 200
       }
       
       try:
           response = await self._make_request("/chat/completions", payload)
           
           if response and "choices" in response and len(response["choices"]) > 0:
               content = response["choices"][0]["message"]["content"]
               
               # Пытаемся извлечь JSON из ответа
               # Иногда модель может добавить текст до или после JSON
               json_start = content.find('{')
               json_end = content.rfind('}') + 1
               
               if json_start >= 0 and json_end > json_start:
                   json_str = content[json_start:json_end]
                   result = json.loads(json_str)
               else:
                   # Если не нашли JSON, пытаемся распарсить весь ответ
                   result = json.loads(content)
               
               relevant = result.get("relevant", False)
               score = float(result.get("score", 0.0))
               reason = result.get("reason", "")
               matched_topics = result.get("matched_topics", [])
               
               logger.info(f"Пост {post_id}: релевантность={relevant}, score={score:.2f}, "
                          f"темы: {', '.join(matched_topics)}, причина: {reason}")
               return relevant, score
           else:
               logger.error(f"Некорректный ответ от LM Studio для поста {post_id}")
               return False, 0.0
               
       except json.JSONDecodeError as e:
           logger.error(f"Ошибка парсинга JSON ответа для поста {post_id}: {e}")
           logger.error(f"Полученный ответ: {content if 'content' in locals() else 'Не получен'}")
           return False, 0.0
       except Exception as e:
           logger.error(f"Ошибка при проверке релевантности поста {post_id}: {e}")
           logger.error(traceback.format_exc())
           return False, 0.0
   
   async def classify_content(self, post_id: str, title: str, content: str, categories: Dict[str, List[str]]) -> Tuple[str, str, float]:
       """
       Классифицирует контент по категориям и подкатегориям
       
       Args:
           post_id: Идентификатор поста
           title: Заголовок поста
           content: Содержание поста
           categories: Словарь категорий и подкатегорий
           
       Returns:
           Tuple[str, str, float]: (категория, подкатегория, уверенность)
       """
       # Формируем список категорий для промпта
       categories_text = []
       for category, subcategories in categories.items():
           subcats = ", ".join(subcategories)
           categories_text.append(f"- {category}: {subcats}")
       categories_list = "\n".join(categories_text)
       
       prompt = f"""Классифицируй эту публикацию по одной из следующих категорий:

{categories_list}

Публикация:
Заголовок: {title}
Содержание: {content[:1500]}...

Выбери ОДНУ наиболее подходящую категорию и подкатегорию.

Ответь ТОЛЬКО в формате JSON:
{{
 "category": "название категории",
 "subcategory": "название подкатегории",
 "confidence": 0.0-1.0
}}"""

       payload = {
           "model": self.classification_model,
           "messages": [
               {"role": "user", "content": prompt}
           ],
           "temperature": self.classification_temperature,
           "max_tokens": 100
       }
       
       try:
           response = await self._make_request("/chat/completions", payload)
           
           if response and "choices" in response and len(response["choices"]) > 0:
               content = response["choices"][0]["message"]["content"]
               
               # Пытаемся извлечь JSON из ответа
               json_start = content.find('{')
               json_end = content.rfind('}') + 1
               
               if json_start >= 0 and json_end > json_start:
                   json_str = content[json_start:json_end]
                   result = json.loads(json_str)
               else:
                   result = json.loads(content)
               
               category = result.get("category", "")
               subcategory = result.get("subcategory", "")
               confidence = float(result.get("confidence", 0.0))
               
               # Валидация категории
               if category in categories:
                   if subcategory not in categories[category]:
                       # Если подкатегория неверная, берем первую из списка
                       subcategory = categories[category][0]
                   
                   logger.info(f"Пост {post_id} классифицирован: {category}/{subcategory} (уверенность: {confidence:.2f})")
                   return category, subcategory, confidence
               else:
                   logger.warning(f"Неизвестная категория '{category}' для поста {post_id}")
                   return "", "", 0.0
           else:
               logger.error(f"Некорректный ответ от LM Studio для классификации поста {post_id}")
               return "", "", 0.0
               
       except json.JSONDecodeError as e:
           logger.error(f"Ошибка парсинга JSON при классификации поста {post_id}: {e}")
           logger.error(f"Полученный ответ: {content if 'content' in locals() else 'Не получен'}")
           return "", "", 0.0
       except Exception as e:
           logger.error(f"Ошибка при классификации поста {post_id}: {e}")
           logger.error(traceback.format_exc())
           return "", "", 0.0
   
   async def analyze_and_summarize(self, posts: List[Dict[str, Any]], max_stories: int = 10) -> str:
       """
       Анализирует список постов и выделяет ключевые сюжеты
       
       Args:
           posts: Список постов с метаданными
           max_stories: Максимальное количество сюжетов
           
       Returns:
           str: Текст анализа для отправки в Telegram
       """
       # Формируем текст с постами для анализа
       posts_text = []
       for i, post in enumerate(posts):
           post_text = f"""[POST_ID:{post['post_id']}]
Источник: {post.get('blog_host', 'Неизвестный источник')}
Заголовок: {post.get('title', 'Без заголовка')}
Категория: {post.get('category', 'Не указана')}/{post.get('subcategory', 'Не указана')}
Содержание: {post.get('content', '')[:500]}...
---"""
           posts_text.append(post_text)
       
       all_posts_text = "\n\n".join(posts_text)
       
       prompt = f"""Проанализируй следующие публикации и выдели {max_stories} наиболее важных сюжетов для бизнеса в области репутационного консалтинга.

Для каждого сюжета:
1. Дай краткое название
2. Опиши суть в 3-5 предложениях
3. Укажи POST_ID основного источника
4. Предложи идею для контент-плана

Фокусируйся на темах: санкции, KYC, Due Diligence, репутационные риски, блокировки счетов, compliance.

Публикации:
{all_posts_text}

Формат ответа для КАЖДОГО сюжета:
СЮЖЕТ: [Название]
СОДЕРЖАНИЕ: [Описание]
POST_ID: [ID основного источника]
ПРЕДЛОЖЕНИЯ ДЛЯ КОНТЕНТ-ПЛАНА: [Идея]

---

Выдели ровно {max_stories} сюжетов."""

       payload = {
           "model": self.analysis_model,
           "messages": [
               {"role": "user", "content": prompt}
           ],
           "temperature": self.analysis_temperature,
           "max_tokens": 3000
       }
       
       try:
           response = await self._make_request("/chat/completions", payload)
           
           if response and "choices" in response and len(response["choices"]) > 0:
               analysis = response["choices"][0]["message"]["content"]
               logger.info(f"Получен анализ от LM Studio, длина: {len(analysis)} символов")
               return analysis
           else:
               logger.error("Некорректный ответ от LM Studio при анализе")
               return ""
               
       except Exception as e:
           logger.error(f"Ошибка при анализе и суммаризации: {e}")
           logger.error(traceback.format_exc())
           return ""
   
   async def test_connection(self) -> bool:
       """
       Проверяет доступность LM Studio API
       
       Returns:
           bool: True если API доступен
       """
       try:
           # Простой запрос для проверки доступности
           payload = {
               "model": self.relevance_model,
               "messages": [
                   {"role": "user", "content": "Hello"}
               ],
               "max_tokens": 10
           }
           
           response = await self._make_request("/chat/completions", payload)
           
           if response and "choices" in response:
               logger.info("Успешное подключение к LM Studio API")
               return True
           else:
               logger.error("LM Studio API недоступен")
               return False
               
       except Exception as e:
           logger.error(f"Ошибка при проверке подключения к LM Studio: {e}")
           return False
   
   async def get_models(self) -> List[str]:
       """
       Получает список доступных моделей в LM Studio
       
       Returns:
           List[str]: Список имен моделей
       """
       try:
           url = f"{self.base_url}/models"
           
           timeout = aiohttp.ClientTimeout(total=10)
           async with aiohttp.ClientSession(timeout=timeout) as session:
               async with session.get(url) as response:
                   if response.status == 200:
                       data = await response.json()
                       models = [model["id"] for model in data.get("data", [])]
                       logger.info(f"Доступные модели в LM Studio: {models}")
                       return models
                   else:
                       logger.error(f"Ошибка получения списка моделей: {response.status}")
                       return []
                       
       except Exception as e:
           logger.error(f"Ошибка при получении списка моделей: {e}")
           return []