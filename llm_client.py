import os
import re
import traceback
import asyncio
from loguru import logger
from dotenv import load_dotenv
from token_estimator import TokenEstimator

# Загрузка переменных окружения
load_dotenv()

# Импортируем LM Studio клиент
from lm_studio_client import LMStudioClient


class LLMClient:
   """
   Клиент для работы с языковыми моделями через LM Studio.
   Этот класс теперь является оберткой над LMStudioClient для обратной совместимости.
   """
   
   def __init__(self):
       """
       Инициализирует клиент для работы с LM Studio
       """
       # Инициализируем LM Studio клиент
       self.lm_client = LMStudioClient()
       
       # Инициализируем оценщик токенов
       self.token_estimator = TokenEstimator()
       
       # Максимальное количество токенов для локальных моделей
       self.max_tokens = 50000  # Увеличено для локальных моделей
       
       logger.info("Инициализирован LLM клиент для работы с LM Studio")
   
   def _clean_and_format_response(self, response_text):
       """
       Очищает и форматирует ответ от модели
       
       Args:
           response_text: Исходный текст ответа
           
       Returns:
           str: Очищенный и отформатированный текст
       """
       if not response_text:
           return ""
       
       # Удаляем лишние пробелы и переносы строк
       cleaned = re.sub(r'\n{3,}', '\n\n', response_text)
       cleaned = re.sub(r' {2,}', ' ', cleaned)
       
       # Удаляем возможные артефакты от модели
       cleaned = re.sub(r'^```[a-z]*\n', '', cleaned, flags=re.MULTILINE)
       cleaned = re.sub(r'\n```$', '', cleaned, flags=re.MULTILINE)
       
       return cleaned.strip()
   
   async def analyze_texts(self, texts, post_mapping=None):
       """
       Анализирует тексты с помощью LM Studio
       
       Args:
           texts (list): Список текстов для анализа
           post_mapping (dict, optional): Словарь соответствия индексов текстов и постов
           
       Returns:
           tuple: (результат анализа, post_mapping) или (None, None) в случае ошибки
       """
       if not texts:
           logger.warning("Нет текстов для анализа")
           return None, None
       
       # Проверяем соединение с LM Studio
       if not await self.lm_client.test_connection():
           logger.error("LM Studio недоступен")
           return None, None
       
       # Подготавливаем данные для анализа
       posts_data = []
       
       for i, text in enumerate(texts):
           # Извлекаем POST_ID из текста
           post_id_match = re.search(r'\[POST_ID:([^\]]+)\]', text)
           post_id = post_id_match.group(1) if post_id_match else f"unknown_{i}"
           
           # Извлекаем источник
           source_match = re.search(r'\[([^\]]+)\]', text.replace(f'[POST_ID:{post_id}]', ''))
           source = source_match.group(1) if source_match else "Неизвестный источник"
           
           # Извлекаем заголовок и контент
           lines = text.split('\n')
           title = ""
           content = text
           
           # Пытаемся найти заголовок (обычно первая строка после метаданных)
           for line in lines:
               if line and not line.startswith('['):
                   title = line.strip()
                   content = '\n'.join(lines[lines.index(line)+1:])
                   break
           
           # Добавляем информацию из post_mapping, если доступна
           if post_mapping and post_id in post_mapping:
               mapping_info = post_mapping[post_id]
               url = mapping_info.get('url', '')
               blog_host = mapping_info.get('blog_host', source)
               title = mapping_info.get('title', title)
           else:
               url = ''
               blog_host = source
           
           post_data = {
               'post_id': post_id,
               'title': title,
               'content': content,
               'blog_host': blog_host,
               'url': url,
               'category': 'Не указана',
               'subcategory': 'Не указана'
           }
           posts_data.append(post_data)
       
       logger.info(f"Подготовлено {len(posts_data)} постов для анализа")
       
       # Анализируем с помощью LM Studio
       try:
           analysis = await self.lm_client.analyze_and_summarize(posts_data, max_stories=10)
           
           if analysis:
               # Очищаем и форматируем ответ
               formatted_analysis = self._clean_and_format_response(analysis)
               
               logger.info(f"Получен анализ от LM Studio, длина: {len(formatted_analysis)} символов")
               
               return formatted_analysis, post_mapping
           else:
               logger.error("Не получен анализ от LM Studio")
               return None, None
               
       except Exception as e:
           logger.error(f"Ошибка при анализе текстов: {e}")
           logger.error(traceback.format_exc())
           return None, None
   
   async def analyze_batch(self, texts, model=None):
       """
       Анализирует батч текстов (для обратной совместимости)
       
       Args:
           texts (list): Список текстов для анализа
           model (str): Игнорируется, используется модель из LM Studio
           
       Returns:
           str: Результат анализа или None в случае ошибки
       """
       # Преобразуем список текстов в формат для analyze_texts
       analysis, _ = await self.analyze_texts(texts)
       return analysis
   
   async def combine_and_filter_results(self, analysis_results):
       """
       Объединяет результаты анализа (для обратной совместимости)
       
       Args:
           analysis_results (list): Список результатов анализа
           
       Returns:
           str: Объединенный результат
       """
       if not analysis_results:
           return None
       
       # Просто объединяем результаты
       combined = "\n\n---\n\n".join(filter(None, analysis_results))
       return self._clean_and_format_response(combined)