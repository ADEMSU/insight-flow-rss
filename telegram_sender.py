import os
import re
import traceback
import asyncio
from datetime import datetime
import telegram
from telegram.error import TimedOut, RetryAfter, NetworkError
from loguru import logger
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

class TelegramSender:
    def __init__(self):
        """
        Инициализирует отправитель сообщений в Telegram с улучшенной обработкой ошибок
        """
        # Telegram
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        # Создаем клиент с увеличенными таймаутами и размером пула соединений
        self.bot = telegram.Bot(
            token=self.telegram_token,
            request=telegram.request.HTTPXRequest(
                connection_pool_size=8,
                read_timeout=30.0,
                write_timeout=30.0,
                connect_timeout=30.0,
            )
        )
    
    def _clean_json_string(self, string):
        """
        Удаляет недопустимые и управляющие символы из строки.
        Также удаляет LaTeX-подобные конструкции, звездочки и другие форматирующие элементы.
        """
        if not string:
            return ""
            
        # Удаляем LaTeX-подобные конструкции
        cleaned = re.sub(r'\\boxed\{|\}$|```|`', '', string)
        
        # Удаляем управляющие символы, но сохраняем базовые символы форматирования
        cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', cleaned)
        
        # Заменяем множественные переносы строк на двойной перенос для лучшей читаемости
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
        
        # Удаляем начальные и конечные пробелы и переносы строк
        cleaned = cleaned.strip()
        
        # Удаляем разделители и закрывающие скобки
        cleaned = cleaned.replace('---', '')
        
        # Удаляем звездочки (** и **)
        cleaned = re.sub(r'\*\*\s*$', '', cleaned)  # Удаляем ** в конце строк
        cleaned = re.sub(r'\*\*', '', cleaned)  # Удаляем все двойные звездочки
        
        # Удаляем фигурные скобки и служебную информацию
        cleaned = re.sub(r'\{\d+:\d+\s+[✓✔]\}', '', cleaned)  # Удаляем {HH:MM ✓} и подобные
        cleaned = re.sub(r'\[\d+:\d+\s+[✓✔]\]', '', cleaned)  # Удаляем [HH:MM ✓] и подобные
        cleaned = re.sub(r'\d+:\d+\s+[✓✔]', '', cleaned)  # Удаляем HH:MM ✓ и подобные
        
        return cleaned.strip()
    
    def _parse_analysis(self, analysis_text):
        """
        Парсит текст анализа и возвращает список сюжетов.
        Улучшенная версия с множественными шаблонами для более надежного парсинга.
        Теперь включает информацию о POST_ID для каждого сюжета.
        
        Args:
            analysis_text (str): Исходный текст анализа от LLM
            
        Returns:
            list: Список словарей с информацией о сюжетах
        """
        if not analysis_text:
            return []
            
        # Очищаем текст от лишних символов
        clean_text = self._clean_json_string(analysis_text)
        
        # Массив различных паттернов для поиска сюжетов
        patterns = [
            # Стандартный паттерн с POST_ID
            r'(?:СЮЖЕТ|СЮЖЕТ\s*\d+|СЮЖЕТ\s*:)\s*:?\s*(.*?)(?:СОДЕРЖАНИЕ\s*:)(.*?)(?:POST_ID\s*:)(.*?)(?:ПРЕДЛОЖЕНИЯ ДЛЯ КОНТЕНТ-ПЛАНА\s*:)(.*?)(?=СЮЖЕТ|$)',
            
            # Альтернативный паттерн без двоеточий, но с POST_ID
            r'(?:СЮЖЕТ|СЮЖЕТ\s*\d+)\s*(.*?)(?:СОДЕРЖАНИЕ)(.*?)(?:POST_ID)(.*?)(?:ПРЕДЛОЖЕНИЯ ДЛЯ КОНТЕНТ-ПЛАНА)(.*?)(?=СЮЖЕТ|$)',
            
            # Стандартный паттерн без POST_ID (запасной)
            r'(?:СЮЖЕТ|СЮЖЕТ\s*\d+|СЮЖЕТ\s*:)\s*:?\s*(.*?)(?:СОДЕРЖАНИЕ\s*:)(.*?)(?:ПРЕДЛОЖЕНИЯ ДЛЯ КОНТЕНТ-ПЛАНА\s*:)(.*?)(?=СЮЖЕТ|$)',
        ]
        
        stories = []
        
        # Проходим по всем паттернам, пока не найдем совпадения
        for pattern in patterns:
            matches = re.findall(pattern, clean_text, re.DOTALL | re.IGNORECASE)
            if matches:
                # Обрабатываем каждый найденный сюжет
                for match in matches:
                    # Проверяем формат совпадения - с POST_ID или без
                    if len(match) == 4:  # С POST_ID
                        title, content, post_id, plan = match
                    elif len(match) == 3:  # Без POST_ID
                        title, content, plan = match
                        post_id = ""
                    else:
                        # Непредвиденный формат, пропускаем
                        continue
                    
                    # Очищаем заголовок от лишних двоеточий, пробелов и звездочек
                    title = title.strip()
                    title = re.sub(r'^:+\s*', '', title)  # Удаляем все двоеточия в начале
                    title = re.sub(r'\*+\s*$', '', title)  # Удаляем звездочки в конце
                    title = re.sub(r'\*\*', '', title)  # Удаляем двойные звездочки в любом месте
                    
                    # Очищаем post_id и извлекаем только числовое значение
                    post_id = post_id.strip()
                    post_id_match = re.search(r'(\d+)', post_id)
                    post_id = post_id_match.group(1) if post_id_match else post_id
                    
                    # Очищаем контент и план от служебных элементов
                    content = self._clean_json_string(content)
                    plan = self._clean_json_string(plan)
                    
                    stories.append({
                        "title": title,
                        "content": content.strip(),
                        "post_id": post_id,
                        "plan": plan.strip()
                    })
                break  # Если нашли совпадения, выходим из цикла паттернов
        
        # Если не нашли сюжеты с помощью регулярных выражений, пробуем разделить по ключевым словам
        if not stories:
            # Разделяем текст на части по ключевому слову "СЮЖЕТ"
            story_parts = re.split(r'(?:\n|^)СЮЖЕТ\s*:?', clean_text)
            
            for part in story_parts:
                if not part.strip():
                    continue
                
                # Пытаемся извлечь заголовок
                title_match = re.search(r'^(.*?)(?:\n|$)', part, re.DOTALL)
                
                # Пытаемся извлечь содержание
                content_match = re.search(r'СОДЕРЖАНИЕ\s*:?\s*(.*?)(?:POST_ID|ПРЕДЛОЖЕНИЯ|$)', part, re.DOTALL | re.IGNORECASE)
                
                # Пытаемся извлечь POST_ID
                post_id_match = re.search(r'POST_ID\s*:?\s*(.*?)(?:ПРЕДЛОЖЕНИЯ|$)', part, re.DOTALL | re.IGNORECASE)
                
                # Пытаемся извлечь план
                plan_match = re.search(r'ПРЕДЛОЖЕНИЯ.*?:?\s*(.*?)$', part, re.DOTALL | re.IGNORECASE)
                
                if title_match:
                    title = title_match.group(1).strip()
                    # Удаляем звездочки из заголовка
                    title = re.sub(r'\*+\s*$', '', title)
                    title = re.sub(r'\*\*', '', title)
                    
                    content = content_match.group(1).strip() if content_match else "Нет содержания"
                    post_id = post_id_match.group(1).strip() if post_id_match else ""
                    
                    # Очищаем post_id и извлекаем только числовое значение
                    post_id_num_match = re.search(r'(\d+)', post_id)
                    post_id = post_id_num_match.group(1) if post_id_num_match else post_id
                    
                    plan = plan_match.group(1).strip() if plan_match else "Нет предложений"
                    
                    # Очищаем контент и план от служебных элементов
                    content = self._clean_json_string(content)
                    plan = self._clean_json_string(plan)
                    
                    stories.append({
                        "title": title,
                        "content": content,
                        "post_id": post_id,
                        "plan": plan
                    })
        
        # Если все равно не нашли структурированный контент, создаем одну запись с исходным текстом
        if not stories:
            logger.warning("Не удалось распарсить сюжеты, используем исходный текст как один сюжет")
            stories.append({
                "title": "Результаты анализа",
                "content": clean_text,
                "post_id": "",
                "plan": ""
            })
            
        return stories
    
    def _format_story_html(self, story_index, story, source_url=None):
        """
        Форматирует один сюжет в HTML-формате с улучшенным экранированием
        и удалением служебных элементов
        
        Args:
            story_index (int): Индекс сюжета
            story (dict): Информация о сюжете
            source_url (str, optional): URL источника
            
        Returns:
            str: Отформатированный текст сюжета в HTML
        """
        # Удаляем звездочки и служебную информацию из всех текстов
        title = self._clean_json_string(story["title"])
        content = self._clean_json_string(story["content"])
        plan = self._clean_json_string(story["plan"])
        
        # Экранируем HTML-символы в тексте
        title = title.replace("<", "&lt;").replace(">", "&gt;")
        content = content.replace("<", "&lt;").replace(">", "&gt;")
        plan = plan.replace("<", "&lt;").replace(">", "&gt;")
        
        # Ограничиваем длину каждого элемента для защиты от превышения лимита
        if len(title) > 1000:
            title = title[:997] + "..."
        if len(content) > 2000:
            content = content[:1997] + "..."
        if len(plan) > 2000:
            plan = plan[:1997] + "..."
            
        # Используем другой формат для разделения номера и названия сюжета
        html = f"<b>Сюжет {story_index}</b>: {title}\n\n"
        html += f"<b>Содержание</b>: {content}\n\n"
        
        # Добавляем ссылку на источник, если она доступна
        if source_url:
            html += f"<b>Источник</b>: <a href=\"{source_url}\">{source_url}</a>\n\n"
        
        html += f"<b>Предложения для контент-плана</b>: {plan}"
        
        # Дополнительная очистка от служебных элементов
        html = re.sub(r'\{\d+:\d+\s+[✓✔]\}', '', html)  # Удаляем {HH:MM ✓} и подобные
        html = re.sub(r'\[\d+:\d+\s+[✓✔]\]', '', html)  # Удаляем [HH:MM ✓] и подобные
        html = re.sub(r'\d+:\d+\s+[✓✔]', '', html)  # Удаляем HH:MM ✓ и подобные
        
        return html
    
    async def _send_message_with_retry(self, text, parse_mode=None, max_retries=3):
        """
        Отправляет сообщение в Telegram с повторными попытками в случае ошибок.
        Дополнительно очищает текст от служебных элементов.
        
        Args:
            text (str): Текст сообщения
            parse_mode (str, optional): Режим парсинга (HTML, Markdown)
            max_retries (int): Максимальное количество повторных попыток
            
        Returns:
            bool: True если отправка успешна, иначе False
        """
        # Дополнительная очистка текста от служебных элементов
        text = re.sub(r'\{\d+:\d+\s+[✓✔]\}', '', text)  # Удаляем {HH:MM ✓} и подобные
        text = re.sub(r'\[\d+:\d+\s+[✓✔]\]', '', text)  # Удаляем [HH:MM ✓] и подобные
        text = re.sub(r'\d+:\d+\s+[✓✔]', '', text)  # Удаляем HH:MM ✓ и подобные
        
        for attempt in range(max_retries + 1):
            try:
                await self.bot.send_message(
                    chat_id=self.telegram_chat_id,
                    text=text,
                    parse_mode=parse_mode,
                    disable_web_page_preview=True  # Отключаем предпросмотр ссылок для компактности
                )
                return True
            except TimedOut as e:
                if attempt < max_retries:
                    wait_time = 2 * (attempt + 1)  # Экспоненциальное увеличение времени ожидания
                    logger.warning(f"Таймаут при отправке, повторная попытка через {wait_time} сек. ({attempt+1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Исчерпаны все попытки отправки: {e}")
                    return False
            except RetryAfter as e:
                wait_time = e.retry_after + 1
                logger.warning(f"Сработало ограничение Telegram API, ожидание {wait_time} сек.")
                await asyncio.sleep(wait_time)
            except NetworkError as e:
                if attempt < max_retries:
                    wait_time = 3 * (attempt + 1)
                    logger.warning(f"Сетевая ошибка, повтор через {wait_time} сек: {e}")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Сетевая ошибка после всех попыток: {e}")
                    return False
            except Exception as e:
                logger.error(f"Неожиданная ошибка при отправке: {e}")
                logger.error(traceback.format_exc())
                return False
        
        return False
    
    async def _send_story_split(self, story_index, story, source_url=None):
        """
        Отправляет сюжет разделенным на части, если он слишком длинный.
        С дополнительной очисткой от служебных элементов.
        
        Args:
            story_index (int): Индекс сюжета
            story (dict): Информация о сюжете
            source_url (str, optional): URL источника
            
        Returns:
            bool: True если отправка успешна, иначе False
        """
        success = True
        
        # Удаляем звездочки и служебные элементы из всех полей
        title = self._clean_json_string(story['title'])
        content = self._clean_json_string(story['content'])
        plan = self._clean_json_string(story['plan'])
        
        # Отправляем заголовок сюжета
        title_success = await self._send_message_with_retry(
            f"<b>Сюжет {story_index}</b>: {title}",
            parse_mode='HTML'
        )
        
        if not title_success:
            return False
        
        # Добавляем паузу между сообщениями
        await asyncio.sleep(1)
        
        # Отправляем содержание
        content_success = await self._send_message_with_retry(
            f"<b>Содержание</b>: {content}",
            parse_mode='HTML'
        )
        
        if not content_success:
            success = False
        
        # Добавляем паузу между сообщениями
        await asyncio.sleep(1)
        
        # Отправляем ссылку на источник, если она доступна
        if source_url:
            source_success = await self._send_message_with_retry(
                f"<b>Источник</b>: <a href=\"{source_url}\">{source_url}</a>",
                parse_mode='HTML',
                max_retries=2
            )
            
            if not source_success:
                # Пробуем отправить без HTML-форматирования
                source_text_success = await self._send_message_with_retry(
                    f"Источник: {source_url}",
                    max_retries=2
                )
                if not source_text_success:
                    success = False
            
            # Добавляем паузу между сообщениями
            await asyncio.sleep(1)
        
        # Отправляем предложения для контент-плана
        plan_success = await self._send_message_with_retry(
            f"<b>Предложения для контент-плана</b>: {plan}",
            parse_mode='HTML'
        )
        
        if not plan_success:
            success = False
        
        return success
    
    async def send_analysis(self, analysis, post_mapping=None):
        """
        Отправляет результаты анализа в Telegram с HTML-форматированием,
        каждый сюжет отправляется отдельным сообщением с обработкой ошибок.
        Добавляет ссылки на источники, если они доступны, и очищает от служебных элементов.
        
        Args:
            analysis (str): Результаты анализа для отправки
            post_mapping (dict, optional): Сопоставление post_id → информация о посте
            
        Returns:
            bool: True если отправка успешна, иначе False
        """
        if not analysis:
            logger.warning("Нет анализа для отправки в Telegram")
            return False
        
        try:
            logger.info(f"Отправка в Telegram чат: {self.telegram_chat_id}")
            
            # Очищаем текст от служебных элементов
            clean_analysis = self._clean_json_string(analysis)
            
            # Парсим сюжеты из текста анализа
            stories = self._parse_analysis(clean_analysis)
            
            if not stories:
                logger.warning("Не найдено сюжетов для отправки")
                return False
            
            # Отправляем заголовок
            header = f"📊 <b>InsightFlow: Отчет за {datetime.now().strftime('%d.%m.%Y')}</b>\n\n"
            header += f"Найдено {len(stories)} сюжетов:"
            
            header_sent = await self._send_message_with_retry(
                header,
                parse_mode='HTML'
            )
            
            if not header_sent:
                logger.warning("Не удалось отправить заголовок отчета")
            
            # Добавляем паузу между сообщениями
            await asyncio.sleep(1)
            
            # Отправляем каждый сюжет отдельным сообщением
            successful_stories = 0
            for i, story in enumerate(stories, 1):
                # Получаем post_id из сюжета и очищаем его от префиксов и пробелов
                post_id = story.get('post_id', '')
                clean_post_id = post_id.replace('POST_ID:', '').strip()
                
                # Ищем URL источника с улучшенной логикой
                source_url = None
                if post_mapping and clean_post_id:
                    # Для отладки
                    logger.debug(f"Ищем URL для post_id: {clean_post_id}")
                    logger.debug(f"Доступные post_mapping ключи: {list(post_mapping.keys())[:5]}... (всего {len(post_mapping)})")
                    
                    # Пробуем прямое обращение по post_id
                    if clean_post_id in post_mapping:
                        source_url = post_mapping[clean_post_id].get('url', '')
                        logger.info(f"Найден URL напрямую для post_id {clean_post_id}: {source_url}")
                    else:
                        # Если прямой поиск не сработал, перебираем ключи для нечеткого сопоставления
                        logger.debug(f"Прямой поиск не нашел post_id: {clean_post_id}")
                        for db_post_id, post_info in post_mapping.items():
                            # Проверка различными способами
                            db_post_id_str = str(db_post_id)
                            if (clean_post_id in db_post_id_str or 
                                db_post_id_str in clean_post_id or 
                                clean_post_id == str(post_info.get('post_id', ''))):
                                source_url = post_info.get('url', '')
                                logger.info(f"Найден URL через нечеткое сопоставление для post_id {clean_post_id}: {source_url}")
                                break
                
                # Проверяем валидность URL перед добавлением
                if source_url:
                    # Убедимся, что URL правильно форматирован
                    if not source_url.startswith(('http://', 'https://')):
                        source_url = 'https://' + source_url
                    
                    # Экранируем URL для HTML
                    source_url = source_url.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                
                # Форматируем сюжет с учетом ссылки на источник
                story_html = self._format_story_html(i, story, source_url)
                
                # Проверяем длину сообщения
                if len(story_html) <= 4000:
                    # Отправляем сообщение с повторными попытками
                    if await self._send_message_with_retry(
                        story_html,
                        parse_mode='HTML'
                    ):
                        successful_stories += 1
                else:
                    # Если сообщение слишком длинное, разбиваем его
                    logger.warning(f"Сюжет {i} слишком длинный, разбиваем на части")
                    if await self._send_story_split(i, story, source_url):
                        successful_stories += 1
                
                # Добавляем паузу между сообщениями
                await asyncio.sleep(1.5)
            
            # Проверяем, все ли сюжеты были отправлены успешно
            if successful_stories < len(stories):
                logger.warning(f"Отправлено только {successful_stories} из {len(stories)} сюжетов")
                
                # Пробуем отправить оставшиеся сюжеты в текстовом формате без HTML
                if successful_stories == 0:
                    logger.info("Пробуем отправить сообщение без HTML-форматирования")
                    simple_message = f"📊 InsightFlow: Отчет за {datetime.now().strftime('%d.%m.%Y')}\n\n"
                    simple_message += "Получены результаты анализа, но возникла проблема с форматированием."
                    
                    await self._send_message_with_retry(simple_message)
                    
                    # Пробуем отправить сырой текст, если он не слишком длинный
                    if analysis and len(str(analysis)) < 4000:
                        await asyncio.sleep(1)
                        # Удаляем звездочки и из сырого текста тоже
                        clean_raw_text = self._clean_json_string(str(analysis)[:4000])
                        await self._send_message_with_retry(clean_raw_text)
            else:
                logger.info(f"Успешно отправлено {successful_stories} сюжетов в Telegram")
            
            # Считаем отправку успешной, если хотя бы часть сюжетов была отправлена
            return successful_stories > 0
            
        except Exception as e:
            logger.error(f"Критическая ошибка при отправке в Telegram: {e}")
            logger.error(traceback.format_exc())
            
            # Более простая резервная отправка
            try:
                logger.info("Попытка упрощенной отправки при критической ошибке")
                # Отправляем крайне простое сообщение в случае ошибки
                short_message = f"📊 InsightFlow: Отчет за {datetime.now().strftime('%d.%m.%Y')}\n\nПолучены результаты анализа, но возникла проблема с форматированием."
                await self._send_message_with_retry(short_message)
                
                return True
            except Exception as e2:
                logger.error(f"Критическая ошибка при отправке без форматирования: {e2}")
                return False