import asyncio
import logging
from typing import List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class TelegramSender:
    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None):
        from os import getenv
        self.bot_token = bot_token or getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or getenv("TELEGRAM_CHAT_ID")
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    async def send_message(self, text: str) -> bool:
        """
        Отправляет простое текстовое сообщение в Telegram.
        
        Args:
            text: текст сообщения
            
        Returns:
            bool: успешность отправки
        """
        return await self._send_message_with_retry(text, parse_mode='HTML')

    async def _send_story_split(self, story: dict, idx: int, url: str) -> bool:
        html = self._format_story_html(story, idx, url)
        return await self._send_message_with_retry(html, parse_mode='HTML')

    async def send_analysis(self, analysis_text: str, post_mapping: dict = None):
        """
        Отправляет анализ в Telegram.
        
        Args:
            analysis_text: текст анализа или список словарей с постами
            post_mapping: словарь {post_id: url} для маппинга URL-ов
        """
        logger.info(f"send_analysis вызван. Type of analysis_text: {type(analysis_text)}")
        logger.info(f"post_mapping: {post_mapping}")
        
        # Если analysis_text это список словарей (из LMStudioClient.analyze_and_summarize)
        if isinstance(analysis_text, list):
            stories = analysis_text
        else:
            # Старый формат - парсим текст
            stories = self._parse_analysis(analysis_text)
        
        logger.info(f"Количество stories для отправки: {len(stories) if stories else 0}")
        
        if not stories:
            logger.warning("Нет сюжетов для отправки в Telegram")
            return False

        success = True
        for idx, story in enumerate(stories, 1):
            post_id = story.get("post_id", "")
            logger.info(f"Story {idx}: post_id='{post_id}', title='{story.get('title', 'N/A')}'")
            
            # Получаем URL из post_mapping или используем дефолтный
            if post_mapping and post_id and post_id in post_mapping:
                url = post_mapping[post_id]
                logger.info(f"URL найден в post_mapping: {url}")
            else:
                # Если URL нет в маппинге, пробуем получить из story (если есть)
                url = story.get("url", "https://example.com")
                logger.warning(f"URL не найден для post_id={post_id}, используется {url}")
            
            result = await self._send_story_split(story, idx, url)
            if not result:
                success = False
                logger.error(f"Не удалось отправить story {idx}")
            else:
                logger.info(f"Story {idx} успешно отправлена")
        
        return success

    async def _send_message_with_retry(self, text: str, retries: int = 3, parse_mode: Optional[str] = None) -> bool:
        logger.info(f"Отправка сообщения в Telegram. Длина текста: {len(text)}")
        logger.debug(f"Chat ID: {self.chat_id}, Bot token присутствует: {bool(self.bot_token)}")
        
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.api_url, data={
                        "chat_id": self.chat_id,
                        "text": text,
                        "parse_mode": parse_mode or "HTML",
                        "disable_web_page_preview": False,
                    }) as resp:
                        if resp.status == 200:
                            logger.info(f"Сообщение успешно отправлено с попытки {attempt + 1}")
                            return True
                        else:
                            error_text = await resp.text()
                            logger.error(f"Telegram API error {resp.status}: {error_text}")
            except Exception as e:
                logger.exception(f"Ошибка при отправке в Telegram (попытка {attempt + 1}): {e}")
            
            if attempt < retries - 1:
                await asyncio.sleep(2)
        
        logger.error(f"Не удалось отправить сообщение после {retries} попыток")
        return False

    def _format_story_html(self, story: dict, idx: int, url: str) -> str:
        title = self._clean_json_string(story["title"])
        content = self._clean_json_string(story.get("summary", story.get("content", "")))

        title = title.replace("<", "&lt;").replace(">", "&gt;")
        content = content.replace("<", "&lt;").replace(">", "&gt;")

        if len(content) > 2000:
            content = content[:1997] + "..."

        html = f"<b>Сюжет {idx}</b>: {title}\n\n"
        html += f"<b>Содержание</b>: {content}\n\n"
        html += f"<b>Источник</b>: <a href=\"{url}\">{url}</a>\n\n"
        return html.strip()

    def _clean_json_string(self, text: str) -> str:
        return text.replace("\n", " ").replace("\"", "\"").strip()
