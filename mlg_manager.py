import os
import traceback
from bs4 import BeautifulSoup
from simhash import Simhash
from datetime import datetime
from loguru import logger
import hashlib
from typing import List
from post import Post, BlogHostType
import zeep
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

class MlgManager:
    def __init__(self):
        self.username = os.getenv("MLG_USERNAME")
        self.password = os.getenv("MLG_PASSWORD")
        self.wsdl_url = os.getenv("MLG_WSDL_URL")
        self.report_id = os.getenv("MLG_REPORT_ID")

        if not all([self.username, self.password, self.wsdl_url, self.report_id]):
            logger.error("Переменные окружения для Медиалогии не заданы")
            raise ValueError("Недостаточно параметров для подключения к Медиалогии")

        try:
            self.client = zeep.Client(wsdl=self.wsdl_url)
            logger.info(f"MlgManager инициализирован: WSDL={self.wsdl_url}")
        except Exception as e:
            logger.error(f"Ошибка инициализации SOAP-клиента: {e}")
            logger.debug(traceback.format_exc())
            raise

    def call_api(self, method_name, **kwargs):
        try:
            method = getattr(self.client.service, method_name)
            logger.info(f"Вызов метода {method_name}")
            reply = method(**kwargs)

            if hasattr(reply, 'Error') and reply.Error:
                logger.error(f"Ошибка API Медиалогии: {reply.Error}")
                raise RuntimeError(f"Mediologia API Error: {reply.Error}")

            return reply
        except Exception as e:
            logger.error(f"Ошибка вызова метода {method_name}: {e}")
            logger.debug(traceback.format_exc())
            raise

    def get_posts(self, date_from: datetime, date_to: datetime, page=1) -> List[Post]:
        logger.info(f"Загрузка постов Медиалогии за период {date_from} — {date_to}")
        posts = []
        try:
            page_posts = self._get_posts_page(date_from, date_to, page)
            posts.extend(page_posts)

            while len(page_posts) == 200:
                page += 1
                page_posts = self._get_posts_page(date_from, date_to, page)
                posts.extend(page_posts)

            logger.info(f"Всего получено {len(posts)} постов из Медиалогии")
        except Exception as e:
            logger.error(f"Ошибка загрузки постов из Медиалогии: {e}")
            logger.debug(traceback.format_exc())

        return posts

    def _get_posts_page(self, date_from, date_to, page_index) -> List[Post]:
        try:
            reply = self.call_api(
                "GetPosts",
                credentials={"Login": self.username, "Password": self.password},
                reportId=self.report_id,
                dateFrom=date_from.strftime("%Y-%m-%dT%H:%M:%S"),
                dateTo=date_to.strftime("%Y-%m-%dT%H:%M:%S"),
                pageIndex=page_index,
                pageSize=200,
            )

            raw_posts = getattr(reply.Posts, 'CubusPost', []) if hasattr(reply, 'Posts') else []
            parsed_posts = []
            skipped = 0

            for item in raw_posts:
                try:
                    url = getattr(item, "Url", "").strip()
                    title = getattr(item, "Title", "").strip()
                    raw_content = getattr(item, "Content", "") or ""
                    blog_host = getattr(item, "ResourceName", "Медиалогия").strip()
                    published_raw = getattr(item, "PublishDate", None)

                    # Преобразование даты
                    published_on = None
                    if isinstance(published_raw, datetime):
                        published_on = published_raw
                    elif isinstance(published_raw, str):
                        try:
                            published_on = datetime.fromisoformat(published_raw)
                        except ValueError:
                            try:
                                published_on = datetime.strptime(published_raw, "%Y-%m-%d %H:%M:%S")
                            except Exception as e:
                                logger.warning(f"Не удалось распарсить дату '{published_raw}' для {url}: {e}")

                    # Очистка HTML
                    text_only = BeautifulSoup(raw_content, "html.parser").get_text(separator=" ").strip()

                    if not url or not title or not text_only:
                        logger.warning(f"Пропущен пост: пустой url/title/text для {url}")
                        skipped += 1
                        continue

                    if not isinstance(published_on, datetime):
                        logger.warning(f"Пропущен пост: некорректная дата публикации для {url} (type={type(published_raw).__name__})")
                        skipped += 1
                        continue

                    post_id = f"mlg_{hashlib.md5(url.encode()).hexdigest()}"
                    post_simhash = str(Simhash(text_only).value) if text_only else ""

                    post = Post(
                        post_id=post_id,
                        title=title,
                        url=url,
                        content=text_only,
                        html_content=raw_content,
                        blog_host=blog_host,
                        blog_host_type=BlogHostType.MEDIA,
                        published_on=published_on,
                        simhash=post_simhash,
                        raw=item,
                    )
                    parsed_posts.append(post)

                except Exception as e:
                    logger.warning(f"Ошибка при обработке поста: {e}")
                    skipped += 1
                    continue

            logger.info(f"Страница {page_index}: получено {len(parsed_posts)} постов, пропущено {skipped}")
            return parsed_posts

        except Exception as e:
            logger.error(f"Ошибка получения страницы {page_index} из Медиалогии: {e}")
            logger.debug(traceback.format_exc())
            return []
