import os
import json
import asyncio
import aiohttp
import time
import traceback
from typing import List, Dict, Any, Optional, Tuple

from dotenv import load_dotenv
from loguru import logger

# ---------------------------------------------------------------------------
#  ENV & LOGGER
# ---------------------------------------------------------------------------
load_dotenv()
lm_logger = logger.bind(channel="LM_STUDIO")


class LMStudioClient:
    """Асинхронный клиент для локального OpenAI‑совместимого API (LM Studio).

    Позволяет:
    * проверять релевантность постов
    * классифицировать контент по категориям/подкатегориям
    * агрегировать и кустарно суммировать несколько постов в сюжеты
    * выполнять базовые служебные запросы (`test_connection`, `get_models`)
    """

    # --------------------------- init -------------------------------------
    def __init__(self) -> None:
        # Базовый URL (можно переопределить в .env)
        self.base_url: str = os.getenv("LM_STUDIO_URL", "http://localhost:1234/v1")

        # Названия моделей (также управляются через .env)
        self.relevance_model: str = os.getenv("LM_STUDIO_RELEVANCE_MODEL", "local-model")
        self.classification_model: str = os.getenv("LM_STUDIO_CLASSIFICATION_MODEL", "local-model")
        self.analysis_model: str = os.getenv("LM_STUDIO_ANALYSIS_MODEL", "local-model")

        # Параметры сети
        self.timeout: int = int(os.getenv("LM_STUDIO_TIMEOUT", "360"))
        self.max_retries: int = int(os.getenv("LM_STUDIO_MAX_RETRIES", "5"))

        # Температуры
        self.relevance_temperature: float = float(os.getenv("LM_STUDIO_RELEVANCE_TEMP", "0.1"))
        self.classification_temperature: float = float(os.getenv("LM_STUDIO_CLASSIFICATION_TEMP", "0.1"))
        self.analysis_temperature: float = float(os.getenv("LM_STUDIO_ANALYSIS_TEMP", "0.3"))

        lm_logger.info(f"Инициализирован LM Studio клиент: {self.base_url}")

    # --------------------------- Low‑level HTTP ---------------------------
    async def _make_request(
        self,
        endpoint: str,
        payload: Dict[str, Any],
        retry_count: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """Низкоуровневый POST‑запрос с простейшим back‑off."""
        start_time = time.perf_counter()
        lm_logger.debug(
            f"➡️  POST {self.base_url}{endpoint} | payload={json.dumps(payload)[:300]}…"
        )

        url = f"{self.base_url}{endpoint}"
        timeout = aiohttp.ClientTimeout(total=self.timeout)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        elapsed = time.perf_counter() - start_time
                        lm_logger.debug(
                            f"⬅️  200 {endpoint} | {elapsed:.2f}s | size={resp.content_length or 'n/a'}"
                        )
                        return await resp.json()

                    # 5xx ➜ повторяем с back‑off
                    err_text = await resp.text()
                    lm_logger.error(
                        f"⚠️  {resp.status} {endpoint} | {err_text[:120]}…"
                    )
                    if resp.status in {500, 502, 503, 504} and retry_count < self.max_retries:
                        wait = 2 ** retry_count
                        lm_logger.info(f"Повторная попытка через {wait}s…")
                        await asyncio.sleep(wait)
                        return await self._make_request(endpoint, payload, retry_count + 1)
                    return None

        except asyncio.TimeoutError:
            lm_logger.error(f"⏱️  Таймаут {self.timeout}s")
            if retry_count < self.max_retries:
                wait = 2 ** retry_count
                await asyncio.sleep(wait)
                return await self._make_request(endpoint, payload, retry_count + 1)
            return None
        except Exception as exc:  # noqa: BLE001
            lm_logger.error(f"Ошибка запроса: {exc}\n{traceback.format_exc()}")
            return None

    # --------------------------- Helpers ----------------------------------
    async def _chat_completion(
        self,
        prompt: str,
        *,
        temperature: float = 0.1,
        max_tokens: int = 256,
        model: str,
    ) -> Optional[Dict[str, Any]]:
        """Унифицированный обёртка над /chat/completions."""
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        return await self._make_request("/chat/completions", payload)

    @staticmethod
    def _extract_content(response: Dict[str, Any]) -> Optional[str]:
        """Извлекает text из OpenAI‑подобного ответа."""
        try:
            return (
                response["choices"][0]["message"]["content"].strip()  # type: ignore[index]
            )
        except (KeyError, IndexError, TypeError):
            return None

    @staticmethod
    def _parse_json_response(response: Optional[Dict[str, Any]]) -> Optional[Any]:
        """Пытается извлечь и распарсить JSON-массив, даже если он обёрнут в Markdown-блоки."""
        if response is None:
            return None
        content = LMStudioClient._extract_content(response)
        if not content:
            return None

        # Убираем Markdown-обёртку, если есть
        if content.strip().startswith("```json"):
            content = content.strip()
            content = content.removeprefix("```json").removesuffix("```").strip()

        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            lm_logger.error(f"Ошибка при декодировании JSON: {e}")
            lm_logger.warning(f"Нераспарсенный content:\n{content}")
            return None


    # --------------------------- Business API -----------------------------
    async def check_relevance(
        self, post_id: str, title: str, content: str
    ) -> Tuple[bool, float]:
        """Возвращает (relevant, score)."""
        lm_logger.info(f"Проверка релевантности {post_id}")

        if len(content) > 100_000:
            content = content[:100_000]

        prompt = f"""Проанализируй текст и определи его релевантность согласно следующим критериям.

РЕЛЕВАНТНЫЕ ТЕМЫ (должен содержать хотя бы одну):

1. KYC/AML/Compliance:
   - KYC, Know Your Customer, "знай своего клиента"
   - AML, Anti-Money Laundering, противодействие отмыванию денег
   - Compliance, комплаенс, соответствие требованиям
   - Проверка благонадежности клиентов
   - private wealth или private management

2. Санкции и проверки:
   - Санкционные списки, OFAC, PEP (политически значимые лица)
   - World-Check, LexisNexis и другие системы проверки
   - Блокировка или закрытие счетов
   - Проверки частного капитала (private wealth)

3. Репутационные риски:
   - Репутационные риски, репутационные кризисы, репутационный ущерб для компаний
   - Онлайн-репутация, цифровая репутация
   - Негативная или ложная информация в поисковой выдаче
   - Негативные или фейковые отзывы о бизнесе
   - Черный PR, информационные атаки, PR-кризисы
   - Управление репутацией, SERM, цифровой профиль

4. Технологии интернет поиска
   - Негативная информация в открытых источниках
   - Технологии поиска в интернете
   - Нейросети и интренет поиск
   - Нейросети и репутационный консалтинг
   - Алгоритмы Bing, Google, Яндекс
   - PR, ORM, SEO, SERM в работе с репутацией

ИСКЛЮЧЕНИЯ (если текст про это - он НЕ релевантен):
- Спорт (футбол, хоккей, теннис и т.д.)
- Шоу-бизнес, артисты, певцы, актеры
- Развлекательный контент

Верни JSON со структурой:
{{
  "relevant": true/false,
  "score": 0.0-1.0,
  "reason": "краткое объяснение",
  "matched_topics": ["список найденных тем"]
}}

Заголовок: {title}
Текст: {content}"""

        resp = await self._chat_completion(
            prompt,
            temperature=self.relevance_temperature,
            max_tokens=512,  # Увеличиваем для более детального ответа
            model=self.relevance_model,
        )
        parsed = self._parse_json_response(resp)
        if not parsed:
            return False, 0.0
        
        relevant = bool(parsed.get("relevant", False))
        score = float(parsed.get("score", 0.0))
        score = score if 0.0 <= score <= 1.0 else 0.0
        
        # Логируем для отладки
        if relevant:
            lm_logger.debug(
                f"Пост {post_id} релевантен: score={score}, "
                f"topics={parsed.get('matched_topics', [])}, "
                f"reason={parsed.get('reason', 'N/A')}"
            )
        
        return relevant, score

    async def classify_content(
        self,
        post_id: str,
        title: str,
        content: str,
        categories: Dict[str, List[str]],
    ) -> Tuple[str, str, float]:
        """Возвращает (category, subcategory, confidence)."""
        lm_logger.info(f"Классификация {post_id}")
        if len(content) > 100_000:
            content = content[:100_000]
        categories_str = "\n".join(
            f"{cat}: {', '.join(subs)}" for cat, subs in categories.items()
        )
        prompt = (
            "Выбери категорию и подкатегорию из списка. Верни JSON "
            "{\"category\": str, \"subcategory\": str, \"confidence\": float}.\n\n"
            f"Список категорий:\n{categories_str}\n\n"
            f"Заголовок: {title}\nТекст: {content}"
        )
        resp = await self._chat_completion(
            prompt,
            temperature=self.classification_temperature,
            model=self.classification_model,
        )
        parsed = self._parse_json_response(resp)
        if not parsed:
            return "", "", 0.0
        cat, sub, conf = (
            parsed.get("category", ""),
            parsed.get("subcategory", ""),
            float(parsed.get("confidence", 0.0)),
        )
        if cat not in categories or (sub and sub not in categories[cat]):
            return "", "", 0.0
        conf = conf if 0.0 <= conf <= 1.0 else 0.0
        return cat, sub, conf

    async def analyze_and_summarize(self, posts: list[dict], max_stories: int = 10) -> list:
        """
        Генерирует краткое содержание для каждого поста.
        
        Args:
            posts: список словарей с ключами: post_id, title, content, url (опционально)
            max_stories: максимальное количество историй
        
        Returns:
            список словарей с ключами: post_id, title, summary
        """
        if not posts:
            lm_logger.warning("Нет постов для суммаризации")
            return []

        # posts уже список словарей, работаем с ним напрямую
        posts_data = []
        for i, post in enumerate(posts[:max_stories], 1):
            post_id = post.get("post_id", "").strip()
            title = post.get("title", "").strip()
            content = post.get("content", "").strip()
            
            if not post_id:
                lm_logger.error(f"Пост {i} не имеет post_id! Keys: {list(post.keys())}")
            
            if not content:
                continue
                
            posts_data.append({
                "index": i,
                "id": post_id,
                "title": title,
                "content": content[:5000]
            })
        
        # Логируем реальные ID
        lm_logger.info("Post IDs для анализа:")
        for data in posts_data:
            lm_logger.info(f"  - {data['id']}")

        # Берем первый реальный post_id для примера
        example_id = posts_data[0]['id'] if posts_data else "rss_29c76252fc46c217f7bba797278a4191"

        # Формируем промпт с примером
        prompt = f"""Проанализируй тексты ниже и создай краткие саммари на русском языке.

    ПРИМЕР ПРАВИЛЬНОГО ОТВЕТА:
    [
    {{
        "post_id": "{example_id}",
        "title": "Заголовок статьи на русском языке",
        "summary": "Краткое содержание статьи на русском языке. Основные моменты и выводы в 5-7 предложений."
    }}
    ]

    ИНСТРУКЦИЯ:
    1. Для каждого текста создай объект с полями: post_id, title, summary
    2. В поле post_id ОБЯЗАТЕЛЬНО скопируй ТОЧНОЕ значение ID из текста ниже
    3. Заголовок и саммари должны быть на русском языке
    4. Верни ТОЛЬКО JSON массив

    ТЕКСТЫ ДЛЯ АНАЛИЗА:
    """

        # Добавляем посты в простом формате
        for data in posts_data:
            prompt += f"\n{'='*60}\n"
            prompt += f"Текст №{data['index']}\n"
            prompt += f"ID: {data['id']}\n"
            prompt += f"Заголовок: {data['title']}\n"
            prompt += f"Содержание: {data['content']}\n"

        prompt += f"\n{'='*60}\n\nСоздай JSON массив для всех текстов выше:"

        resp = await self._chat_completion(
            prompt,
            temperature=self.analysis_temperature,
            max_tokens=2048,
            model=self.analysis_model,
        )

        if not resp:
            lm_logger.error("Не получен ответ от LM Studio")
            return []

        content = self._extract_content(resp)
        if not content:
            lm_logger.error("Пустой контент в ответе")
            return []

        # Парсим ответ
        try:
            # Убираем markdown блоки если есть
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            
            content = content.strip()
            parsed = json.loads(content)
            
            if isinstance(parsed, list):
                # Валидируем и логируем результаты
                valid_posts = []
                expected_ids = {data['id'] for data in posts_data}
                
                for item in parsed:
                    post_id = item.get('post_id', '')
                    if post_id in expected_ids:
                        valid_posts.append(item)
                        lm_logger.info(f"✅ Валидный post_id: {post_id}")
                    else:
                        lm_logger.warning(f"❌ Невалидный post_id: {post_id}")
                
                if not valid_posts and parsed:
                    lm_logger.error("Все post_id невалидные, возвращаем как есть для отладки")
                    return parsed
                    
                return valid_posts
            else:
                lm_logger.error(f"Ответ не является списком: {type(parsed)}")
                return []
                
        except json.JSONDecodeError as e:
            lm_logger.error(f"Ошибка парсинга JSON: {e}")
            lm_logger.error(f"Контент: {content[:500]}...")
            return []

    # --------------------------- Service API ------------------------------
    async def test_connection(self) -> bool:
        """Быстрый пинг‑check."""
        resp = await self._chat_completion(
            "ping",
            temperature=0.0,
            model=self.relevance_model,
            max_tokens=5,
        )
        ok = bool(resp and "choices" in resp)
        lm_logger.info("LM Studio API доступен" if ok else "LM Studio API недоступен")
        return ok

    async def get_models(self) -> List[str]:
        """Возвращает список моделей."""
        url = f"{self.base_url}/models"
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return [m["id"] for m in data.get("data", [])]
                    lm_logger.error(f"Ошибка {resp.status} при GET /models")
        except Exception as exc:  # noqa: BLE001
            lm_logger.error(f"get_models error: {exc}")
        return []
