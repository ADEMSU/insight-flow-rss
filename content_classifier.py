import os
import traceback
import asyncio
import json
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime
from typing import Optional

# Загрузка переменных окружения
load_dotenv()

# Импорт необходимых модулей
from db_manager import DBManager
from lm_studio_client import LMStudioClient
from post import Post


class ContentClassifier:
    def __init__(self):
        try:
            self.db_manager = DBManager()
            logger.info("Подключение к базе данных успешно инициализировано")
        except Exception as e:
            logger.error(f"Ошибка при инициализации подключения к базе данных: {e}")
            self.db_manager = None
            raise

        try:
            self.lm_client = LMStudioClient()
            logger.info("LM Studio клиент успешно инициализирован")
        except Exception as e:
            logger.error(f"Ошибка при инициализации LM Studio клиента: {e}")
            self.lm_client = None
            raise

        self.categories = self._load_categories()
        self.batch_size = 5
        self.max_concurrent = 2

        logger.info("Инициализирован классификатор контента с LM Studio")

    def _load_categories(self):
        return {
        "Политика": [
            "Внутренняя политика", "Международные отношения", "Выборы",
            "Партии и движения", "Государственное управление", "Коррупционные скандалы"
        ],
        "Экономика": [
            "Макроэкономика", "Финансы и банки", "Фондовый рынок",
            "Налоги и законодательство", "Бизнес и корпорации", "Криптовалюты и блокчейн"
        ],
        "Технологии": [
            "IT и софтвер", "Гаджеты и устройства", "Искусственный интеллект",
            "Кибербезопасность", "Космические технологии", "Стартапы и инновации"
        ],
        "Общество": [
            "Социальные проблемы", "Образование", "Здравоохранение",
            "Демография", "Религия", "Благотворительность"
        ],
        "Культура и искусство": [
            "Кино и сериалы", "Музыка", "Литература",
            "Театр и танцы", "Архитектура", "Мода и дизайн"
        ],
        "Спорт": [
            "Футбол", "Хоккей", "Баскетбол", "Олимпийские игры",
            "Экстремальные виды спорта", "Электронный спорт (киберспорт)"
        ],
        "Наука": [
            "Медицина и биотехнологии", "Физика и астрономия", "Химия и материалы",
            "Экология и климат", "Археология", "Генетика"
        ],
        "Право и криминал": [
            "Уголовные дела", "Суды и законодательство", "Права человека",
            "Терроризм", "Киберпреступность", "Юридические услуги"
        ],
        "Экология и устойчивое развитие": [
            "Загрязнение окружающей среды", "Возобновляемая энергетика",
            "Защита животных", "Изменение климата", "Переработка отходов"
        ],
        "Авто и транспорт": [
            "Автопром", "Электромобили", "ДТП и безопасность",
            "Общественный транспорт", "Автогонки"
        ],
        "Недвижимость": [
            "Рынок жилья", "Ипотека", "Коммерческая недвижимость",
            "Строительство", "Дизайн интерьеров"
        ],
        "Туризм и путешествия": [
            "Авиаперевозки", "Гостиничный бизнес", "Культурный туризм",
            "Экотуризм", "Виза и миграция"
        ],
        "Сельское хозяйство": [
            "Агротехнологии", "Экспорт/импорт продуктов",
            "Животноводство", "Продовольственная безопасность"
        ],
        "Энергетика": [
            "Нефть и газ", "Атомная энергетика", "Энергоэффективность",
            "Энергетические кризисы"
        ],
        "Киберпространство": [
            "Социальные сети", "Виртуальная реальность (VR/AR)",
            "NFT и метавселенные", "Цифровая идентичность"
        ],
        "Здоровый образ жизни": [
            "Диеты и питание", "Фитнес", "Ментальное здоровье",
            "Альтернативная медицина"
        ],
        "Региональные новости": [
            "Местное самоуправление", "Городские проекты",
            "Культура регионов", "Гиперлокальные события"
        ],
        "Международные конфликты": [
            "Войны и санкции", "Дипломатические кризисы",
            "Гуманитарные катастрофы", "Миротворческие миссии"
        ],
        "Образование и карьера": [
            "Онлайн-образование", "Трудоустройство",
            "Профессии будущего", "Языковые курсы"
        ],
        "Развлечения": [
            "Знаменитости", "Юмор и мемы", "Ивенты и фестивали", "Телешоу"
        ],
        "Крипто и Web3": [
            "Децентрализованные финансы (DeFi)", "Регулирование крипторынка",
            "Майнинг", "DAO-организации"
        ],
        "Маркетинг и PR": [
            "Реклама и медиапланирование", "Цифровой маркетинг",
            "SEO и поисковые системы (Яндекс, Google, Bing)",
            "PR и управление репутацией", "SERM (управление результатами в поиске)",
            "Нейросети в рекламе и PR", "Контент-маркетинг и копирайтинг",
            "Influencer marketing и блогеры", "Аналитика и веб-трекинг",
            "CRM и автоматизация маркетинга"
        ],
        "Финансовое регулирование и комплаенс": [
            "KYC и проверка клиентов", "AML (борьба с отмыванием) и аудит",
            "Санкционные списки и OFAC", "Проверки благонадежности (World-Check, LexisNexis)",
            "Закрытие счетов и регуляторные меры", "Политически значимые лица (PEP)"
        ],
        "Репутационные риски": [
            "Репутационные кризисы компаний", "Фейковая информация и SERM",
            "Негатив в поиске и отзывах", "PR-антикризисные стратегии"
        ],
        "Интернет-поиск и нейросети": [
            "Алгоритмы поисковиков (Яндекс, Google, Bing)", "Технологии ранжирования и индексации",
            "Нейросети в поиске", "AI в репутационном консалтинге", "Мультимодальный поиск и анализ"
        ]
    }

    async def classify_single_post(self, post) -> tuple:
        try:
            post_id = post.post_id
            title = post.title or ""
            content = post.content or ""

            if len(title) + len(content) < 50:
                logger.warning(f"Пост {post_id} слишком короткий для классификации")
                return post_id, "", "", 0.0

            category, subcategory, confidence = await self.lm_client.classify_content(
                post_id, title, content, self.categories
            )
            return post_id, category, subcategory, confidence

        except Exception as e:
            logger.error(f"Ошибка при классификации поста {post.post_id}: {e}")
            logger.error(traceback.format_exc())
            return post.post_id, "", "", 0.0

    async def classify_posts_batch(self, posts: list) -> dict:
        results = {}
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def classify_with_semaphore(post):
            async with semaphore:
                return await self.classify_single_post(post)

        tasks = [classify_with_semaphore(post) for post in posts]
        classifications = await asyncio.gather(*tasks)

        for post_id, category, subcategory, confidence in classifications:
            logger.debug(f"[{post_id}] → {category}/{subcategory} ({confidence})")
            if category and subcategory:
                results[post_id] = (category, subcategory, confidence)
            else:
                logger.warning(f"❌ Пропущен {post_id} — пустая category или subcategory")

        return results

    async def process_relevant_unclassified_posts(self, limit: Optional[int] = None) -> int:
        if not self.db_manager:
            logger.error("База данных недоступна")
            return 0

        if not self.lm_client:
            logger.error("LM Studio клиент недоступен")
            return 0

        try:
            if not await self.lm_client.test_connection():
                logger.error("LM Studio API недоступен")
                return 0

            posts = self.db_manager.get_relevant_unclassified_posts(limit=limit)
            if not posts:
                logger.info("Нет релевантных неклассифицированных постов для обработки")
                return 0

            logger.info(f"Начинаем классификацию {len(posts)} релевантных постов")
            classified_count = 0

            for i in range(0, len(posts), self.batch_size):
                batch = posts[i:i + self.batch_size]
                logger.info(f"Обработка батча {i//self.batch_size + 1}, размер: {len(batch)}")

                results = await self.classify_posts_batch(batch)

                if results:
                    update_count = self.db_manager.update_posts_classification(results)
                    classified_count += update_count

                    logger.info(f"Батч обработан: классифицировано {len(results)} постов")

                    category_counts = {}
                    for _, (category, _, _) in results.items():
                        category_counts[category] = category_counts.get(category, 0) + 1

                    for category, count in sorted(category_counts.items()):
                        logger.info(f"  - {category}: {count}")

                if i + self.batch_size < len(posts):
                    await asyncio.sleep(1)

            logger.info(f"Завершена классификация. Классифицировано {classified_count} постов")
            self._log_statistics()
            return classified_count

        except Exception as e:
            logger.error(f"Критическая ошибка при классификации: {e}")
            logger.error(traceback.format_exc())
            return 0

    def _log_statistics(self):
        try:
            stats = self.db_manager.get_categories_statistics(only_relevant=True)
            if not isinstance(stats, dict):
                logger.error(f"Неверный тип stats: {type(stats)}, ожидается dict")
                return
            most_common = sorted(stats.items(), key=lambda x: -x[1])[:5]
            for cat, count in most_common:
                logger.info(f"  - {cat}: {count}")
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")


async def classify_relevant_posts_task(limit: Optional[int] = None) -> int:
    try:
        classifier = ContentClassifier()
        return await classifier.process_relevant_unclassified_posts(limit)
    except Exception as e:
        logger.error(f"Ошибка при выполнении задачи классификации: {e}")
        return 0


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Классификация релевантных постов')
    parser.add_argument('--limit', type=int, default=None, help='Максимальное количество постов для классификации')
    args = parser.parse_args()

    logger.info(f"Запуск классификации с лимитом {args.limit}")
    classified = asyncio.run(classify_relevant_posts_task(args.limit))
    logger.info(f"Классифицировано {classified} постов")
