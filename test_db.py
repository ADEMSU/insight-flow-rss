
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from loguru import logger
import sys
from db_manager import DBManager
from rss_manager import RSSManager

# Настройка логирования
logger.remove()
logger.add(sys.stderr, level="INFO")

async def test_db_connection():
    """
    Тестирует подключение к базе данных и базовые операции
    """
    logger.info("Тестирование подключения к базе данных...")
    
    # Создаем экземпляр DBManager
    try:
        db_manager = DBManager()
        db_manager.create_tables()
        logger.info("Подключение к базе данных успешно!")
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
        return False
    
    return True

async def test_rss_manager():
    """
    Тестирует работу RSS-менеджера с базой данных
    """
    logger.info("Тестирование RSS-менеджера с базой данных...")
    
    # Создаем экземпляр RSSManager
    try:
        rss_manager = RSSManager()
        
        # Получаем записи за последние 7 дней
        now = datetime.now(ZoneInfo('UTC'))
        date_from = now - timedelta(days=7)
        date_to = now
        
        logger.info(f"Получение записей с {date_from} по {date_to}...")
        entries = await rss_manager.fetch_all_rss()
        
        if not entries:
            logger.warning("Не получено записей из RSS-источников")
            return False
            
        logger.info(f"Получено {len(entries)} записей из RSS-источников")
        
        # Фильтруем записи по дате
        filtered_entries = rss_manager._filter_entries_by_date(entries, date_from, date_to)
        logger.info(f"Отфильтровано {len(filtered_entries)} записей по дате")
        
        # Преобразуем записи в объекты Post и сохраняем в БД
        posts = rss_manager.convert_entries_to_posts(filtered_entries)
        logger.info(f"Создано {len(posts)} объектов Post")
        
        # Получаем записи из БД
        if rss_manager.db_manager:
            db_posts = rss_manager.db_manager.get_posts_by_date_range(date_from, date_to)
            logger.info(f"Получено {len(db_posts)} записей из базы данных")
            
        return True
    except Exception as e:
        logger.error(f"Ошибка при тестировании RSS-менеджера: {e}")
        logger.error(traceback.format_exc())
        return False

async def main():
    """
    Основная функция тестирования
    """
    logger.info("Начало тестирования базы данных и RSS...")
    
    # Тестируем подключение к БД
    db_success = await test_db_connection()
    
    if db_success:
        # Тестируем RSS-менеджер
        rss_success = await test_rss_manager()
        
        if rss_success:
            logger.info("Тестирование успешно завершено!")
        else:
            logger.error("Тестирование RSS-менеджера не удалось")
    else:
        logger.error("Тестирование подключения к базе данных не удалось")

if __name__ == "__main__":
    asyncio.run(main())