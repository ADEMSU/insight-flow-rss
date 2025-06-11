import asyncio
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
from post import Post
from content_classifier import ContentClassifier

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logger.remove()
logger.add(sys.stderr, level='INFO')
logger.add('/app/logs/classify_remaining_{time}.log', rotation='10 MB', level='INFO')

# Инициализация подключения к базе данных
host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "insightflow")
user = os.getenv("POSTGRES_USER", "insightflow")
password = os.getenv("POSTGRES_PASSWORD", "insightflow_password")
conn_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
engine = create_engine(conn_string)
Session = sessionmaker(bind=engine)

async def classify_remaining(limit=50):
    """
    Классифицирует неклассифицированные публикации
    """
    logger.info(f"Запуск классификации оставшихся публикаций (лимит: {limit})")
    
    try:
        # Создаем сессию
        session = Session()
        
        # Получаем неклассифицированные публикации
        query = text(f"""
            SELECT id, post_id, title, content, blog_host, url 
            FROM texts 
            WHERE category IS NULL 
            ORDER BY published_on DESC 
            LIMIT {limit}
        """)
        
        result = session.execute(query)
        
        # Преобразуем результаты в объекты Post
        posts = []
        for row in result:
            post = Post(
                post_id=row.post_id,
                title=row.title,
                content=row.content,
                blog_host=row.blog_host,
                url=row.url
            )
            posts.append(post)
        
        session.close()
        
        if not posts:
            logger.info("Не найдено неклассифицированных публикаций")
            return 0
        
        logger.info(f"Найдено {len(posts)} неклассифицированных публикаций")
        
        # Инициализируем классификатор
        classifier = ContentClassifier()
        
        # Классифицируем публикации
        classifications = await classifier.classify_posts(posts)
        
        if not classifications:
            logger.warning("Не получено результатов классификации")
            return 0
        
        # Обновляем категории в базе данных
        session = Session()
        updated_count = 0
        
        try:
            for post_id, (category, subcategory, confidence) in classifications.items():
                # Создаем запрос на обновление
                update_query = text("""
                    UPDATE texts 
                    SET category = :category, 
                        subcategory = :subcategory, 
                        classification_confidence = :confidence,
                        classified_at = NOW() 
                    WHERE post_id = :post_id
                """)
                
                # Выполняем запрос
                session.execute(update_query, {
                    'category': category, 
                    'subcategory': subcategory, 
                    'confidence': confidence,
                    'post_id': post_id
                })
                updated_count += 1
            
            # Фиксируем изменения
            session.commit()
            logger.info(f"Успешно обновлены категории для {updated_count} публикаций")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при обновлении категорий: {e}")
        finally:
            session.close()
        
        return updated_count
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        return 0

if __name__ == "__main__":
    updated = asyncio.run(classify_remaining(50))
    print(f"Классифицировано {updated} публикаций")