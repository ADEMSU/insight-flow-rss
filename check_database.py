# check_database.py
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Получаем параметры подключения
db_params = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "insightflow"),
    "user": os.getenv("POSTGRES_USER", "insightflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "insightflow_password")
}

def check_database_connection():
    """Проверяет подключение к базе данных"""
    try:
        conn = psycopg2.connect(**db_params)
        print("✅ Успешное подключение к базе данных PostgreSQL")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Ошибка подключения к базе данных: {e}")
        return False

def check_tables():
    """Проверяет наличие необходимых таблиц в базе данных"""
    required_tables = ['rss_sources', 'texts']
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Получаем список всех таблиц в базе данных
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        
        # Проверяем наличие необходимых таблиц
        missing_tables = [table for table in required_tables if table not in tables]
        
        if missing_tables:
            print(f"❌ Отсутствуют следующие таблицы: {', '.join(missing_tables)}")
        else:
            print("✅ Все необходимые таблицы существуют")
        
        cursor.close()
        conn.close()
        
        return len(missing_tables) == 0
    except Exception as e:
        print(f"❌ Ошибка при проверке таблиц: {e}")
        return False

def check_partitions():
    """Проверяет наличие партиций для текущего месяца"""
    try:
        # Текущая дата
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month
        partition_name = f"texts_y{year}m{month:02d}"
        
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Проверяем существование партиции
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{partition_name}'
            )
        """)
        
        partition_exists = cursor.fetchone()[0]
        
        if partition_exists:
            print(f"✅ Партиция {partition_name} существует")
        else:
            print(f"❌ Партиция {partition_name} отсутствует")
            
            # Создаем партицию, если она отсутствует
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year+1}-01-01" if month == 12 else f"{year}-{month+1:02d}-01"
            
            create_partition_sql = f"""
            CREATE TABLE {partition_name} PARTITION OF texts
                FOR VALUES FROM ('{start_date}') TO ('{end_date}')
            """
            
            cursor.execute(create_partition_sql)
            conn.commit()
            print(f"✅ Партиция {partition_name} создана")
        
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"❌ Ошибка при проверке партиций: {e}")
        return False

def check_rss_sources():
    """Проверяет наличие RSS-источников в базе данных"""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Получаем количество RSS-источников
        cursor.execute("SELECT COUNT(*) FROM rss_sources")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"✅ В базе данных найдено {count} RSS-источников")
        else:
            print("❌ В базе данных нет RSS-источников")
        
        cursor.close()
        conn.close()
        
        return count > 0
    except Exception as e:
        print(f"❌ Ошибка при проверке RSS-источников: {e}")
        return False

if __name__ == "__main__":
    print("=== Проверка состояния базы данных ===")
    
    # Проверяем подключение
    if not check_database_connection():
        print("❌ Проверка не пройдена: не удалось подключиться к базе данных")
        exit(1)
    
    # Проверяем таблицы
    tables_ok = check_tables()
    
    # Проверяем партиции
    partitions_ok = check_partitions()
    
    # Проверяем RSS-источники
    sources_ok = check_rss_sources()
    
    # Выводим общий результат
    if tables_ok and partitions_ok and sources_ok:
        print("✅ База данных в рабочем состоянии")
    else:
        print("⚠️ База данных требует внимания, см. предупреждения выше")