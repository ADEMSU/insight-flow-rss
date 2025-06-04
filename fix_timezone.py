# fix_timezone.py
from datetime import datetime
from zoneinfo import ZoneInfo

def get_msk_date_range():
    """
    Получает диапазон дат с 8:00 до 8:00 в московском часовом поясе
    с учетом часовых поясов для корректного сравнения
    """
    # Получаем текущее время в московском часовом поясе
    msk_now = datetime.now(ZoneInfo('Europe/Moscow'))
    
    # Устанавливаем конечную дату как сегодня в 8:00
    date_to = msk_now.replace(hour=8, minute=0, second=0, microsecond=0)
    
    # Если сейчас раньше 8:00, значит нам нужен период с позавчера 8:00 до вчера 8:00
    if msk_now.hour < 8:
        date_to = date_to - timedelta(days=1)
    
    # Начальная дата - это конечная минус 24 часа
    date_from = date_to - timedelta(days=1)
    
    # Возвращаем даты с часовым поясом UTC для совместимости
    date_from = date_from.astimezone(ZoneInfo('UTC'))
    date_to = date_to.astimezone(ZoneInfo('UTC'))
    
    return date_from, date_to