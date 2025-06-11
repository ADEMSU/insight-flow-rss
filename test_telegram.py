#!/usr/bin/env python3
"""
Тестирование подключения к Telegram
"""
import os
import asyncio
import telegram
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

async def test_connection():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    print(f"🔧 Настройки:")
    print(f"Token: {token[:10]}..." if token else "Token: НЕ УСТАНОВЛЕН")
    print(f"Chat ID: {chat_id}" if chat_id else "Chat ID: НЕ УСТАНОВЛЕН")
    print()
    
    if not token or not chat_id:
        print("❌ Установите TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID в .env файле")
        return
    
    bot = telegram.Bot(token=token)
    
    # Тест 1: Информация о боте
    try:
        me = await bot.get_me()
        print(f"✅ Бот подключен: @{me.username} ({me.first_name})")
    except Exception as e:
        print(f"❌ Ошибка подключения к боту: {e}")
        return
    
    # Тест 2: Отправка сообщения
    try:
        message = await bot.send_message(
            chat_id=chat_id,
            text="✅ Тестовое сообщение от InsightFlow"
        )
        print(f"✅ Сообщение отправлено успешно!")
        print(f"   Message ID: {message.message_id}")
    except telegram.error.Forbidden as e:
        print(f"❌ Бот не имеет доступа к чату: {e}")
        print("   Решение: Добавьте бота в чат/группу")
    except telegram.error.BadRequest as e:
        if "Chat not found" in str(e):
            print(f"❌ Чат не найден: {e}")
            print("   Решение: Проверьте правильность TELEGRAM_CHAT_ID")
        else:
            print(f"❌ Неверный запрос: {e}")
    except Exception as e:
        print(f"❌ Неизвестная ошибка: {e}")

if __name__ == "__main__":
    print("🚀 Тестирование подключения к Telegram...\n")
    asyncio.run(test_connection())