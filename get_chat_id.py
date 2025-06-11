#!/usr/bin/env python3
"""
Скрипт для получения Telegram Chat ID
"""
import os
import requests
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

def get_chat_ids():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        print("❌ TELEGRAM_BOT_TOKEN не найден в .env файле")
        return
    
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        if not data.get("ok"):
            print(f"❌ Ошибка API: {data.get('description', 'Unknown error')}")
            return
        
        updates = data.get("result", [])
        
        if not updates:
            print("⚠️  Нет сообщений. Отправьте сообщение боту и запустите скрипт снова.")
            return
        
        print("📋 Найденные чаты:\n")
        
        seen_chats = set()
        for update in updates:
            message = update.get("message", {})
            chat = message.get("chat", {})
            
            if chat:
                chat_id = chat.get("id")
                chat_type = chat.get("type")
                chat_title = chat.get("title", chat.get("username", chat.get("first_name", "Unknown")))
                
                if chat_id and chat_id not in seen_chats:
                    seen_chats.add(chat_id)
                    print(f"ID: {chat_id}")
                    print(f"Тип: {chat_type}")
                    print(f"Название: {chat_title}")
                    print("-" * 40)
        
        print(f"\n✅ Скопируйте нужный ID и вставьте в .env файл:")
        print(f"TELEGRAM_CHAT_ID={list(seen_chats)[0] if seen_chats else 'YOUR_CHAT_ID'}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    print("🔍 Поиск Telegram Chat ID...\n")
    get_chat_ids()