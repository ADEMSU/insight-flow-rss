# InsightFlow

## Автоматизированный сервис для анализа новостных публикаций с использованием ИИ

InsightFlow — это сервис, который собирает данные из RSS-фидов **и Медиалогии (через SOAP API)**, классифицирует их, анализирует с помощью языковых моделей и отправляет insights в Telegram. Проект использует PostgreSQL для хранения данных и обеспечивает полный цикл обработки контента.

## Основные возможности

* **Сбор данных**: Автоматический сбор новостей из RSS-фидов и Медиалогии каждый час
* **Классификация**: Автоматическая классификация публикаций по категориям и подкатегориям
* **Дедупликация**: Двухэтапная фильтрация контента с использованием TF-IDF и simhash
* **AI-анализ**: Обработка текстов с помощью языковых моделей для выделения инсайтов
* **Доставка**: Отправка аналитических отчетов в Telegram с ссылками на источники
* **Масштабируемость**: Поддержка нескольких API-ключей с автоматической ротацией

## Архитектура

```
╭────────────────────╮     ╭───────────────────╮     ╭──────────────────╮
│    Источники       │     │ Классификация     │     │    LLM-анализ     │
│  RSS + Медиалогия  │────▶│  (перманентная)   │────▶│   (ежедневный)    │
╰────────────────────╯     ╰───────────────────╯     ╰──────────────────╯
         │                           │                        │
         ▼                           ▼                        ▼
╭──────────────────────────────────────────────────────────────────────╮
│                               PostgreSQL                             │
╰──────────────────────────────────────────────────────────────────────╯
                                     │
                                     ▼
                          ╭────────────────────╮
                          │     Telegram Bot   │
                          │     (доставка)     │
                          ╰────────────────────╯
```

## Установка и запуск

### Предварительные требования

* Docker и Docker Compose
* API-ключи OpenRouter для доступа к языковым моделям
* Telegram бот и ID чата для отправки отчетов
* SOAP-доступ к Медиалогии (логин, пароль, WSDL, report ID)

### Быстрый старт

1. Клонируйте репозиторий:

   ```bash
   git clone https://github.com/your-username/insightflow.git
   cd insightflow
   ```

2. Создайте файл `.env` на основе примера:

   ```bash
   cp .env.example .env
   ```

3. Отредактируйте файл `.env`, заполнив необходимые параметры:

   ```
   # Подключение RSS
   RSS_CONFIG_FILE=your_rss_sources.json

   # Медиалогия
   MLG_USERNAME=your_username
   MLG_PASSWORD=your_password
   MLG_WSDL_URL=https://api.mlg.ru/soap/wsdl
   MLG_REPORT_ID=123456

   # Telegram Bot
   TELEGRAM_BOT_TOKEN=your-telegram-bot-token
   TELEGRAM_CHAT_ID=your-telegram-chat-id

   # PostgreSQL настройки
   POSTGRES_HOST=postgres
   POSTGRES_PORT=5432
   POSTGRES_DB=bd
   POSTGRES_USER=bd
   POSTGRES_PASSWORD=your-secure-password
   ```

4. Запустите сервис с Docker Compose:

   ```bash
   docker-compose up -d
   ```

5. Проверьте работу сервиса:

   ```bash
   docker-compose logs -f
   ```

## Конфигурация

### RSS-источники

Файл `rss_sources.json`:

```json
{
  "sources": [
    {
      "name": "Example News",
      "url": "https://example.com/rss/news.xml",
      "category": "news"
    },
    {
      "name": "Finance Blog",
      "url": "https://financeblog.com/feed",
      "category": "finance"
    }
  ]
}
```

## Планировщик задач

Система использует следующие интервалы:

* **Сбор данных (RSS + Медиалогия)**: каждый час
* **Классификация**: каждые 15 минут
* **Анализ и отправка**: один раз в сутки (09:00 МСК)

## Структура проекта

```
insightflow/
├── batch_manager.py         # Управление батчами для обработки
├── classify_remaining.py    # Классификация публикаций
├── content_classifier.py    # Классификатор
├── data_manager.py          # Объединение RSS и Медиалогии
├── db_manager.py            # Работа с базой данных
├── insightflow_service.py   # Основной сервис
├── mlg_manager.py           # Менеджер SOAP-доступа к Медиалогии
├── post.py                  # Модель данных публикации
├── requirements.txt         # Зависимости Python
├── rss_manager.py           # Работа с RSS-источниками
├── scheduler.py             # Планировщик задач
├── telegram_sender.py       # Отправка в Telegram
└── ...
```

## License

MIT License — см. файл `LICENSE`.
