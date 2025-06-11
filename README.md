# InsightFlow

## Автоматизированный сервис для анализа новостных публикаций с использованием ИИ

InsightFlow - это сервис, который собирает данные из RSS-фидов, классифицирует их, анализирует с помощью языковых моделей и отправляет insights в Telegram. Проект использует PostgreSQL для хранения данных и обеспечивает полный цикл обработки контента.

## Основные возможности

- **Сбор данных**: Автоматический сбор новостей из RSS-фидов каждый час
- **Классификация**: Автоматическая классификация публикаций по категориям и подкатегориям
- **Дедупликация**: Двухэтапная фильтрация контента с использованием TF-IDF и simhash
- **AI-анализ**: Обработка текстов с помощью языковых моделей для выделения инсайтов
- **Доставка**: Отправка аналитических отчетов в Telegram с ссылками на источники
- **Масштабируемость**: Поддержка нескольких API-ключей с автоматической ротацией

## Архитектура

```
╭───────────────────╮      ╭───────────────────╮      ╭───────────────────╮
│                   │      │                   │      │                   │
│   RSS-источники   │─────▶│  Классификация    │─────▶│  LLM-анализ      │
│   (ежечасный сбор)│      │  (перманентная)   │      │  (ежедневный)     │
│                   │      │                   │      │                   │
╰───────────────────╯      ╰───────────────────╯      ╰───────────────────╯
          │                          │                          │
          ▼                          ▼                          ▼
╭───────────────────────────────────────────────────────────────────────────╮
│                                                                           │
│                             PostgreSQL                                    │
│                                                                           │
╰───────────────────────────────────────────────────────────────────────────╯
                                      │
                                      ▼
                           ╭───────────────────╮
                           │                   │
                           │  Telegram Bot     │
                           │  (доставка)       │
                           │                   │
                           ╰───────────────────╯
```

## Установка и запуск

### Предварительные требования

- Docker и Docker Compose
- API-ключи OpenRouter для доступа к языковым моделям
- Telegram бот и ID чата для отправки отчетов

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
   # OpenRouter для доступа к языковым моделям
   OPENROUTER_API_KEY=your-api-key
   OPENROUTER_API_KEY_2=your-backup-api-key
   
   # Настройки сайта
   SITE_URL=https://example.com
   SITE_NAME=InsightFlow
   
   # Telegram Bot
   TELEGRAM_BOT_TOKEN=your-telegram-bot-token
   TELEGRAM_CHAT_ID=your-telegram-chat-id
   
   # PostgreSQL настройки
   POSTGRES_HOST=postgres
   POSTGRES_PORT=5432
   POSTGRES_DB=insightflow
   POSTGRES_USER=insightflow
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

Для настройки RSS-источников отредактируйте файл `rss_sources.json`:

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

### Планировщик задач

Система использует следующие интервалы:
- **RSS-сбор**: Каждый час
- **Классификация**: Каждые 15 минут, по 10 постов за раз
- **Анализ и отправка**: Один раз в сутки, только для классифицированных постов

## Мониторинг

### Логи

Все логи хранятся в директории `logs/`:

```bash
docker-compose exec insightflow-service-rss cat /app/logs/insightflow_latest.log
```

### База данных

Проверка состояния базы данных:

```bash
python check_database.py
```

### Проверка API-ключей

Тестирование доступности API-ключей:

```python
from content_classifier import ContentClassifier
import asyncio

async def test_keys():
    classifier = ContentClassifier()
    results = await classifier.test_api_keys()
    print(results)

asyncio.run(test_keys())
```

## Разработка

### Структура проекта

```
insightflow/
├── batch_manager.py         # Управление батчами для обработки
├── check_database.py        # Скрипт проверки БД
├── classify_remaining.py    # Классификация публикаций
├── content_classifier.py    # Классификатор с поддержкой ротации ключей
├── data_manager.py          # Управление данными из RSS
├── db_manager.py            # Работа с базой данных
├── docker-compose.yml       # Конфигурация Docker Compose
├── Dockerfile               # Инструкции сборки Docker
├── insightflow_service.py   # Основной сервис
├── llm_client.py            # Клиент для языковых моделей
├── post.py                  # Модель данных публикации
├── requirements.txt         # Зависимости Python
├── rss_manager.py           # Управление RSS-фидами
├── rss_sources.json         # Конфигурация источников
├── scheduler.py             # Планировщик задач
├── sql/                     # SQL-скрипты
└── telegram_sender.py       # Отправка в Telegram
```

## Вклад в проект

1. Форкните репозиторий
2. Создайте ветку с вашими изменениями: `git checkout -b feature/amazing-feature`
3. Сделайте коммит изменений: `git commit -m 'Add amazing feature'`
4. Отправьте изменения: `git push origin feature/amazing-feature`
5. Создайте Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for full details.

### Key Permissions:
- ✅ Commercial use
- ✅ Modification
- ✅ Distribution
- ✅ Private use

### Limitations:
- ❌ Liability
- ❌ Warranty

### Conditions:
- © Must include copyright notice
- © Must include license text

For more information about MIT License, visit [choosealicense.com](https://choosealicense.com/licenses/mit/).
