# InsightFlow RSS - Документация проекта (обновлено 11.06.2025)

## Общая архитектура

Проект InsightFlow RSS представляет собой систему для:
1. Сбора данных из RSS-лент
2. Классификации контента
3. Анализа и агрегации информации
4. Отправки результатов в Telegram

Основные компоненты системы:

```
┌────────────────┐   ┌────────────────┐   ┌─────────────────────┐
│  RSSManager    │──▶│  DataManager   │──▶│  ContentClassifier  │
└────────────────┘   └────────────────┘   └─────────────────────┘
        │                     │                      │
        ▼                     ▼                      ▼
┌────────────────┐   ┌────────────────┐   ┌─────────────────────┐
│ BatchManager   │   │ RelevanceChecker│   │    TokenEstimator   │
└────────────────┘   └────────────────┘   └─────────────────────┘
        │                     │                      │
        ▼                     ▼                      ▼
┌────────────────┐   ┌────────────────┐   ┌─────────────────────┐
│ LMStudioClient │◀─▶│ InsightFlow    │◀──│      DBManager      │
└────────────────┘   └────────────────┘   └─────────────────────┘
        ▲                     │                      ▲
        │                     ▼                      │
┌────────────────┐   ┌────────────────┐   ┌─────────────────────┐
│  TelegramSender│◀───│  Scheduler    │───▶│  TextPreprocessor   │
└────────────────┘   └────────────────┘   └─────────────────────┘
```

## Файлы проекта

### Основные модули
- batch_manager.py - управление батчами постов
- content_classifier.py - классификация контента
- data_manager.py - управление данными
- db_manager.py - работа с базой данных
- insightflow_service.py - основной сервис
- lm_studio_client.py - клиент для LM Studio
- rss_manager.py - управление RSS-лентами
- scheduler.py - планировщик задач
- telegram_sender.py - отправка в Telegram
- text_preprocessing.py - предобработка текста
- token_estimator.py - оценка количества токенов

### Вспомогательные файлы
- docker-compose.yml - конфигурация Docker
- Dockerfile - конфигурация контейнера
- requirements.txt - зависимости Python
- rss_sources.json - источники RSS
- rss_sources_cleaned.json - очищенные источники
- rss_urls.json - URL RSS-лент
- README.md - основная информация о проекте
- documentation.md - данная документация

### SQL скрипты
- sql/init.sql - инициализация БД
- sql/migration_add_html_content.sql - миграция
- sql/add_relevance_fields.sql - добавление полей

## Вспомогательные/отладочные файлы (не участвуют в основном проекте)
- .DS_Store
- .env.example
- fix_timezone.py
- get_chat_id.py
- test_telegram.py
- send_test_digest.py
- check_database.py
- classify_remaining.py

## Основные изменения в архитектуре (обновления)

1. Добавлен новый модуль `text_preprocessing.py` с функциями:
   - Нормализация текста
   - Удаление стоп-слов
   - Очистка HTML-тегов

2. Обновлен `TokenEstimator`:
   - Добавлена поддержка новых моделей токенизации
   - Улучшена оценка токенов для длинных текстов

3. Изменения в `LMStudioClient`:
   - Добавлен кэш запросов
   - Улучшена обработка ошибок
   - Добавлены таймауты

4. Обновления в `TelegramSender`:
   - Поддержка Markdown и HTML форматирования
   - Обработка длинных сообщений (разбивка на части)
   - Улучшенная система повторных попыток

5. Новые функции в `BatchManager`:
   - Группировка по тематикам
   - Автоматическое определение оптимального размера батча
   - Поддержка динамического порога схожести

## Детализация изменений

### TextPreprocessor (новый модуль)
- `clean_html(html_text)` - очистка HTML
- `normalize_text(text)` - нормализация текста
- `remove_stopwords(text, language='ru')` - удаление стоп-слов

### LMStudioClient обновления
- Добавлен параметр `timeout` в методы:
  - `check_relevance()`
  - `classify_content()`
  - `analyze_and_summarize()`
- Кэширование запросов для одинаковых текстов
- Логирование ошибок API

### TokenEstimator улучшения
- Поддержка моделей:
  - cl100k_base (по умолчанию)
  - p50k_base
  - r50k_base
- Метод `estimate_batch_tokens()` для оценки батчей

## Актуальная последовательность работы

1. **Получение данных (RSSManager)**
   - Загрузка конфигурации из rss_sources.json
   - Параллельное получение записей
   - Предварительная очистка текста (TextPreprocessor)

2. **Обработка (DataManager)**
   - Сохранение в БД
   - Проверка релевантности (RelevanceChecker)
   - Классификация (ContentClassifier)

3. **Анализ (InsightFlow)**
   - Группировка постов (BatchManager)
   - Анализ через LM Studio
   - Формирование отчетов

4. **Отправка (TelegramSender)**
   - Форматирование сообщений
   - Отправка с обработкой ошибок
