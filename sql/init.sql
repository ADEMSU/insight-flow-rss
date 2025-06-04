-- Пользователи
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255),  -- Может быть NULL для OAuth-пользователей
    last_login_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL DEFAULT 'active',  -- 'active', 'blocked', 'deleted'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- OAuth-привязки
CREATE TABLE oauth_providers (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider VARCHAR(50) NOT NULL, -- 'vk', 'yandex'
    provider_id VARCHAR(255) NOT NULL,
    access_token TEXT,
    refresh_token TEXT,
    expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (provider, provider_id)
);

-- Сессии пользователей
CREATE TABLE sessions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token VARCHAR(255) NOT NULL UNIQUE,
    ip_address VARCHAR(45),
    user_agent TEXT,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Тарифы подписок
CREATE TABLE tariffs (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    price NUMERIC(10, 2) NOT NULL,
    duration_days INTEGER NOT NULL,
    max_projects INTEGER NOT NULL DEFAULT 1,  -- Количество проектов по тарифу
    max_texts_per_day INTEGER NOT NULL DEFAULT 100,  -- Лимит обрабатываемых текстов в день
    features JSONB,  -- Список доступных функций
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Подписки пользователей
CREATE TABLE subscriptions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tariff_id INTEGER REFERENCES tariffs(id),
    status VARCHAR(20) NOT NULL, -- 'active', 'expired', 'cancelled'
    starts_at TIMESTAMP WITH TIME ZONE NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    auto_renew BOOLEAN DEFAULT FALSE,
    last_payment_id INTEGER, -- Связь будет установлена после создания таблицы payments
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Платежи
CREATE TABLE payments (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    subscription_id INTEGER REFERENCES subscriptions(id),
    amount NUMERIC(10, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'RUB',
    status VARCHAR(20) NOT NULL, -- 'pending', 'completed', 'failed', 'refunded'
    payment_method VARCHAR(50), -- 'card', 'apple_pay', etc.
    payment_provider VARCHAR(50), -- 'stripe', 'cloudpayments', etc.
    payment_provider_id VARCHAR(255), -- ID в системе платежного провайдера
    payment_data JSONB, -- Дополнительные данные от платежной системы
    invoice_id VARCHAR(255),  -- Номер счета
    receipt_url VARCHAR(512),  -- Ссылка на чек
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Устанавливаем foreign key для last_payment_id в subscriptions
ALTER TABLE subscriptions 
ADD CONSTRAINT fk_subscriptions_last_payment 
FOREIGN KEY (last_payment_id) REFERENCES payments(id);

-- Предопределенные категории контента
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    slug VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    parent_id INTEGER REFERENCES categories(id),
    is_default BOOLEAN DEFAULT FALSE,  -- Используется по умолчанию в новых проектах
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- RSS-источники
CREATE TABLE rss_sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    url VARCHAR(512) NOT NULL UNIQUE,
    category_id INTEGER REFERENCES categories(id),
    priority INTEGER DEFAULT 0, -- Приоритет источника
    status VARCHAR(20) NOT NULL DEFAULT 'active',  -- 'active', 'inactive', 'error'
    last_fetched_at TIMESTAMP WITH TIME ZONE,
    error_count INTEGER DEFAULT 0,  -- Счетчик ошибок
    last_error TEXT,  -- Последняя ошибка
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Проекты пользователей
CREATE TABLE projects (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    selected_categories INTEGER[] NOT NULL,  -- Массив ID выбранных категорий
    custom_prompt TEXT,  -- Уточняющий промт, введенный пользователем
    processing_frequency VARCHAR(20) DEFAULT 'daily',  -- 'daily', 'weekly', 'manual'
    last_processed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL DEFAULT 'active',  -- 'active', 'paused', 'deleted'
    notification_email BOOLEAN DEFAULT FALSE,  -- Отправлять ли уведомления на email
    notification_telegram BOOLEAN DEFAULT FALSE,  -- Отправлять ли в Telegram
    telegram_chat_id VARCHAR(255),  -- ID чата Telegram для уведомлений
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Тексты из RSS
CREATE TABLE texts (
    id SERIAL PRIMARY KEY,
    post_id VARCHAR(255) NOT NULL, -- Уникальный ID поста из RSS
    source_id INTEGER REFERENCES rss_sources(id),
    blog_host VARCHAR(255),
    blog_host_type INTEGER,
    title TEXT,
    content TEXT,
    url VARCHAR(1024),
    published_on TIMESTAMP WITH TIME ZONE,
    fetched_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    simhash VARCHAR(255), -- Для обнаружения дубликатов
    categories INTEGER[],  -- Массив ID категорий, определенных автоматически
    is_processed BOOLEAN DEFAULT FALSE,
    processing_status VARCHAR(20) DEFAULT 'pending',  -- 'pending', 'processing', 'processed', 'error'
    UNIQUE (post_id)
) PARTITION BY RANGE (published_on);

-- Создание партиций по месяцам для таблицы texts
CREATE TABLE texts_y2024m04 PARTITION OF texts
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');

-- Результаты обработки текстов по проектам
CREATE TABLE processed_results (
    id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    text_id INTEGER NOT NULL REFERENCES texts(id) ON DELETE CASCADE,
    relevance_score FLOAT,  -- Оценка релевантности текста к проекту
    analysis_text TEXT,  -- Результат анализа от ИИ
    summary TEXT,  -- Краткое содержание
    categories INTEGER[],  -- Категории, определенные ИИ для этого проекта
    sentiment SMALLINT,  -- Тональность текста (-2 до 2)
    context JSONB,  -- Дополнительный контекст
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (project_id, text_id)
);

-- Аналитические сюжеты (объединенные результаты)
CREATE TABLE stories (
    id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    source_post_id VARCHAR(255),  -- ID основного поста-источника
    suggestions TEXT,  -- Предложения для контент-плана
    categories INTEGER[],  -- Связанные категории
    status VARCHAR(20) DEFAULT 'active',  -- 'active', 'archived'
    user_rating SMALLINT,  -- Оценка пользователя от 1 до 5
    user_notes TEXT,  -- Заметки пользователя
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Связь сюжетов с текстами
CREATE TABLE story_texts (
    story_id INTEGER NOT NULL REFERENCES stories(id) ON DELETE CASCADE,
    text_id INTEGER NOT NULL REFERENCES texts(id) ON DELETE CASCADE,
    relevance_score FLOAT,  -- Оценка релевантности текста к сюжету
    PRIMARY KEY (story_id, text_id)
);

-- Запросы к нейросети
CREATE TABLE ai_requests (
    id SERIAL PRIMARY KEY,
    project_id INTEGER REFERENCES projects(id) ON DELETE SET NULL,
    request_type VARCHAR(50) NOT NULL,  -- 'categorization', 'analysis', 'summary'
    prompt TEXT NOT NULL,  -- Исходный промт
    response TEXT,  -- Полный ответ нейросети
    tokens_used INTEGER,  -- Количество использованных токенов
    provider VARCHAR(50) NOT NULL,  -- 'openai', 'openrouter', etc.
    model VARCHAR(100) NOT NULL,  -- Название модели
    status VARCHAR(20) NOT NULL,  -- 'success', 'error'
    error_message TEXT,
    processing_time INTEGER,  -- Время обработки в миллисекундах
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Пользовательские уведомления
CREATE TABLE notifications (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    type VARCHAR(50) NOT NULL,  -- 'project_update', 'payment', 'system'
    is_read BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Логи системы
CREATE TABLE system_logs (
    id SERIAL PRIMARY KEY,
    level VARCHAR(20) NOT NULL,  -- 'info', 'warning', 'error', 'critical'
    component VARCHAR(100) NOT NULL,  -- Компонент системы
    message TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Настройки системы
CREATE TABLE system_settings (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT NOT NULL,
    description TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации запросов
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_oauth_providers_user_id ON oauth_providers(user_id);
CREATE INDEX idx_sessions_user_id ON sessions(user_id);
CREATE INDEX idx_sessions_token ON sessions(token);
CREATE INDEX idx_subscriptions_user_id ON subscriptions(user_id);
CREATE INDEX idx_subscriptions_status ON subscriptions(status);
CREATE INDEX idx_payments_user_id ON payments(user_id);
CREATE INDEX idx_projects_user_id ON projects(user_id);
CREATE INDEX idx_texts_published_on ON texts(published_on);
CREATE INDEX idx_texts_simhash ON texts(simhash);
CREATE INDEX idx_texts_source_id ON texts(source_id);
CREATE INDEX idx_texts_categories ON texts USING GIN(categories);
CREATE INDEX idx_processed_results_project_id ON processed_results(project_id);
CREATE INDEX idx_processed_results_text_id ON processed_results(text_id);
CREATE INDEX idx_processed_results_categories ON processed_results USING GIN(categories);
CREATE INDEX idx_stories_project_id ON stories(project_id);
CREATE INDEX idx_stories_categories ON stories USING GIN(categories);
CREATE INDEX idx_story_texts_text_id ON story_texts(text_id);
CREATE INDEX idx_ai_requests_project_id ON ai_requests(project_id);
CREATE INDEX idx_notifications_user_id ON notifications(user_id);
CREATE INDEX idx_notifications_is_read ON notifications(is_read);