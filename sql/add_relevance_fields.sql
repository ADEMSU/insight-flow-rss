-- Файл: sql/add_relevance_fields.sql
-- Миграция для добавления полей релевантности в таблицу texts

-- Добавляем поля для релевантности
ALTER TABLE texts 
ADD COLUMN IF NOT EXISTS relevance BOOLEAN DEFAULT NULL;

ALTER TABLE texts 
ADD COLUMN IF NOT EXISTS relevance_score FLOAT DEFAULT 0.0;

ALTER TABLE texts 
ADD COLUMN IF NOT EXISTS relevance_checked_at TIMESTAMP;

-- Создаем индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_texts_relevance 
ON texts(relevance) 
WHERE relevance IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_texts_relevance_category 
ON texts(relevance, category) 
WHERE relevance = true AND category IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_texts_relevance_unchecked 
ON texts(id) 
WHERE relevance IS NULL;

CREATE INDEX IF NOT EXISTS idx_texts_relevant_unclassified 
ON texts(id) 
WHERE relevance = true AND category IS NULL;

-- Создаем составной индекс для ежедневного анализа
CREATE INDEX IF NOT EXISTS idx_texts_daily_analysis 
ON texts(published_on, relevance, category) 
WHERE relevance = true AND category IS NOT NULL;

-- Добавляем комментарии к новым полям
COMMENT ON COLUMN texts.relevance IS 'Флаг релевантности поста для бизнеса (NULL - не проверено, TRUE/FALSE - результат проверки)';
COMMENT ON COLUMN texts.relevance_score IS 'Численная оценка релевантности от LM Studio (0.0 - 1.0)';
COMMENT ON COLUMN texts.relevance_checked_at IS 'Время проверки релевантности';

-- Создаем функцию для автоматического создания партиций
CREATE OR REPLACE FUNCTION create_monthly_partition_if_not_exists(table_name text, start_date date)
RETURNS void AS $$
DECLARE
   partition_name text;
   start_date_str text;
   end_date_str text;
   year int;
   month int;
BEGIN
   year := EXTRACT(YEAR FROM start_date);
   month := EXTRACT(MONTH FROM start_date);
   
   partition_name := format('%s_y%sm%s', table_name, year, LPAD(month::text, 2, '0'));
   start_date_str := TO_CHAR(start_date, 'YYYY-MM-DD');
   
   -- Вычисляем конечную дату (первое число следующего месяца)
   IF month = 12 THEN
       end_date_str := format('%s-01-01', year + 1);
   ELSE
       end_date_str := format('%s-%s-01', year, LPAD((month + 1)::text, 2, '0'));
   END IF;
   
   -- Проверяем существование партиции
   IF NOT EXISTS (
       SELECT 1 FROM pg_tables 
       WHERE schemaname = 'public' 
       AND tablename = partition_name
   ) THEN
       -- Создаем партицию
       EXECUTE format(
           'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
           partition_name, table_name, start_date_str, end_date_str
       );
       
       RAISE NOTICE 'Created partition % for table %', partition_name, table_name;
   END IF;
END;
$$ LANGUAGE plpgsql;

-- Создаем триггерную функцию для автоматического создания партиций
CREATE OR REPLACE FUNCTION auto_create_partition_trigger()
RETURNS trigger AS $$
BEGIN
   -- Создаем партицию для месяца вставляемой записи
   PERFORM create_monthly_partition_if_not_exists('texts', DATE_TRUNC('month', NEW.published_on));
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Создаем триггер для автоматического создания партиций
-- (Закомментировано, так как может влиять на производительность вставки)
-- CREATE TRIGGER auto_create_partition
-- BEFORE INSERT ON texts
-- FOR EACH ROW
-- EXECUTE FUNCTION auto_create_partition_trigger();

-- Создаем представление для статистики
CREATE OR REPLACE VIEW v_posts_statistics AS
SELECT 
   COUNT(*) AS total_posts,
   COUNT(CASE WHEN relevance IS NOT NULL THEN 1 END) AS checked_posts,
   COUNT(CASE WHEN relevance = true THEN 1 END) AS relevant_posts,
   COUNT(CASE WHEN relevance = false THEN 1 END) AS irrelevant_posts,
   COUNT(CASE WHEN relevance IS NULL THEN 1 END) AS unchecked_posts,
   COUNT(CASE WHEN relevance = true AND category IS NOT NULL THEN 1 END) AS classified_relevant_posts,
   COUNT(CASE WHEN relevance = true AND category IS NULL THEN 1 END) AS unclassified_relevant_posts,
   AVG(CASE WHEN relevance = true THEN relevance_score END) AS avg_relevance_score
FROM texts;

-- Создаем представление для статистики по категориям
CREATE OR REPLACE VIEW v_category_statistics AS
SELECT 
   category,
   subcategory,
   COUNT(*) AS post_count,
   AVG(relevance_score) AS avg_relevance_score,
   AVG(classification_confidence) AS avg_classification_confidence
FROM texts
WHERE relevance = true AND category IS NOT NULL
GROUP BY category, subcategory
ORDER BY category, post_count DESC;

-- Создаем функцию для очистки старых данных
CREATE OR REPLACE FUNCTION cleanup_old_posts(days_to_keep integer DEFAULT 90)
RETURNS integer AS $$
DECLARE
   deleted_count integer;
BEGIN
   DELETE FROM texts
   WHERE published_on < CURRENT_DATE - INTERVAL '1 day' * days_to_keep
   AND relevance = false;
   
   GET DIAGNOSTICS deleted_count = ROW_COUNT;
   
   RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Добавляем комментарий к функции
COMMENT ON FUNCTION cleanup_old_posts(integer) IS 'Удаляет нерелевантные посты старше указанного количества дней';

-- Выводим информацию о выполнении миграции
DO $$
BEGIN
   RAISE NOTICE 'Migration completed successfully!';
   RAISE NOTICE 'Added relevance fields to texts table';
   RAISE NOTICE 'Created indexes for optimized queries';
   RAISE NOTICE 'Created helper functions and views';
END $$;