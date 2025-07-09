FROM python:3.10-slim

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы проекта
COPY . .

# Копируем .env файл
COPY .env.example .env

# Устанавливаем зависимости Python
RUN pip install --no-cache-dir -r requirements.txt

# Создаем директории для данных и логов
RUN mkdir -p /app/data /app/logs

# Устанавливаем права доступа
RUN chmod +x scheduler.py

# Устанавливаем часовой пояс
ENV TZ=Europe/Moscow

# Volumes для постоянного хранения данных
VOLUME ["/app/data", "/app/logs"]

# Запускаем планировщик
CMD ["python", "scheduler.py"]
