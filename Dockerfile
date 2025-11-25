FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Europe/Moscow \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Базовые системные пакеты. libxml2/libxslt — на случай lxml/zeep.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
    tzdata \
    libxml2 \
    libxslt1.1 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Сначала ставим зависимости — лучше кэш
COPY requirements.txt ./
RUN python -m pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

# Потом код
COPY . .

# Каталоги под тома (хотя вы и так монтируете их томами/volume в compose)
RUN mkdir -p /app/data /app/logs

# Если нужно — делаем файл исполняемым
RUN chmod +x scheduler.py || true

VOLUME ["/app/data", "/app/logs"]

CMD ["python", "scheduler.py"]