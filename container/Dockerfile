FROM python:3.12-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    xvfb \
    chromium \
    fonts-liberation \
    fonts-noto-color-emoji \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libcairo2 \
    libgdk-pixbuf2.0-0 \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Копирование requirements.txt
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Установка Playwright chromium
RUN python -c "from playwright.sync_api import sync_playwright; \
    p = sync_playwright().start(); \
    p.chromium.install(); \
    p.stop()"

# Копирование кода приложения
COPY . .

# Переменные окружения по умолчанию
ENV PYTHONUNBUFFERED=1

# Запуск главного процесса
CMD ["python", "main.py"]
