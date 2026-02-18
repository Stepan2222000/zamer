FROM python:3.12-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    git \
    curl \
    xvfb \
    x11-utils \
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
    libgdk-pixbuf-2.0-0 \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# Node.js 20.x + OpenAI Codex CLI (для GPT-5.2 валидации)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    npm install -g @openai/codex && \
    rm -rf /var/lib/apt/lists/*

# Конфиг Codex CLI (авторизация через файл, метод chatgpt)
RUN mkdir -p /root/.codex && \
    printf 'forced_login_method = "chatgpt"\ncli_auth_credentials_store = "file"\n' > /root/.codex/config.toml

# Рабочая директория
WORKDIR /app

# Копирование requirements.txt
COPY requirements.txt .

# Установка Python зависимостей (кэшируется пока requirements.txt не изменится)
RUN pip install --no-cache-dir -r requirements.txt

# Установка Playwright chromium (кэшируется пока playwright версия не изменится)
RUN playwright install chromium

# Avito parsing library — отдельный слой, всегда свежая версия с GitHub
# ARG меняется при каждой сборке → Docker инвалидирует кэш только этого слоя и далее
ARG AVITO_LIB_CACHE_BUST=0
RUN pip install --no-cache-dir --no-deps \
    git+https://github.com/Stepan2222000/avito-library.git@main#egg=avito-library

# Копирование кода приложения
COPY . .

# Переменные окружения по умолчанию
ENV PYTHONUNBUFFERED=1

# Запуск главного процесса
CMD ["python", "main.py"]
