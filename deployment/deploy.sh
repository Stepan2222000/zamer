#!/bin/bash
set -e  # Exit on error

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Логирование
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"
}

# Переменные
PROJECT_DIR="/root/zam/zamer"
CONTAINER_DIR="${PROJECT_DIR}/container"
CURRENT_COMMIT=""
PREVIOUS_COMMIT=""
NEEDS_REBUILD=false

# Функция rollback
rollback() {
    error "Deployment failed! Rolling back to previous commit..."
    cd "$PROJECT_DIR"
    git reset --hard "$PREVIOUS_COMMIT"
    cd "$CONTAINER_DIR"
    docker compose up -d --build
    error "Rollback completed. System restored to previous state."
    exit 1
}

# Trap errors for rollback
trap 'rollback' ERR

log "🚀 Starting deployment..."

# Сохранить текущий коммит для возможного rollback
cd "$PROJECT_DIR"
PREVIOUS_COMMIT=$(git rev-parse HEAD)
log "Current commit: $PREVIOUS_COMMIT"

# Pull latest code
log "📥 Pulling latest code from GitHub..."
git fetch origin main
CURRENT_COMMIT=$(git rev-parse origin/main)

if [ "$CURRENT_COMMIT" = "$PREVIOUS_COMMIT" ]; then
    log "✅ Already up to date. No deployment needed."
    exit 0
fi

log "New commit available: $CURRENT_COMMIT"

# Проверить изменения в критических файлах
log "🔍 Checking for critical file changes..."

# Проверка Dockerfile
if git diff --name-only "$PREVIOUS_COMMIT" "$CURRENT_COMMIT" | grep -q "container/Dockerfile"; then
    warn "Dockerfile changed - rebuild required"
    NEEDS_REBUILD=true
fi

# Проверка requirements.txt
if git diff --name-only "$PREVIOUS_COMMIT" "$CURRENT_COMMIT" | grep -q "container/requirements.txt"; then
    warn "requirements.txt changed - rebuild required"
    NEEDS_REBUILD=true
fi

# Проверка docker-compose.yml
if git diff --name-only "$PREVIOUS_COMMIT" "$CURRENT_COMMIT" | grep -q "container/docker-compose.yml"; then
    warn "docker-compose.yml changed"
fi

# Pull code
git pull origin main

log "📦 Code updated successfully"

# Остановить контейнеры
cd "$CONTAINER_DIR"

log "🛑 Stopping containers gracefully..."
if docker compose ps | grep -q "avito_parser"; then
    # Graceful shutdown с таймаутом 5 минут
    docker compose down --timeout 300
    log "Containers stopped"
else
    warn "No running containers found"
fi

# Rebuild если нужно
if [ "$NEEDS_REBUILD" = true ]; then
    log "🔨 Rebuilding Docker image..."
    docker compose build --no-cache
    log "Build completed"
else
    log "ℹ️  No rebuild needed, using existing image"
fi

# Запустить контейнеры
log "▶️  Starting containers..."
docker compose up -d

# Ждем запуска
log "⏳ Waiting for container to start..."
sleep 10

# Проверка здоровья
log "🏥 Health check..."

# Проверка что контейнер запущен
if ! docker compose ps | grep -q "avito_parser.*Up"; then
    error "Container is not running!"
    rollback
fi

# Проверка логов на критические ошибки
if docker compose logs --tail=50 | grep -i "error\|fatal\|exception" | grep -v "ERROR_COUNTER"; then
    warn "Found errors in logs (check if critical)"
fi

# Вывод статуса воркеров
log "📊 Checking workers status..."
docker compose logs --tail=20 | grep -i "worker\|started" || true

log "✅ Deployment completed successfully!"
log "Previous commit: $PREVIOUS_COMMIT"
log "Current commit: $CURRENT_COMMIT"
log "Rebuild: $NEEDS_REBUILD"

# Показать статус контейнера
log "📦 Container status:"
docker compose ps

exit 0
