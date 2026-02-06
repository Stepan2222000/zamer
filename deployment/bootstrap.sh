#!/bin/bash
set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[BOOTSTRAP]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Конфигурация
PROJECT_DIR="/root/zam/zamer"
REPO_URL="https://github.com/Stepan2222000/zamer.git"

log "========================================="
log "Bootstrap: Настройка чистого сервера"
log "========================================="

# Проверка root
if [ "$EUID" -ne 0 ]; then
    error "Требуются root права"
    exit 1
fi

# Установка системных пакетов
if ! command -v git &> /dev/null || ! command -v curl &> /dev/null; then
    log "Установка системных пакетов..."
    apt-get update -qq
    apt-get install -y -qq git curl ca-certificates gnupg lsb-release
fi

# Установка Docker
if ! command -v docker &> /dev/null; then
    log "Установка Docker..."
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    rm get-docker.sh
    systemctl start docker
    systemctl enable docker
    log "Docker установлен: $(docker --version)"
else
    log "Docker уже установлен: $(docker --version)"
fi

# Клонирование проекта
if [ ! -d "$PROJECT_DIR" ]; then
    log "Клонирование проекта..."
    mkdir -p "$(dirname $PROJECT_DIR)"
    git clone "$REPO_URL" "$PROJECT_DIR"
    log "Проект склонирован в $PROJECT_DIR"
else
    log "Проект уже существует в $PROJECT_DIR"
fi

cd "$PROJECT_DIR"

# .env файл опциональный - все дефолты в config.py
ENV_FILE="$PROJECT_DIR/container/.env"

if [ -f "$ENV_FILE" ]; then
    log ".env существует (пользовательские переопределения)"
else
    log ".env отсутствует - будут использованы дефолты из config.py"
fi

log "========================================="
log "Bootstrap завершен успешно!"
log "========================================="
log "Запуск deploy.sh..."

# Передача управления deploy.sh
bash "$PROJECT_DIR/deployment/deploy.sh"
