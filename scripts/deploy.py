#!/usr/bin/env python3
"""
Скрипт деплоя на несколько серверов

Использование:
    python scripts/deploy.py              # Деплой на все сервера
    python scripts/deploy.py --server X   # Деплой только на сервер X
    python scripts/deploy.py --dry-run    # Показать конфиг без деплоя
"""

import argparse
import os
import stat
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

try:
    import paramiko
    import yaml
except ImportError:
    print("Установите зависимости: pip install paramiko pyyaml")
    sys.exit(1)

# Пути
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONTAINER_DIR = PROJECT_ROOT / "container"
CONFIG_PATH = SCRIPT_DIR / "data" / "servers.yaml"
REMOTE_PATH = "/root/container"

# Логирование с потокобезопасным выводом
print_lock = threading.Lock()


def log(server_name: str, message: str, status: str = ""):
    """Потокобезопасный вывод логов"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    status_icon = {"ok": "\u2713", "error": "\u2717", "info": "\u2022", "wait": "\u25cb"}.get(status, "")
    with print_lock:
        print(f"[{timestamp}] [{server_name}] {status_icon} {message}")


def load_config() -> dict:
    """Загрузка конфигурации из YAML"""
    if not CONFIG_PATH.exists():
        print(f"Конфиг не найден: {CONFIG_PATH}")
        print("Создайте файл scripts/data/servers.yaml по образцу")
        sys.exit(1)

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    # Валидация
    if "servers" not in config or not config["servers"]:
        print("В конфиге нет серверов")
        sys.exit(1)

    return config


def create_ssh_client(host: str, user: str, password: str) -> paramiko.SSHClient:
    """Создание SSH клиента"""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=password, timeout=30)
    return client


def exec_command(client: paramiko.SSHClient, command: str, timeout: int = 300) -> tuple:
    """Выполнение команды на сервере"""
    stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
    exit_code = stdout.channel.recv_exit_status()
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    return exit_code, out, err


def check_docker(client: paramiko.SSHClient, server_name: str) -> bool:
    """Проверка и установка Docker"""
    log(server_name, "Проверка Docker...", "info")

    exit_code, _, _ = exec_command(client, "docker --version")
    if exit_code == 0:
        log(server_name, "Docker установлен", "ok")
        return True

    log(server_name, "Docker не найден, устанавливаем...", "wait")
    exit_code, out, err = exec_command(client, "curl -fsSL https://get.docker.com | sh", timeout=600)

    if exit_code != 0:
        log(server_name, f"Ошибка установки Docker: {err}", "error")
        return False

    log(server_name, "Docker установлен", "ok")
    return True


def check_docker_compose(client: paramiko.SSHClient, server_name: str) -> bool:
    """Проверка и установка Docker Compose"""
    log(server_name, "Проверка Docker Compose...", "info")

    exit_code, _, _ = exec_command(client, "docker compose version")
    if exit_code == 0:
        log(server_name, "Docker Compose доступен", "ok")
        return True

    # Пробуем установить docker-compose-plugin
    log(server_name, "Docker Compose не найден, устанавливаем плагин...", "wait")

    # Определяем дистрибутив и устанавливаем
    install_cmd = """
    if command -v apt-get &> /dev/null; then
        apt-get update && apt-get install -y docker-compose-plugin
    elif command -v yum &> /dev/null; then
        yum install -y docker-compose-plugin
    elif command -v dnf &> /dev/null; then
        dnf install -y docker-compose-plugin
    else
        # Fallback: установка standalone docker-compose
        curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
        chmod +x /usr/local/bin/docker-compose
        ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose
    fi
    """

    exit_code, out, err = exec_command(client, install_cmd, timeout=300)

    # Проверяем еще раз
    exit_code, _, _ = exec_command(client, "docker compose version")
    if exit_code == 0:
        log(server_name, "Docker Compose установлен", "ok")
        return True

    # Пробуем standalone версию
    exit_code, _, _ = exec_command(client, "docker-compose version")
    if exit_code == 0:
        log(server_name, "Docker Compose (standalone) установлен", "ok")
        return True

    log(server_name, f"Не удалось установить Docker Compose: {err}", "error")
    return False


def get_compose_command(client: paramiko.SSHClient) -> str:
    """Определить какую команду docker compose использовать"""
    exit_code, _, _ = exec_command(client, "docker compose version")
    if exit_code == 0:
        return "docker compose"
    return "docker-compose"


def stop_container(client: paramiko.SSHClient, server_name: str) -> bool:
    """Остановка контейнера (graceful)"""
    log(server_name, "Остановка контейнера...", "info")

    # Проверяем существует ли директория
    exit_code, _, _ = exec_command(client, f"test -d {REMOTE_PATH}")
    if exit_code != 0:
        log(server_name, "Директория не существует, пропускаем остановку", "ok")
        return True

    compose_cmd = get_compose_command(client)

    # Graceful stop
    exit_code, out, err = exec_command(
        client,
        f"cd {REMOTE_PATH} && {compose_cmd} down --timeout 60",
        timeout=120
    )

    if exit_code != 0 and "no configuration file" not in err.lower():
        log(server_name, f"Предупреждение при остановке: {err}", "info")

    log(server_name, "Контейнер остановлен", "ok")
    return True


def upload_files(client: paramiko.SSHClient, server_name: str) -> bool:
    """Копирование файлов через SFTP"""
    log(server_name, "Копирование файлов...", "info")

    sftp = client.open_sftp()
    files_count = 0

    # Создаем директорию если не существует
    try:
        sftp.stat(REMOTE_PATH)
    except FileNotFoundError:
        exec_command(client, f"mkdir -p {REMOTE_PATH}")

    # Рекурсивно копируем container/
    for local_path in CONTAINER_DIR.rglob("*"):
        if local_path.is_file():
            # Пропускаем __pycache__, .pyc, logs, .env
            if "__pycache__" in str(local_path) or local_path.suffix == ".pyc":
                continue
            if local_path.name == ".env" or "logs" in local_path.parts:
                continue

            relative = local_path.relative_to(CONTAINER_DIR)
            remote_file = f"{REMOTE_PATH}/{relative}"
            remote_dir = str(Path(remote_file).parent)

            # Создаем директорию на сервере
            exec_command(client, f"mkdir -p {remote_dir}")

            # Копируем файл
            sftp.put(str(local_path), remote_file)
            files_count += 1

    sftp.close()
    log(server_name, f"Скопировано {files_count} файлов", "ok")
    return True


def create_env_file(client: paramiko.SSHClient, server_name: str, env_vars: dict, server_config: dict) -> bool:
    """Создание .env файла на сервере"""
    log(server_name, "Создание .env файла...", "info")

    # Собираем переменные
    all_vars = dict(env_vars)
    all_vars["TOTAL_BROWSER_WORKERS"] = str(server_config.get("browser_workers", 15))
    all_vars["TOTAL_VALIDATION_WORKERS"] = str(server_config.get("validation_workers", 15))

    # Формируем содержимое
    env_content = "\n".join(f"{k}={v}" for k, v in all_vars.items())

    # Записываем через echo
    # Экранируем кавычки и специальные символы
    escaped_content = env_content.replace("'", "'\\''")
    exit_code, _, err = exec_command(
        client,
        f"echo '{escaped_content}' > {REMOTE_PATH}/.env"
    )

    if exit_code != 0:
        log(server_name, f"Ошибка создания .env: {err}", "error")
        return False

    browser = server_config.get("browser_workers", 15)
    validation = server_config.get("validation_workers", 15)
    log(server_name, f".env создан (browser={browser}, validation={validation})", "ok")
    return True


def build_container(client: paramiko.SSHClient, server_name: str) -> bool:
    """Сборка Docker образа"""
    log(server_name, "Сборка образа (может занять несколько минут)...", "wait")

    compose_cmd = get_compose_command(client)

    exit_code, out, err = exec_command(
        client,
        f"cd {REMOTE_PATH} && {compose_cmd} build --no-cache",
        timeout=900  # 15 минут
    )

    if exit_code != 0:
        log(server_name, f"Ошибка сборки: {err[-500:]}", "error")
        return False

    log(server_name, "Образ собран", "ok")
    return True


def start_container(client: paramiko.SSHClient, server_name: str) -> bool:
    """Запуск контейнера"""
    log(server_name, "Запуск контейнера...", "info")

    compose_cmd = get_compose_command(client)

    exit_code, out, err = exec_command(
        client,
        f"cd {REMOTE_PATH} && {compose_cmd} up -d",
        timeout=60
    )

    if exit_code != 0:
        log(server_name, f"Ошибка запуска: {err}", "error")
        return False

    log(server_name, "Контейнер запущен", "ok")
    return True


def deploy_to_server(server_config: dict, env_vars: dict) -> dict:
    """Деплой на один сервер"""
    name = server_config["name"]
    host = server_config["host"]
    user = server_config["user"]
    password = server_config["password"]

    result = {"server": name, "success": False, "error": None}

    try:
        log(name, f"Подключение к {host}...", "info")
        client = create_ssh_client(host, user, password)
        log(name, "Подключено", "ok")

        # Шаги деплоя
        steps = [
            ("Docker", lambda: check_docker(client, name)),
            ("Docker Compose", lambda: check_docker_compose(client, name)),
            ("Остановка", lambda: stop_container(client, name)),
            ("Копирование", lambda: upload_files(client, name)),
            ("Создание .env", lambda: create_env_file(client, name, env_vars, server_config)),
            ("Сборка", lambda: build_container(client, name)),
            ("Запуск", lambda: start_container(client, name)),
        ]

        for step_name, step_func in steps:
            if not step_func():
                result["error"] = f"Ошибка на шаге: {step_name}"
                client.close()
                return result

        client.close()
        result["success"] = True
        log(name, "=" * 40, "")
        log(name, "ДЕПЛОЙ ЗАВЕРШЕН УСПЕШНО", "ok")
        log(name, "=" * 40, "")

    except paramiko.AuthenticationException:
        result["error"] = "Ошибка аутентификации (неверный логин/пароль)"
        log(name, result["error"], "error")
    except paramiko.SSHException as e:
        result["error"] = f"SSH ошибка: {e}"
        log(name, result["error"], "error")
    except Exception as e:
        result["error"] = f"Неизвестная ошибка: {e}"
        log(name, result["error"], "error")

    return result


def show_logs(server_config: dict, lines: int = 100) -> None:
    """Показать логи контейнера на сервере"""
    name = server_config["name"]
    host = server_config["host"]
    user = server_config["user"]
    password = server_config["password"]

    try:
        log(name, f"Подключение к {host}...", "info")
        client = create_ssh_client(host, user, password)
        log(name, "Подключено", "ok")

        compose_cmd = get_compose_command(client)
        log(name, f"Получение логов (последние {lines} строк)...", "info")

        exit_code, out, err = exec_command(
            client,
            f"cd {REMOTE_PATH} && {compose_cmd} logs --tail {lines}",
            timeout=30
        )

        client.close()

        if out:
            print("\n" + "=" * 60)
            print(f"ЛОГИ {name}")
            print("=" * 60)
            print(out)
        else:
            log(name, "Логи пусты или контейнер не запущен", "info")

    except Exception as e:
        log(name, f"Ошибка: {e}", "error")


def main():
    parser = argparse.ArgumentParser(description="Деплой на сервера")
    parser.add_argument("--server", help="Деплой только на указанный сервер")
    parser.add_argument("--dry-run", action="store_true", help="Показать конфиг без деплоя")
    parser.add_argument("--logs", action="store_true", help="Показать логи контейнера")
    parser.add_argument("--lines", type=int, default=100, help="Количество строк логов (по умолчанию 100)")
    args = parser.parse_args()

    # Загружаем конфиг
    config = load_config()
    env_vars = config.get("env", {})
    servers = config["servers"]

    # Фильтруем сервера если указан --server
    if args.server:
        servers = [s for s in servers if s["name"] == args.server]
        if not servers:
            print(f"Сервер '{args.server}' не найден в конфиге")
            sys.exit(1)

    # Показать логи
    if args.logs:
        for server in servers:
            show_logs(server, args.lines)
        return

    # Dry run
    if args.dry_run:
        print("\n=== КОНФИГУРАЦИЯ ===\n")
        print("Переменные окружения:")
        for k, v in env_vars.items():
            # Скрываем пароли
            if "password" in k.lower() or "token" in k.lower():
                v = "***"
            print(f"  {k}={v}")

        print("\nСервера:")
        for s in servers:
            print(f"  - {s['name']}: {s['host']} (user={s['user']}, "
                  f"browser={s.get('browser_workers', 15)}, "
                  f"validation={s.get('validation_workers', 15)})")
        print()
        return

    # Проверяем наличие container/
    if not CONTAINER_DIR.exists():
        print(f"Директория container/ не найдена: {CONTAINER_DIR}")
        sys.exit(1)

    # Запускаем деплой
    print("\n" + "=" * 60)
    print(f"ДЕПЛОЙ НА {len(servers)} СЕРВЕР(ОВ)")
    print("=" * 60 + "\n")

    # Параллельный деплой
    results = []
    with ThreadPoolExecutor(max_workers=len(servers)) as executor:
        futures = {
            executor.submit(deploy_to_server, server, env_vars): server
            for server in servers
        }
        for future in as_completed(futures):
            results.append(future.result())

    # Итоги
    print("\n" + "=" * 60)
    print("ИТОГИ ДЕПЛОЯ")
    print("=" * 60)

    success_count = sum(1 for r in results if r["success"])
    failed_count = len(results) - success_count

    for r in results:
        status = "\u2713 OK" if r["success"] else f"\u2717 ОШИБКА: {r['error']}"
        print(f"  {r['server']}: {status}")

    print()
    print(f"Успешно: {success_count}/{len(results)}")
    if failed_count > 0:
        print(f"С ошибками: {failed_count}/{len(results)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
