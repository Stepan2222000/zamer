#!/usr/bin/env python3
"""
Скрипт деплоя на несколько серверов

Использование:
    python scripts/deploy.py              # Сборка + деплой + мониторинг логов
    python scripts/deploy.py --server X   # Сборка + деплой только на сервер X
    python scripts/deploy.py --no-build   # Деплой без сборки (образ уже в Docker Hub)
    python scripts/deploy.py --build-only # Только сборка + push в Docker Hub
    python scripts/deploy.py --no-follow  # Деплой без мониторинга логов
    python scripts/deploy.py --local-build # Старый режим: сборка на сервере
    python scripts/deploy.py --dry-run    # Показать конфиг без деплоя
"""

import argparse
import socket
import subprocess
import sys
import threading
import time
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
AUTH_JSON_PATH = SCRIPT_DIR / "data" / "auth.json"

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


# ─────────────────────────────────────────────────
# ФАЗА 1: Локальная сборка и push в Docker Hub
# ─────────────────────────────────────────────────

def build_and_push_image(image_name: str, builder: str = None) -> bool:
    """Сборка образа через Docker Build Cloud и push в Docker Hub.

    Выполняется локально на Mac, один раз перед деплоем на серверы.
    Образ собирается на удалённом EC2 (Build Cloud) под linux/amd64
    и пушится напрямую в Docker Hub.
    """
    tag = f"{image_name}:latest"

    print("\n" + "=" * 60)
    print("ФАЗА 1: СБОРКА ОБРАЗА")
    print("=" * 60)

    # Проверяем наличие docker buildx
    result = subprocess.run(
        ["docker", "buildx", "version"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        log("build", "docker buildx не найден. Установите Docker Desktop", "error")
        return False

    # Формируем команду сборки
    cache_bust = int(time.time())
    cmd = [
        "docker", "buildx", "build",
        "--platform", "linux/amd64",
        "--tag", tag,
        "--push",
        "--progress=plain",
        "--build-arg", f"AVITO_LIB_CACHE_BUST={cache_bust}",
    ]

    if builder:
        cmd.extend(["--builder", builder])
        log("build", f"Builder: {builder} (Build Cloud)", "info")
    else:
        log("build", "Builder: default (без Build Cloud)", "info")

    cmd.append(str(CONTAINER_DIR))

    log("build", f"Образ: {tag}", "info")
    log("build", "Сборка запущена...", "wait")
    log("build", f"Команда: {' '.join(cmd)}", "info")
    print()

    # Запускаем сборку с real-time выводом
    start_time = time.time()
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    for line in process.stdout:
        line = line.rstrip()
        if line:
            log("build", f"  | {line}")

    process.wait()
    elapsed = int(time.time() - start_time)

    if process.returncode != 0:
        log("build", f"Сборка завершилась с ошибкой (код {process.returncode})", "error")
        return False

    log("build", f"Образ {tag} собран и запушен в Docker Hub ({elapsed}с)", "ok")
    print()
    return True


# ─────────────────────────────────────────────────
# SSH-утилиты
# ─────────────────────────────────────────────────

def create_ssh_client(host: str, user: str, password: str, retries: int = 3,
                      jump_host: dict = None) -> paramiko.SSHClient:
    """Создание SSH клиента с retry и автоматическим fallback на jump host.

    Если прямое подключение не удаётся и задан jump_host, пробуем через него.
    jump_host = {"host": "...", "user": "...", "password": "..."}
    """
    last_error = None

    # Попытка прямого подключения
    for attempt in range(1, retries + 1):
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(host, username=user, password=password, timeout=15, banner_timeout=30)
            return client
        except (paramiko.SSHException, socket.error, EOFError) as e:
            last_error = e
            if attempt == retries:
                break
            delay = min(2 ** attempt, 10)
            log(host, f"SSH попытка {attempt}/{retries} не удалась: {e}. Повтор через {delay}с...", "wait")
            time.sleep(delay)

    # Fallback на jump host
    if jump_host:
        log(host, f"Прямое подключение не удалось, пробуем через jump host ({jump_host['host']})...", "wait")
        try:
            # Подключаемся к jump host
            jump = paramiko.SSHClient()
            jump.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            jump.connect(
                jump_host["host"], username=jump_host["user"],
                password=jump_host["password"], timeout=15, banner_timeout=30
            )

            # Открываем канал к целевому серверу через jump host
            jump_transport = jump.get_transport()
            channel = jump_transport.open_channel(
                "direct-tcpip", dest_addr=(host, 22), src_addr=("127.0.0.1", 0)
            )

            # Подключаемся к целевому серверу через канал
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(host, username=user, password=password, sock=channel,
                           timeout=30, banner_timeout=60)
            # Сохраняем jump client чтобы не потерять ссылку (иначе GC закроет)
            client._jump_client = jump
            log(host, f"Подключено через jump host {jump_host['host']}", "ok")
            return client
        except Exception as e:
            log(host, f"Jump host тоже не помог: {e}", "error")
            raise paramiko.SSHException(
                f"Не удалось подключиться ни напрямую, ни через jump host: {e}"
            )

    raise paramiko.SSHException(f"Не удалось подключиться после {retries} попыток: {last_error}")


def exec_command(client: paramiko.SSHClient, command: str, timeout: int = 300) -> tuple:
    """Выполнение команды на сервере"""
    _, stdout, stderr = client.exec_command(command, timeout=timeout)
    # Читаем вывод ДО recv_exit_status, иначе deadlock при большом выводе
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    exit_code = stdout.channel.recv_exit_status()
    return exit_code, out, err


def exec_command_stream(client: paramiko.SSHClient, command: str, server_name: str, timeout: int = 18000) -> int:
    """Выполнение команды с построчной трансляцией вывода в реальном времени"""
    transport = client.get_transport()
    channel = transport.open_session()
    channel.set_combine_stderr(True)
    channel.exec_command(command)

    buf = ""
    start_time = time.time()

    while True:
        # Проверка таймаута
        if time.time() - start_time > timeout:
            channel.close()
            log(server_name, f"Таймаут ({timeout}с) превышен", "error")
            return -1

        if channel.recv_ready():
            chunk = channel.recv(4096).decode("utf-8", errors="replace")
            buf += chunk
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                line = line.rstrip("\r")
                if line:
                    log(server_name, f"  | {line}")
        elif channel.exit_status_ready():
            # Дочитываем остаток
            while channel.recv_ready():
                chunk = channel.recv(4096).decode("utf-8", errors="replace")
                buf += chunk
            if buf.strip():
                for line in buf.strip().split("\n"):
                    log(server_name, f"  | {line.rstrip()}")
            break
        else:
            time.sleep(0.1)

    return channel.recv_exit_status()


# ─────────────────────────────────────────────────
# ФАЗА 2: Шаги деплоя на сервер
# ─────────────────────────────────────────────────

def check_docker(client: paramiko.SSHClient, server_name: str) -> bool:
    """Проверка и установка Docker"""
    log(server_name, "Проверка Docker...", "info")

    exit_code, _, _ = exec_command(client, "docker --version")
    if exit_code == 0:
        log(server_name, "Docker установлен", "ok")
        return True

    log(server_name, "Docker не найден, устанавливаем...", "wait")
    exit_code, _, err = exec_command(client, "curl -fsSL https://get.docker.com | sh", timeout=600)

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

    exec_command(client, install_cmd, timeout=300)

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

    log(server_name, "Не удалось установить Docker Compose", "error")
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
    _, _, err = exec_command(
        client,
        f"cd {REMOTE_PATH} && {compose_cmd} down --timeout 60",
        timeout=120
    )

    if err and "no configuration file" not in err.lower():
        log(server_name, f"Предупреждение при остановке: {err}", "info")

    log(server_name, "Контейнер остановлен", "ok")
    return True


def upload_compose_file(client: paramiko.SSHClient, server_name: str) -> bool:
    """Копирование только docker-compose.yml (для режима pull из Docker Hub)"""
    log(server_name, "Копирование docker-compose.yml...", "info")

    sftp = client.open_sftp()

    # Создаем директорию если не существует
    exec_command(client, f"mkdir -p {REMOTE_PATH}")

    # Копируем только docker-compose.yml
    local_compose = str(CONTAINER_DIR / "docker-compose.yml")
    sftp.put(local_compose, f"{REMOTE_PATH}/docker-compose.yml")

    sftp.close()
    log(server_name, "docker-compose.yml скопирован", "ok")
    return True


def upload_auth_json(client: paramiko.SSHClient, server_name: str) -> bool:
    """Копирование auth.json для авторизации Codex CLI (GPT-5.2)"""
    log(server_name, "Настройка auth.json для Codex CLI...", "info")

    exec_command(client, f"mkdir -p {REMOTE_PATH}")

    if AUTH_JSON_PATH.exists():
        sftp = client.open_sftp()
        sftp.put(str(AUTH_JSON_PATH), f"{REMOTE_PATH}/auth.json")
        sftp.close()
        log(server_name, "auth.json скопирован", "ok")
    else:
        # Создаём placeholder чтобы Docker не создал директорию вместо файла
        exec_command(client, f'test -f {REMOTE_PATH}/auth.json || echo \'{{}}\' > {REMOTE_PATH}/auth.json')
        log(server_name, "auth.json не найден локально, создан placeholder", "info")

    return True


def upload_all_files(client: paramiko.SSHClient, server_name: str) -> bool:
    """Копирование всех файлов через SFTP (для режима --local-build)"""
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


def pull_image(client: paramiko.SSHClient, server_name: str) -> bool:
    """Скачивание образа из Docker Hub"""
    log(server_name, "Скачивание образа из Docker Hub...", "wait")

    compose_cmd = get_compose_command(client)

    # 2>&1 — прогресс-бары docker compose идут в stderr,
    # без перенаправления возможен deadlock в exec_command (переполнение буфера stderr)
    exit_code, out, err = exec_command(
        client,
        f"cd {REMOTE_PATH} && {compose_cmd} pull 2>&1",
        timeout=600  # 10 мин — первый pull может быть долгим
    )

    if exit_code != 0:
        log(server_name, f"Ошибка скачивания образа: {err or out}", "error")
        return False

    log(server_name, "Образ скачан", "ok")
    return True


def build_container(client: paramiko.SSHClient, server_name: str) -> bool:
    """Сборка Docker образа на сервере (для режима --local-build)"""
    log(server_name, "Сборка образа на сервере (лог транслируется ниже)...", "wait")

    compose_cmd = get_compose_command(client)
    build_log = f"{REMOTE_PATH}/build.log"

    exit_code = exec_command_stream(
        client,
        f"cd {REMOTE_PATH} && {compose_cmd} build --progress=plain 2>&1 | tee {build_log}",
        server_name,
        timeout=18000  # 5 часов
    )

    if exit_code != 0:
        log(server_name, "Сборка завершилась с ошибкой", "error")
        # Показываем последние строки лога с ошибкой
        _, tail, _ = exec_command(client, f"tail -30 {build_log}")
        if tail:
            log(server_name, "--- Последние строки build.log ---", "info")
            for line in tail.split("\n"):
                log(server_name, f"  | {line}")
            log(server_name, f"--- Полный лог: {build_log} на сервере ---", "info")
        return False

    log(server_name, "Образ собран", "ok")
    return True


def start_container(client: paramiko.SSHClient, server_name: str) -> bool:
    """Запуск контейнера"""
    log(server_name, "Запуск контейнера...", "info")

    compose_cmd = get_compose_command(client)

    exit_code, _, err = exec_command(
        client,
        f"cd {REMOTE_PATH} && {compose_cmd} up -d",
        timeout=120
    )

    if exit_code != 0:
        log(server_name, f"Ошибка запуска: {err}", "error")
        return False

    log(server_name, "Контейнер запущен", "ok")
    return True


# ─────────────────────────────────────────────────
# Оркестрация деплоя
# ─────────────────────────────────────────────────

def deploy_to_server(server_config: dict, env_vars: dict, local_build: bool = False,
                     jump_host: dict = None) -> dict:
    """Деплой на один сервер"""
    name = server_config["name"]
    host = server_config["host"]
    user = server_config["user"]
    password = server_config["password"]

    result = {"server": name, "success": False, "error": None}

    try:
        log(name, f"Подключение к {host}...", "info")
        client = create_ssh_client(host, user, password, jump_host=jump_host)
        log(name, "Подключено", "ok")

        # Шаги деплоя зависят от режима
        if local_build:
            # Старый режим: загрузить ВСЕ файлы и собрать на сервере
            steps = [
                ("Docker", lambda: check_docker(client, name)),
                ("Docker Compose", lambda: check_docker_compose(client, name)),
                ("Остановка", lambda: stop_container(client, name)),
                ("Копирование", lambda: upload_all_files(client, name)),
                ("Auth Codex", lambda: upload_auth_json(client, name)),
                ("Создание .env", lambda: create_env_file(client, name, env_vars, server_config)),
                ("Сборка", lambda: build_container(client, name)),
                ("Запуск", lambda: start_container(client, name)),
            ]
        else:
            # Новый режим: загрузить только compose, скачать образ из Docker Hub
            steps = [
                ("Docker", lambda: check_docker(client, name)),
                ("Docker Compose", lambda: check_docker_compose(client, name)),
                ("Остановка", lambda: stop_container(client, name)),
                ("Копирование", lambda: upload_compose_file(client, name)),
                ("Auth Codex", lambda: upload_auth_json(client, name)),
                ("Создание .env", lambda: create_env_file(client, name, env_vars, server_config)),
                ("Скачивание образа", lambda: pull_image(client, name)),
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


# ─────────────────────────────────────────────────
# ФАЗА 3: Мониторинг логов в реальном времени
# ─────────────────────────────────────────────────

def stream_logs_from_server(server_config: dict, stop_event: threading.Event, jump_host: dict = None) -> None:
    """Streaming логов с одного сервера через SSH.

    Подключается к серверу, запускает docker compose logs --follow,
    и построчно выводит логи до получения stop_event.
    """
    name = server_config["name"]
    host = server_config["host"]
    user = server_config["user"]
    password = server_config["password"]

    client = None
    channel = None

    try:
        log(name, "Подключение для мониторинга логов...", "wait")
        client = create_ssh_client(host, user, password, jump_host=jump_host)
        compose_cmd = get_compose_command(client)
        log(name, "Подключено, ожидание логов...", "ok")

        # Открываем SSH channel для streaming
        transport = client.get_transport()
        channel = transport.open_session()
        channel.set_combine_stderr(True)
        channel.exec_command(
            f"cd {REMOTE_PATH} && {compose_cmd} logs --follow --tail 50 2>&1"
        )

        buf = ""
        start_time = time.time()
        last_progress_time = start_time
        got_first_line = False

        while not stop_event.is_set():
            if channel.recv_ready():
                chunk = channel.recv(4096).decode("utf-8", errors="replace")
                buf += chunk

                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.rstrip("\r")
                    if line:
                        if not got_first_line:
                            got_first_line = True
                        log(name, f"  | {line}")

                last_progress_time = time.time()

            elif channel.exit_status_ready():
                # docker logs завершился (контейнер удалён?)
                if buf.strip():
                    for line in buf.strip().split("\n"):
                        log(name, f"  | {line.rstrip()}")
                log(name, "Поток логов завершился", "info")
                break
            else:
                # Нет данных — показываем прогресс ожидания
                now = time.time()
                if not got_first_line and (now - last_progress_time) >= 15:
                    elapsed = int(now - start_time)
                    log(name, f"Ожидание логов... ({elapsed}с)", "wait")
                    last_progress_time = now
                time.sleep(0.2)

    except paramiko.SSHException as e:
        log(name, f"SSH соединение потеряно: {e}", "error")
    except socket.error as e:
        log(name, f"Ошибка сети: {e}", "error")
    except Exception as e:
        if not stop_event.is_set():
            log(name, f"Ошибка мониторинга: {e}", "error")
    finally:
        if channel:
            try:
                channel.close()
            except Exception:
                pass
        if client:
            try:
                client.close()
            except Exception:
                pass


def follow_logs(servers: list, results: list, jump_host: dict = None) -> None:
    """Фаза 3: мониторинг логов со всех успешных серверов.

    Запускает параллельный streaming логов. Завершается по Ctrl+C.
    """
    # Сопоставляем серверы с результатами
    successful_names = {r["server"] for r in results if r["success"]}
    successful_servers = [s for s in servers if s["name"] in successful_names]

    if not successful_servers:
        return

    print("\n" + "=" * 60)
    print("ФАЗА 3: МОНИТОРИНГ ЛОГОВ (Ctrl+C для остановки)")
    print("=" * 60 + "\n")

    stop_event = threading.Event()
    threads = []

    for server in successful_servers:
        t = threading.Thread(
            target=stream_logs_from_server,
            args=(server, stop_event, jump_host),
            daemon=True,
        )
        threads.append(t)
        t.start()

    try:
        while not stop_event.is_set():
            stop_event.wait(timeout=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        print()
        log("deploy", "Остановка мониторинга логов...", "info")
        stop_event.set()

        for t in threads:
            t.join(timeout=5)

        log("deploy", "Мониторинг остановлен", "ok")


def show_logs(server_config: dict, lines: int = 100, jump_host: dict = None) -> None:
    """Показать логи контейнера на сервере"""
    name = server_config["name"]
    host = server_config["host"]
    user = server_config["user"]
    password = server_config["password"]

    try:
        log(name, f"Подключение к {host}...", "info")
        client = create_ssh_client(host, user, password, jump_host=jump_host)
        log(name, "Подключено", "ok")

        compose_cmd = get_compose_command(client)
        log(name, f"Получение логов (последние {lines} строк)...", "info")

        _, out, _ = exec_command(
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
    parser.add_argument("--no-build", action="store_true", help="Пропустить сборку (образ уже в Docker Hub)")
    parser.add_argument("--build-only", action="store_true", help="Только собрать и запушить образ")
    parser.add_argument("--local-build", action="store_true", help="Старый режим: сборка на сервере")
    parser.add_argument("--no-follow", action="store_true", help="Не запускать мониторинг логов после деплоя")
    parser.add_argument("--jump", metavar="HOST:USER:PASS",
                        help="Jump host для SSH (формат: host:user:password). Используется если прямое подключение не удаётся")
    args = parser.parse_args()

    # Парсим jump host
    jump_host = None
    if args.jump:
        parts = args.jump.split(":")
        if len(parts) == 3:
            jump_host = {"host": parts[0], "user": parts[1], "password": parts[2]}
        elif len(parts) == 1:
            # Только IP — берём user=root, ищем пароль среди серверов конфига
            jump_host = {"host": parts[0], "user": "root", "password": "Samara2008"}
        else:
            print(f"Неверный формат --jump: {args.jump}")
            print("Формат: HOST или HOST:USER:PASSWORD")
            sys.exit(1)

    # Загружаем конфиг
    config = load_config()
    env_vars = config.get("env", {})
    servers = config["servers"]
    docker_hub = config.get("docker_hub", {})

    # Фильтруем сервера если указан --server
    if args.server:
        servers = [s for s in servers if s["name"] == args.server]
        if not servers:
            print(f"Сервер '{args.server}' не найден в конфиге")
            sys.exit(1)

    # Показать логи
    if args.logs:
        for server in servers:
            show_logs(server, args.lines, jump_host=jump_host)
        return

    # Dry run
    if args.dry_run:
        print("\n=== КОНФИГУРАЦИЯ ===\n")

        if docker_hub:
            print("Docker Hub:")
            print(f"  Образ: {docker_hub.get('image', 'не указан')}:latest")
            print(f"  Builder: {docker_hub.get('builder', 'default')}")
            print()

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

    # ─── ФАЗА 1: Сборка и push в Docker Hub ───
    if not args.local_build and not args.no_build:
        image_name = docker_hub.get("image")
        if not image_name:
            print("Ошибка: в конфиге нет docker_hub.image")
            print("Добавьте в servers.yaml секцию docker_hub с полем image")
            print("Или используйте --local-build для сборки на сервере")
            sys.exit(1)

        builder = docker_hub.get("builder")

        if not build_and_push_image(image_name, builder):
            print("\nОшибка сборки образа. Деплой прерван.")
            sys.exit(1)

        if args.build_only:
            return

    # ─── ФАЗА 2: Деплой на серверы ───
    print("\n" + "=" * 60)
    mode = "локальная сборка" if args.local_build else "pull из Docker Hub"
    print(f"ФАЗА 2: ДЕПЛОЙ НА {len(servers)} СЕРВЕР(ОВ) ({mode})")
    print("=" * 60 + "\n")

    # Параллельный деплой
    results = []
    with ThreadPoolExecutor(max_workers=len(servers)) as executor:
        futures = {
            executor.submit(deploy_to_server, server, env_vars, args.local_build, jump_host): server
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

    # ─── ФАЗА 3: Мониторинг логов ───
    if success_count > 0 and not args.no_follow:
        follow_logs(servers, results, jump_host=jump_host)

    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
