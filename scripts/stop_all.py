#!/usr/bin/env python3
"""
Остановка всех контейнеров на серверах

Дополняет deploy.py — подключается к серверам из servers.yaml
и гарантированно останавливает ВСЕ Docker-контейнеры.

Использование:
    python scripts/stop_all.py              # Остановить всё на всех серверах
    python scripts/stop_all.py --server X   # Только на сервере X
    python scripts/stop_all.py --dry-run    # Показать что запущено, не останавливая
"""

import argparse
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
CONFIG_PATH = SCRIPT_DIR / "data" / "servers.yaml"
REMOTE_PATH = "/root/container"

# Логирование
print_lock = threading.Lock()


def log(server_name: str, message: str, status: str = ""):
    timestamp = datetime.now().strftime("%H:%M:%S")
    icon = {"ok": "✓", "error": "✗", "info": "•", "wait": "○"}.get(status, "")
    with print_lock:
        print(f"[{timestamp}] [{server_name}] {icon} {message}")


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        print(f"Конфиг не найден: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    if "servers" not in config or not config["servers"]:
        print("В конфиге нет серверов")
        sys.exit(1)
    return config


def create_ssh_client(host: str, user: str, password: str) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=password, timeout=30)
    return client


def exec_command(client: paramiko.SSHClient, command: str, timeout: int = 120) -> tuple:
    _, stdout, stderr = client.exec_command(command, timeout=timeout)
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    exit_code = stdout.channel.recv_exit_status()
    return exit_code, out, err


def stop_server(server_config: dict, dry_run: bool = False) -> dict:
    """Остановка всех контейнеров на одном сервере."""
    name = server_config["name"]
    host = server_config["host"]
    user = server_config["user"]
    password = server_config["password"]

    result = {"server": name, "success": False, "stopped": 0, "removed": 0}

    try:
        log(name, f"Подключение к {host}...", "info")
        client = create_ssh_client(host, user, password)
        log(name, "Подключено", "ok")

        # 1. Показать что сейчас запущено
        _, out, _ = exec_command(client, "docker ps --format 'table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Image}}'")
        if out and out.count("\n") > 0:
            log(name, "Запущенные контейнеры:", "info")
            for line in out.split("\n"):
                log(name, f"  | {line}")
        else:
            log(name, "Нет запущенных контейнеров", "ok")

        # Показать ВСЕ контейнеры (включая остановленные)
        _, out_all, _ = exec_command(client, "docker ps -a --format 'table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Image}}'")
        stopped_exist = out_all and out_all.count("\n") > (out.count("\n") if out else 0)
        if stopped_exist:
            log(name, "Все контейнеры (включая остановленные):", "info")
            for line in out_all.split("\n"):
                log(name, f"  | {line}")

        if dry_run:
            log(name, "Режим --dry-run, пропускаем остановку", "info")
            result["success"] = True
            client.close()
            return result

        # 2. docker compose down в рабочей директории (если есть)
        _, dir_exists, _ = exec_command(client, f"test -d {REMOTE_PATH} && echo yes || echo no")
        if dir_exists == "yes":
            log(name, f"docker compose down в {REMOTE_PATH}...", "wait")
            # Определяем compose команду
            exit_code, _, _ = exec_command(client, "docker compose version")
            compose_cmd = "docker compose" if exit_code == 0 else "docker-compose"
            exec_command(client, f"cd {REMOTE_PATH} && {compose_cmd} down --timeout 30 2>&1", timeout=60)
            log(name, "docker compose down выполнен", "ok")

        # 3. Остановить ВСЕ запущенные контейнеры
        _, running_ids, _ = exec_command(client, "docker ps -q")
        if running_ids:
            count = len(running_ids.split("\n"))
            log(name, f"Остановка {count} контейнер(ов)...", "wait")
            exec_command(client, "docker stop $(docker ps -q) 2>&1", timeout=120)
            result["stopped"] = count
            log(name, f"Остановлено: {count}", "ok")

        # 4. Удалить ВСЕ контейнеры (включая остановленные)
        _, all_ids, _ = exec_command(client, "docker ps -a -q")
        if all_ids:
            count = len(all_ids.split("\n"))
            log(name, f"Удаление {count} контейнер(ов)...", "wait")
            exec_command(client, "docker rm -f $(docker ps -a -q) 2>&1", timeout=60)
            result["removed"] = count
            log(name, f"Удалено: {count}", "ok")

        # 5. Проверка — ничего не осталось
        _, check, _ = exec_command(client, "docker ps -a -q")
        if check:
            log(name, f"ВНИМАНИЕ: остались контейнеры: {check}", "error")
            result["success"] = False
        else:
            log(name, "Все контейнеры остановлены и удалены", "ok")
            result["success"] = True

        client.close()

    except paramiko.AuthenticationException:
        log(name, "Ошибка аутентификации", "error")
    except paramiko.SSHException as e:
        log(name, f"SSH ошибка: {e}", "error")
    except Exception as e:
        log(name, f"Ошибка: {e}", "error")

    return result


def main():
    parser = argparse.ArgumentParser(description="Остановка всех контейнеров на серверах")
    parser.add_argument("--server", help="Только указанный сервер")
    parser.add_argument("--dry-run", action="store_true", help="Показать что запущено, не останавливая")
    args = parser.parse_args()

    config = load_config()
    servers = config["servers"]

    if args.server:
        servers = [s for s in servers if s["name"] == args.server]
        if not servers:
            print(f"Сервер '{args.server}' не найден в конфиге")
            sys.exit(1)

    action = "ПРОСМОТР" if args.dry_run else "ОСТАНОВКА ВСЕХ КОНТЕЙНЕРОВ"
    print("\n" + "=" * 60)
    print(f"{action} НА {len(servers)} СЕРВЕР(АХ)")
    print("=" * 60 + "\n")

    # Параллельная обработка
    results = []
    with ThreadPoolExecutor(max_workers=len(servers)) as executor:
        futures = {
            executor.submit(stop_server, server, args.dry_run): server
            for server in servers
        }
        for future in as_completed(futures):
            results.append(future.result())

    # Итоги
    print("\n" + "=" * 60)
    print("ИТОГИ")
    print("=" * 60)

    for r in results:
        if r["success"]:
            print(f"  {r['server']}: ✓ OK (остановлено: {r['stopped']}, удалено: {r['removed']})")
        else:
            print(f"  {r['server']}: ✗ ОШИБКА")

    success = sum(1 for r in results if r["success"])
    print(f"\nУспешно: {success}/{len(results)}")

    if success < len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
