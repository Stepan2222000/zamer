"""Управление виртуальными дисплеями Xvfb"""

import subprocess
import signal
import os
import platform
import time
from typing import Dict, List, Optional

from config import TOTAL_BROWSER_WORKERS, XVFB_DISPLAY_START, XVFB_RESOLUTION


# Хранилище запущенных Xvfb процессов
_xvfb_processes: Dict[int, subprocess.Popen] = {}

# Определяем, нужен ли Xvfb (только для Linux в продакшене)
USE_XVFB = os.getenv('USE_XVFB', 'auto')

def should_use_xvfb() -> bool:
    """Определяет, нужно ли использовать Xvfb"""
    if USE_XVFB == 'false':
        return False
    if USE_XVFB == 'true':
        return True
    # auto - используем только на Linux
    return platform.system() == 'Linux'


def get_display_for_worker(worker_id: int) -> int:
    """
    Возвращает номер DISPLAY для воркера.

    Маппинг: worker_id → DISPLAY номер
    Формула: XVFB_DISPLAY_START + worker_id
    Например: worker_id=1 → DISPLAY=:100 (если XVFB_DISPLAY_START=99)
    """
    return XVFB_DISPLAY_START + worker_id


def wait_for_display_ready(display_num: int, timeout: int = 10) -> bool:
    """
    Ожидает готовности X display с retry логикой.

    Проверяет доступность DISPLAY через переменные окружения.
    Возвращает True если дисплей готов, False если таймаут.
    """
    start_time = time.time()
    display_env = f":{display_num}"

    while time.time() - start_time < timeout:
        try:
            # Пытаемся подключиться к DISPLAY через простую проверку
            # Устанавливаем DISPLAY в окружении для проверки
            env = os.environ.copy()
            env['DISPLAY'] = display_env

            # Проверяем доступность через xdpyinfo (если установлен)
            # Если xdpyinfo нет - просто делаем задержку
            result = subprocess.run(
                ['xdpyinfo', '-display', display_env],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=2
            )

            if result.returncode == 0:
                print(f"DISPLAY={display_env} готов к работе")
                return True

        except FileNotFoundError:
            # xdpyinfo не установлен - используем простую задержку
            print(f"xdpyinfo не найден, ожидание {timeout}с для DISPLAY={display_env}")
            time.sleep(timeout)
            return True
        except subprocess.TimeoutExpired:
            pass
        except Exception:
            pass

        # Небольшая задержка перед следующей попыткой
        time.sleep(0.5)

    print(f"ВНИМАНИЕ: Таймаут ожидания готовности DISPLAY={display_env}")
    return False


def create_xvfb_display(display_num: int) -> subprocess.Popen:
    """
    Создает один виртуальный дисплей Xvfb.

    Команда: Xvfb :{display_num} -screen 0 {resolution}
    Возвращает процесс Xvfb.
    """
    cmd = [
        'Xvfb',
        f':{display_num}',
        '-screen', '0', XVFB_RESOLUTION,
        '-ac',  # отключить access control
        '+extension', 'GLX',  # включить OpenGL
        '+render',  # включить рендеринг
        '-noreset',  # не сбрасывать после закрытия последнего клиента
    ]

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,  # создать новую session для изоляции
        )

        print(f"Запущен Xvfb DISPLAY=:{display_num} (PID={process.pid})")

        # Проверяем, что процесс не упал сразу
        time.sleep(0.2)
        if process.poll() is not None:
            raise RuntimeError(f"Xvfb :{display_num} завершился сразу после запуска (код={process.returncode})")

        # Ждем готовности DISPLAY
        print(f"Ожидание готовности DISPLAY=:{display_num}...")
        if not wait_for_display_ready(display_num, timeout=10):
            raise RuntimeError(f"DISPLAY=:{display_num} не стал доступен в течение 10 секунд")

        return process

    except FileNotFoundError:
        raise RuntimeError(
            "Xvfb не найден. Установите: apt-get install xvfb"
        )
    except Exception as e:
        raise RuntimeError(f"Ошибка при запуске Xvfb :{display_num}: {e}")


def init_xvfb_displays() -> None:
    """
    Создает TOTAL_BROWSER_WORKERS виртуальных дисплеев при старте.

    Для каждого воркера создается отдельный DISPLAY.
    Если Xvfb не нужен (macOS, Windows) - пропускаем.
    """
    if not should_use_xvfb():
        print("Xvfb отключен (используется headless режим браузера)")
        return

    print(f"Инициализация {TOTAL_BROWSER_WORKERS} Xvfb дисплеев...")

    for worker_id in range(1, TOTAL_BROWSER_WORKERS + 1):
        display_num = get_display_for_worker(worker_id)

        # Создаем Xvfb процесс
        process = create_xvfb_display(display_num)

        # Сохраняем процесс в хранилище
        _xvfb_processes[display_num] = process

    print(f"Все {TOTAL_BROWSER_WORKERS} дисплеев запущены")


def cleanup_displays() -> None:
    """
    Останавливает все Xvfb процессы при завершении.
    """
    print("Остановка всех Xvfb дисплеев...")

    for display_num, process in _xvfb_processes.items():
        try:
            # Отправляем SIGTERM для graceful shutdown
            process.terminate()

            # Ждем до 5 секунд
            try:
                process.wait(timeout=5)
                print(f"Xvfb DISPLAY=:{display_num} остановлен")
            except subprocess.TimeoutExpired:
                # Если не завершился - убиваем
                process.kill()
                process.wait()
                print(f"Xvfb DISPLAY=:{display_num} убит (SIGKILL)")

        except Exception as e:
            print(f"Ошибка при остановке Xvfb :{display_num}: {e}")

    _xvfb_processes.clear()
    print("Все Xvfb дисплеи остановлены")


def get_display_env(worker_id: int) -> Optional[str]:
    """
    Возвращает строку DISPLAY для установки в environment воркера.

    Пример: worker_id=1 → ":100"
    Если Xvfb не используется, возвращает None.
    """
    if not should_use_xvfb():
        return None

    display_num = get_display_for_worker(worker_id)
    return f":{display_num}"
