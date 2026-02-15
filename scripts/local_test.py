#!/usr/bin/env python3
"""
Локальный тестовый запуск парсера БЕЗ Docker.

Использует ту же логику что и container/main.py.
Единственные отличия — env переменные и рабочая директория.

Использование:
    python local_test.py -b 2 -v 1          # 2 browser + 1 validation
    python local_test.py -b 1 -v 0          # только 1 browser worker
    python local_test.py -b 2 --headless    # headless режим
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Пути
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
CONTAINER_DIR = PROJECT_ROOT / 'container'


def parse_args():
    parser = argparse.ArgumentParser(
        description='Локальный тестовый запуск парсера',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
    python local_test.py -b 2 -v 1          # 2 browser + 1 validation worker
    python local_test.py -b 1 -v 0          # только 1 browser worker
    python local_test.py -b 2 --headless    # headless режим (без окон)
        """
    )
    parser.add_argument('-b', '--browser-workers', type=int, default=1,
                        help='Количество browser workers (default: 1)')
    parser.add_argument('-v', '--validation-workers', type=int, default=1,
                        help='Количество validation workers (default: 1)')
    parser.add_argument('--headless', action='store_true', default=False,
                        help='Headless режим (без видимых окон браузера)')
    parser.add_argument('--proxy', action='store_true', default=False,
                        help='Использовать прокси из БД (по умолчанию: без прокси)')
    parser.add_argument('--db-host', type=str, default='81.30.105.134',
                        help='Хост БД (default: 81.30.105.134)')
    parser.add_argument('--db-port', type=int, default=5419,
                        help='Порт БД (default: 5419)')
    return parser.parse_args()


def setup_environment(args):
    """Настройка env переменных для локального запуска"""
    # БД
    os.environ['DB_HOST'] = args.db_host
    os.environ['DB_PORT'] = str(args.db_port)
    os.environ.setdefault('DB_NAME', 'zamer_sys')
    os.environ.setdefault('DB_USER', 'admin')
    os.environ.setdefault('DB_PASSWORD', 'Password123')

    # Воркеры
    os.environ['TOTAL_BROWSER_WORKERS'] = str(args.browser_workers)
    os.environ['TOTAL_VALIDATION_WORKERS'] = str(args.validation_workers)

    # Режим
    os.environ['REPARSE_MODE'] = 'false'
    os.environ['SKIP_OBJECT_PARSING'] = 'true'

    # Headless (для browser_worker.py)
    os.environ['LOCAL_HEADLESS'] = str(args.headless).lower()

    # Прокси: по умолчанию БЕЗ прокси для локального тестирования
    if not args.proxy:
        os.environ['NO_PROXY'] = 'true'


def main():
    args = parse_args()
    setup_environment(args)

    print("=" * 60)
    print("ЛОКАЛЬНЫЙ ТЕСТОВЫЙ ЗАПУСК")
    print("=" * 60)
    print(f"Browser Workers: {args.browser_workers}")
    print(f"Validation Workers: {args.validation_workers}")
    print(f"Headless: {args.headless}")
    print(f"Прокси: {'ДА' if args.proxy else 'НЕТ (NO_PROXY)'}")
    print(f"БД: {args.db_host}:{args.db_port}")
    print("=" * 60)

    # Меняем рабочую директорию на container/
    os.chdir(CONTAINER_DIR)
    sys.path.insert(0, str(CONTAINER_DIR))

    # Импортируем и запускаем main.py
    from main import main as run_main
    asyncio.run(run_main())


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nОстановлено пользователем")
        sys.exit(0)
