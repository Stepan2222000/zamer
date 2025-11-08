#!/usr/bin/env python3
"""Тестовый запуск Stage 5 (парсинг объявлений)"""

import os
import sys
import subprocess
import time

# Переходим в папку container
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
container_dir = os.path.join(project_root, 'container')
os.chdir(container_dir)

def main():
    print("=" * 60)
    print("ТЕСТОВЫЙ ЗАПУСК STAGE 5")
    print("=" * 60)
    print("\nКонфигурация загружается из .env файла")
    print(f"Запуск из директории: {os.getcwd()}")
    print("Запуск main.py...\n")

    # Запускаем main.py (конфигурация загрузится из .env)
    # Используем python3 напрямую, а не sys.executable (чтобы избежать проблем с venv)
    try:
        subprocess.run(['python3', 'main.py'])
    except KeyboardInterrupt:
        print("\n\nПолучен сигнал остановки (Ctrl+C)")
        print("Система остановлена")
        return 0

if __name__ == '__main__':
    sys.exit(main())
