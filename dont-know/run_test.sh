#!/bin/bash

# Загружаем переменные окружения
set -a
source .env
set +a

# Переходим в директорию container
cd container

# Запускаем main.py через виртуальное окружение
../.venv/bin/python main.py
