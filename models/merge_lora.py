#!/usr/bin/env python3
"""
Скрипт для слияния LoRA-адаптера с базовой моделью Qwen2.5-VL-7B-Instruct.
"""

import torch
import os
import sys
import gc
from pathlib import Path

# Пути
ADAPTER_PATH = Path("/Users/stepanorlov/Desktop/DONE/zamer/models/oem-fake-classifier/tuned-model-xl49vygz/bb500f/oem-fake-classifier-v1/checkpoint")
OUTPUT_PATH = Path("/Users/stepanorlov/Desktop/DONE/zamer/models/oem-fake-classifier-merged")
BASE_MODEL = "Qwen/Qwen2.5-VL-7B-Instruct"

def main():
    print("=" * 60)
    print("Слияние LoRA-адаптера с базовой моделью")
    print("=" * 60)
    print(f"Базовая модель: {BASE_MODEL}")
    print(f"Адаптер: {ADAPTER_PATH}")
    print(f"Результат: {OUTPUT_PATH}")
    print("=" * 60)

    # Проверяем доступное устройство
    if torch.cuda.is_available():
        device_map = "auto"
        dtype = torch.float16
        print("Используем CUDA GPU")
    elif torch.backends.mps.is_available():
        device_map = "mps"
        dtype = torch.float32  # MPS лучше работает с fp32
        print("Используем Apple MPS")
    else:
        device_map = "cpu"
        dtype = torch.float32
        print("Используем CPU (это займёт больше времени)")

    print(f"Тип данных: {dtype}")
    print()

    # Шаг 1: Загружаем базовую модель
    print("[1/5] Загрузка базовой модели...")
    from transformers import Qwen2_5_VLForConditionalGeneration, AutoProcessor

    model = Qwen2_5_VLForConditionalGeneration.from_pretrained(
        BASE_MODEL,
        torch_dtype=dtype,
        device_map=device_map,
        trust_remote_code=True,
        low_cpu_mem_usage=True,
    )
    print(f"      Модель загружена: {model.__class__.__name__}")

    # Шаг 2: Загружаем processor (tokenizer + image processor)
    print("[2/5] Загрузка processor...")
    processor = AutoProcessor.from_pretrained(BASE_MODEL, trust_remote_code=True)
    print("      Processor загружен")

    # Шаг 3: Применяем LoRA-адаптер
    print("[3/5] Загрузка и применение LoRA-адаптера...")
    from peft import PeftModel

    model = PeftModel.from_pretrained(
        model,
        str(ADAPTER_PATH),
        torch_dtype=dtype,
    )
    print("      Адаптер применён")

    # Шаг 4: Слияние весов
    print("[4/5] Слияние весов адаптера с базовой моделью...")
    model = model.merge_and_unload(progressbar=True, safe_merge=True)
    print("      Слияние завершено")

    # Освобождаем память
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    # Шаг 5: Сохранение результата
    print("[5/5] Сохранение слитой модели...")
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    model.save_pretrained(OUTPUT_PATH, safe_serialization=True)
    processor.save_pretrained(OUTPUT_PATH)

    # Проверяем сохранённые файлы
    saved_files = list(OUTPUT_PATH.glob("*"))
    print(f"      Сохранено {len(saved_files)} файлов:")
    for f in sorted(saved_files):
        size_mb = f.stat().st_size / (1024 * 1024) if f.is_file() else 0
        print(f"        - {f.name} ({size_mb:.1f} MB)" if size_mb > 0 else f"        - {f.name}/")

    print()
    print("=" * 60)
    print("СЛИЯНИЕ УСПЕШНО ЗАВЕРШЕНО!")
    print(f"Результат сохранён в: {OUTPUT_PATH}")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
