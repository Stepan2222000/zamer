---
library_name: transformers
base_model: Qwen/Qwen2.5-1.5B-Instruct
license: apache-2.0
tags:
  - qwen2.5
  - avito
  - validation
  - classification
  - text-generation
  - merged-lora
language:
  - ru
pipeline_tag: text-generation
---

# Avito Validation Model (Merged)

Fine-tuned Qwen2.5-1.5B-Instruct для валидации объявлений Avito.
LoRA адаптер смержен с базовой моделью для удобства развертывания.

## Model Details

- **Base Model:** [Qwen/Qwen2.5-1.5B-Instruct](https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct)
- **Training Method:** LoRA (merged)
- **LoRA Rank:** 16
- **LoRA Alpha:** 32
- **Target Modules:** q_proj, k_proj, v_proj, o_proj, gate_proj, up_proj, down_proj
- **Training Platform:** Fireworks.ai (December 2024)

## Training Stats

- **Epochs:** 2
- **Steps:** 3,333
- **Training Sequences:** 34,672
- **Training Tokens:** ~101M
- **Final Loss:** 0.125

## Usage

### Direct Usage

```python
from transformers import AutoModelForCausalLM, AutoTokenizer

model = AutoModelForCausalLM.from_pretrained("Stepan222/avito-validation-merged")
tokenizer = AutoTokenizer.from_pretrained("Stepan222/avito-validation-merged")

# Example input
messages = [
    {"role": "system", "content": "Ты эксперт по валидации объявлений. Всегда отвечай строго в JSON формате."},
    {"role": "user", "content": '''АРТИКУЛ: "06L121011B"
ОБЪЯВЛЕНИЯ: [{"id": "7655180983", "title": "Насос водяной VAG 06L121011B", "snippet": "...", "price": 9890.0}]'''}
]

text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
inputs = tokenizer(text, return_tensors="pt")
outputs = model.generate(**inputs, max_new_tokens=512)
print(tokenizer.decode(outputs[0]))
```

## Input Format

```json
АРТИКУЛ: "<articulum>"
ОБЪЯВЛЕНИЯ: [
  {"id": "...", "title": "...", "snippet": "...", "price": ..., "seller_reviews": ...},
  ...
]
```

## Output Format

```json
{
  "passed_ids": ["id1", "id2", ...],
  "rejected": [
    {"id": "id3", "reason": "Причина отклонения"}
  ]
}
```

## License

Apache 2.0
