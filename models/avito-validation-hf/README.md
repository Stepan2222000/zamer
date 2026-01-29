---
library_name: peft
base_model: Qwen/Qwen2.5-1.5B-Instruct
license: apache-2.0
tags:
  - lora
  - qwen2.5
  - avito
  - validation
  - classification
language:
  - ru
---

# Avito Validation LoRA Adapter

LoRA адаптер для валидации объявлений Avito, обученный на базе Qwen2.5-1.5B-Instruct.

## Model Details

- **Base Model:** [Qwen/Qwen2.5-1.5B-Instruct](https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct)
- **LoRA Rank (r):** 16
- **LoRA Alpha:** 32
- **LoRA Dropout:** 0.05
- **Target Modules:** q_proj, k_proj, v_proj, o_proj, gate_proj, up_proj, down_proj
- **Task Type:** CAUSAL_LM
- **Training:** Fine-tuned on Fireworks.ai (December 2024)

## Training Stats

- **Epochs:** 2
- **Steps:** 3,333
- **Training Sequences:** 34,672
- **Training Tokens:** ~101M
- **Final Loss:** 0.125

## Usage

```python
from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import PeftModel

# Load base model
base_model = AutoModelForCausalLM.from_pretrained("Qwen/Qwen2.5-1.5B-Instruct")
tokenizer = AutoTokenizer.from_pretrained("Qwen/Qwen2.5-1.5B-Instruct")

# Load LoRA adapter
model = PeftModel.from_pretrained(base_model, "Stepan222/avito-validation-lora")
```

## License

Apache 2.0
