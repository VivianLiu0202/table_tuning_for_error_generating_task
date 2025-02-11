import json
import torch
import numpy as np
from datasets import Dataset
from transformers import AutoTokenizer, AutoModelForCausalLM, TrainingArguments, Trainer, DataCollatorForSeq2Seq
from peft import LoraConfig, get_peft_model

# 设置随机种子，保证可复现性
def set_seed(seed=42):
    torch.manual_seed(seed)
    np.random.seed(seed)

set_seed(42)

# 读取 JSONL 格式数据
data_path = "./huanhuan.json"  # 修改为你的数据路径
with open(data_path, "r", encoding="utf-8") as f:
    data = [json.loads(line) for line in f]

# 转换为 Hugging Face Dataset
dataset = Dataset.from_list(data)

# 打印示例数据格式
print("✅ 数据示例:", dataset[0])


# 加载 LLaMA3 Tokenizer
model_path = "/root/autodl-tmp/LLM-Research/Meta-Llama-3-8B-Instruct"  # 替换成你的模型路径
tokenizer = AutoTokenizer.from_pretrained(model_path, use_fast=False)
tokenizer.pad_token = tokenizer.eos_token

# 预处理函数
def process_func(example):
    MAX_LENGTH = 512  # 控制输入最大长度
    user_input = f"User:\n{example['instruction']}\nInput:\n{example['input']}\nAssistant:\n"
    output = f"{example['output']}"

    # Tokenization
    input_enc = tokenizer(user_input, add_special_tokens=True, truncation=True, max_length=MAX_LENGTH)
    output_enc = tokenizer(output, add_special_tokens=False, truncation=True, max_length=MAX_LENGTH)

    # 组合输入和输出
    input_ids = input_enc["input_ids"] + output_enc["input_ids"] + [tokenizer.pad_token_id]
    attention_mask = input_enc["attention_mask"] + output_enc["attention_mask"] + [1]
    labels = [-100] * len(input_enc["input_ids"]) + output_enc["input_ids"] + [tokenizer.pad_token_id]

    return {
        "input_ids": input_ids,
        "attention_mask": attention_mask,
        "labels": labels
    }

# 处理数据
dataset = dataset.map(process_func, remove_columns=dataset.column_names)

# 打印示例
print("✅ 预处理示例:\n", tokenizer.decode(dataset[0]["input_ids"]))

# 加载 LLaMA3-8B 模型
model = AutoModelForCausalLM.from_pretrained(
    model_path,
    device_map="auto",
    torch_dtype=torch.bfloat16
)

# LoRA 配置
lora_config = LoraConfig(
    task_type="CAUSAL_LM",
    target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
    inference_mode=False,
    r=8,  # Lora 秩
    lora_alpha=32,
    lora_dropout=0.1
)

# 应用 LoRA
model = get_peft_model(model, lora_config)
model.print_trainable_parameters()


# 训练参数
training_args = TrainingArguments(
    output_dir="./output/llama3",
    per_device_train_batch_size=4,
    gradient_accumulation_steps=4,
    save_steps=100,
    learning_rate=1e-4,
    num_train_epochs=3,
    logging_steps=10,
    save_on_each_node=True,
    gradient_checkpointing=True,
    fp16=True,
    save_total_limit=2,
    report_to="none"  # 不向 WandB 之类的工具报告
)

# Trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=dataset,
    data_collator=DataCollatorForSeq2Seq(tokenizer=tokenizer, padding=True),
)

print("✅ 开始训练...")
trainer.train()


peft_model_id = "./llama3_lora"
trainer.model.save_pretrained(peft_model_id)
tokenizer.save_pretrained(peft_model_id)

print("✅ LoRA 微调完成，模型已保存！")

