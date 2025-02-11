import os
import sys
import copy
sys.path.append("/Users/liuvivian/table_tuning_for_error_generating_task")
ROOT = "/Users/liuvivian/table_tuning_for_error_generating_task"
import json
import pandas as pd
import numpy as np
from multiprocessing import Pool
from tqdm import tqdm
from typing import Optional, Union
from table_task.base_table_task import BaseTableTask
from table_task.table_task_factory import TableTaskFactory
from table_serializer import TableSerializer
from concurrent.futures import ThreadPoolExecutor
import random
from collections import defaultdict

class DataGenerator:
    def __init__(self,
                 table_task: Union[str, BaseTableTask],
                 source_dir: str = os.path.join(ROOT, "source"),
                 sample_size: int = 5,
                 n_jobs: int = 1,
                 verbose: bool = True):
        if isinstance(table_task, str):
            self.table_task = TableTaskFactory.get_table_task(table_task, sample_size=sample_size)
        else:
            self.table_task = table_task
        self.source_dir = source_dir
        self.n_jobs = n_jobs
        self.verbose = verbose

    def generate_data(self, split_ratio: float = 0.8, single_file_test: Optional[str] = None,
                      use_fewshot: bool = False ):
        """
        读取 `source/` 目录中的 `_annotation.json` 文件，并生成训练数据
        :param single_file_test: 如果提供，则只处理该文件
        """
        if single_file_test:
            self.print_log(f"Running test on single annotation file: {single_file_test}")
            return self._process_single_file(single_file_test, use_fewshot=use_fewshot)

        self.print_log("Scanning annotation files in source directory...")

        annotation_files = []
        # annotation_files= ['/Users/liuvivian/table_tuning_for_error_generating_task/source/Company/Company_annotation.jsonl']
        annotation_files = [
            '/Users/liuvivian/table_tuning_for_error_generating_task/source/hospital/hospital_annotation.jsonl']
        # print("检查目录是否存在:", os.path.exists(self.source_dir))
        # print("source_dir 内容:", os.listdir(self.source_dir))
        # print("source_dir 是否是目录:", os.path.isdir(self.source_dir))
        # for root, _, files in os.walk(self.source_dir):
        #     for file in files:
        #         if file.endswith("_annotation.jsonl"):
        #             full_path = os.path.join(root, file)
        #             annotation_files.append(full_path)

        print(annotation_files)

        all_data = []
        for file in tqdm(annotation_files, desc="Processing annotation files", unit="file"):
            all_data.extend(self.process_annotation_file(file, use_fewshot=use_fewshot))

        train_data, test_data_zeroshot = self.split_data(all_data, split_ratio)


        # 处理train data的Fewshot

        for i,item in enumerate(train_data):
            num_example = random.randint(0, 4)
            example_idxs = random.sample(range(len(train_data)), num_example)  # 确保索引不重复

            for j,idx in enumerate(example_idxs):
                if i == idx:
                    continue
                train_data[idx]['instruction'] += f"\nInput:\n{item['input']}\nOutput:\n{item['output']}\n\n"


        # 处理test data的fewshot和zeroshot
        test_data_fewshot = copy.deepcopy(test_data_zeroshot)  # 深拷贝，确保修改不影响原数据

        for i,item in enumerate(test_data_fewshot):
            num_example = 2  # 固定 few-shot 示例个数
            example_idxs = random.sample(range(len(test_data_fewshot)), num_example)  # 确保索引不重复

            for j,idx in enumerate(example_idxs):
                if i == idx:
                    continue
                else:
                    test_data_fewshot[idx]['instruction'] += f"\nInput:\n{item['input']}\nOutput:\n{item['output']}\n\n"


        # # **再处理 Few-shot 版本的测试集**
        # for file in tqdm(annotation_files, desc="Processing Few-shot Test Data", unit="file"):
        #     test_data_fewshot.extend(self.process_annotation_file(file, use_fewshot=True))

        # **保存所有数据**
        self.save_dataset(train_data, test_data_zeroshot, test_data_fewshot)

    def _process_single_file(self, file_path: str, use_fewshot: bool = False) -> pd.DataFrame:
        """ 仅处理单个 `_annotation.json` 文件 """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"❌ File not found: {file_path}")

        self.print_log(f"📄 Processing file: {file_path}")
        results = self.process_annotation_file(file_path, use_fewshot=use_fewshot)

        if results:
            return pd.DataFrame(results)
        else:
            self.print_log("⚠️ No valid data in file.")
            return pd.DataFrame(columns=["instruction", "input", "output"])

    def process_annotation_file(self, file_path: str, use_fewshot: bool = False):
        """ 解析单个 `_annotation.jsonl` 文件，并生成数据（并行解析 JSON） """
        self.print_log(f"Processing file: {file_path}")

        max_samples_per_type = 10000  # 每种 error_type 的最大数量
        error_type_dict = defaultdict(list)  # 记录每种 error_type 的数据

        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()  # 读取所有行，获取文件总行数

        # **先统计 & 按 error_type 归类**
        for line in lines:
            try:
                entry = json.loads(line)
                error_type = entry["error_type"]
                error_type_dict[error_type].append(line)  # 以 error_type 作为 key 存储
            except json.JSONDecodeError as e:
                print(f"JSON 解析错误 {file_path}: {e}")
                continue  # 跳过有问题的行

        # **📌 处理超过 5000 条的 error_type**
        filtered_lines = []
        for error_type, records in error_type_dict.items():
            if len(records) > max_samples_per_type:
                random.shuffle(records)  # 先打乱
                selected_records = records[:max_samples_per_type]  # 选前 5000 条
                print(f"🔍 {error_type}: {len(records)} records, selected {len(selected_records)}")
            else:
                selected_records = records  # 直接用全部
            filtered_lines.extend(selected_records)  # 加入最终数据

        results = []
        with ThreadPoolExecutor(max_workers=self.n_jobs) as executor:
            for data in tqdm(
                    executor.map(lambda line: self.safe_parse_json(line, file_path), filtered_lines),
                    total=len(filtered_lines), desc=f"Processing {os.path.basename(file_path)}", unit="entry"
            ):
                if data:
                    results.append(data)

        return results

    def safe_parse_json(self, line, file_path, fewshot_examples=None):
        """ 安全解析 JSON，防止崩溃 """
        try:
            entry = json.loads(line)
            return self.generate_data_entry(entry, file_path, fewshot_examples)
        except json.JSONDecodeError as e:
            print(f"JSON 解析错误 {file_path}: {e}")
            return None  # 跳过有问题的行

    def get_fewshot_samples(self, lines, file_path, max_samples=5):
        """ 从 `lines` 里随机选取 `max_samples` 作为 Few-shot 示例，并转换为 (instruction, input, output) 格式 """
        if len(lines) == 0:
            return []

        num_samples = min(len(lines), random.randint(0, max_samples))
        sampled_lines = random.sample(lines, num_samples)

        fewshot_examples = []
        for line in sampled_lines:
            try:
                entry = line
                processed_entry = self.generate_data_entry(entry, file_path)
                fewshot_examples.append(processed_entry)  # ✅ 确保它有 "input" 和 "output"
            except json.JSONDecodeError as e:
                print(f"JSON 解析错误 {file_path}: {e}")
                continue

        return fewshot_examples

    def generate_data_entry(self, entry, file_path, fewshot_examples=None):
        """ 生成 (instruction, input, output) 三元组，并支持 Few-shot 示例 """
        error_type = entry["error_type"]

        # 生成 instruction
        instruction = f"{self.table_task.get_task_descriptions(error_type)} {self.table_task.get_error_type_descriptions(error_type)}"

        # 解析 input 表格
        input_table = self.table_task.construct_input(entry, file_path)

        # 解析 output
        output_json = self.table_task.construct_output(entry)

        return {
            "instruction":instruction,
            "input": input_table,
            "output": output_json
        }

    def split_data(self, data, train_ratio=0.8):
        """ 按比例划分训练集和测试集 """
        random.shuffle(data)
        split_idx = int(len(data) * train_ratio)
        return data[:split_idx], data[split_idx:]

    def save_dataset(self, train_data, test_data_zeroshot, test_data_fewshot):
        """ 保存训练集和测试集（同时保存 Zero-shot 和 Few-shot 测试集） """
        os.makedirs("dataset/train", exist_ok=True)
        os.makedirs("dataset/test", exist_ok=True)
        task_name = self.table_task.__class__.__name__.replace("Task", "").lower()

        train_file = f"dataset/train/train_{task_name}_1.jsonl"
        test_file_zeroshot = f"dataset/test/test_{task_name}_zeroshot_1.jsonl"
        test_file_fewshot = f"dataset/test/test_{task_name}_fewshot_1.jsonl"

        # **保存训练集**
        with open(train_file, "w", encoding="utf-8") as f:
            for entry in train_data:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        # **保存 Zero-shot 测试集**
        with open(test_file_zeroshot, "w", encoding="utf-8") as f:
            for entry in test_data_zeroshot:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        # **保存 Few-shot 测试集**
        with open(test_file_fewshot, "w", encoding="utf-8") as f:
            for entry in test_data_fewshot:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        print(f"任务 {task_name} 训练集和两种测试集已生成！")

    def print_log(self, *args):
        """ 打印日志 """
        if self.verbose:
            print(*args)

