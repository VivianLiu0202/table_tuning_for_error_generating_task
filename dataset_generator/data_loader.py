import os
import sys
import argparse

import numpy as np

ROOT = "/Users/liuvivian/table_tuning_for_error_generating_task"
sys.path.append("/Users/liuvivian/table_tuning_for_error_generating_task")
from dataset_generator.data_generator import DataGenerator
from table_task.table_task_factory import TableTaskFactory
import random
from typing import Optional  # ✅ 添加这一行

def random_everything():
    random.seed(2)
    np.random.seed(2)


def main(task_name: str, single_file_test: str = None, use_fewshot: bool = False):
    source_dir = os.path.join(ROOT,"source")
    output_file = f"dataset/{'test' if single_file_test else 'train'}/{'test' if single_file_test else 'train'}_{task_name}.jsonl"


    # 获取对应的任务处理类
    table_task = TableTaskFactory.get_table_task(task_name)
    data_generator = DataGenerator(table_task, source_dir=source_dir, verbose=True)


    data_generator.generate_data(use_fewshot=use_fewshot)
    print(f"✅ 数据集已保存至 {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate dataset for error table tasks")
    parser.add_argument("task_name", type=str, choices=["Error_Generation", "Error_Detection", "Error_Correction"],
                        help="Task type to generate dataset")
    # parser.add_argument("--test-file", type=str, default=None,
    #                     help="Optional: Provide a specific file for testing")
    # parser.add_argument("--fewshot-test", action="store_true",
    #                     help="Enable Few-shot examples in test set")

    # args = parser.parse_args()
    #main(args.task_name, args.test_file, args.fewshot_test)
    #main(args.task_name)
    main("Error_Generation")