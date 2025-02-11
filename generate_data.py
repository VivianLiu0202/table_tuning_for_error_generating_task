import os
import sys
import argparse
import json

sys.path.append("/Users/liuvivian/table_tuning_for_error_generating_task")  # 修改为你的 Pycharm 项目路径
from dataset_generator.data_generator import DataGenerator
from dataset_generator.table_task.table_task_factory import TableTaskFactory


def main():
    parser = argparse.ArgumentParser(description="Generate dataset for table-based error tasks")

    # 📌 选择任务类型
    parser.add_argument("--task", required=True, choices=["error_generation", "error_detection", "error_correction"],
                        help="Specify the task type to generate data.")

    # 📌 选择模式（训练 or 测试）
    parser.add_argument("--mode", required=True, choices=["train", "test"],
                        help="Mode: train or test")

    # 📌 数据目录
    parser.add_argument("--source_dir", default="source", help="Directory containing annotation files.")
    parser.add_argument("--save_dir", default="dataset", help="Directory to save generated data.")

    # 📌 Few-shot 参数
    parser.add_argument("--num_test_fewshot_samples", default=3, type=int,
                        help="Number of few-shot samples for test data.")
    parser.add_argument("--prob_train_fewshot", default=0.5, type=float,
                        help="Probability of using few-shot examples in training.")

    # 📌 其他参数
    parser.add_argument("--seed", default=1, type=int, help="Random seed for reproducibility.")
    parser.add_argument("--augment", action="store_true", help="Enable data augmentation.")
    parser.add_argument("--n_jobs", default=8, type=int, help="Number of parallel jobs.")

    args = parser.parse_args()

    # 📌 任务数据目录
    task_data_dir = os.path.join(args.source_dir, args.task)
    train_data_dir = os.path.join(task_data_dir, "train")
    test_data_dir = os.path.join(task_data_dir, "test")

    print(f"🚀 Generating {args.mode} data for {args.task}...")

    if args.mode == "train":
        data_generator = DataGenerator(
            args.task,
            mode="train",
            use_random_template=True,
            n_jobs=args.n_jobs,
            random_state=args.seed,
            augment=args.augment,
            verbose=True,
        )
        data = data_generator.generate_data(train_data_dir, train_data_dir)
        save_name = f"{args.mode}_{args.task}"
        if args.augment:
            save_name += "_augment"

    else:  # 处理测试数据
        test_data_generator = DataGenerator(
            args.task,
            mode="test",
            use_random_template=False,
            n_jobs=args.n_jobs,
            random_state=args.seed,
            num_test_fewshot_samples=args.num_test_fewshot_samples,
            verbose=True,
        )
        data = test_data_generator.generate_data(test_data_dir, train_data_dir)
        save_name = f"{args.mode}_{args.task}"
        if args.num_test_fewshot_samples == 0:
            save_name += "_zeroshot"
        else:
            save_name += "_fewshot"

    # 📌 确保保存目录存在
    save_path = os.path.join(args.save_dir, args.mode)
    os.makedirs(save_path, exist_ok=True)

    # 📌 保存数据到 JSONL 文件
    output_file = os.path.join(save_path, f"{save_name}.jsonl")
    data.to_json(output_file, orient="records", lines=True, force_ascii=False)

    print(f"✅ 数据集已保存至 {output_file}")


if __name__ == "__main__":
    main()